"""Running several Payload Hub instances against one database and one Kafka cluster (for failover and scale).

What each instance shares through PostgreSQL:
- finished transactions: the instance whose result sink finishes a transaction tells the others (NOTIFY), so an
  HTTP caller waiting on another instance gets its answer;
- ownership: work that must happen exactly once - pollers (database tables, FTP/SFTP folders, server folders),
  workflow database triggers and the alert watcher - is owned by one instance at a time through a PostgreSQL
  advisory lock held on a dedicated connection. If that instance dies, its connection drops, the locks are freed
  and another instance takes the work over at its next supervision round.

Queue consumers (Kafka, RabbitMQ, ActiveMQ, IBM MQ) and HTTP ports run on every instance - they share the work.
With a single instance everything behaves exactly as before.
"""
import asyncio
import logging
import os
import socket
import uuid
import zlib

import psycopg

from . import db

log = logging.getLogger("cluster")

INSTANCE = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:4]}"
CHANNEL = "payloadhub_txn_done"
SUPERVISE_SECONDS = float(os.getenv("PAYLOAD_HUB_SUPERVISE_SECONDS", "30"))
_LOCK_NAMESPACE = 7272  # first half of the two-key advisory lock

_lock_conn: psycopg.AsyncConnection | None = None
_lock_guard = asyncio.Lock()
_owned: set[str] = set()
_tasks: list[asyncio.Task] = []
on_txn_done = None  # set by the pipeline: async callback(txn_id)


def _key(name: str) -> int:
    return zlib.crc32(name.encode()) - 2**31  # fits a signed int4


async def _connection() -> psycopg.AsyncConnection:
    global _lock_conn
    if _lock_conn is None or _lock_conn.closed:
        _lock_conn = await psycopg.AsyncConnection.connect(db.DATABASE_URL, autocommit=True,
                                                           application_name=f"payloadhub-locks-{INSTANCE}")
        _owned.clear()  # a new connection holds no locks
    return _lock_conn


async def own(name: str) -> bool:
    """Try to become the owner of `name` (or confirm we already are)."""
    async with _lock_guard:
        if name in _owned:
            return True
        try:
            conn = await _connection()
            cur = await conn.execute("SELECT pg_try_advisory_lock(%s, %s)", (_LOCK_NAMESPACE, _key(name)))
            got = (await cur.fetchone())[0]
        except psycopg.Error as e:
            log.warning("ownership check for %s failed: %s", name, e)
            return False
        if got:
            _owned.add(name)
        return got


def holds(name: str) -> bool:
    return name in _owned


async def release(name: str):
    async with _lock_guard:
        if name not in _owned:
            return
        _owned.discard(name)
        try:
            await (await _connection()).execute("SELECT pg_advisory_unlock(%s, %s)", (_LOCK_NAMESPACE, _key(name)))
        except psycopg.Error:
            pass


async def _keep_locks():
    """If the lock connection drops, every lock is gone - forget them so supervisors stop that work."""
    global _lock_conn
    while True:
        await asyncio.sleep(5)
        async with _lock_guard:
            if _lock_conn is None:
                continue
            try:
                await _lock_conn.execute("SELECT 1")
            except psycopg.Error:
                log.warning("lost the lock connection - releasing ownership of %s", sorted(_owned))
                _owned.clear()
                try:
                    await _lock_conn.close()
                except Exception:  # noqa: BLE001
                    pass
                _lock_conn = None


async def notify_done(txn_id: str):
    try:
        async with db.pool.connection() as conn:
            await conn.execute("SELECT pg_notify(%s, %s)", (CHANNEL, txn_id))
    except Exception:  # noqa: BLE001 - a waiting caller falls back to polling
        log.exception("notify for %s failed", txn_id)


async def _listen():
    """Hear about transactions other instances finished."""
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(db.DATABASE_URL, autocommit=True,
                                                             application_name=f"payloadhub-listen-{INSTANCE}") as conn:
                await conn.execute(f"LISTEN {CHANNEL}")
                async for note in conn.notifies():
                    if on_txn_done:
                        await on_txn_done(note.payload)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("transaction notifications interrupted (%s) - reconnecting", e)
            await asyncio.sleep(2)


def status() -> dict:
    return {"instance": INSTANCE, "owns": sorted(_owned)}


async def start():
    _tasks.extend([asyncio.create_task(_listen(), name="cluster-listen"),
                   asyncio.create_task(_keep_locks(), name="cluster-locks")])


async def stop():
    global _lock_conn
    for t in _tasks:
        t.cancel()
    await asyncio.gather(*_tasks, return_exceptions=True)
    _tasks.clear()
    if _lock_conn is not None:
        await _lock_conn.close()  # frees every lock at once, so another instance can take over
        _lock_conn = None
    _owned.clear()
