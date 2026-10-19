"""Partner adapters at runtime: intake (HTTP, MQ, Kafka -> pipeline) and outbound delivery.

    inbound  ──► ingest() ──► Kafka <queue>.in ──► transformer ──► <queue>.out ──► result sink
                                                                                     │
                                                       deliver() ◄── on_completed ◄──┘
                                                          │
                                       RabbitMQ / ActiveMQ / IBM MQ / HTTP / Kafka
"""
import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone

from . import adapters, db, pipeline

log = logging.getLogger("connectors")

DELIVERY_ATTEMPTS = 3
SUPERVISE_SECONDS = 15

_adapters: dict[str, tuple[str, adapters.Adapter]] = {}  # connection id -> (updated_at, adapter)
_listeners: dict[str, dict] = {}  # partner code -> {"listener", "key", "state", "detail", "since"}
_supervisor: asyncio.Task | None = None
_sync_lock = asyncio.Lock()


class IngestError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def adapter_for(conn_id: str) -> adapters.Adapter:
    """Adapter for a stored connection, rebuilt whenever the connection is edited."""
    conn = await db.get_connection(conn_id, with_secret=True)
    if not conn:
        raise adapters.AdapterError(f"Connection '{conn_id}' no longer exists.")
    stamp = str(conn["updated_at"])
    cached = _adapters.get(conn_id)
    if cached and cached[0] == stamp:
        return cached[1]
    if cached:
        await cached[1].close()
    adapter = adapters.make(conn)
    _adapters[conn_id] = (stamp, adapter)
    return adapter


# ---------- intake ----------

async def ingest(partner: dict, payload: str, source: str, wait: float | None = 15):
    """Create a transaction for a partner's payload and publish it to the partner's Kafka queue.
    With wait, returns the finished transaction (or None if still running after `wait` seconds)."""
    if not partner["active"]:
        raise IngestError(409, f"Partner '{partner['name']}' is inactive.")
    if not partner["expression"]:
        raise IngestError(409, f"Partner '{partner['name']}' has no mapping yet.")
    msg = {"mapping_name": partner["code"], "input_format": partner["input_format"],
           "output_format": partner["output_format"], "expression": partner["expression"],
           "topic_in": partner["topic_in"], "topic_out": partner["topic_out"], "topic_dlq": partner["topic_dlq"],
           "partner_code": partner["code"]}
    txn_id = str(uuid.uuid4())
    await db.create_transaction(txn_id, msg["mapping_name"], msg["input_format"], msg["output_format"],
                                msg["expression"], payload, msg["partner_code"], msg["topic_in"], source)
    result = await pipeline.submit(txn_id, {"id": txn_id, "payload": payload, **msg}, wait_seconds=wait or 0.01)
    return txn_id, result


# ---------- outbound delivery ----------

async def deliver(msg: dict):
    """Called when a transaction completes: send the output to the partner's outbound adapter."""
    code = msg.get("partner_code")
    if not code:
        return
    partner = await db.get_partner(code)
    if not partner or not partner["outbound_connection"]:
        return
    txn_id = msg["id"]
    content_type = "application/xml" if msg["output_format"] == "xml" else "application/json"
    headers = {"X-Transaction-Id": txn_id, "X-Partner": code}
    target = partner["outbound_target"] or ""
    last_error = None
    for attempt in range(1, DELIVERY_ATTEMPTS + 1):
        try:
            adapter = await adapter_for(partner["outbound_connection"])
            detail = await adapter.send(target, msg["output"].encode(), headers, content_type)
        except Exception as e:  # noqa: BLE001
            last_error = str(e) or type(e).__name__
            log.warning("delivery %s attempt %d failed: %s", txn_id, attempt, last_error)
            if attempt < DELIVERY_ATTEMPTS:
                await db.record_event(txn_id, "DELIVERY_RETRY", f"attempt {attempt} failed: {last_error}")
                await asyncio.sleep(2 ** attempt)
            continue
        await db.set_delivery(txn_id, "DELIVERED")
        await db.record_event(txn_id, "DELIVERED", f"{detail} via connection '{partner['outbound_connection']}'")
        return
    await db.set_delivery(txn_id, "DELIVERY_FAILED")
    await db.record_event(txn_id, "DELIVERY_FAILED", f"gave up after {DELIVERY_ATTEMPTS} attempts: {last_error}")
    try:
        await pipeline.producer.send_and_wait(msg["topic_dlq"], {**msg, "delivery_error": last_error},
                                              key=txn_id.encode())
    except Exception:  # noqa: BLE001
        log.exception("could not park %s on %s", txn_id, msg["topic_dlq"])


# ---------- inbound listeners ----------

async def sync():
    """Start, restart or stop inbound listeners so they match the partners' settings."""
    async with _sync_lock:
        wanted = {p["code"]: p for p in await db.partners_with_inbound_adapters()}
        for code in list(_listeners):
            entry = _listeners[code]
            p = wanted.get(code)
            key = (p["inbound_connection"], p["inbound_source"]) if p else None
            if key != entry["key"] or not _alive(entry):
                await _stop(code)
        for code, p in wanted.items():
            if code not in _listeners:
                await _start(p)


def _alive(entry) -> bool:
    listener = entry.get("listener")
    if listener is None:
        return False
    alive = getattr(listener, "alive", None)
    return alive() if alive else True


async def _start(p):
    code, conn_id, source = p["code"], p["inbound_connection"], p["inbound_source"]
    entry = {"key": (conn_id, source), "listener": None, "state": "starting", "detail": "", "since": _now()}
    _listeners[code] = entry

    async def handler(body: bytes, meta: dict):
        partner = await db.get_partner(code)
        if partner is None:
            raise RuntimeError(f"partner {code} was removed")
        await ingest(partner, body.decode("utf-8", errors="replace"), meta["source"], wait=None)

    try:
        adapter = await adapter_for(conn_id)
        listener = await adapter.listen(source, handler)
    except Exception as e:  # noqa: BLE001
        entry.update(state="error", detail=str(e) or type(e).__name__)
        log.warning("inbound listener for %s failed to start: %s", code, entry["detail"])
        return
    entry.update(listener=listener, state="running", detail=f"listening on {listener.describe}")
    log.info("inbound listener for %s: %s", code, listener.describe)


async def _stop(code):
    entry = _listeners.pop(code, None)
    if entry and entry.get("listener"):
        try:
            await entry["listener"].stop()
        except Exception:  # noqa: BLE001
            log.exception("stopping listener for %s", code)


async def _supervise():
    while True:
        await asyncio.sleep(SUPERVISE_SECONDS)
        try:
            await sync()
        except Exception:  # noqa: BLE001
            log.exception("listener supervisor")


def listener_status() -> dict:
    return {code: {k: e[k] for k in ("state", "detail", "since")} | {"connection": e["key"][0], "source": e["key"][1]}
            for code, e in _listeners.items()}


# ---------- lifecycle ----------

async def start():
    global _supervisor
    pipeline.on_completed.append(deliver)
    await sync()
    _supervisor = asyncio.create_task(_supervise(), name="listener-supervisor")


async def stop():
    if _supervisor:
        _supervisor.cancel()
    for code in list(_listeners):
        await _stop(code)
    for _, adapter in _adapters.values():
        try:
            await adapter.close()
        except Exception:  # noqa: BLE001
            pass


async def test_connection(conn_id: str) -> tuple[bool, str]:
    started = time.perf_counter()
    try:
        adapter = await adapter_for(conn_id)
        detail = await asyncio.wait_for(adapter.test(), 30)
        ok = True
    except asyncio.TimeoutError:
        ok, detail = False, "Timed out after 30 s"
    except Exception as e:  # noqa: BLE001
        ok, detail = False, str(e) or type(e).__name__
    detail = f"{detail} ({(time.perf_counter() - started) * 1000:.0f} ms)"
    await db.record_connection_test(conn_id, ok, detail)
    return ok, detail
