"""Partner adapters at runtime: intake (HTTP, MQ, Kafka -> pipeline) and outbound delivery.

    inbound  ──► ingest() ──► Kafka <queue>.in ──► transformer ──► <queue>.out ──► result sink
                                                                                     │
                                                       deliver() ◄── on_completed ◄──┘
                                                          │
                                       RabbitMQ / ActiveMQ / IBM MQ / HTTP / Kafka
"""
import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone

from . import adapters, db, pipeline
from .transform import TransformError, convert_response, to_csv

log = logging.getLogger("connectors")

SUPERVISE_SECONDS = 15
SYNC_SOURCES = ("HTTP", "UI")  # callers waiting for a reply; others (MQ, Kafka) get the forward done in the background

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
    stamp = str(conn["updated_at"])  # also bumped when its named secret changes
    cached = _adapters.get(conn_id)
    if cached and cached[0] == stamp:
        return cached[1]
    if cached:
        await cached[1].close()
    from . import vault

    if conn.get("secret_alias"):
        secret = await vault.secret_value(conn["secret_alias"])
        if secret is None:
            raise adapters.AdapterError(f"The stored secret '{conn['secret_alias']}' can't be read - re-enter it on "
                                        "the Secrets page.")
    else:
        secret = vault.unseal(conn.get("secret"))
    adapter = adapters.make({**conn, "secret": secret})
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
           "partner_code": partner["code"], "source": source}
    txn_id = str(uuid.uuid4())
    await db.create_transaction(txn_id, msg["mapping_name"], msg["input_format"], msg["output_format"],
                                msg["expression"], payload, msg["partner_code"], msg["topic_in"], source)
    result = await pipeline.submit(txn_id, {"id": txn_id, "payload": payload, **msg}, wait_seconds=wait or 0.01)
    return txn_id, result


# ---------- forward to another API and reply ----------

def _for_system_b(partner: dict, txn: dict) -> tuple[str, str]:
    """The payload the target system receives: the transformed output as is, or converted to CSV."""
    if partner.get("send_format") == "csv":
        return to_csv(txn["output_payload"], txn["output_format"], partner.get("csv_delimiter") or ","), "text/csv"
    return txn["output_payload"], "application/xml" if txn["output_format"] == "xml" else "application/json"


async def forward(partner: dict, txn: dict) -> dict:
    """Send a completed transaction's output to the target system (the partner's reply connection) and turn the target system's
    answer into what the caller gets back. HTTP APIs answer with their own response; files, databases and
    queues answer with a delivery receipt. Returns {"ok", "status", "body", "format", "error"}."""
    txn_id = str(txn["id"])
    started = time.perf_counter()
    status = ctype = body = where = None
    error = None
    try:
        payload, content_type = _for_system_b(partner, txn)
        conn = await db.get_connection(partner["reply_connection"]) or {}
        adapter = await adapter_for(partner["reply_connection"])
        headers = {"X-Transaction-Id": txn_id, "X-Partner": partner["code"]}
        policy = adapters.retry_policy(adapter.config)
        is_http = isinstance(adapter, adapters.HTTP)
        retry_statuses = adapter._retry_statuses() if is_http else set()
        for attempt in range(1, policy["attempts"] + 1):
            try:
                if is_http:
                    status, ctype, body, url = await adapter.request(
                        partner["reply_path"] or "", payload.encode(), headers, content_type, partner.get("reply_method"))
                    where = f"{(partner.get('reply_method') or adapter.config.get('method') or 'POST').upper()} {url}"
                else:
                    detail = await adapter.send(partner["reply_path"] or "", payload.encode(), headers, content_type)
                    status, ctype, where = 200, "application/json", f"{conn.get('name')} ({conn.get('type')})"
                    body = json.dumps({"status": "DELIVERED", "system": conn.get("name"), "type": conn.get("type"),
                                       "target": partner["reply_path"] or "", "detail": detail,
                                       "format": "csv" if content_type == "text/csv" else txn["output_format"],
                                       "transactionId": txn_id})
                error = None
            except adapters.AdapterError as e:
                error, status = str(e), None
                if not e.retryable:
                    break
            if status is not None and status not in retry_statuses:
                break
            if attempt < policy["attempts"]:
                wait = min(policy["backoff"] * 2 ** (attempt - 1), policy["backoff_max"], 5)  # caller is waiting
                await db.record_event(txn_id, "FORWARD_RETRY", f"attempt {attempt}: "
                                      + (error or f"HTTP {status}") + f" - retrying in {wait} s")
                await asyncio.sleep(wait)
    except Exception as e:  # noqa: BLE001
        error = str(e) or type(e).__name__
    if status is None:
        await db.record_event(txn_id, "FORWARD_FAILED", error)
        await db.set_response(txn_id, None, None)
        return {"ok": False, "status": 502, "error": f"Target system unreachable: {error}"}
    ms = (time.perf_counter() - started) * 1000
    await db.record_event(txn_id, "FORWARDED", f"{where} → {'HTTP ' + str(status) if is_http else 'delivered'} "
                                               f"in {ms:.0f} ms")
    try:
        converted, fmt = convert_response(body, ctype, partner["response_expression"], partner["response_format"],
                                          txn["input_format"])
    except TransformError as e:
        await db.record_event(txn_id, "RESPONSE_FAILED", f"could not convert the target system's response: {e}")
        await db.set_response(txn_id, body, status)
        return {"ok": False, "status": 502, "error": f"Could not convert the target system's response: {e}"}
    await db.set_response(txn_id, converted, status)
    await db.record_event(txn_id, "RESPONDED", f"{len(converted)} bytes of {fmt.upper()} for the caller "
                                               f"(target system status {status})")
    return {"ok": status < 400, "status": status, "body": converted, "format": fmt, "content_type": ctype}


async def reply_for(partner: dict, txn_id: str, result: dict | None) -> dict | None:
    """What a waiting caller gets back: the transformed payload, or (forward mode) the upstream API's response."""
    if not result or result["status"] != "COMPLETED" or partner.get("reply_mode") != "forward":
        return None
    return await forward(partner, result)


# ---------- outbound delivery ----------

async def deliver(msg: dict):
    """Called when a transaction completes: forward it in the background for MQ/Kafka sources (in forward
    mode), then send the output to the partner's outbound adapter."""
    code = msg.get("partner_code")
    if not code:
        return
    partner = await db.get_partner(code)
    if not partner:
        return
    if partner.get("reply_mode") == "forward" and msg.get("source") not in SYNC_SOURCES:
        txn = await db.get_transaction(msg["id"])
        if txn:
            await forward(partner, txn)
    if not partner["outbound_connection"]:
        return
    txn_id = msg["id"]
    content_type = "application/xml" if msg["output_format"] == "xml" else "application/json"
    headers = {"X-Transaction-Id": txn_id, "X-Partner": code}
    target = partner["outbound_target"] or ""
    last_error, attempts = None, 1
    try:
        adapter = await adapter_for(partner["outbound_connection"])
        policy = adapters.retry_policy(adapter.config)
    except Exception as e:  # noqa: BLE001
        adapter, policy, last_error = None, {"attempts": 0}, str(e) or type(e).__name__
    for attempt in range(1, policy["attempts"] + 1):
        attempts = attempt
        try:
            detail = await adapter.send(target, msg["output"].encode(), headers, content_type)
        except Exception as e:  # noqa: BLE001
            last_error = str(e) or type(e).__name__
            log.warning("delivery %s attempt %d failed: %s", txn_id, attempt, last_error)
            if not getattr(e, "retryable", True):
                await db.record_event(txn_id, "DELIVERY_ERROR", f"attempt {attempt}: permanent error, not retrying")
                break
            if attempt < policy["attempts"]:
                wait = min(policy["backoff"] * 2 ** (attempt - 1), policy["backoff_max"])
                await db.record_event(txn_id, "DELIVERY_RETRY",
                                      f"attempt {attempt} failed: {last_error} - retrying in {wait} s")
                await asyncio.sleep(wait)
            continue
        await db.set_delivery(txn_id, "DELIVERED")
        await db.record_event(txn_id, "DELIVERED", f"{detail} via connection '{partner['outbound_connection']}'"
                                                  + (f" (attempt {attempt})" if attempt > 1 else ""))
        return
    await db.set_delivery(txn_id, "DELIVERY_FAILED")
    await db.record_event(txn_id, "DELIVERY_FAILED", f"gave up after {attempts} attempt(s): {last_error}")
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
        text = body.decode("utf-8", errors="replace")
        if meta.get("format") == "json" and partner["input_format"] == "xml":  # e.g. database rows for an XML partner
            import json

            import xmltodict

            text = xmltodict.unparse({"row": json.loads(text)}, pretty=True)
        await ingest(partner, text, meta["source"], wait=None)

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

async def store(msg: dict):
    """Called when a transaction completes: write its data into the partner's table if storing is on."""
    from . import datastore

    code = msg.get("partner_code")
    if not code:
        return
    partner = await db.get_partner(code)
    if partner and partner.get("store_enabled") and partner.get("store_table"):
        txn = await db.get_transaction(msg["id"])
        if txn:
            await datastore.store_for(partner, txn)


async def start():
    global _supervisor
    pipeline.on_completed.append(store)
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
