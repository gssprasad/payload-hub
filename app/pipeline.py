"""Kafka pipeline: payload.in -> transformer -> payload.out -> result sink (failures -> payload.dlq).

This plays the role Apache Camel routes would play in the Java version:
    from("kafka:payload.in").process(transform).to("kafka:payload.out")
"""
import asyncio
import json
import logging
import os
import time

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.helpers import create_ssl_context
from prometheus_client import Counter, Gauge, Histogram

from . import db
from .transform import TransformError, transform

log = logging.getLogger("pipeline")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN, TOPIC_OUT, TOPIC_DLQ = "payload.in", "payload.out", "payload.dlq"
TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "2"))


def _connection() -> dict:
    """Kafka connection settings. Local Kafka needs none; hosted Kafka (e.g. Aiven) uses SASL over TLS:
    KAFKA_SECURITY_PROTOCOL=SASL_SSL, KAFKA_SASL_MECHANISM=SCRAM-SHA-256, KAFKA_USERNAME, KAFKA_PASSWORD,
    and KAFKA_CA_CERT holding the provider's CA certificate (PEM text)."""
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    conn = {"bootstrap_servers": BOOTSTRAP, "security_protocol": protocol}
    if protocol in ("SSL", "SASL_SSL"):
        conn["ssl_context"] = create_ssl_context(cadata=os.getenv("KAFKA_CA_CERT") or None)
    if protocol.startswith("SASL"):
        conn.update(sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
                    sasl_plain_username=os.getenv("KAFKA_USERNAME"),
                    sasl_plain_password=os.getenv("KAFKA_PASSWORD"))
    return conn


async def _ensure_topics():
    """Create the pipeline topics if they don't exist (hosted Kafka often disables auto-creation)."""
    admin = AIOKafkaAdminClient(**_connection())
    await admin.start()
    try:
        missing = {TOPIC_IN, TOPIC_OUT, TOPIC_DLQ} - set(await admin.list_topics())
        if missing:
            # replication_factor -1 = use the broker's default
            await admin.create_topics([NewTopic(t, TOPIC_PARTITIONS, -1) for t in sorted(missing)])
            log.info("created topics %s", sorted(missing))
    finally:
        await admin.close()

TXN_TOTAL = Counter("payload_transactions_total", "Transactions by final status", ["status", "mapping"])
TRANSFORM_SECONDS = Histogram("payload_transform_seconds", "Time spent in the transformer step")
END_TO_END_SECONDS = Histogram("payload_end_to_end_seconds", "API receive -> result stored")
IN_FLIGHT = Gauge("payload_in_flight", "Transactions published but not yet completed")

producer: AIOKafkaProducer | None = None
_tasks: list[asyncio.Task] = []
_waiters: dict[str, asyncio.Future] = {}
_started_at: dict[str, float] = {}


def _encode(obj) -> bytes:
    return json.dumps(obj).encode()


async def start():
    global producer
    await _ensure_topics()
    producer = AIOKafkaProducer(**_connection(), value_serializer=_encode, acks="all")
    await producer.start()
    for name, coro in (("transformer", _transformer), ("result-sink", _result_sink)):
        consumer = AIOKafkaConsumer(
            TOPIC_IN if name == "transformer" else TOPIC_OUT,
            **_connection(),
            group_id=f"payload-hub-{name}",
            auto_offset_reset="earliest",
            value_deserializer=json.loads,
        )
        await consumer.start()
        task = asyncio.create_task(coro(consumer), name=name)
        task.add_done_callback(_log_crash)
        _tasks.append(task)


async def stop():
    for t in _tasks:
        t.cancel()
    await asyncio.gather(*_tasks, return_exceptions=True)
    if producer:
        await producer.stop()


async def submit(txn_id: str, message: dict, wait_seconds: float = 15):
    """Publish to payload.in and wait (briefly) for the pipeline to finish this transaction."""
    fut = asyncio.get_running_loop().create_future()
    _waiters[txn_id] = fut
    _started_at[txn_id] = time.perf_counter()
    IN_FLIGHT.inc()
    try:
        meta = await producer.send_and_wait(TOPIC_IN, message, key=txn_id.encode())
        await db.record_event(
            txn_id, "PUBLISHED", f"{TOPIC_IN} partition {meta.partition} offset {meta.offset}", status="QUEUED"
        )
        return await asyncio.wait_for(asyncio.shield(fut), wait_seconds)
    except asyncio.TimeoutError:
        return None  # still processing; the UI polls the transaction log
    finally:
        _waiters.pop(txn_id, None)


async def _transformer(consumer: AIOKafkaConsumer):
    try:
        async for rec in consumer:
            msg = rec.value
            txn_id = msg["id"]
            await db.record_event(
                txn_id, "TRANSFORMING", f"consumed from {rec.topic}[{rec.partition}]@{rec.offset}", status="TRANSFORMING"
            )
            t0 = time.perf_counter()
            try:
                output = transform(msg["payload"], msg["input_format"], msg["expression"], msg["output_format"])
                elapsed = (time.perf_counter() - t0) * 1000
                TRANSFORM_SECONDS.observe(elapsed / 1000)
                await producer.send_and_wait(TOPIC_OUT, {**msg, "output": output, "transform_ms": elapsed}, key=rec.key)
                await db.record_event(
                    txn_id, "TRANSFORMED", f"{len(output)} bytes of {msg['output_format'].upper()} in {elapsed:.1f} ms",
                    status="TRANSFORMED", transform_ms=elapsed,
                )
            except TransformError as e:
                elapsed = (time.perf_counter() - t0) * 1000
                await producer.send_and_wait(TOPIC_DLQ, {**msg, "error": str(e)}, key=rec.key)
                await _finish(txn_id, msg["mapping_name"], "FAILED", error=str(e), transform_ms=elapsed,
                              detail=f"sent to {TOPIC_DLQ}: {e}")
            except Exception:  # keep the consumer alive on unexpected errors
                log.exception("transformer crashed on %s", txn_id)
                await _finish(txn_id, msg["mapping_name"], "FAILED", error="Internal error in transformer",
                              detail="unexpected exception - see server log")
    finally:
        await consumer.stop()


async def _result_sink(consumer: AIOKafkaConsumer):
    try:
        async for rec in consumer:
            msg = rec.value
            await _finish(msg["id"], msg["mapping_name"], "COMPLETED", output_payload=msg["output"],
                          detail=f"consumed from {rec.topic}[{rec.partition}]@{rec.offset}, stored output")
    finally:
        await consumer.stop()


def _log_crash(task: asyncio.Task):
    if not task.cancelled() and task.exception():
        log.error("consumer %s stopped", task.get_name(), exc_info=task.exception())


async def _finish(txn_id, mapping, status, detail, **fields):
    started = _started_at.pop(txn_id, None)
    total_ms = (time.perf_counter() - started) * 1000 if started else None
    if started:
        END_TO_END_SECONDS.observe(total_ms / 1000)
        IN_FLIGHT.dec()
    TXN_TOTAL.labels(status=status, mapping=mapping).inc()
    await db.record_event(txn_id, status, detail, status=status, total_ms=total_ms, **fields)
    fut = _waiters.get(txn_id)
    if fut and not fut.done():
        fut.set_result(await db.get_transaction(txn_id))
