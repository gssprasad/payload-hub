"""Kafka pipeline: <queue>.in -> transformer -> <queue>.out -> result sink (failures -> <queue>.dlq).

Queues are either the shared ones (payload.in / payload.out / payload.dlq) or a partner's dedicated
ones (partner.<code>.in / .out / .dlq). The workers subscribe by topic pattern, so a newly onboarded
partner's dedicated queues are picked up within seconds without a restart.

This plays the role Apache Camel routes would play in the Java version:
    from("kafka:payload.in").process(transform).to("kafka:payload.out")
"""
import asyncio
import json
import logging
import os
import re
import time

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.helpers import create_ssl_context
from prometheus_client import Counter, Gauge, Histogram

from . import db
from .transform import TransformError, transform

log = logging.getLogger("pipeline")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
# Optional prefix for every topic and consumer group, so several instances (e.g. the test suite) can share one
# Kafka cluster without seeing each other's messages.
PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "")
TOPIC_IN, TOPIC_OUT, TOPIC_DLQ = f"{PREFIX}payload.in", f"{PREFIX}payload.out", f"{PREFIX}payload.dlq"
SHARED_TOPICS = (TOPIC_IN, TOPIC_OUT, TOPIC_DLQ)
TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "2"))
IN_PATTERN = rf"^{re.escape(PREFIX)}(payload|partner\.[a-z0-9-]+)\.in$"
OUT_PATTERN = rf"^{re.escape(PREFIX)}(payload|partner\.[a-z0-9-]+)\.out$"
GROUP_TRANSFORMER, GROUP_SINK = f"{PREFIX}payload-hub-transformer", f"{PREFIX}payload-hub-result-sink"

TXN_TOTAL = Counter("payload_transactions_total", "Transactions by final status",
                    ["status", "mapping", "partner"])
TRANSFORM_SECONDS = Histogram("payload_transform_seconds", "Time spent in the transformer step")
END_TO_END_SECONDS = Histogram("payload_end_to_end_seconds", "API receive -> result stored")
IN_FLIGHT = Gauge("payload_in_flight", "Transactions published but not yet completed")

producer: AIOKafkaProducer | None = None
_tasks: list[asyncio.Task] = []
_waiters: dict[str, asyncio.Future] = {}
_started_at: dict[str, float] = {}
_queue_cache: tuple[float, list] = (0.0, [])
on_completed: list = []  # async callbacks(msg) run after a transaction completes, e.g. outbound delivery


def partner_topics(code: str) -> tuple[str, str, str]:
    return f"{PREFIX}partner.{code}.in", f"{PREFIX}partner.{code}.out", f"{PREFIX}partner.{code}.dlq"


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


async def ensure_topics(names) -> list[str]:
    """Create any of the given topics that don't exist yet. Returns the ones created."""
    admin = AIOKafkaAdminClient(**_connection())
    await admin.start()
    try:
        missing = sorted(set(names) - set(await admin.list_topics()))
        if missing:
            brokers = len((await admin.describe_cluster()).get("brokers", [])) or 1
            replication = int(os.getenv("KAFKA_REPLICATION_FACTOR", min(3, brokers)))
            await admin.create_topics([NewTopic(t, TOPIC_PARTITIONS, replication) for t in missing])
            log.info("created topics %s", missing)
        return missing
    finally:
        await admin.close()


def _encode(obj) -> bytes:
    return json.dumps(obj).encode()


async def start():
    global producer
    await ensure_topics(SHARED_TOPICS)
    producer = AIOKafkaProducer(**_connection(), value_serializer=_encode, acks="all")
    await producer.start()
    for name, group, pattern, coro in (("transformer", GROUP_TRANSFORMER, IN_PATTERN, _transformer),
                                       ("result-sink", GROUP_SINK, OUT_PATTERN, _result_sink)):
        consumer = AIOKafkaConsumer(
            **_connection(),
            group_id=group,
            auto_offset_reset="earliest",
            value_deserializer=json.loads,
            metadata_max_age_ms=5000,  # notice new partner topics within ~5 s
        )
        consumer.subscribe(pattern=pattern)
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
    """Publish to the message's input queue and wait (briefly) for the pipeline to finish it."""
    fut = asyncio.get_running_loop().create_future()
    _waiters[txn_id] = fut
    _started_at[txn_id] = time.perf_counter()
    IN_FLIGHT.inc()
    try:
        meta = await producer.send_and_wait(message["topic_in"], message, key=txn_id.encode())
        await db.record_event(
            txn_id, "PUBLISHED", f"{message['topic_in']} partition {meta.partition} offset {meta.offset}",
            status="QUEUED",
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
            topic_out, topic_dlq = msg.get("topic_out", TOPIC_OUT), msg.get("topic_dlq", TOPIC_DLQ)
            await db.record_event(
                txn_id, "TRANSFORMING", f"consumed from {rec.topic}[{rec.partition}]@{rec.offset}", status="TRANSFORMING"
            )
            t0 = time.perf_counter()
            try:
                output = transform(msg["payload"], msg["input_format"], msg["expression"], msg["output_format"])
                elapsed = (time.perf_counter() - t0) * 1000
                TRANSFORM_SECONDS.observe(elapsed / 1000)
                # record first: once published, the result sink may mark it COMPLETED at any moment
                await db.record_event(
                    txn_id, "TRANSFORMED",
                    f"{len(output)} bytes of {msg['output_format'].upper()} in {elapsed:.1f} ms -> {topic_out}",
                    status="TRANSFORMED", transform_ms=elapsed,
                )
                await producer.send_and_wait(topic_out, {**msg, "output": output, "transform_ms": elapsed}, key=rec.key)
            except TransformError as e:
                elapsed = (time.perf_counter() - t0) * 1000
                await producer.send_and_wait(topic_dlq, {**msg, "error": str(e)}, key=rec.key)
                await _finish(msg, "FAILED", error=str(e), transform_ms=elapsed, detail=f"sent to {topic_dlq}: {e}")
            except Exception:  # keep the consumer alive on unexpected errors
                log.exception("transformer crashed on %s", txn_id)
                await _finish(msg, "FAILED", error="Internal error in transformer",
                              detail="unexpected exception - see server log")
    finally:
        await consumer.stop()


async def _result_sink(consumer: AIOKafkaConsumer):
    try:
        async for rec in consumer:
            msg = rec.value
            await _finish(msg, "COMPLETED", output_payload=msg["output"],
                          detail=f"consumed from {rec.topic}[{rec.partition}]@{rec.offset}, stored output")
            for callback in on_completed:
                asyncio.create_task(callback(msg))
    finally:
        await consumer.stop()


def _log_crash(task: asyncio.Task):
    if not task.cancelled() and task.exception():
        log.error("consumer %s stopped", task.get_name(), exc_info=task.exception())


async def _finish(msg, status, detail, **fields):
    txn_id = msg["id"]
    started = _started_at.pop(txn_id, None)
    total_ms = (time.perf_counter() - started) * 1000 if started else None
    if started:
        END_TO_END_SECONDS.observe(total_ms / 1000)
        IN_FLIGHT.dec()
    TXN_TOTAL.labels(status=status, mapping=msg["mapping_name"], partner=msg.get("partner_code") or "-").inc()
    await db.record_event(txn_id, status, detail, status=status, total_ms=total_ms, **fields)
    if status == "FAILED":
        from . import alerts

        who = msg.get("partner_code") or msg["mapping_name"]
        alerts.emit("transaction_failed", f"Transaction failed for {who}", fields.get("error") or detail,
                    partner=msg.get("partner_code"), transaction=txn_id)
    fut = _waiters.get(txn_id)
    if fut and not fut.done():
        fut.set_result(await db.get_transaction(txn_id))


async def queue_stats(max_age: float = 5.0) -> list[dict]:
    """Messages and consumer backlog for every pipeline topic (cached for a few seconds)."""
    global _queue_cache
    if time.monotonic() - _queue_cache[0] < max_age:
        return _queue_cache[1]
    admin = AIOKafkaAdminClient(**_connection())
    probe = AIOKafkaConsumer(**_connection(), enable_auto_commit=False)
    await admin.start()
    await probe.start()
    try:
        topics = sorted(t for t in await admin.list_topics() if re.match(
            rf"^{re.escape(PREFIX)}(payload|partner\.[a-z0-9-]+)\.(in|out|dlq)$", t))
        described = await admin.describe_topics(topics) if topics else []
        parts = [TopicPartition(d["topic"], p["partition"]) for d in described for p in d["partitions"]]
        ends = await probe.end_offsets(parts) if parts else {}
        starts = await probe.beginning_offsets(parts) if parts else {}
        committed = {}
        for group in (GROUP_TRANSFORMER, GROUP_SINK):
            committed[group] = {tp: m.offset for tp, m in (await admin.list_consumer_group_offsets(group)).items()}
        rows = []
        for t in topics:
            tps = [tp for tp in parts if tp.topic == t]
            kind = t.rsplit(".", 1)[-1]
            group = GROUP_TRANSFORMER if kind == "in" else GROUP_SINK if kind == "out" else None
            lag = None
            if group:
                lag = sum(max(0, ends[tp] - max(committed[group].get(tp, starts[tp]), starts[tp])) for tp in tps)
            rows.append({
                "topic": t,
                "queue": "shared" if t.startswith(f"{PREFIX}payload.") else t[len(PREFIX):].split(".")[1],
                "kind": kind,
                "partitions": len(tps),
                "messages": sum(ends[tp] - starts[tp] for tp in tps),
                "backlog": lag,
            })
        _queue_cache = (time.monotonic(), rows)
        return rows
    finally:
        await probe.stop()
        await admin.close()
