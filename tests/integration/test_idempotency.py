"""Nothing happens twice: partner retries (Idempotency-Key), Kafka delivering a message again, interrupted
deliveries."""
import asyncio
import json
from concurrent.futures import ThreadPoolExecutor

import httpx
import psycopg
import pytest

from tests.conftest import TEST_DATABASE_URL

from .conftest import wait_for
from .test_partner_flow import ORDER, make_partner

pytestmark = pytest.mark.integration


def call(base_url, p, body=ORDER, key=None):
    return httpx.post(f"{base_url}/inbound/{p['code']}", content=json.dumps(body).encode(), timeout=60,
                      headers={"X-API-Key": p["api_key"], "Content-Type": "application/json",
                               **({"Idempotency-Key": key} if key else {})})


def rows(sql, *args):
    with psycopg.connect(TEST_DATABASE_URL) as conn:
        return conn.execute(sql, args).fetchall()


def received(admin, name):
    return [m for m in admin.get("/api/test-receiver", params={"limit": 100}).json() if m["name"] == name]


def test_partner_retry_returns_the_original(admin, base_url):
    p = make_partner(admin, "Idem Retry")
    first, again = call(base_url, p, key="order-A-100"), call(base_url, p, key="order-A-100")
    assert first.status_code == again.status_code == 200
    assert again.headers["X-Transaction-Id"] == first.headers["X-Transaction-Id"]
    assert again.headers.get("Idempotent-Replayed") == "true" and "Idempotent-Replayed" not in first.headers
    assert again.json() == first.json()
    assert rows("SELECT count(*) FROM transactions WHERE partner_code = %s", p["code"]) == [(1,)]
    assert call(base_url, p, key="order-A-101").headers["X-Transaction-Id"] != first.headers["X-Transaction-Id"]


def test_same_key_different_payload_is_refused(admin, base_url):
    p = make_partner(admin, "Idem Conflict")
    assert call(base_url, p, key="k1").status_code == 200
    r = call(base_url, p, body={"order": {"id": "OTHER", "total": 1, "customer": {"name": "x"}}}, key="k1")
    assert r.status_code == 409 and "different payload" in r.json()["detail"]


def test_simultaneous_retries_make_one_transaction(admin, base_url):
    p = make_partner(admin, "Idem Burst")
    with ThreadPoolExecutor(10) as pool:
        answers = list(pool.map(lambda _: call(base_url, p, key="burst-1"), range(10)))
    assert {a.status_code for a in answers} == {200}
    assert len({a.headers["X-Transaction-Id"] for a in answers}) == 1
    assert rows("SELECT count(*) FROM transactions WHERE partner_code = %s", p["code"]) == [(1,)]


def test_replay_does_not_call_the_target_system_again(admin, base_url, receiver):
    p = make_partner(admin, "Idem Forward")
    admin.put(f"/api/partners/{p['code']}/reply", json={"reply_mode": "forward", "reply_connection": receiver,
                                                        "reply_path": "idem-forward", "response_format": "unchanged"})
    first = call(base_url, p, key="fw-1")
    assert first.status_code == 200 and len(received(admin, "idem-forward")) == 1
    again = call(base_url, p, key="fw-1")
    assert again.status_code == 200 and again.json() == first.json()
    assert len(received(admin, "idem-forward")) == 1  # the target system saw it once


def _produce(topic, msg):
    from aiokafka import AIOKafkaProducer

    from app import pipeline

    async def run():
        producer = AIOKafkaProducer(**pipeline._connection(), value_serializer=lambda v: json.dumps(v).encode())
        await producer.start()
        try:
            await producer.send_and_wait(topic, msg, key=msg["id"].encode())
        finally:
            await producer.stop()

    asyncio.run(run())


def test_kafka_delivering_a_message_twice_changes_nothing(admin, base_url, receiver):
    p = make_partner(admin, "Idem Kafka")
    admin.put(f"/api/partners/{p['code']}/adapters", json={"outbound_connection": receiver,
                                                           "outbound_target": "idem-kafka"})
    r = call(base_url, p)
    txn_id = r.headers["X-Transaction-Id"]
    wait_for(lambda: received(admin, "idem-kafka"), lambda m: len(m) == 1)
    txn = admin.get(f"/api/transactions/{txn_id}").json()
    msg = {"id": txn_id, "payload": json.dumps(ORDER), "mapping_name": p["code"], "input_format": "json",
           "output_format": "json", "expression": p["expression"], "topic_in": p["topic_in"],
           "topic_out": p["topic_out"], "topic_dlq": p["topic_dlq"], "partner_code": p["code"], "source": "HTTP"}
    _produce(p["topic_out"], {**msg, "output": txn["output_payload"], "transform_ms": 1})  # result again
    _produce(p["topic_in"], msg)  # input again
    events = wait_for(lambda: [e["stage"] for e in admin.get(f"/api/transactions/{txn_id}").json()["events"]],
                      lambda s: s.count("DUPLICATE") >= 2, timeout=30)
    assert events.count("DUPLICATE") == 2 and events.count("COMPLETED") == 1
    assert len(received(admin, "idem-kafka")) == 1  # delivered once
    assert received(admin, "idem-kafka")[0]["headers"].get("idempotency-key") == txn_id
    assert admin.get(f"/api/transactions/{txn_id}").json()["status"] == "COMPLETED"


def test_interrupted_delivery_is_flagged_not_resent(admin, base_url):
    p = make_partner(admin, "Idem Interrupted")
    txn_id = call(base_url, p).headers["X-Transaction-Id"]
    with psycopg.connect(TEST_DATABASE_URL) as conn:  # as if an instance died while delivering it
        conn.execute("UPDATE transactions SET delivery_status = 'DELIVERING', "
                     "delivery_claimed_at = now() - interval '20 minutes' WHERE id = %s", (txn_id,))
    txn = wait_for(lambda: admin.get(f"/api/transactions/{txn_id}").json(),
                   lambda t: t["delivery_status"] == "DELIVERY_FAILED", timeout=20)
    assert txn["delivery_status"] == "DELIVERY_FAILED"
    assert any("not resent automatically" in (e["detail"] or "") for e in txn["events"])
