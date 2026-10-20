"""Alerts: webhook and email channels, rules per event and partner, cool-down, error-rate watcher."""
import json
import socket

import httpx
import pytest

from .conftest import wait_for
from .test_partner_flow import make_partner

pytestmark = pytest.mark.integration


def received(admin, name="alerts"):
    return [m for m in admin.get("/api/test-receiver", params={"limit": 100}).json() if m["name"] == name]


@pytest.fixture(scope="module")
def webhook(admin, base_url):
    r = admin.post("/api/alerts/channels", json={"name": "Ops webhook", "type": "webhook",
                                                 "config": {"url": f"{base_url}/test-receiver/alerts"}})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def rule(admin, name, event, channels, **kw):
    r = admin.post("/api/alerts/rules", json={"name": name, "event": event, "channels": channels, **kw})
    assert r.status_code == 201, r.text
    return r.json()


def log_for(admin, rule_id, n):
    return wait_for(lambda: [e for e in admin.get("/api/alerts").json()["log"] if e["rule_id"] == rule_id],
                    lambda e: len(e) >= n, timeout=20)


def test_channel_test_message(admin, webhook):
    before = len(received(admin))
    r = admin.post(f"/api/alerts/channels/{webhook}/test").json()
    assert r["ok"], r
    msg = received(admin)[0]
    assert len(received(admin)) == before + 1 and json.loads(msg["body"])["subject"] == "Test alert"


def test_failed_transaction_alert_with_cooldown(admin, base_url, webhook):
    p = make_partner(admin, "Alert Test Partner")
    other = make_partner(admin, "Alert Test Quiet")
    r = rule(admin, "Acme failures", "transaction_failed", [webhook], filter={"partner": p["code"]}, cooldown_minutes=15)
    for partner in (p, p, other):
        httpx.post(f"{base_url}/inbound/{partner['code']}", content=b"{broken", timeout=60,
                   headers={"X-API-Key": partner["api_key"], "Content-Type": "application/json"})
    events = log_for(admin, r["id"], 2)
    assert sorted(e["status"] for e in events) == ["sent", "suppressed"]  # the other partner doesn't match
    sent = next(e for e in events if e["status"] == "sent")
    assert p["code"] in sent["subject"] and sent["results"][0]["ok"]
    body = json.loads(next(m for m in received(admin) if p["code"] in m["body"])["body"])
    assert body["event"] == "transaction_failed" and body["attributes"]["partner"] == p["code"]


def test_failed_workflow_alert(admin, webhook):
    r = rule(admin, "Workflow failures", "workflow_failed", [webhook], cooldown_minutes=0)
    wf = admin.post("/api/workflows", json={"name": "Alert failing wf", "steps": [
        {"type": "db", "label": "Missing", "connection": "builtin", "table": "no_such_table", "operation": "select_all",
         "save_as": "rows"}]}).json()
    assert admin.post(f"/api/workflows/{wf['id']}/run", json={}).json()["status"] == "FAILED"
    (event,) = log_for(admin, r["id"], 1)
    assert event["status"] == "sent" and "Alert failing wf" in event["subject"]


def test_error_rate_watcher(admin, base_url, webhook):
    p = make_partner(admin, "Alert Rate Partner")
    r = rule(admin, "Rate", "error_rate", [webhook], filter={"partner": p["code"]},
             threshold={"count": 2, "minutes": 10})
    for _ in range(2):
        httpx.post(f"{base_url}/inbound/{p['code']}", content=b"<nope", timeout=60,
                   headers={"X-API-Key": p["api_key"], "Content-Type": "application/json"})
    (event,) = log_for(admin, r["id"], 1)
    assert event["subject"].startswith("2 failed transactions in 10 min")


def test_email_channel(admin):
    from aiosmtpd.controller import Controller

    inbox = []

    class Handler:
        async def handle_DATA(self, server, session, envelope):
            inbox.append(envelope)
            return "250 OK"

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    smtp = Controller(Handler(), hostname="127.0.0.1", port=port)
    smtp.start()
    try:
        r = admin.post("/api/alerts/channels", json={"name": "Ops email", "type": "email", "config": {
            "host": "127.0.0.1", "port": port, "security": "none", "from": "hub@example.com",
            "to": "ops@example.com, oncall@example.com"}})
        assert r.status_code == 201, r.text
        assert admin.post(f"/api/alerts/channels/{r.json()['id']}/test").json()["ok"]
        (mail,) = inbox
        assert mail.rcpt_tos == ["ops@example.com", "oncall@example.com"] and b"Test alert" in mail.content
    finally:
        smtp.stop()


@pytest.mark.parametrize("body, message", [
    ({"type": "webhook", "config": {"url": "ftp://x"}}, "http"),
    ({"type": "email", "config": {"host": "smtp.example.com"}}, "recipient"),
])
def test_invalid_channels(admin, body, message):
    r = admin.post("/api/alerts/channels", json={"name": "Bad channel", **body})
    assert r.status_code == 422 and message in r.json()["detail"]


def test_rule_needs_a_channel(admin):
    r = admin.post("/api/alerts/rules", json={"name": "No channel", "event": "workflow_failed", "channels": []})
    assert r.status_code == 422
