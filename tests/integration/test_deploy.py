"""Deployment packages between two environments: DEV (the test server) -> TEST (a second instance with its own,
empty database)."""
import os
import subprocess
import sys
import time

import httpx
import psycopg
import pytest
from psycopg import sql

from tests.conftest import TEST_DATABASE_URL

from .conftest import ADMIN, login
from .test_partner_flow import ORDER, make_partner

pytestmark = pytest.mark.integration
TARGET_DB = "payloadhub_test_target"


@pytest.fixture(scope="module")
def target(base_url, tmp_path_factory):
    """A second Payload Hub: environment TEST, its own database and Kafka topics."""
    admin_url = TEST_DATABASE_URL.rsplit("/", 1)[0] + "/postgres"
    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TARGET_DB)))
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TARGET_DB)))
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    env = {**os.environ, "DATABASE_URL": TEST_DATABASE_URL.rsplit("/", 1)[0] + f"/{TARGET_DB}",
           "KAFKA_TOPIC_PREFIX": os.environ["KAFKA_TOPIC_PREFIX"] + "target.", "PAYLOAD_HUB_ENVIRONMENT": "TEST"}
    log = open(tmp_path_factory.mktemp("target") / "target.log", "w")
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port",
                             str(port), "--log-level", "warning"], env=env, stdout=log, stderr=subprocess.STDOUT)
    url = f"http://127.0.0.1:{port}"
    for _ in range(180):
        try:
            if httpx.get(f"{url}/health", timeout=2).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    else:
        proc.kill()
        pytest.fail("the target environment didn't start")
    with login(url, *ADMIN) as client:
        yield url, client
    proc.terminate()
    proc.wait(timeout=30)
    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TARGET_DB)))


@pytest.fixture(scope="module")
def dev_setup(admin, base_url, tmp_path_factory):
    """In DEV: a secret, an API connection using it, a partner delivering to it, a workflow, a schedule, a port,
    an alert channel and rule."""
    admin.post("/api/secrets", json={"alias": "orders.api.token", "kind": "token", "value": "dev-token"})
    conn = admin.post("/api/connections", json={"name": "Deploy Orders API", "type": "http",
                                                "secret_alias": "orders.api.token",
                                                "config": {"base_url": f"{base_url}/test-receiver"}}).json()
    p = make_partner(admin, "Deploy Partner")
    admin.put(f"/api/partners/{p['code']}/adapters", json={"outbound_connection": conn["id"], "outbound_target": "dev"})
    wf = admin.post("/api/workflows", json={"name": "Deploy wf", "steps": [
        {"type": "api_call", "label": "Ping", "connection": conn["id"], "method": "GET", "path": "x",
         "save_as": "ping", "on_error": "continue"}]}).json()
    task = admin.post("/api/schedules", json={"name": "Deploy nightly", "schedule": {"kind": "cron", "cron": "0 2 * * *"},
                                              "action": {"type": "workflow", "workflow_id": wf["id"]}}).json()
    folder = tmp_path_factory.mktemp("drop")
    port = admin.post("/api/ports", json={"name": "Deploy folder", "type": "file", "target_type": "partner",
                                          "target_id": p["code"], "config": {"monitor_dir": str(folder)}}).json()
    ch = admin.post("/api/alerts/channels", json={"name": "Deploy webhook", "type": "webhook",
                                                  "config": {"url": f"{base_url}/test-receiver/alerts"}}).json()
    rule = admin.post("/api/alerts/rules", json={"name": "Deploy failures", "event": "transaction_failed",
                                                 "channels": [ch["id"]]}).json()
    return {"conn": conn["id"], "partner": p, "workflow": wf["id"], "task": task["id"], "port": port["id"],
            "channel": ch["id"], "rule": rule["id"]}


def export(admin, dev, name="release-1"):
    r = admin.post("/api/deploy/export", json={"name": name, "selection": {
        "partners": [dev["partner"]["code"]], "schedules": [dev["task"]], "ports": [dev["port"]],
        "alert_rules": [dev["rule"]]}})
    assert r.status_code == 200, r.text
    return r.json()


def test_export_includes_dependencies(admin, dev_setup):
    pkg = export(admin, dev_setup)
    assets = pkg["assets"]
    assert pkg["source_environment"] == "DEV" and pkg["schema_version"]
    assert [s["alias"] for s in assets["secrets"]] == ["orders.api.token"]
    assert "value" not in assets["secrets"][0] and "dev-token" not in str(pkg)  # never secret values
    assert {c["id"] for c in assets["connections"]} == {dev_setup["conn"]}
    assert [w["id"] for w in assets["workflows"]] == [dev_setup["workflow"]]
    assert [c["id"] for c in assets["alert_channels"]] == [dev_setup["channel"]]
    assert "api_key_hash" not in str(assets["partners"])


def test_deploy_into_another_environment(admin, dev_setup, target):
    t_url, t = target
    pkg = export(admin, dev_setup)
    plan = t.post("/api/deploy/plan", json={"package": pkg}).json()
    assert plan["target_environment"] == "TEST" and not plan["can_apply"]
    assert any("orders.api.token" in p["problem"] for p in plan["problems"])  # the secret must exist there first
    assert t.post("/api/deploy/apply", json={"package": pkg}).status_code == 422

    t.post("/api/secrets", json={"alias": "orders.api.token", "kind": "token", "value": "test-token"})
    overrides = {"connections": {dev_setup["conn"]: {"config": {"base_url": f"{t_url}/test-receiver"}}},
                 "schedules": {dev_setup["task"]: {"enabled": False}},
                 "partners": {dev_setup["partner"]["code"]: {"outbound_target": "test-orders"}}}
    plan = t.post("/api/deploy/plan", json={"package": pkg, "overrides": overrides}).json()
    assert plan["can_apply"] and plan["counts"]["create"] == 7 and plan["counts"]["missing"] == 0
    r = t.post("/api/deploy/apply", json={"package": pkg, "overrides": overrides}).json()
    assert r["status"] == "DEPLOYED", r
    assert r["backup"] and r["backup"].endswith(".dump")

    code = dev_setup["partner"]["code"]
    tp = t.get(f"/api/partners/{code}").json()
    assert tp["outbound_target"] == "test-orders" and tp["api_key_hint"] != dev_setup["partner"]["api_key_hint"]
    conn = next(c for c in t.get("/api/connections").json() if c["id"] == dev_setup["conn"])
    assert conn["config"]["base_url"] == f"{t_url}/test-receiver" and conn["secret_alias"] == "orders.api.token"
    task = next(x for x in t.get("/api/schedules").json()["tasks"] if x["id"] == dev_setup["task"])
    assert task["enabled"] is False
    # the partner works in the new environment, with its own key
    key = t.get(f"/api/partners/{code}/api-key").json()["api_key"]
    call = httpx.post(f"{t_url}/inbound/{code}", json=ORDER, headers={"X-API-Key": key}, timeout=60)
    assert call.status_code == 200 and call.json()["order_id"] == "A-100"
    (dep, *_) = t.get("/api/deploy/history").json()
    assert dep["status"] == "DEPLOYED" and dep["source_environment"] == "DEV" and dep["target_environment"] == "TEST"
    assert any(e["action"].startswith("deployed") for e in t.get("/api/audit").json())

    # deploying the same package again changes nothing
    again = t.post("/api/deploy/plan", json={"package": pkg, "overrides": overrides}).json()
    assert again["counts"]["create"] == again["counts"]["update"] == 0


def test_changes_show_as_updates(admin, dev_setup, target):
    _, t = target
    code = dev_setup["partner"]["code"]
    admin.put(f"/api/partners/{code}", json={"expression": "{order_id: order.id}"})
    pkg = export(admin, dev_setup, "release-2")
    overrides = {"connections": {dev_setup["conn"]: {"config": {"base_url": "http://x"}}}}
    plan = t.post("/api/deploy/plan", json={"package": pkg, "overrides": overrides}).json()
    partner = next(i for i in plan["items"] if i["type"] == "partners")
    assert partner["action"] == "update" and "expression" in partner["changes"]


def test_bad_packages_are_refused(target):
    _, t = target
    assert t.post("/api/deploy/plan", json={"package": {"hello": 1}}).status_code == 422
    newer = {"format": "payload-hub-package/1", "schema_version": "9999_future", "assets": {}}
    r = t.post("/api/deploy/plan", json={"package": newer})
    assert r.status_code == 422 and "newer version" in r.json()["detail"]


def test_only_admins_deploy(admin, base_url):
    admin.post("/api/users", json={"username": "deploy-writer", "password": "writer-pass", "can_write": True})
    with login(base_url, "deploy-writer", "writer-pass") as w:
        assert w.get("/api/deploy/inventory").status_code == 403
        assert w.post("/api/deploy/plan", json={"package": {}}).status_code == 403
