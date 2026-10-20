"""Scheduled tasks: run on their schedule, run now, pause/resume, history, and validation."""
from datetime import datetime, timedelta

import pytest

from .conftest import wait_for
from .test_partner_flow import make_partner

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def counter_workflow(admin):
    r = admin.post("/api/workflows", json={"name": "Scheduler test", "steps": [
        {"type": "transform", "label": "Echo", "save_as": "echo", "expression": "trigger"}]})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def task(admin, name, schedule, action, **kw):
    r = admin.post("/api/schedules", json={"name": name, "schedule": schedule, "action": action, **kw})
    assert r.status_code == 201, r.text
    return r.json()


def test_interval_task_runs_on_its_own(admin, counter_workflow):
    t = task(admin, "Every five seconds", {"kind": "interval", "every": 5, "unit": "seconds"},
             {"type": "workflow", "workflow_id": counter_workflow, "input": {"hello": "world"}})
    assert t["schedule_text"] == "every 5 seconds" and t["next_run_at"]
    done = wait_for(lambda: admin.get("/api/schedules").json()["tasks"],
                    lambda ts: next(x for x in ts if x["id"] == t["id"])["run_count"] >= 1, timeout=30)
    me = next(x for x in done if x["id"] == t["id"])
    assert me["last_status"] == "OK", me
    (run, *_) = admin.get(f"/api/schedules/{t['id']}/runs").json()
    assert run["trigger"] == "schedule" and run["status"] == "OK"
    wf_run = admin.get(f"/api/workflow-runs/{run['ref']}").json()
    assert wf_run["context"]["echo"] == {"hello": "world"}
    paused = admin.put(f"/api/schedules/{t['id']}/enabled", json={"enabled": False}).json()
    assert paused["next_run_at"] is None
    resumed = admin.put(f"/api/schedules/{t['id']}/enabled", json={"enabled": True}).json()
    assert resumed["next_run_at"]
    admin.delete(f"/api/schedules/{t['id']}")


def test_run_now_partner_message(admin):
    p = make_partner(admin, "Scheduler Partner")
    t = task(admin, "Daily partner push", {"kind": "daily", "at": "06:00", "days": ["mon"], "timezone": "UTC"},
             {"type": "partner", "partner_code": p["code"]})
    r = admin.post(f"/api/schedules/{t['id']}/run").json()
    assert r["status"] == "OK", r
    txn = admin.get(f"/api/transactions/{r['ref']}").json()
    assert txn["source"] == "SCHEDULER" and txn["partner_code"] == p["code"]
    (run,) = admin.get(f"/api/schedules/{t['id']}/runs").json()
    assert run["trigger"] == "manual"
    # a manual run doesn't move the schedule
    after = next(x for x in admin.get("/api/schedules").json()["tasks"] if x["id"] == t["id"])
    assert after["next_run_at"] == t["next_run_at"] and after["run_count"] == 1


def test_api_call_task(admin, receiver, mock_api):
    ok = task(admin, "Ping receiver", {"kind": "cron", "cron": "0 3 * * *"},
              {"type": "http", "connection": receiver, "method": "POST", "path": "ping", "body": "{}"})
    assert admin.post(f"/api/schedules/{ok['id']}/run").json()["status"] == "OK"
    bad = task(admin, "Missing page", {"kind": "cron", "cron": "0 3 * * *"},
               {"type": "http", "connection": mock_api, "method": "GET", "path": "nope"})
    r = admin.post(f"/api/schedules/{bad['id']}/run").json()
    assert r["status"] == "FAILED" and "HTTP 404" in r["detail"]


def test_once_task_disables_itself(admin, counter_workflow):
    soon = (datetime.now() + timedelta(seconds=62)).replace(second=0, microsecond=0)
    t = task(admin, "One-off", {"kind": "once", "run_at": soon.isoformat(timespec="minutes")},
             {"type": "workflow", "workflow_id": counter_workflow})
    assert t["enabled"] and t["schedule_text"].startswith("once at")


@pytest.mark.parametrize("schedule, message", [
    ({"kind": "cron", "cron": "61 * * * *"}, "out of range"),
    ({"kind": "interval", "every": 1, "unit": "seconds"}, "every 5 seconds"),
    ({"kind": "once", "run_at": "2020-01-01T00:00"}, "already passed"),
    ({"kind": "daily", "at": "25:00"}, "HH:MM"),
])
def test_invalid_schedules(admin, counter_workflow, schedule, message):
    r = admin.post("/api/schedules", json={"name": "Bad task", "schedule": schedule,
                                           "action": {"type": "workflow", "workflow_id": counter_workflow}})
    assert r.status_code == 422 and message in r.json()["detail"]


def test_invalid_action(admin):
    r = admin.post("/api/schedules", json={"name": "Bad action", "schedule": {"kind": "cron", "cron": "* * * * *"},
                                           "action": {"type": "workflow", "workflow_id": "missing"}})
    assert r.status_code == 422 and "workflow" in r.json()["detail"]
