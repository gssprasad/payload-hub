"""Workflows end to end: DB connector, if/else branch, retry with carry-on, dry run, rerun from the failed step."""
import pytest

from .conftest import query

pytestmark = pytest.mark.integration


def followup_steps(db, api):
    return [
        {"type": "db", "label": "Find customer", "connection": db, "table": "customers", "operation": "select_one",
         "required": False, "where": [{"column": "id", "op": "=", "value": "{{ trigger.customer_id }}"}],
         "save_as": "customer"},
        {"type": "branch", "label": "Known?", "expression": "customer != null",
         "then": [
             {"type": "api_call", "label": "Broken API", "connection": api, "method": "GET", "path": "nope/404",
              "save_as": "broken", "retry_attempts": 2, "retry_delay": 0, "on_error": "continue"},
             {"type": "api_call", "label": "Profile", "connection": api, "method": "GET",
              "path": "customers/{{ customer.id }}", "save_as": "profile"},
             {"type": "db", "label": "Log follow-up", "connection": db, "table": "followups", "operation": "insert",
              "values": {"customer_id": "{{ customer.id }}", "note": "{{ profile.data.segment }}"}}],
         "else": [{"type": "transform", "label": "Unknown", "expression": "{unknown: trigger.customer_id}",
                   "save_as": "unknown"}]},
    ]


def run(admin, wf, **body):
    r = admin.post(f"/api/workflows/{wf}/run", json=body, timeout=120)
    assert r.status_code == 200, r.text
    return r.json()


def by_path(result):
    return {s["path"]: s for s in result["steps_log"]}


@pytest.fixture(scope="module")
def workflow(admin, shop_db, mock_api):
    r = admin.post("/api/workflows", json={"name": "Test followup", "steps": followup_steps(shop_db["id"], mock_api)})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_known_customer_takes_yes_path(admin, workflow, shop_db):
    result = run(admin, workflow, input={"customer_id": "C1"})
    assert result["status"] == "COMPLETED", result
    steps = by_path(result)
    assert steps["2"]["output"].endswith("→ then")
    assert steps["2.then.1"]["status"] == "FAILED_CONTINUED" and steps["2.then.1"]["attempts"] == 2
    assert len(steps["2.then.1"]["retries"]) == 1
    assert steps["2.then.3"]["status"] == "OK"
    assert result["context"]["broken"] is None
    assert query(shop_db["path"], "SELECT customer_id FROM followups") == [("C1",)]


def test_unknown_customer_takes_no_path(admin, workflow):
    steps = by_path(run(admin, workflow, input={"customer_id": "ZZ"}))
    assert steps["2"]["output"].endswith("→ else") and steps["2.else.1"]["status"] == "OK"
    assert "2.then.1" not in steps


def test_dry_run_changes_nothing(admin, workflow, shop_db):
    before = query(shop_db["path"], "SELECT count(*) FROM followups")
    steps = by_path(run(admin, workflow, input={"customer_id": "C2"}, dry_run=True))
    assert steps["2.then.3"]["status"] == "SKIPPED" and "INSERT INTO followups" in steps["2.then.3"]["output"]
    assert query(shop_db["path"], "SELECT count(*) FROM followups") == before


def test_invalid_workflow_is_rejected(admin, shop_db):
    r = admin.post("/api/workflows", json={"name": "Test invalid", "steps": [
        {"type": "db", "connection": shop_db["id"], "table": "customers", "operation": "delete"}]})
    assert r.status_code == 422 and "every row" in r.json()["detail"]


def test_rerun_from_failed_step(admin, shop_db):
    import sqlite3

    steps = [{"type": "db", "label": "Read", "connection": shop_db["id"], "table": "customers",
              "operation": "select_one", "where": [{"column": "id", "op": "=", "value": "C1"}], "save_as": "c"},
             {"type": "db", "label": "Write", "connection": shop_db["id"], "table": "audit", "operation": "insert",
              "values": {"who": "{{ c.name }}"}}]
    wf = admin.post("/api/workflows", json={"name": "Test rerun", "steps": steps}).json()["id"]
    first = run(admin, wf)
    assert first["status"] == "FAILED" and first["failed_step"] == 1 and "no table 'audit'" in first["error"]
    con = sqlite3.connect(shop_db["path"])
    con.execute("CREATE TABLE audit (who TEXT)")
    con.commit()
    con.close()
    second = admin.post(f"/api/workflow-runs/{first['id']}/rerun", timeout=120).json()
    assert second["status"] == "COMPLETED"
    assert [s["status"] for s in second["steps_log"]] == ["REUSED", "OK"]
    assert query(shop_db["path"], "SELECT who FROM audit") == [("Asha",)]
