"""File polling ports: files in a folder go to a partner or a workflow, then to the completion or error folder."""
import json
import os
import time

import pytest

from .conftest import login, wait_for
from .test_partner_flow import ORDER, make_partner

pytestmark = pytest.mark.integration


def drop(folder, name, content):
    path = folder / name
    path.write_text(content)
    old = time.time() - 60
    os.utime(path, (old, old))  # older than the minimum age
    return path


def file_port(admin, name, folder, target_type, target_id, **cfg):
    r = admin.post("/api/ports", json={"name": name, "type": "file", "target_type": target_type,
                                       "target_id": target_id,
                                       "config": {"monitor_dir": str(folder), "interval_seconds": 2,
                                                  "min_age_seconds": 1, **cfg}})
    assert r.status_code == 201, r.text
    return r.json()


def events(admin, port, n):
    return wait_for(lambda: admin.get(f"/api/ports/{port['id']}/events").json(), lambda e: len(e) >= n, timeout=30)


def test_files_go_to_a_partner(admin, tmp_path):
    partner = make_partner(admin, "Port Test Partner")
    port = file_port(admin, "Orders folder", tmp_path, "partner", partner["code"], filter="*.json")
    assert port["config"]["work_dir"] == str(tmp_path / ".work")
    drop(tmp_path, "order1.json", json.dumps(ORDER))
    drop(tmp_path, "broken.json", "{not json")
    drop(tmp_path, "ignored.txt", "not matching the filter")
    admin.post(f"/api/ports/{port['id']}/poll")
    got = {e["item"]: e for e in events(admin, port, 2)}
    assert got["order1.json"]["status"] == "OK" and got["broken.json"]["status"] == "FAILED"
    txn = admin.get(f"/api/transactions/{got['order1.json']['ref']}").json()
    assert txn["source"] == f"PORT:{port['id']}" and txn["partner_code"] == partner["code"]
    done, errors = os.listdir(tmp_path / "done"), os.listdir(tmp_path / "error")
    assert len(done) == 1 and done[0].endswith("_order1.json")
    assert sorted(n.split("_", 1)[1] for n in errors) == ["broken.json", "broken.json.error.txt"]
    assert (tmp_path / "ignored.txt").exists() and os.listdir(tmp_path / ".work") == []
    port = admin.get("/api/ports").json()
    assert next(p for p in port if p["name"] == "Orders folder")["ok_count"] == 1


def test_csv_file_starts_a_workflow(admin, tmp_path):
    wf = admin.post("/api/workflows", json={"name": "Port csv test", "steps": [
        {"type": "transform", "label": "Count", "save_as": "summary",
         "expression": "{file: trigger.file.name, rows: length(trigger.file.data), first: trigger.file.data[0].sku}"}]
    }).json()
    port = file_port(admin, "CSV folder", tmp_path, "workflow", wf["id"], after_success="delete")
    drop(tmp_path, "stock.csv", "sku;qty\nA1;5\nB2;7\n")
    admin.post(f"/api/ports/{port['id']}/poll")
    (event,) = events(admin, port, 1)
    assert event["status"] == "OK", event
    run = admin.get(f"/api/workflow-runs/{event['ref']}").json()
    assert run["context"]["summary"] == {"file": "stock.csv", "rows": 2, "first": "A1"}
    assert os.listdir(tmp_path / "done") == []  # deleted after success


def test_disabled_port_leaves_files(admin, tmp_path):
    partner = make_partner(admin, "Port Test Paused")
    port = file_port(admin, "Paused folder", tmp_path, "partner", partner["code"])
    assert admin.put(f"/api/ports/{port['id']}/enabled", json={"enabled": False}).status_code == 200
    drop(tmp_path, "wait.json", json.dumps(ORDER))
    assert admin.post(f"/api/ports/{port['id']}/poll").status_code == 409
    time.sleep(3)
    assert (tmp_path / "wait.json").exists()


@pytest.mark.parametrize("config, message", [
    ({"monitor_dir": "relative/path"}, "full path"),
    ({"monitor_dir": "/tmp/x", "work_dir": "/tmp/x"}, "must all be different"),
    ({"monitor_dir": "/tmp/x", "interval_seconds": 1}, "every 2 seconds"),
])
def test_invalid_file_ports(admin, config, message):
    partner = make_partner(admin, f"Port Invalid {abs(hash(message)) % 1000}")
    r = admin.post("/api/ports", json={"name": "Bad port", "type": "file", "target_type": "partner",
                                       "target_id": partner["code"], "config": config})
    assert r.status_code == 422 and message in r.json()["detail"]


def test_only_admins_manage_ports(admin, base_url, tmp_path):
    admin.post("/api/users", json={"username": "port-writer", "password": "writer-pass", "can_write": True})
    with login(base_url, "port-writer", "writer-pass") as writer:
        r = writer.post("/api/ports", json={"name": "Sneaky", "type": "file", "target_type": "partner",
                                            "target_id": "x", "config": {"monitor_dir": str(tmp_path)}})
        assert r.status_code == 403
        assert writer.get("/api/ports").status_code == 200


def test_ports_overview_lists_everything(admin):
    rows = admin.get("/api/ports-overview").json()
    kinds = {r["managed"] for r in rows}
    assert {"built-in", "port"} <= kinds
    assert any(r["name"] == "Primary port" and r["state"] == "running" for r in rows)
