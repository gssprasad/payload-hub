"""Two Payload Hub instances on one database and Kafka: answers reach the right caller, pollers run once,
and when an instance dies the other takes its work over."""
import json
import os
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest

from .conftest import ADMIN, _wait_for_pipeline, login, wait_for
from .test_partner_flow import ORDER, make_partner
from .test_ports import drop

pytestmark = pytest.mark.integration


@pytest.fixture
def second_instance(base_url, tmp_path):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    log = open(tmp_path / "instance-b.log", "w")
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port),
                             "--log-level", "warning"], env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT,
                            cwd=os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{url}/health", timeout=2).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        if proc.poll() is not None:
            pytest.fail(open(tmp_path / "instance-b.log").read()[-3000:])
        time.sleep(0.5)
    else:
        proc.kill()
        pytest.fail("the second instance didn't start")
    yield url, proc
    if proc.poll() is None:
        proc.terminate()
        proc.wait(timeout=30)


def test_answers_reach_the_caller_on_either_instance(admin, base_url, second_instance):
    b_url, _ = second_instance
    p = make_partner(admin, "Cluster Partner")
    for url in [base_url, b_url] * 6:
        r = httpx.post(f"{url}/inbound/{p['code']}", content=json.dumps(ORDER).encode(), timeout=60,
                       headers={"X-API-Key": p["api_key"], "Content-Type": "application/json"})
        assert r.status_code == 200 and r.json()["order_id"] == "A-100", (url, r.status_code, r.text[:200])


def test_poller_runs_once_and_fails_over(admin, base_url, second_instance, tmp_path):
    b_url, b_proc = second_instance
    p = make_partner(admin, "Cluster Files")
    folder = tmp_path / "inbox"
    folder.mkdir()
    with login(b_url, *ADMIN) as b:
        port = b.post("/api/ports", json={"name": "Cluster folder", "type": "file", "target_type": "partner",
                                          "target_id": p["code"], "config": {"monitor_dir": str(folder),
                                                                             "interval_seconds": 2,
                                                                             "min_age_seconds": 1}}).json()
        b_view = wait_for(lambda: next(x for x in b.get("/api/ports").json() if x["id"] == port["id"])["status"],
                          lambda st: st["state"] == "running", timeout=20)
        assert b_view["state"] == "running"  # instance B created it and owns it
    a_view = wait_for(lambda: next(x for x in admin.get("/api/ports").json() if x["id"] == port["id"])["status"],
                      lambda s: s["state"] == "standby", timeout=20)
    assert a_view["state"] == "standby"
    for i in range(6):
        drop(folder, f"order{i}.json", json.dumps(ORDER))
    events = wait_for(lambda: admin.get(f"/api/ports/{port['id']}/events").json(), lambda e: len(e) >= 6, timeout=40)
    time.sleep(5)  # give a second (wrong) poller time to show up
    events = admin.get(f"/api/ports/{port['id']}/events").json()
    assert sorted(e["item"] for e in events) == [f"order{i}.json" for i in range(6)]  # each file exactly once

    b_proc.send_signal(signal.SIGKILL)  # instance B dies without cleaning up
    b_proc.wait(timeout=10)
    after = wait_for(lambda: next(x for x in admin.get("/api/ports").json() if x["id"] == port["id"])["status"],
                     lambda s: s["state"] == "running", timeout=40)
    assert after["state"] == "running", after
    drop(folder, "after-failover.json", json.dumps(ORDER))
    # the file is picked up at once, but its Kafka message waits until Kafka hands the dead instance's partitions
    # over (its session times out) - so allow for that delay; nothing is lost
    events = wait_for(lambda: admin.get(f"/api/ports/{port['id']}/events").json(),
                      lambda e: any(x["item"] == "after-failover.json" for x in e), timeout=120)
    assert next(x for x in events if x["item"] == "after-failover.json")["status"] in ("OK", "ACCEPTED")
    # Kafka hands the dead instance's partitions over after its session times out - let that finish
    _wait_for_pipeline(base_url)
