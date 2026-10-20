"""Extra HTTP listener ports: own port number, access mode (selected partners) and IP access rules."""
import json
import socket

import httpx
import pytest

from .conftest import wait_for
from .test_partner_flow import ORDER, make_partner

pytestmark = pytest.mark.integration


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http_port(admin, name, **cfg):
    r = admin.post("/api/ports", json={"name": name, "type": "http", "config": {"bind": "127.0.0.1", **cfg}})
    assert r.status_code == 201, r.text
    return r.json()


def post(port, partner):
    return httpx.post(f"http://127.0.0.1:{port}/inbound/{partner['code']}", content=json.dumps(ORDER).encode(),
                      headers={"X-API-Key": partner["api_key"], "Content-Type": "application/json"}, timeout=60)


@pytest.fixture(scope="module")
def partners(admin):
    return make_partner(admin, "HTTP Port Allowed"), make_partner(admin, "HTTP Port Other")


def test_port_serves_selected_partners_only(admin, partners):
    allowed, other = partners
    n = free_port()
    p = http_port(admin, "Partner gateway", port=n, access_mode="selected", partners=[allowed["code"]])
    assert p["status"]["state"] == "running", p
    assert httpx.get(f"http://127.0.0.1:{n}/health").json()["status"] == "up"
    r = post(n, allowed)
    assert r.status_code == 200 and r.json()["order_id"] == "A-100"
    assert post(n, other).status_code == 404
    # only partner endpoints are served - not the admin API
    assert httpx.get(f"http://127.0.0.1:{n}/api/partners").status_code == 404
    stats = wait_for(lambda: next(x for x in admin.get("/api/ports").json() if x["id"] == p["id"])["status"],
                     lambda s: s.get("stats", {}).get("requests", 0) >= 4)
    assert stats["stats"]["rejected"] == 1


def test_ip_access_rules(admin, partners):
    allowed, _ = partners
    n1, n2 = free_port(), free_port()
    http_port(admin, "Deny local", port=n1, ip_mode="deny", ip_list=["127.0.0.0/8"])
    http_port(admin, "Allow local", port=n2, ip_mode="allow", ip_list=["127.0.0.1"])
    assert post(n1, allowed).status_code == 403
    assert post(n2, allowed).status_code == 200


def test_disable_and_delete_close_the_port(admin):
    n = free_port()
    p = http_port(admin, "Short lived", port=n)
    assert httpx.get(f"http://127.0.0.1:{n}/health").status_code == 200
    admin.put(f"/api/ports/{p['id']}/enabled", json={"enabled": False})
    with pytest.raises(httpx.ConnectError):
        httpx.get(f"http://127.0.0.1:{n}/health", timeout=3)
    admin.delete(f"/api/ports/{p['id']}")


def test_port_in_use_is_reported(admin):
    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        n = busy.getsockname()[1]
        p = http_port(admin, "Busy port", port=n)
        assert p["status"]["state"] == "error" and "in use" in p["status"]["detail"]
    admin.delete(f"/api/ports/{p['id']}")


@pytest.mark.parametrize("cfg, message", [
    ({"port": 70000}, "between 1 and 65535"),
    ({"port": 9999, "ip_mode": "allow", "ip_list": ["not-an-ip"]}, "isn't an IP"),
    ({"port": 9999, "protocol": "https"}, "certificate file"),
    ({"port": 9999, "access_mode": "selected"}, "Choose the partners"),
])
def test_invalid_http_ports(admin, cfg, message):
    r = admin.post("/api/ports", json={"name": "Bad http", "type": "http", "config": cfg})
    assert r.status_code == 422 and message in r.json()["detail"]
