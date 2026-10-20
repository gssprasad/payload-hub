"""Smoke test a running Payload Hub: sign in, create a partner, send it a message, take and verify a backup.

    python scripts/smoke_test.py http://localhost:8000 <admin password> [http://localhost:8501]

Used by CI against the Docker Compose stack; also handy after a deployment. Exits non-zero on failure.
"""
import sys
import time
import uuid

import httpx


def main(api: str, password: str, console: str | None = None) -> int:
    c = httpx.Client(base_url=api, timeout=60)
    for _ in range(60):  # wait for the stack
        try:
            if c.get("/health").status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(3)
    r = c.post("/api/auth/login", json={"username": "Administrator", "password": password})
    r.raise_for_status()
    c.headers["Authorization"] = "Bearer " + r.json()["token"]
    schema = c.get("/api/system/schema").json()
    assert not schema["pending"], schema
    name = f"Smoke {uuid.uuid4().hex[:6]}"
    p = c.post("/api/partners", json={"name": name, "input_format": "json", "output_format": "xml",
                                      "expression": "{order_id: order.id}"}).json()
    key = c.get(f"/api/partners/{p['code']}/api-key").json()["api_key"]
    for _ in range(20):
        r = httpx.post(f"{api}/inbound/{p['code']}", json={"order": {"id": "S-1"}}, headers={"X-API-Key": key}, timeout=60)
        if r.status_code == 200:
            break
        time.sleep(3)
    assert r.status_code == 200 and "<order_id>S-1</order_id>" in r.text, (r.status_code, r.text[:300])
    backup = c.post("/api/backups").json()
    assert c.post(f"/api/backups/{backup['file']}/verify").json()["ok"], backup
    c.delete(f"/api/partners/{p['code']}")
    if console:
        assert httpx.get(f"{console}/_stcore/health", timeout=10).text == "ok"
    print(f"smoke test passed: schema {schema['current']}, partner call OK, backup {backup['file']} verified")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    sys.exit(main(*sys.argv[1:4]))
