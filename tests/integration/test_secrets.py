"""Secrets: encrypted storage, named secrets used by connections, master key rotation, audit log."""
import psycopg
import pytest

from tests.conftest import TEST_DATABASE_URL

from .conftest import login

pytestmark = pytest.mark.integration


def raw(sql, *args):
    with psycopg.connect(TEST_DATABASE_URL) as conn:
        return conn.execute(sql, args).fetchall()


def test_connection_passwords_are_encrypted(admin):
    c = admin.post("/api/connections", json={"name": "Secret test MQ", "type": "rabbitmq", "secret": "hunter2",
                                             "config": {"host": "127.0.0.1", "port": 1, "username": "u"}}).json()
    ((stored,),) = raw("SELECT secret FROM connections WHERE id = %s", c["id"])
    assert stored.startswith("enc:v1:") and "hunter2" not in stored
    assert admin.get(f"/api/connections/{c['id']}/secret").json()["secret"] == "hunter2"
    log = admin.get("/api/audit", params={"object_type": "connection"}).json()
    assert any(e["action"] == "revealed password" and e["object_id"] == c["id"] for e in log)


def test_named_secret_used_by_a_connection(admin, base_url):
    r = admin.post("/api/secrets", json={"alias": "test.api.token", "kind": "token", "value": "s3cr3t",
                                         "description": "for the tests"})
    assert r.status_code == 201, r.text
    assert "value_enc" not in r.json() and "value" not in r.json()
    ((stored,),) = raw("SELECT value_enc FROM secrets WHERE alias = 'test.api.token'")
    assert "s3cr3t" not in stored
    c = admin.post("/api/connections", json={"name": "Secret alias API", "type": "http", "secret_alias": "test.api.token",
                                             "config": {"base_url": f"{base_url}/test-receiver", "auth": "bearer"}}).json()
    assert c["secret_alias"] == "test.api.token"
    # named secrets are never revealed
    assert admin.get(f"/api/connections/{c['id']}/secret").json() == {"secret": None, "alias": "test.api.token"}
    listed = {s["alias"]: s for s in admin.get("/api/secrets").json()["secrets"]}
    assert listed["test.api.token"]["used_by"] == [c["id"]]
    # in use -> can't delete; a new value bumps the version
    assert admin.delete("/api/secrets/test.api.token").status_code == 409
    assert admin.put("/api/secrets/test.api.token", json={"alias": "test.api.token", "kind": "token",
                                                          "value": "n3w"}).json()["version"] == 2


def test_unknown_alias_is_rejected(admin):
    r = admin.post("/api/connections", json={"name": "Bad alias", "type": "http", "secret_alias": "nope",
                                             "config": {"base_url": "http://x"}})
    assert r.status_code == 422


def test_only_admins_see_secrets(admin, base_url):
    admin.post("/api/users", json={"username": "secret-writer", "password": "writer-pass", "can_write": True})
    with login(base_url, "secret-writer", "writer-pass") as writer:
        assert writer.get("/api/secrets").status_code == 403
        assert writer.get("/api/audit").status_code == 403


def test_master_key_rotation_keeps_everything_readable(admin):
    before = admin.get("/api/secrets").json()
    assert before["check"]["unreadable"] == [] and before["check"]["total"] >= 2
    r = admin.post("/api/secrets/master-key/rotate")
    # the tests take the key from PAYLOAD_HUB_SECRET_KEY, which can't be rotated from the app
    assert r.status_code == 409 and "PAYLOAD_HUB_SECRET_KEY" in r.json()["detail"]


def test_changes_are_audited(admin):
    admin.post("/api/partners", json={"name": "Audit Test Partner", "input_format": "json", "output_format": "json"})
    log = admin.get("/api/audit").json()
    entry = next(e for e in log if e["object_type"] == "partner" and e["action"] == "created")
    assert entry["username"] == "Administrator" and entry["detail"] == "POST /api/partners"
    assert any(e["object_type"] == "secret" and e["action"] == "created" for e in log)
    assert not any("s3cr3t" in (e["detail"] or "") for e in log)


def test_master_key_rotation_with_a_key_file(admin, tmp_path):
    import os

    from app import vault

    key_file, original_file = tmp_path / "secret.key", vault.KEY_FILE
    key_file.write_text(os.environ.pop("PAYLOAD_HUB_SECRET_KEY"))
    vault.KEY_FILE, vault._fernet = key_file, None
    try:
        old = admin.get("/api/secrets").json()["key"]["fingerprint"]
        mq = next(c for c in admin.get("/api/connections").json() if c["name"] == "Secret test MQ")
        r = admin.post("/api/secrets/master-key/rotate")
        assert r.status_code == 200, r.text
        assert r.json()["reencrypted"] >= 2 and r.json()["fingerprint"] != old
        assert (tmp_path / r.json()["backup"]).exists()
        after = admin.get("/api/secrets").json()
        assert after["check"]["unreadable"] == [] and after["key"]["fingerprint"] == r.json()["fingerprint"]
        assert admin.get(f"/api/connections/{mq['id']}/secret").json()["secret"] == "hunter2"
    finally:
        # the rest of the session continues with whatever key is now in the file
        os.environ["PAYLOAD_HUB_SECRET_KEY"] = key_file.read_text().strip()
        vault.KEY_FILE, vault._fernet = original_file, None


def test_audit_says_what_changed(admin, receiver):
    from .test_partner_flow import make_partner

    p = make_partner(admin, "Audit Diff Partner")
    admin.put(f"/api/partners/{p['code']}/reply", json={"reply_mode": "forward", "reply_connection": receiver,
                                                        "reply_path": "orders"})
    (entry, *_) = admin.get(f"/api/partners/{p['code']}/history").json()
    assert entry["action"] == "changed reply" and entry["username"] == "Administrator"
    assert "reply_mode: transformed → forward" in entry["detail"]
    assert f"reply_connection: — → {receiver}" in entry["detail"]
    assert "api_key" not in entry["detail"]


def test_partner_api_keys_are_hashed_and_encrypted(admin, base_url):
    import httpx

    from .test_partner_flow import ORDER, make_partner

    p = make_partner(admin, "Key Test Partner")
    ((h, enc, hint),) = raw("SELECT api_key_hash, api_key_enc, api_key_hint FROM partners WHERE code = %s", p["code"])
    assert p["api_key"] not in (h, enc) and enc.startswith("enc:v1:") and hint == f"{p['api_key'][:5]}…{p['api_key'][-4:]}"
    assert "api_key_enc" not in admin.get(f"/api/partners/{p['code']}").json()
    # readers see the hint, not the key
    admin.post("/api/users", json={"username": "key-reader", "password": "reader-pass"})
    with login(base_url, "key-reader", "reader-pass") as reader:
        assert reader.get(f"/api/partners/{p['code']}").json()["api_key_hint"] == hint
        assert reader.get(f"/api/partners/{p['code']}/api-key").status_code == 403

    def call(key):
        return httpx.post(f"{base_url}/inbound/{p['code']}", json=ORDER, headers={"X-API-Key": key}, timeout=60)

    assert call(p["api_key"]).status_code == 200
    new = admin.post(f"/api/partners/{p['code']}/api-key").json()["api_key"]
    assert call(p["api_key"]).status_code == 401 and call(new).status_code == 200
    assert any(e["action"] == "revealed api key" for e in admin.get("/api/audit", params={"object_type": "partner"}).json())
