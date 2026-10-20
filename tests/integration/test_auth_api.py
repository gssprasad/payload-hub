"""Sign-in, sessions and per-user permissions."""
import httpx
import pytest

from .conftest import login

pytestmark = pytest.mark.integration


def test_api_needs_sign_in(base_url):
    assert httpx.get(f"{base_url}/api/partners").status_code == 401


def test_wrong_password_is_rejected(base_url):
    r = httpx.post(f"{base_url}/api/auth/login", json={"username": "Administrator", "password": "nope"})
    assert r.status_code == 401


def test_admin_signs_in(admin):
    me = admin.get("/api/auth/me").json()
    assert me["username"] == "Administrator" and me["is_admin"]


def test_read_only_user(admin, base_url):
    r = admin.post("/api/users", json={"username": "reader", "password": "reader-pass"})
    assert r.status_code == 201, r.text
    with login(base_url, "reader", "reader-pass") as reader:
        assert reader.get("/api/partners").status_code == 200
        assert reader.post("/api/partners", json={"name": "Nope Inc"}).status_code == 403
        assert reader.get("/api/users").status_code == 403
        # previews are allowed for readers
        assert reader.post("/api/db/builtin/build-sql", json={"table": "x", "operation": "select_all"}).status_code == 200


def test_sign_out_ends_the_session(base_url):
    client = login(base_url, "Administrator", "test-admin-pass")
    assert client.post("/api/auth/logout").status_code == 204
    assert client.get("/api/partners").status_code == 401


def _login(base_url, user, password, address):
    return httpx.post(f"{base_url}/api/auth/login", json={"username": user, "password": password},
                      headers={"X-Client-Address": address})


def test_account_locks_after_five_wrong_passwords(admin, base_url):
    admin.post("/api/users", json={"username": "lock-me", "password": "right-pass"})
    for _ in range(5):
        assert _login(base_url, "lock-me", "wrong", "10.0.0.5").status_code == 401
    r = _login(base_url, "lock-me", "right-pass", "10.0.0.6")
    assert r.status_code == 429 and "locked" in r.json()["detail"]
    user = next(u for u in admin.get("/api/users").json() if u["username"] == "lock-me")
    assert user["locked_until"]
    assert admin.put("/api/users/lock-me", json={"unlock": True}).status_code == 200
    assert _login(base_url, "lock-me", "right-pass", "10.0.0.6").status_code == 200


def test_too_many_failures_from_one_address(base_url):
    for i in range(20):
        _login(base_url, f"nobody-{i}", "x", "10.9.9.9")
    r = _login(base_url, "Administrator", "test-admin-pass", "10.9.9.9")
    assert r.status_code == 429 and "address" in r.json()["detail"]
    assert _login(base_url, "Administrator", "test-admin-pass", "10.9.9.10").status_code == 200


def test_idle_sessions_expire(base_url):
    import psycopg

    from tests.conftest import TEST_DATABASE_URL

    client = login(base_url, "Administrator", "test-admin-pass")
    assert client.get("/api/auth/me").status_code == 200
    with psycopg.connect(TEST_DATABASE_URL) as conn:
        conn.execute("UPDATE sessions SET last_seen_at = now() - interval '2 hours' "
                     "WHERE token_hash = encode(sha256(%s::bytea), 'hex')",
                     (client.headers["Authorization"][7:].encode(),))
    assert client.get("/api/auth/me").status_code == 401


def test_single_sign_on_exchange(admin, base_url, monkeypatch):
    sso = f"{base_url}/api/auth/sso"
    assert httpx.post(sso, json={"email": "a@example.com"}).status_code == 404  # off until configured
    monkeypatch.setenv("PAYLOAD_HUB_SSO_SECRET", "shared-secret")
    assert httpx.post(sso, json={"email": "a@example.com"}, headers={"X-SSO-Secret": "nope"}).status_code == 401
    ok = {"X-SSO-Secret": "shared-secret"}
    r = httpx.post(sso, json={"email": "new.person@example.com"}, headers=ok)
    assert r.status_code == 403 and "ask an administrator" in r.json()["detail"]
    monkeypatch.setenv("PAYLOAD_HUB_SSO_AUTO_CREATE", "true")
    r = httpx.post(sso, json={"email": "New.Person@example.com"}, headers=ok)
    assert r.status_code == 200, r.text
    user = r.json()["user"]
    assert user["username"] == "new.person@example.com" and user["auth_source"] == "sso"
    assert user["can_read"] and not user["can_write"] and not user["is_admin"]
    me = httpx.get(f"{base_url}/api/auth/me", headers={"Authorization": f"Bearer {r.json()['token']}"})
    assert me.json()["username"] == "new.person@example.com"
    # SSO accounts have no password to guess
    assert _login(base_url, "new.person@example.com", "!sso", "10.1.1.1").status_code == 401
