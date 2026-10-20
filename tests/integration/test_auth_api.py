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
