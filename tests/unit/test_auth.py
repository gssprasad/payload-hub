"""Password hashing and which permission each API call needs."""
import pytest

from app import auth


def test_password_hash_round_trip():
    stored = auth.hash_password("s3cret!")
    assert stored.startswith("pbkdf2_sha256$")
    assert auth.verify_password("s3cret!", stored)
    assert not auth.verify_password("wrong", stored)


def test_hashes_are_salted():
    assert auth.hash_password("same") != auth.hash_password("same")


@pytest.mark.parametrize("stored", ["", "garbage", "a$b$c", None])
def test_malformed_hash_never_verifies(stored):
    assert auth.verify_password("x", stored) is False


@pytest.mark.parametrize("method, path, needed", [
    ("GET", "/api/partners", "read"),
    ("POST", "/api/partners", "write"),
    ("PUT", "/api/partners/acme/reply", "write"),
    ("DELETE", "/api/partners/acme", "delete"),
    ("GET", "/api/users", "admin"),
    ("POST", "/api/system/reset", "admin"),
    ("GET", "/api/connections/x/secret", "admin"),
    ("PUT", "/api/ai/providers/anthropic", "admin"),
    ("GET", "/api/ai/providers", "read"),
    ("POST", "/api/auth/logout", "signed_in"),
    # read-only POSTs: previews and tests
    ("POST", "/api/db/builtin/build-sql", "read"),
    ("POST", "/api/db/erp/ddl", "read"),
    ("POST", "/api/connections/x/test", "read"),
    ("POST", "/api/workflows/pdf-preview", "read"),
    ("POST", "/api/workflows/wf/run", "write"),
])
def test_required_permission(method, path, needed):
    assert auth.required_permission(method, path) == needed


def test_allowed():
    reader = {"can_read": True, "can_write": False, "can_delete": False, "is_admin": False}
    assert auth.allowed(reader, "read") and auth.allowed(reader, "signed_in")
    assert not auth.allowed(reader, "write") and not auth.allowed(reader, "admin")
