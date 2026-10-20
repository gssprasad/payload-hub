"""Users, password hashing, sessions and per-request permission checks.

Permissions: read (GET), write (create / edit / send / run), delete, and admin (manage users).
Partner HTTP endpoints (/inbound/...) use partner API keys instead and are not affected.
"""
import hashlib
import hmac
import os
import re
import secrets

from . import db

ITERATIONS = 200_000
SESSION_HOURS = 12
DEFAULT_ADMIN = "Administrator"

# POST endpoints that only read or test, so a read-only user may call them.
READ_ONLY_POSTS = (
    re.compile(r"^/api/mapping-doc$"),
    re.compile(r"^/api/partners/[^/]+/test$"),
    re.compile(r"^/api/connections/[^/]+/test$"),
    re.compile(r"^/api/data/query$"),  # the statement itself is checked against the user's permissions
    re.compile(r"^/api/connections/[^/]+/query$"),
    re.compile(r"^/api/db/[^/]+/ddl$"),  # SQL preview only
    re.compile(r"^/api/db/[^/]+/build-sql$"),  # SQL preview only
    re.compile(r"^/api/workflows/pdf-preview$"),
    re.compile(r"^/api/db/[^/]+/tables/[^/]+/alter$"),  # checked per change inside (write or delete)
    re.compile(r"^/api/partners/[^/]+/store/preview$"),
)


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, ITERATIONS)
    return f"pbkdf2_sha256${ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, iterations, salt, digest = stored.split("$")
        candidate = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), int(iterations))
        return hmac.compare_digest(candidate.hex(), digest)
    except (ValueError, AttributeError):
        return False


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


async def ensure_admin():
    """Create the Administrator account on first start (password from ADMIN_PASSWORD, default 'manage')."""
    if await db.count_users() == 0:
        await db.create_user(DEFAULT_ADMIN, hash_password(os.getenv("ADMIN_PASSWORD") or "manage"),
                             can_read=True, can_write=True, can_delete=True, is_admin=True)


async def login(username: str, password: str):
    user = await db.get_user(username, with_hash=True)
    if not user or not user["active"] or not verify_password(password, user["password_hash"]):
        return None
    token = secrets.token_urlsafe(32)
    await db.create_session(token_hash(token), user["username"], SESSION_HOURS)
    await db.touch_login(user["username"])
    user.pop("password_hash", None)
    return token, user


async def user_for_token(token: str | None):
    if not token:
        return None
    return await db.get_session_user(token_hash(token))


def required_permission(method: str, path: str) -> str:
    if path.startswith(("/api/users", "/api/system", "/api/secrets", "/api/audit")):
        return "admin"
    if path.startswith("/api/connections/") and path.endswith("/secret"):
        return "admin"  # revealing a stored connection password
    if path.startswith("/api/ai/") and method in ("PUT", "POST", "DELETE"):
        return "admin"  # pasting, testing and removing AI keys, and choosing the default model
    if path.startswith("/api/ports") and method in ("POST", "PUT", "DELETE") and not path.endswith("/poll"):
        return "admin"  # ports read folders on the server
    if path.startswith("/api/auth/"):
        return "signed_in"
    if method in ("GET", "HEAD", "OPTIONS"):
        return "read"
    if method == "DELETE":
        return "delete"
    if method == "POST" and any(p.match(path) for p in READ_ONLY_POSTS):
        return "read"
    return "write"


def allowed(user: dict, permission: str) -> bool:
    return {
        "signed_in": True,
        "read": user["can_read"],
        "write": user["can_write"],
        "delete": user["can_delete"],
        "admin": user["is_admin"],
    }[permission]
