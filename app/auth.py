"""Users, password hashing, sessions and per-request permission checks.

Permissions: read (GET), write (create / edit / send / run), delete, and admin (manage users).
Partner HTTP endpoints (/inbound/...) use partner API keys instead and are not affected.
"""
import hashlib
import hmac
import os
import re
import secrets
from datetime import datetime, timezone

from . import db

ITERATIONS = 200_000
SESSION_HOURS = 12
IDLE_MINUTES = int(os.getenv("PAYLOAD_HUB_IDLE_MINUTES", "60"))  # signed out after this long without activity
MAX_FAILURES = 5  # wrong passwords in a row before an account is locked
MAX_FAILURES_PER_ADDRESS = 20  # failed sign-ins from one address before it is refused for a while
LOCK_MINUTES = 15
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
    re.compile(r"^/api/alerts/channels/[^/]+/test$"),
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


class LoginBlocked(Exception):
    pass


async def login(username: str, password: str, address: str | None = None):
    """Check a password. Too many wrong passwords lock the account for a while; too many failed attempts from one
    address are refused (LoginBlocked)."""
    if address and await db.failed_attempts_from(address, LOCK_MINUTES) >= MAX_FAILURES_PER_ADDRESS:
        raise LoginBlocked(f"Too many failed sign-ins from your address - try again in {LOCK_MINUTES} minutes.")
    user = await db.get_user(username, with_hash=True)
    if user and user.get("locked_until") and user["locked_until"] > datetime.now(timezone.utc):
        await db.record_login_attempt(address, username, False)
        raise LoginBlocked(f"This account is locked after {MAX_FAILURES} wrong passwords - try again after "
                           f"{user['locked_until'].astimezone():%H:%M}, or ask an administrator to unlock it.")
    if not user or not user["active"] or user.get("auth_source") == "sso" \
            or not verify_password(password, user["password_hash"]):
        await db.record_login_attempt(address, username, False)
        if user:
            await db.register_failed_login(user["username"], MAX_FAILURES, LOCK_MINUTES)
        return None
    await db.record_login_attempt(address, username, True)
    await db.clear_failed_logins(user["username"])
    return await start_session(user)


async def start_session(user: dict):
    token = secrets.token_urlsafe(32)
    await db.create_session(token_hash(token), user["username"], SESSION_HOURS)
    await db.touch_login(user["username"])
    user = {k: v for k, v in user.items() if k != "password_hash"}
    return token, user


async def sso_login(email: str):
    """A user signed in through the company identity provider (verified by the web console)."""
    email = email.strip().lower()
    user = await db.get_user(email)
    if user is None:
        if os.getenv("PAYLOAD_HUB_SSO_AUTO_CREATE", "false").lower() not in ("1", "true", "yes"):
            return None
        await db.create_user(email, "!sso", True, False, False, False, auth_source="sso")
        user = await db.get_user(email)
    if not user["active"]:
        return None
    return await start_session(user)


async def user_for_token(token: str | None):
    if not token:
        return None
    return await db.get_session_user(token_hash(token), IDLE_MINUTES)


def required_permission(method: str, path: str) -> str:
    if path.startswith(("/api/users", "/api/system", "/api/secrets", "/api/audit", "/api/backups",
                        "/api/alerts/channels")) and not (method == "GET" and path == "/api/system/schema"):
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
