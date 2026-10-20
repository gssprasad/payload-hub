"""Secrets: encryption of stored credentials, the named secret store (like webMethods' Passman), and the master key.

Every credential kept in the database is encrypted with the master key: connection passwords, AI provider keys
and the named secrets. Connections can use a named secret ("alias") instead of their own password, so one
password change reaches every connection that uses it.

The master key comes from PAYLOAD_HUB_SECRET_KEY, or is generated once into data/secret.key (git-ignored,
readable only by you). It can be rotated from the Secrets page (file-based keys): every stored credential is
re-encrypted in one database transaction, then the new key replaces the old one (a timestamped backup of the
old key is kept next to it). If the key is lost, stored credentials can't be decrypted and must be entered again.
"""
import hashlib
import os
from datetime import datetime
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

KEY_FILE = Path(__file__).resolve().parent.parent / "data" / "secret.key"
PREFIX = "enc:v1:"  # marks an encrypted value in a column that used to hold plain text
KINDS = ("password", "api_key", "token", "certificate", "other")
_fernet: Fernet | None = None


class VaultError(ValueError):
    pass


def _key() -> bytes:
    key = os.getenv("PAYLOAD_HUB_SECRET_KEY")
    if key:
        return key.encode()
    if not KEY_FILE.exists():
        KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
        KEY_FILE.write_bytes(Fernet.generate_key())
        KEY_FILE.chmod(0o600)
    return KEY_FILE.read_text().strip().encode()


def _get() -> Fernet:
    global _fernet
    if _fernet is None:
        _fernet = Fernet(_key())
    return _fernet


def encrypt(plain: str) -> str:
    return _get().encrypt(plain.encode()).decode()


def decrypt(token: str | None) -> str | None:
    if not token:
        return None
    try:
        return _get().decrypt(token.encode()).decode()
    except InvalidToken:
        return None  # encrypted with a different key - the user has to paste it again


def seal(plain: str | None) -> str | None:
    """Encrypt a value for a column that may still hold old plain-text values."""
    return PREFIX + encrypt(plain) if plain else plain


def unseal(value: str | None) -> str | None:
    """Plain text of a sealed value (old plain-text values are returned as they are)."""
    if value and value.startswith(PREFIX):
        return decrypt(value[len(PREFIX):])
    return value


def key_info() -> dict:
    from_env = bool(os.getenv("PAYLOAD_HUB_SECRET_KEY"))
    return {"source": "environment variable PAYLOAD_HUB_SECRET_KEY" if from_env else f"file {KEY_FILE}",
            "rotatable": not from_env,
            "fingerprint": hashlib.sha256(_key()).hexdigest()[:16],
            "created": None if from_env or not KEY_FILE.exists()
            else datetime.fromtimestamp(KEY_FILE.stat().st_mtime).isoformat(timespec="seconds")}


# ---------- named secrets ----------

async def secret_value(alias: str) -> str | None:
    from . import db

    row = await db.get_secret(alias, with_value=True)
    return decrypt(row["value_enc"]) if row else None


async def migrate():
    """Encrypt connection passwords that are still stored as plain text (from older versions)."""
    from . import db

    async with db.pool.connection() as conn, conn.transaction():
        cur = await conn.execute("SELECT id, secret FROM connections WHERE secret IS NOT NULL AND secret <> '' "
                                 "AND secret NOT LIKE 'enc:v1:%%'")
        rows = await cur.fetchall()
        for r in rows:
            await conn.execute("UPDATE connections SET secret = %s WHERE id = %s", (seal(r["secret"]), r["id"]))
    if rows:
        await db.audit(None, "encrypted", "connection", ", ".join(r["id"] for r in rows),
                       "plain-text passwords encrypted with the master key")


async def check() -> dict:
    """How many stored credentials there are, and which can't be decrypted with the current key."""
    from . import db

    unreadable, total = [], 0
    async with db.pool.connection() as conn:
        for sql, label in (("SELECT id AS name, secret AS v FROM connections WHERE secret LIKE 'enc:v1:%%'", "connection"),
                           ("SELECT alias AS name, value_enc AS v FROM secrets", "secret"),
                           ("SELECT provider AS name, api_key_enc AS v FROM ai_providers WHERE api_key_enc IS NOT NULL",
                            "AI key")):
            for r in await (await conn.execute(sql)).fetchall():
                total += 1
                value = r["v"][len(PREFIX):] if r["v"].startswith(PREFIX) else r["v"]
                try:
                    _get().decrypt(value.encode())
                except InvalidToken:
                    unreadable.append(f"{label} {r['name']}")
    return {"total": total, "unreadable": unreadable}


async def rotate_master_key(username: str) -> dict:
    """New master key; re-encrypt every stored credential in one transaction, then swap the key file."""
    from . import db

    global _fernet
    if os.getenv("PAYLOAD_HUB_SECRET_KEY"):
        raise VaultError("The master key comes from PAYLOAD_HUB_SECRET_KEY - rotate it where that variable is set.")
    status = await check()
    if status["unreadable"]:
        raise VaultError("Some credentials can't be decrypted with the current key - fix or re-enter them first: "
                         + ", ".join(status["unreadable"]))
    old, new_key = _get(), Fernet.generate_key()
    new = Fernet(new_key)
    pending = KEY_FILE.with_name("secret.key.new")
    pending.write_bytes(new_key)
    pending.chmod(0o600)

    def again(token: str) -> str:
        return new.encrypt(old.decrypt(token.encode())).decode()

    count = 0
    async with db.pool.connection() as conn, conn.transaction():
        for r in await (await conn.execute("SELECT id, secret FROM connections WHERE secret LIKE 'enc:v1:%%'")).fetchall():
            await conn.execute("UPDATE connections SET secret = %s WHERE id = %s",
                               (PREFIX + again(r["secret"][len(PREFIX):]), r["id"]))
            count += 1
        for r in await (await conn.execute("SELECT alias, value_enc FROM secrets")).fetchall():
            await conn.execute("UPDATE secrets SET value_enc = %s WHERE alias = %s", (again(r["value_enc"]), r["alias"]))
            count += 1
        for r in await (await conn.execute(
                "SELECT provider, api_key_enc FROM ai_providers WHERE api_key_enc IS NOT NULL")).fetchall():
            await conn.execute("UPDATE ai_providers SET api_key_enc = %s WHERE provider = %s",
                               (again(r["api_key_enc"]), r["provider"]))
            count += 1
    backup = KEY_FILE.with_name(f"secret.key.{datetime.now():%Y%m%dT%H%M%S}.bak")
    os.replace(KEY_FILE, backup)
    os.replace(pending, KEY_FILE)
    _fernet = None
    await db.audit(username, "rotated", "master key", None, f"{count} credential(s) re-encrypted; old key kept as "
                                                              f"{backup.name}")
    return {"reencrypted": count, "fingerprint": key_info()["fingerprint"], "backup": backup.name}
