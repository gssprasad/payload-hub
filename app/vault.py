"""Encryption for secrets stored in the database (AI provider API keys).

The key comes from PAYLOAD_HUB_SECRET_KEY, or is generated once into data/secret.key (git-ignored,
readable only by you). If the key is lost, stored secrets can't be decrypted and must be entered again.
"""
import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

KEY_FILE = Path(__file__).resolve().parent.parent / "data" / "secret.key"
_fernet: Fernet | None = None


def _get() -> Fernet:
    global _fernet
    if _fernet is None:
        key = os.getenv("PAYLOAD_HUB_SECRET_KEY")
        if not key:
            if not KEY_FILE.exists():
                KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
                KEY_FILE.write_bytes(Fernet.generate_key())
                KEY_FILE.chmod(0o600)
            key = KEY_FILE.read_text().strip()
        _fernet = Fernet(key.encode() if isinstance(key, str) else key)
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
