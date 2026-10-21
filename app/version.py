"""Which version of Payload Hub this is: PAYLOAD_HUB_VERSION (set in release images), else the git tag/commit."""
import os
import subprocess
from pathlib import Path


def _from_git() -> str:
    try:
        out = subprocess.run(["git", "describe", "--tags", "--always", "--dirty"], capture_output=True, text=True,
                             timeout=3, cwd=Path(__file__).resolve().parent.parent)
        return out.stdout.strip() or "dev"
    except Exception:  # noqa: BLE001 - no git (e.g. in an image built without it)
        return "dev"


VERSION = os.getenv("PAYLOAD_HUB_VERSION") or _from_git()
COMMIT = os.getenv("PAYLOAD_HUB_COMMIT", "")
