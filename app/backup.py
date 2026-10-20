"""Database backups and restore.

A backup is a pg_dump of the Payload Hub database (custom format, compressed) plus a manifest: when it was
taken, its size and SHA-256, the schema version and the master-key fingerprint. Backups go to
PAYLOAD_HUB_BACKUP_DIR (default data/backups); the newest PAYLOAD_HUB_BACKUP_KEEP (default 14) are kept.

What a backup does NOT contain: the master key (data/secret.key or PAYLOAD_HUB_SECRET_KEY) - keep that
separately, because stored credentials can't be decrypted without it; data in other databases you connected;
files in polled folders.

Restore is a command, not a button - stop the app first:

    python -m app.backup list
    python -m app.backup verify  payloadhub_20261020T020000.dump
    python -m app.backup restore payloadhub_20261020T020000.dump --yes
    python -m app.backup restore payloadhub_20261020T020000.dump --into postgresql://localhost/restore_drill --yes
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class BackupError(RuntimeError):
    pass


def backup_dir() -> Path:
    d = Path(os.getenv("PAYLOAD_HUB_BACKUP_DIR") or ROOT / "data" / "backups").expanduser()
    d.mkdir(parents=True, exist_ok=True)
    return d


def keep() -> int:
    return max(1, int(os.getenv("PAYLOAD_HUB_BACKUP_KEEP", "14")))


def _tool(name: str) -> str:
    path = os.getenv(f"PAYLOAD_HUB_{name.upper()}") or shutil.which(name)
    if not path:
        raise BackupError(f"{name} isn't installed (it comes with the PostgreSQL client tools).")
    return path


def _libpq_url(url: str | None = None) -> str:
    from .db import DATABASE_URL

    return (url or DATABASE_URL).replace("postgresql+psycopg://", "postgresql://")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe(name: str) -> Path:
    path = backup_dir() / Path(name).name
    if not path.name.endswith(".dump") or not path.exists():
        raise BackupError(f"No backup called '{name}'.")
    return path


def create(note: str = "") -> dict:
    """Take a backup now (blocking - call it in a thread). Returns the manifest."""
    from . import migrate, vault

    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    final = backup_dir() / f"payloadhub_{stamp}.dump"
    partial = final.with_suffix(".partial")
    started = datetime.now(timezone.utc)
    run = subprocess.run([_tool("pg_dump"), "--format=custom", "--compress=6", "--no-password",
                          f"--file={partial}", f"--dbname={_libpq_url()}"], capture_output=True, text=True, timeout=3600)
    if run.returncode != 0:
        partial.unlink(missing_ok=True)
        raise BackupError(f"pg_dump failed: {(run.stderr or run.stdout).strip()[:500]}")
    partial.rename(final)
    manifest = {"file": final.name, "created_at": started.isoformat(timespec="seconds"),
                "seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 1),
                "size": final.stat().st_size, "sha256": _sha256(final), "schema_version": migrate.current(),
                "key_fingerprint": vault.key_info()["fingerprint"],
                "pg_dump": subprocess.run([_tool("pg_dump"), "--version"], capture_output=True, text=True).stdout.strip(),
                "note": note}
    final.with_suffix(".json").write_text(json.dumps(manifest, indent=2))
    prune()
    return manifest


def prune() -> list[str]:
    """Delete the oldest backups beyond the retention count."""
    removed = []
    for old in sorted(backup_dir().glob("payloadhub_*.dump"), reverse=True)[keep():]:
        old.unlink(missing_ok=True)
        old.with_suffix(".json").unlink(missing_ok=True)
        removed.append(old.name)
    return removed


def list_backups() -> list[dict]:
    out = []
    for dump in sorted(backup_dir().glob("payloadhub_*.dump"), reverse=True):
        meta = dump.with_suffix(".json")
        info = json.loads(meta.read_text()) if meta.exists() else {"file": dump.name, "size": dump.stat().st_size}
        out.append(info)
    return out


def verify(name: str) -> dict:
    """Checksum matches the manifest, and pg_restore can read the file's table of contents."""
    path = _safe(name)
    meta = path.with_suffix(".json")
    manifest = json.loads(meta.read_text()) if meta.exists() else {}
    checksum = _sha256(path)
    if manifest.get("sha256") and manifest["sha256"] != checksum:
        return {"ok": False, "detail": "The checksum doesn't match - the file was changed or damaged."}
    toc = subprocess.run([_tool("pg_restore"), "--list", str(path)], capture_output=True, text=True, timeout=600)
    if toc.returncode != 0:
        return {"ok": False, "detail": f"pg_restore can't read it: {toc.stderr.strip()[:300]}"}
    tables = sum(1 for line in toc.stdout.splitlines() if " TABLE DATA " in line)
    return {"ok": True, "detail": f"checksum OK · {tables} tables · readable by pg_restore"}


def delete(name: str):
    path = _safe(name)
    path.unlink()
    path.with_suffix(".json").unlink(missing_ok=True)


def path_of(name: str) -> Path:
    return _safe(name)


def restore(name: str, into: str | None = None, create_db: bool = False, ignore_key: bool = False) -> str:
    """Restore a backup over a database (default: the app's own). The app should be stopped."""
    from . import vault

    path = Path(name) if Path(name).is_file() else _safe(name)
    meta = path.with_suffix(".json")
    manifest = json.loads(meta.read_text()) if meta.exists() else {}
    if manifest.get("sha256") and manifest["sha256"] != _sha256(path):
        raise BackupError("The checksum doesn't match the manifest - not restoring a damaged file.")
    if manifest.get("key_fingerprint") and manifest["key_fingerprint"] != vault.key_info()["fingerprint"] \
            and not ignore_key:
        raise BackupError("This backup was taken with a different master key (fingerprint "
                          f"{manifest['key_fingerprint'][:8]}, current {vault.key_info()['fingerprint'][:8]}). Put the "
                          "matching key in place first, or pass --ignore-key and re-enter the stored credentials.")
    target = _libpq_url(into)
    if create_db:
        from urllib.parse import urlparse

        dbname = urlparse(target).path.lstrip("/")
        admin_url = target.rsplit("/", 1)[0] + "/postgres"
        made = subprocess.run([_tool("psql"), admin_url, "-v", "ON_ERROR_STOP=1", "-c", f'CREATE DATABASE "{dbname}"'],
                              capture_output=True, text=True)
        if made.returncode != 0 and "already exists" not in made.stderr:
            raise BackupError(f"Couldn't create the database: {made.stderr.strip()[:300]}")
    run = subprocess.run([_tool("pg_restore"), "--clean", "--if-exists", "--single-transaction", "--no-password",
                          f"--dbname={target}", str(path)], capture_output=True, text=True, timeout=3600)
    if run.returncode != 0:
        raise BackupError(f"pg_restore failed (nothing was changed): {run.stderr.strip()[:800]}")
    return f"restored {path.name} into {target.rsplit('@', 1)[-1]}"


def main(argv) -> int:
    args = argv[1:]
    if not args or args[0] not in ("list", "create", "verify", "restore"):
        print(__doc__)
        return 1
    try:
        if args[0] == "list":
            for b in list_backups():
                print(f"{b['file']}  {b.get('created_at', '?')}  {b.get('size', 0) / 1e6:.1f} MB  "
                      f"schema {b.get('schema_version', '?')}  key {str(b.get('key_fingerprint', '?'))[:8]}")
        elif args[0] == "create":
            print(json.dumps(create("command line"), indent=2))
        elif args[0] == "verify":
            r = verify(args[1])
            print(("OK: " if r["ok"] else "FAILED: ") + r["detail"])
            return 0 if r["ok"] else 2
        else:
            if "--yes" not in args:
                print("Restoring replaces the database's contents. Stop the app, then run again with --yes.")
                return 1
            into = args[args.index("--into") + 1] if "--into" in args else None
            print(restore(args[1], into, create_db=bool(into), ignore_key="--ignore-key" in args))
            print("Start the app again; it will apply any newer migrations.")
    except BackupError as e:
        print(f"Error: {e}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
