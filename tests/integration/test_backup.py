"""Backups: create, verify, retention, download, and a real restore into another database."""
import shutil

import psycopg
import pytest
from psycopg import sql

from app import backup
from tests.conftest import TEST_DATABASE_URL

pytestmark = [pytest.mark.integration,
              pytest.mark.skipif(not shutil.which("pg_dump"), reason="PostgreSQL client tools not installed")]


def test_backup_verify_and_restore(admin):
    m = admin.post("/api/backups").json()
    assert m["file"].endswith(".dump") and m["size"] > 0 and m["schema_version"] == "0002_alerts"
    assert any(b["file"] == m["file"] for b in admin.get("/api/backups").json()["backups"])
    v = admin.post(f"/api/backups/{m['file']}/verify").json()
    assert v["ok"], v
    r = admin.get(f"/api/backups/{m['file']}/download")
    assert r.status_code == 200 and len(r.content) == m["size"]

    target = TEST_DATABASE_URL.rsplit("/", 1)[0] + "/payloadhub_test_restore"
    admin_url = TEST_DATABASE_URL.rsplit("/", 1)[0] + "/postgres"
    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP DATABASE IF EXISTS payloadhub_test_restore WITH (FORCE)"))
    try:
        assert "restored" in backup.restore(m["file"], into=target, create_db=True)
        with psycopg.connect(TEST_DATABASE_URL) as a, psycopg.connect(target) as b:
            for table in ("partners", "connections", "workflows", "secrets", "alembic_version"):
                q = f"SELECT count(*) FROM {table}"
                assert a.execute(q).fetchone() == b.execute(q).fetchone(), table
    finally:
        with psycopg.connect(admin_url, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS payloadhub_test_restore WITH (FORCE)"))


def test_damaged_backup_is_detected(admin):
    m = admin.post("/api/backups").json()
    path = backup.path_of(m["file"])
    with open(path, "r+b") as f:
        f.seek(100)
        f.write(b"garbage")
    v = admin.post(f"/api/backups/{m['file']}/verify").json()
    assert not v["ok"] and "checksum" in v["detail"]
    with pytest.raises(backup.BackupError, match="checksum"):
        backup.restore(m["file"], into="postgresql://localhost/never_used")
    admin.delete(f"/api/backups/{m['file']}")


def test_retention_keeps_the_newest(admin, monkeypatch):
    import time

    monkeypatch.setenv("PAYLOAD_HUB_BACKUP_KEEP", "2")
    made = []
    for _ in range(3):
        made.append(admin.post("/api/backups").json()["file"])
        time.sleep(1.1)  # file names are per second
    names = [b["file"] for b in admin.get("/api/backups").json()["backups"]]
    assert names == list(reversed(made[1:]))


def test_scheduled_backup_task(admin):
    t = admin.post("/api/schedules", json={"name": "Nightly backup", "schedule": {"kind": "cron", "cron": "0 2 * * *"},
                                           "action": {"type": "backup"}}).json()
    assert t["action_text"] == "💾 back up the database"
    r = admin.post(f"/api/schedules/{t['id']}/run").json()
    assert r["status"] == "OK" and r["ref"].endswith(".dump")


def test_restore_refuses_a_different_master_key(admin, monkeypatch):
    m = admin.post("/api/backups").json()
    from app import vault

    monkeypatch.setattr(vault, "key_info", lambda: {"fingerprint": "0" * 16})
    with pytest.raises(backup.BackupError, match="different master key"):
        backup.restore(m["file"], into="postgresql://localhost/never_used")
