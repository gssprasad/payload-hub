"""Schema migrations: the app migrates at start-up; old databases are brought up to date; downgrades work."""
import psycopg
import pytest
from psycopg import sql

from app import db, migrate
from tests.conftest import TEST_DATABASE_URL

pytestmark = pytest.mark.integration


def _admin_url():
    return TEST_DATABASE_URL.rsplit("/", 1)[0] + "/postgres"


@pytest.fixture
def scratch_db(base_url):
    """An extra empty database; migrate's functions are pointed at it for the test."""
    name = "payloadhub_test_migrations"
    with psycopg.connect(_admin_url(), autocommit=True) as conn:
        conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    url = TEST_DATABASE_URL.rsplit("/", 1)[0] + f"/{name}"
    original, db.DATABASE_URL = db.DATABASE_URL, url
    try:
        yield url
    finally:
        db.DATABASE_URL = original
        with psycopg.connect(_admin_url(), autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


def tables(url):
    with psycopg.connect(url) as conn:
        return {r[0] for r in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")}


def test_running_app_is_on_the_latest_version(admin):
    s = admin.get("/api/system/schema").json()
    assert s["current"] == s["head"] and s["pending"] == []


def test_database_from_an_older_version_is_upgraded(scratch_db):
    from importlib import import_module

    baseline = import_module("migrations.versions.0001_baseline")
    with psycopg.connect(scratch_db, autocommit=True) as conn:  # what an older Payload Hub created at start-up
        conn.execute(baseline.SCHEMA)
        conn.execute("INSERT INTO connections (id, name, type) VALUES ('kept', 'Kept', 'http')")
    pending = migrate.pending()
    assert migrate.current() is None and pending[:2] == ["0001_baseline", "0002_alerts"] and pending[-1] == migrate.head()
    migrate.upgrade()
    assert migrate.current() == migrate.head()
    assert {"alert_rules", "alert_channels", "alert_events"} <= tables(scratch_db)
    with psycopg.connect(scratch_db) as conn:
        assert conn.execute("SELECT name FROM connections").fetchall() == [("Kept",)]


def test_downgrade_and_upgrade_again(scratch_db):
    migrate.upgrade()
    migrate.downgrade("0001_baseline")
    assert migrate.current() == "0001_baseline" and "alert_rules" not in tables(scratch_db)
    migrate.upgrade()
    assert "alert_rules" in tables(scratch_db)


def test_without_auto_migration_the_app_refuses_an_old_schema(scratch_db, monkeypatch):
    monkeypatch.setenv("PAYLOAD_HUB_AUTO_MIGRATE", "false")
    with pytest.raises(migrate.MigrationError, match="python -m app.migrate upgrade"):
        migrate.on_startup()
    migrate.upgrade()
    migrate.on_startup()  # up to date: starts fine
