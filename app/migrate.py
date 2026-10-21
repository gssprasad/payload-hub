"""Database migrations (Alembic): run at start-up or by hand.

    python -m app.migrate status        current and latest schema version, and what's pending
    python -m app.migrate upgrade       apply pending migrations
    python -m app.migrate downgrade REV go back to revision REV (e.g. 0002_alerts); the baseline can't be undone
    python -m app.migrate history       every migration
    python -m app.migrate new "message" create migrations/versions/<next>_<message>.py

Set PAYLOAD_HUB_AUTO_MIGRATE=false to stop the app migrating at start-up: it then refuses to start until
`python -m app.migrate upgrade` has been run (useful when a DBA applies changes).
"""
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCK_ID = 727274  # pg_advisory_lock key shared by every instance
log = logging.getLogger("migrate")


class MigrationError(RuntimeError):
    pass


def sqlalchemy_url() -> str:
    from sqlalchemy.engine import make_url

    from .db import DATABASE_URL

    return make_url(DATABASE_URL).set(drivername="postgresql+psycopg").render_as_string(hide_password=False)


def _config():
    from alembic.config import Config

    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    return cfg


def head() -> str:
    from alembic.script import ScriptDirectory

    return ScriptDirectory.from_config(_config()).get_current_head()


def current() -> str | None:
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import create_engine

    engine = create_engine(sqlalchemy_url())
    try:
        with engine.connect() as conn:
            return MigrationContext.configure(conn).get_current_revision()
    finally:
        engine.dispose()


def pending() -> list[str]:
    from alembic.script import ScriptDirectory

    script, now = ScriptDirectory.from_config(_config()), current()
    return [r.revision for r in reversed(list(script.walk_revisions(now or "base", "heads"))) if r.revision != now]


def status() -> dict:
    return {"current": current(), "head": head(), "pending": pending()}


def upgrade(revision: str = "head"):
    from alembic import command

    command.upgrade(_config(), revision)


def downgrade(revision: str):
    from alembic import command

    command.downgrade(_config(), revision)


def _newer_than_me() -> str | None:
    """The database's revision, if it was migrated by a newer release than this one (e.g. after a rollback)."""
    from alembic.script import ScriptDirectory

    now = current()
    if now is None:
        return None
    try:
        ScriptDirectory.from_config(_config()).get_revision(now)
        return None
    except Exception:  # noqa: BLE001 - alembic: unknown revision
        return now


def on_startup():
    """Bring the schema up to date (or check it is, when auto-migration is off). Runs before the app starts.

    Rolling back to the previous release leaves the database on the newer schema. That's allowed (migrations are
    written to be backward compatible for one release - add columns/tables, don't drop or rename in the same
    release), so an older version starts with a warning instead of refusing; set PAYLOAD_HUB_ALLOW_NEWER_SCHEMA=false
    to refuse instead."""
    if newer := _newer_than_me():
        if os.getenv("PAYLOAD_HUB_ALLOW_NEWER_SCHEMA", "true").lower() in ("0", "false", "no"):
            raise MigrationError(f"The database is on {newer}, newer than this release ({head()}).")
        log.warning("the database is on %s, newer than this release (%s) - running anyway (rollback)", newer, head())
        return
    if os.getenv("PAYLOAD_HUB_AUTO_MIGRATE", "true").lower() in ("0", "false", "no"):
        todo = pending()
        if todo:
            raise MigrationError(f"The database needs migrations {', '.join(todo)} - run `python -m app.migrate "
                                 "upgrade` (PAYLOAD_HUB_AUTO_MIGRATE is off).")
        return
    before = current()
    upgrade()
    after = current()
    if before != after:
        log.info("database migrated %s -> %s", before or "empty", after)


def main(argv):
    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    for name in ("migrate", "alembic.runtime.migration"):
        logging.getLogger(name).setLevel(logging.INFO)
    logging.getLogger("alembic.runtime.migration").addFilter(
        lambda r: not r.getMessage().startswith(("Context impl", "Will assume")))
    cmd = argv[1] if len(argv) > 1 else "status"
    if cmd == "status":
        s = status()
        print(f"current: {s['current'] or '(none)'}\nlatest:  {s['head']}\npending: {', '.join(s['pending']) or 'none'}")
    elif cmd == "upgrade":
        upgrade(argv[2] if len(argv) > 2 else "head")
        print(f"now at {current()}")
    elif cmd == "downgrade" and len(argv) > 2:
        downgrade(argv[2])
        print(f"now at {current()}")
    elif cmd == "history":
        from alembic import command

        command.history(_config())
    elif cmd == "new" and len(argv) > 2:
        from alembic import command
        from alembic.script import ScriptDirectory

        n = len(list(ScriptDirectory.from_config(_config()).walk_revisions())) + 1
        slug = "_".join(argv[2].lower().split())[:40]
        command.revision(_config(), message=argv[2], rev_id=f"{n:04d}_{slug}")
    else:
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
