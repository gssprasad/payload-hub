"""Alembic environment: runs migrations against DATABASE_URL, one transaction per migration, under a
PostgreSQL advisory lock so two app instances starting together don't migrate at the same time."""
from alembic import context
from sqlalchemy import create_engine, text

from app.migrate import LOCK_ID, sqlalchemy_url


def run():
    engine = create_engine(sqlalchemy_url())
    with engine.connect() as conn:
        conn.execute(text("SELECT pg_advisory_lock(:id)"), {"id": LOCK_ID})
        conn.commit()
        try:
            context.configure(connection=conn, transaction_per_migration=True, target_metadata=None)
            context.run_migrations()
            conn.commit()
        finally:
            conn.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": LOCK_ID})
            conn.commit()
    engine.dispose()


if context.is_offline_mode():
    raise SystemExit("Offline (SQL script) mode isn't supported - run against a database.")
run()
