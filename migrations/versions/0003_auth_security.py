"""Sign-in security: failed-attempt lockout, idle session timeout, single sign-on users.

Revision ID: 0003_auth_security
Revises: 0002_alerts
"""
from alembic import op

revision = "0003_auth_security"
down_revision = "0002_alerts"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
    ALTER TABLE users ADD COLUMN failed_logins integer NOT NULL DEFAULT 0;
    ALTER TABLE users ADD COLUMN locked_until timestamptz;
    ALTER TABLE users ADD COLUMN auth_source text NOT NULL DEFAULT 'password';   -- password | sso
    ALTER TABLE sessions ADD COLUMN last_seen_at timestamptz NOT NULL DEFAULT now();
    CREATE TABLE login_attempts (
        id        bigserial PRIMARY KEY,
        at        timestamptz NOT NULL DEFAULT now(),
        address   text,
        username  text,
        ok        boolean NOT NULL
    );
    CREATE INDEX login_attempts_address_idx ON login_attempts (address, at DESC);
    """)


def downgrade():
    op.execute("""
    DROP TABLE login_attempts;
    ALTER TABLE sessions DROP COLUMN last_seen_at;
    ALTER TABLE users DROP COLUMN auth_source, DROP COLUMN locked_until, DROP COLUMN failed_logins;
    """)
