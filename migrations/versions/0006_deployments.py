"""Deployment history: configuration packages imported into this environment.

Revision ID: 0006_deployments
Revises: 0005_idempotency
"""
from alembic import op

revision = "0006_deployments"
down_revision = "0005_idempotency"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
    CREATE TABLE deployments (
        id                  bigserial PRIMARY KEY,
        at                  timestamptz NOT NULL DEFAULT now(),
        username            text,
        package_name        text,
        source_environment  text,
        app_version         text,
        target_environment  text,
        status              text NOT NULL,
        error               text,
        backup_file         text,
        package_sha256      text,
        summary             jsonb NOT NULL DEFAULT '{}'
    );
    CREATE INDEX deployments_at_idx ON deployments (at DESC);
    """)


def downgrade():
    op.execute("DROP TABLE deployments")
