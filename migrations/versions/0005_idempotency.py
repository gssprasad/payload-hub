"""Exactly-once effects: Idempotency-Key for partner calls, and claimed deliveries.

Revision ID: 0005_idempotency
Revises: 0004_partner_key_encryption
"""
from alembic import op

revision = "0005_idempotency"
down_revision = "0004_partner_key_encryption"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
    ALTER TABLE transactions ADD COLUMN idempotency_key text;
    ALTER TABLE transactions ADD COLUMN request_hash text;
    ALTER TABLE transactions ADD COLUMN delivery_claimed_at timestamptz;
    CREATE UNIQUE INDEX transactions_idempotency_idx ON transactions (partner_code, idempotency_key)
        WHERE idempotency_key IS NOT NULL;
    """)


def downgrade():
    op.execute("""
    DROP INDEX transactions_idempotency_idx;
    ALTER TABLE transactions DROP COLUMN delivery_claimed_at, DROP COLUMN request_hash, DROP COLUMN idempotency_key;
    """)
