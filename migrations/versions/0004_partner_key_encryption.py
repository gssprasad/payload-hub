"""Partner API keys: keep a SHA-256 hash (to check calls) and an encrypted copy (to show to writers) instead of
plain text.

Revision ID: 0004_partner_key_encryption
Revises: 0003_auth_security
"""
import hashlib

import sqlalchemy as sa
from alembic import op

revision = "0004_partner_key_encryption"
down_revision = "0003_auth_security"
branch_labels = None
depends_on = None


def upgrade():
    from app import vault

    op.execute("ALTER TABLE partners ADD COLUMN api_key_hash text, ADD COLUMN api_key_enc text, "
               "ADD COLUMN api_key_hint text")
    conn = op.get_bind()
    for code, key in conn.execute(sa.text("SELECT code, api_key FROM partners WHERE api_key IS NOT NULL")).all():
        conn.execute(sa.text("UPDATE partners SET api_key_hash = :h, api_key_enc = :e, api_key_hint = :t WHERE code = :c"),
                     {"h": hashlib.sha256(key.encode()).hexdigest(), "e": vault.seal(key),
                      "t": f"{key[:5]}…{key[-4:]}", "c": code})
    op.execute("ALTER TABLE partners DROP COLUMN api_key")


def downgrade():
    from app import vault

    op.execute("ALTER TABLE partners ADD COLUMN api_key text")
    conn = op.get_bind()
    for code, enc in conn.execute(sa.text("SELECT code, api_key_enc FROM partners WHERE api_key_enc IS NOT NULL")).all():
        conn.execute(sa.text("UPDATE partners SET api_key = :k WHERE code = :c"), {"k": vault.unseal(enc), "c": code})
    op.execute("ALTER TABLE partners DROP COLUMN api_key_hash, DROP COLUMN api_key_enc, DROP COLUMN api_key_hint")
