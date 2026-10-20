"""Alerts: channels (webhook, email), rules (which events go where) and the alert log.

Revision ID: 0002_alerts
Revises: 0001_baseline
"""
from alembic import op

revision = "0002_alerts"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
    CREATE TABLE alert_channels (
        id            text PRIMARY KEY,
        name          text NOT NULL,
        type          text NOT NULL,                 -- webhook | email
        config        jsonb NOT NULL DEFAULT '{}',
        secret_alias  text REFERENCES secrets(alias) ON UPDATE CASCADE,
        enabled       boolean NOT NULL DEFAULT true,
        last_sent_at  timestamptz,
        last_error    text,
        created_at    timestamptz NOT NULL DEFAULT now(),
        updated_at    timestamptz NOT NULL DEFAULT now()
    );
    CREATE TABLE alert_rules (
        id                text PRIMARY KEY,
        name              text NOT NULL,
        enabled           boolean NOT NULL DEFAULT true,
        event             text NOT NULL,
        filter            jsonb NOT NULL DEFAULT '{}',   -- e.g. {"partner": "acme"}
        threshold         jsonb NOT NULL DEFAULT '{}',   -- error_rate: {"count": 5, "minutes": 10}
        channels          jsonb NOT NULL DEFAULT '[]',
        cooldown_minutes  integer NOT NULL DEFAULT 15,
        last_fired_at     timestamptz,
        fired_count       integer NOT NULL DEFAULT 0,
        created_at        timestamptz NOT NULL DEFAULT now(),
        updated_at        timestamptz NOT NULL DEFAULT now()
    );
    CREATE TABLE alert_events (
        id        bigserial PRIMARY KEY,
        at        timestamptz NOT NULL DEFAULT now(),
        rule_id   text REFERENCES alert_rules(id) ON DELETE SET NULL,
        event     text NOT NULL,
        key       text,
        subject   text NOT NULL,
        detail    text,
        status    text NOT NULL,                     -- sent | partial | failed | suppressed | no_channel
        results   jsonb NOT NULL DEFAULT '[]'
    );
    CREATE INDEX alert_events_at_idx ON alert_events (at DESC);
    CREATE INDEX alert_events_rule_key_idx ON alert_events (rule_id, key, at DESC);
    """)


def downgrade():
    op.execute("DROP TABLE alert_events; DROP TABLE alert_rules; DROP TABLE alert_channels;")
