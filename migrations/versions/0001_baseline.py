"""Baseline: the schema as it was before migrations were introduced.

Written with IF NOT EXISTS throughout, so it's safe on a database created by an older version (it adds whatever
is missing) as well as on an empty one. Never edit this file - add a new migration instead.

Revision ID: 0001_baseline
Revises:
"""
from alembic import op

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    id             uuid PRIMARY KEY,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    status         text NOT NULL,
    mapping_name   text NOT NULL,
    input_format   text NOT NULL,
    output_format  text NOT NULL,
    expression     text NOT NULL,
    input_payload  text NOT NULL,
    output_payload text,
    error          text,
    transform_ms   double precision,
    total_ms       double precision
);
CREATE INDEX IF NOT EXISTS transactions_created_at_idx ON transactions (created_at DESC);
CREATE INDEX IF NOT EXISTS transactions_status_idx ON transactions (status);

CREATE TABLE IF NOT EXISTS transaction_events (
    id      bigserial PRIMARY KEY,
    txn_id  uuid NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    at      timestamptz NOT NULL DEFAULT now(),
    stage   text NOT NULL,
    detail  text
);
CREATE INDEX IF NOT EXISTS transaction_events_txn_idx ON transaction_events (txn_id, at);

CREATE TABLE IF NOT EXISTS mappings (
    name          text PRIMARY KEY,
    label         text NOT NULL,
    input_format  text NOT NULL,
    output_format text NOT NULL,
    expression    text NOT NULL,
    request       text,
    created_by    text NOT NULL DEFAULT 'agent',
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_runs (
    id            bigserial PRIMARY KEY,
    created_at    timestamptz NOT NULL DEFAULT now(),
    request       text NOT NULL,
    status        text NOT NULL,
    mapping_name  text,
    tool_calls    int NOT NULL,
    input_tokens  int NOT NULL,
    output_tokens int NOT NULL,
    duration_ms   int NOT NULL,
    explanation   text,
    steps         jsonb NOT NULL
);
ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS mapping_doc_name text;
ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS mapping_doc text;
ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS partner_code text;

CREATE TABLE IF NOT EXISTS partners (
    code            text PRIMARY KEY,
    name            text NOT NULL,
    description     text,
    contact         text,
    input_format    text NOT NULL,
    output_format   text NOT NULL,
    sample_input    text,
    expected_output text,
    expression      text,
    mapping_source  text,
    queue_mode      text NOT NULL CHECK (queue_mode IN ('shared', 'dedicated')),
    topic_in        text NOT NULL,
    topic_out       text NOT NULL,
    topic_dlq       text NOT NULL,
    active          boolean NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS partner_documents (
    id           bigserial PRIMARY KEY,
    partner_code text NOT NULL REFERENCES partners(code) ON DELETE CASCADE,
    filename     text NOT NULL,
    content      bytea NOT NULL,
    text         text NOT NULL,
    sheets       jsonb NOT NULL,
    uploaded_at  timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS partner_code text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS topic text;
CREATE INDEX IF NOT EXISTS transactions_partner_idx ON transactions (partner_code, created_at DESC);

CREATE TABLE IF NOT EXISTS connections (
    id               text PRIMARY KEY,
    name             text NOT NULL,
    type             text NOT NULL,
    config           jsonb NOT NULL DEFAULT '{}',
    secret           text,
    last_test_at     timestamptz,
    last_test_ok     boolean,
    last_test_detail text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE partners ADD COLUMN IF NOT EXISTS api_key text;
ALTER TABLE partners ADD COLUMN IF NOT EXISTS inbound_connection text REFERENCES connections(id);
ALTER TABLE partners ADD COLUMN IF NOT EXISTS inbound_source text;
ALTER TABLE partners ADD COLUMN IF NOT EXISTS outbound_connection text REFERENCES connections(id);
ALTER TABLE partners ADD COLUMN IF NOT EXISTS outbound_target text;
UPDATE partners SET api_key = 'ph_' || md5(random()::text || code) WHERE api_key IS NULL;

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS source text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS delivery_status text;

ALTER TABLE partners ADD COLUMN IF NOT EXISTS reply_mode text NOT NULL DEFAULT 'transformed';
ALTER TABLE partners ADD COLUMN IF NOT EXISTS reply_method text;
ALTER TABLE partners ADD COLUMN IF NOT EXISTS send_format text NOT NULL DEFAULT 'output';
ALTER TABLE partners ADD COLUMN IF NOT EXISTS csv_delimiter text;
ALTER TABLE connections ADD COLUMN IF NOT EXISTS owner_partner text;  -- NULL = common to all partners
ALTER TABLE partners ADD COLUMN IF NOT EXISTS reply_connection text REFERENCES connections(id);
ALTER TABLE partners ADD COLUMN IF NOT EXISTS reply_path text;
ALTER TABLE partners ADD COLUMN IF NOT EXISTS response_format text NOT NULL DEFAULT 'input';
ALTER TABLE partners ADD COLUMN IF NOT EXISTS response_expression text;
ALTER TABLE partners ADD COLUMN IF NOT EXISTS store_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE partners ADD COLUMN IF NOT EXISTS store_table text;
ALTER TABLE partners ADD COLUMN IF NOT EXISTS store_source text NOT NULL DEFAULT 'output';
ALTER TABLE partners ADD COLUMN IF NOT EXISTS store_row_path text;
ALTER TABLE partners ADD COLUMN IF NOT EXISTS store_columns jsonb;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS response_payload text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS upstream_status int;

ALTER TABLE partners ADD COLUMN IF NOT EXISTS store_connection text REFERENCES connections(id);

CREATE TABLE IF NOT EXISTS adapter_state (
    key        text PRIMARY KEY,
    value      text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflows (
    id             text PRIMARY KEY,
    name           text NOT NULL,
    description    text,
    trigger_type   text NOT NULL DEFAULT 'manual',
    trigger_config jsonb NOT NULL DEFAULT '{}',
    steps          jsonb NOT NULL DEFAULT '[]',
    active         boolean NOT NULL DEFAULT true,
    next_run_at    timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id           uuid PRIMARY KEY,
    workflow_id  text NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    status       text NOT NULL,
    source       text,
    dry_run      boolean NOT NULL DEFAULT false,
    input        jsonb,
    context      jsonb,
    steps_log    jsonb NOT NULL DEFAULT '[]',
    failed_step  int,
    error        text,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    duration_ms  double precision
);
CREATE INDEX IF NOT EXISTS workflow_runs_wf_idx ON workflow_runs (workflow_id, started_at DESC);

CREATE TABLE IF NOT EXISTS workflow_files (
    id           bigserial PRIMARY KEY,
    run_id       uuid NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    filename     text NOT NULL,
    content_type text NOT NULL,
    content      bytea NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ai_providers (
    provider    text PRIMARY KEY,
    api_key_enc text,
    key_hint    text,
    config      jsonb NOT NULL DEFAULT '{}',
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app_settings (
    key        text PRIMARY KEY,
    value      jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS provider text;
ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS model text;

CREATE TABLE IF NOT EXISTS users (
    username      text PRIMARY KEY,
    password_hash text NOT NULL,
    can_read      boolean NOT NULL DEFAULT true,
    can_write     boolean NOT NULL DEFAULT false,
    can_delete    boolean NOT NULL DEFAULT false,
    is_admin      boolean NOT NULL DEFAULT false,
    active        boolean NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now(),
    last_login_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower_idx ON users (lower(username));

CREATE TABLE IF NOT EXISTS sessions (
    token_hash text PRIMARY KEY,
    username   text NOT NULL REFERENCES users(username) ON DELETE CASCADE ON UPDATE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);

-- Named secrets (encrypted with the master key) that connections can use instead of their own password.
CREATE TABLE IF NOT EXISTS secrets (
    alias        text PRIMARY KEY,
    kind         text NOT NULL DEFAULT 'password',
    description  text,
    value_enc    text NOT NULL,
    version      integer NOT NULL DEFAULT 1,
    created_at   timestamptz NOT NULL DEFAULT now(),
    created_by   text,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    updated_by   text
);
ALTER TABLE connections ADD COLUMN IF NOT EXISTS secret_alias text REFERENCES secrets(alias) ON UPDATE CASCADE;

-- Who changed what (secrets, keys, connection passwords, ports, users). Never holds secret values.
CREATE TABLE IF NOT EXISTS audit_log (
    id           bigserial PRIMARY KEY,
    at           timestamptz NOT NULL DEFAULT now(),
    username     text,
    action       text NOT NULL,
    object_type  text NOT NULL,
    object_id    text,
    detail       text
);
CREATE INDEX IF NOT EXISTS audit_log_at_idx ON audit_log (at DESC);

-- Scheduler: tasks that run a workflow, send a message to a partner or call an API on a schedule.
CREATE TABLE IF NOT EXISTS schedules (
    id           text PRIMARY KEY,
    name         text NOT NULL,
    description  text,
    enabled      boolean NOT NULL DEFAULT true,
    schedule     jsonb NOT NULL,
    action       jsonb NOT NULL,
    next_run_at  timestamptz,
    last_run_at  timestamptz,
    last_status  text,
    last_detail  text,
    run_count    integer NOT NULL DEFAULT 0,
    fail_count   integer NOT NULL DEFAULT 0,
    running      boolean NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS schedule_runs (
    id           bigserial PRIMARY KEY,
    schedule_id  text NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    trigger      text NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    status       text NOT NULL DEFAULT 'RUNNING',
    detail       text,
    ref          text,
    duration_ms  double precision
);
CREATE INDEX IF NOT EXISTS schedule_runs_schedule_idx ON schedule_runs (schedule_id, started_at DESC);

-- Ports: inbound listeners defined on their own (file polling on a server folder, or a listener on a
-- connection), each feeding a partner or a workflow.
CREATE TABLE IF NOT EXISTS ports (
    id            text PRIMARY KEY,
    name          text NOT NULL,
    type          text NOT NULL,
    enabled       boolean NOT NULL DEFAULT true,
    config        jsonb NOT NULL DEFAULT '{}',
    target_type   text NOT NULL,
    target_id     text NOT NULL,
    last_poll_at  timestamptz,
    last_item     text,
    last_error    text,
    ok_count      integer NOT NULL DEFAULT 0,
    fail_count    integer NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS port_events (
    id        bigserial PRIMARY KEY,
    port_id   text NOT NULL REFERENCES ports(id) ON DELETE CASCADE,
    at        timestamptz NOT NULL DEFAULT now(),
    item      text,
    status    text NOT NULL,
    detail    text,
    ref       text
);
CREATE INDEX IF NOT EXISTS port_events_port_idx ON port_events (port_id, at DESC);
"""


def upgrade():
    op.execute(SCHEMA)


def downgrade():
    raise NotImplementedError("The baseline can't be undone - restore a backup instead.")
