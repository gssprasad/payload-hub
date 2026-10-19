"""PostgreSQL transaction log: one row per transaction plus an audit trail of pipeline events."""
import os
import re

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/payloadhub")

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
"""

PARTNER_FIELDS = ("name", "description", "contact", "input_format", "output_format", "sample_input",
                  "expected_output", "expression", "mapping_source", "queue_mode", "topic_in", "topic_out",
                  "topic_dlq", "active", "api_key", "inbound_connection", "inbound_source", "outbound_connection",
                  "outbound_target")
CONNECTION_COLUMNS = "id, name, type, config, secret IS NOT NULL AND secret <> '' AS has_secret, " \
                     "last_test_at, last_test_ok, last_test_detail, created_at, updated_at"

pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=10, open=False, kwargs={"row_factory": dict_row})


async def init():
    await pool.open(wait=True, timeout=15)
    async with pool.connection() as conn:
        await conn.execute(SCHEMA)


async def close():
    await pool.close()


async def create_transaction(txn_id, mapping_name, input_format, output_format, expression, payload,
                             partner_code=None, topic=None, source=None):
    async with pool.connection() as conn:
        await conn.execute(
            """INSERT INTO transactions (id, status, mapping_name, input_format, output_format, expression, input_payload,
                                         partner_code, topic, source)
               VALUES (%s, 'RECEIVED', %s, %s, %s, %s, %s, %s, %s, %s)""",
            (txn_id, mapping_name, input_format, output_format, expression, payload, partner_code, topic, source),
        )
        source = f"{source or 'UI'}" + (f" for partner {partner_code}" if partner_code else "")
        await conn.execute(
            "INSERT INTO transaction_events (txn_id, stage, detail) VALUES (%s, 'RECEIVED', %s)",
            (txn_id, f"{len(payload)} bytes of {input_format.upper()} from {source}"),
        )


async def record_event(txn_id, stage, detail=None, **fields):
    """Append an audit event and optionally update columns on the transaction row."""
    sets = ", ".join(f"{k} = %s" for k in fields)
    async with pool.connection() as conn:
        async with conn.transaction():
            if fields:
                await conn.execute(
                    f"UPDATE transactions SET {sets}, updated_at = now() WHERE id = %s",
                    (*fields.values(), txn_id),
                )
            await conn.execute(
                "INSERT INTO transaction_events (txn_id, stage, detail) VALUES (%s, %s, %s)",
                (txn_id, stage, detail),
            )


async def list_transactions(limit=50, status=None, q=None, since=None, until=None, partner=None):
    """Transaction log with optional search: q matches the transaction id (or its prefix), the mapping
    name, or any text in the input, output or error."""
    clauses, args = [], []
    if status:
        clauses.append("status = %s")
        args.append(status)
    if q:
        escaped = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        like = f"%{escaped}%"
        text_match = ("mapping_name ILIKE %s OR partner_code ILIKE %s OR input_payload ILIKE %s"
                      " OR output_payload ILIKE %s OR error ILIKE %s")
        args += [like] * 5
        if re.fullmatch(r"[0-9a-fA-F-]{4,36}", q):  # looks like (part of) a transaction id
            text_match = "id::text LIKE %s OR " + text_match
            args.insert(len(args) - 5, q.lower() + "%")
        clauses.append(f"({text_match})")
    if partner == "-":
        clauses.append("partner_code IS NULL")
    elif partner:
        clauses.append("partner_code = %s")
        args.append(partner)
    if since:
        clauses.append("created_at >= %s")
        args.append(since)
    if until:
        clauses.append("created_at < %s")
        args.append(until)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"""SELECT id, created_at, status, partner_code, topic, source, delivery_status, mapping_name, input_format,
                       output_format, error, transform_ms, total_ms, count(*) OVER () AS matches
                FROM transactions {where} ORDER BY created_at DESC LIMIT %s""",
            (*args, limit),
        )
        return await cur.fetchall()


async def get_transaction(txn_id):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM transactions WHERE id = %s", (txn_id,))
        txn = await cur.fetchone()
        if txn:
            cur = await conn.execute(
                "SELECT at, stage, detail FROM transaction_events WHERE txn_id = %s ORDER BY at, id", (txn_id,)
            )
            txn["events"] = await cur.fetchall()
        return txn


async def stats(partner=None):
    """Totals, latency and per-minute counts; partner limits to one partner ("-" = no partner)."""
    if partner == "-":
        cond, args = "partner_code IS NULL", ()
    elif partner:
        cond, args = "partner_code = %s", (partner,)
    else:
        cond, args = "", ()
    where = f"WHERE {cond}" if cond else ""
    join_cond = f" AND t.{cond}" if cond else ""
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT count(*) AS total,
                      count(*) FILTER (WHERE status = 'COMPLETED') AS completed,
                      count(*) FILTER (WHERE status = 'FAILED') AS failed,
                      count(*) FILTER (WHERE status NOT IN ('COMPLETED', 'FAILED')) AS in_flight,
                      round(avg(total_ms) FILTER (WHERE status = 'COMPLETED')::numeric, 1) AS avg_ms,
                      round((percentile_cont(0.95) WITHIN GROUP (ORDER BY total_ms)
                             FILTER (WHERE status = 'COMPLETED'))::numeric, 1) AS p95_ms
               FROM transactions """ + where, args
        )
        summary = await cur.fetchone()
        cur = await conn.execute(
            """SELECT to_char(m, 'HH24:MI') AS minute,
                      count(t.id) FILTER (WHERE t.status = 'COMPLETED') AS completed,
                      count(t.id) FILTER (WHERE t.status = 'FAILED') AS failed
               FROM generate_series(date_trunc('minute', now()) - interval '29 minutes',
                                    date_trunc('minute', now()), interval '1 minute') AS m
               LEFT JOIN transactions t ON date_trunc('minute', t.created_at) = m""" + join_cond
            + " GROUP BY m ORDER BY m", args
        )
        summary["per_minute"] = await cur.fetchall()
        return summary


async def save_mapping(name, label, input_format, output_format, expression, request):
    async with pool.connection() as conn:
        await conn.execute(
            """INSERT INTO mappings (name, label, input_format, output_format, expression, request)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (name) DO UPDATE SET label = EXCLUDED.label, input_format = EXCLUDED.input_format,
                   output_format = EXCLUDED.output_format, expression = EXCLUDED.expression,
                   request = EXCLUDED.request, updated_at = now()""",
            (name, label, input_format, output_format, expression, request),
        )


async def list_mappings():
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT name, label, input_format, output_format, expression, request FROM mappings ORDER BY updated_at DESC"
        )
        return await cur.fetchall()


async def log_agent_run(request, result, mapping_doc=None, partner_code=None):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """INSERT INTO agent_runs (request, status, mapping_name, tool_calls, input_tokens, output_tokens,
                                       duration_ms, explanation, steps, mapping_doc_name, mapping_doc, partner_code)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (request, result["status"], (result["mapping"] or {}).get("name"), len(result["steps"]),
             result["usage"]["input_tokens"], result["usage"]["output_tokens"], result["duration_ms"],
             result["explanation"], Jsonb(result["steps"]),
             mapping_doc and mapping_doc["filename"], mapping_doc and mapping_doc["text"], partner_code),
        )
        return (await cur.fetchone())["id"]


async def list_agent_runs(limit=20):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT id, created_at, request, status, mapping_name, tool_calls, input_tokens, output_tokens,
                      duration_ms, explanation, mapping_doc_name, partner_code FROM agent_runs ORDER BY id DESC LIMIT %s""",
            (limit,),
        )
        return await cur.fetchall()


async def partner_stats():
    """One row per partner (plus ad-hoc traffic) for the monitoring dashboard."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT coalesce(p.code, t.partner_code, '-') AS partner_code,
                      coalesce(p.name, CASE WHEN t.partner_code IS NULL THEN 'Ad-hoc (no partner)' END) AS name,
                      p.queue_mode, p.topic_in, p.active,
                      count(t.id) AS total,
                      count(t.id) FILTER (WHERE t.status = 'COMPLETED') AS completed,
                      count(t.id) FILTER (WHERE t.status = 'FAILED') AS failed,
                      count(t.id) FILTER (WHERE t.status NOT IN ('COMPLETED', 'FAILED')) AS in_flight,
                      round(avg(t.total_ms) FILTER (WHERE t.status = 'COMPLETED')::numeric, 1) AS avg_ms,
                      count(t.id) FILTER (WHERE t.delivery_status = 'DELIVERED') AS delivered,
                      count(t.id) FILTER (WHERE t.delivery_status = 'DELIVERY_FAILED') AS delivery_failed,
                      max(t.created_at) AS last_seen
               FROM partners p
               FULL OUTER JOIN transactions t ON t.partner_code = p.code
               GROUP BY 1, 2, 3, 4, 5
               ORDER BY total DESC, 1"""
        )
        return await cur.fetchall()


async def list_partners():
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT p.code, p.name, p.description, p.input_format, p.output_format, p.queue_mode, p.topic_in,
                      p.active, p.inbound_connection, p.inbound_source, p.outbound_connection, p.outbound_target, p.expression IS NOT NULL AND p.expression <> '' AS has_mapping, p.mapping_source,
                      p.created_at, p.updated_at,
                      (SELECT count(*) FROM partner_documents d WHERE d.partner_code = p.code) AS documents,
                      (SELECT count(*) FROM transactions t WHERE t.partner_code = p.code) AS transactions
               FROM partners p ORDER BY p.name"""
        )
        return await cur.fetchall()


async def get_partner(code):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM partners WHERE code = %s", (code,))
        return await cur.fetchone()


async def create_partner(code, **fields):
    cols = ["code", *fields]
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"INSERT INTO partners ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
            "ON CONFLICT (code) DO NOTHING RETURNING code",
            (code, *fields.values()),
        )
        return (await cur.fetchone()) is not None


async def update_partner(code, **fields):
    fields = {k: v for k, v in fields.items() if k in PARTNER_FIELDS}
    if not fields:
        return
    sets = ", ".join(f"{k} = %s" for k in fields)
    async with pool.connection() as conn:
        await conn.execute(f"UPDATE partners SET {sets}, updated_at = now() WHERE code = %s",
                           (*fields.values(), code))


async def delete_partner(code):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM partners WHERE code = %s", (code,))


async def add_partner_document(code, filename, content, text, sheets):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """INSERT INTO partner_documents (partner_code, filename, content, text, sheets)
               VALUES (%s, %s, %s, %s, %s) RETURNING id, filename, uploaded_at""",
            (code, filename, content, text, Jsonb(sheets)),
        )
        return await cur.fetchone()


async def list_partner_documents(code, with_text=False):
    extra = ", text, sheets" if with_text else ""
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"""SELECT id, filename, uploaded_at, length(content) AS size{extra}
                FROM partner_documents WHERE partner_code = %s ORDER BY uploaded_at DESC""",
            (code,),
        )
        return await cur.fetchall()


async def get_partner_document(code, doc_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT * FROM partner_documents WHERE partner_code = %s AND id = %s", (code, doc_id)
        )
        return await cur.fetchone()


async def delete_partner_document(code, doc_id):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM partner_documents WHERE partner_code = %s AND id = %s", (code, doc_id))


async def get_agent_run(run_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT id, created_at, request, status, mapping_name, tool_calls, input_tokens, output_tokens,
                      duration_ms, explanation, steps, mapping_doc_name, partner_code
               FROM agent_runs WHERE id = %s""",
            (run_id,),
        )
        return await cur.fetchone()


async def list_connections():
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"""SELECT {CONNECTION_COLUMNS},
                       (SELECT coalesce(json_agg(code ORDER BY code), '[]') FROM partners p
                        WHERE p.inbound_connection = c.id OR p.outbound_connection = c.id) AS used_by
                FROM connections c ORDER BY name"""
        )
        return await cur.fetchall()


async def get_connection(conn_id, with_secret=False):
    cols = "*" if with_secret else CONNECTION_COLUMNS
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT {cols} FROM connections WHERE id = %s", (conn_id,))
        return await cur.fetchone()


async def create_connection(conn_id, name, type_, config, secret):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """INSERT INTO connections (id, name, type, config, secret) VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (id) DO NOTHING RETURNING id""",
            (conn_id, name, type_, Jsonb(config), secret),
        )
        return (await cur.fetchone()) is not None


async def update_connection(conn_id, name, config, secret=None):
    """secret=None keeps the stored secret."""
    async with pool.connection() as conn:
        if secret is None:
            await conn.execute("UPDATE connections SET name = %s, config = %s, updated_at = now() WHERE id = %s",
                               (name, Jsonb(config), conn_id))
        else:
            await conn.execute(
                "UPDATE connections SET name = %s, config = %s, secret = %s, updated_at = now() WHERE id = %s",
                (name, Jsonb(config), secret, conn_id))


async def record_connection_test(conn_id, ok, detail):
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE connections SET last_test_at = now(), last_test_ok = %s, last_test_detail = %s WHERE id = %s",
            (ok, detail, conn_id))


async def delete_connection(conn_id):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM connections WHERE id = %s", (conn_id,))


async def partners_with_inbound_adapters():
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT code, name, inbound_connection, inbound_source FROM partners
               WHERE active AND inbound_connection IS NOT NULL AND coalesce(inbound_source, '') <> ''"""
        )
        return await cur.fetchall()


async def set_delivery(txn_id, status):
    async with pool.connection() as conn:
        await conn.execute("UPDATE transactions SET delivery_status = %s, updated_at = now() WHERE id = %s",
                           (status, txn_id))


async def ping():
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")
