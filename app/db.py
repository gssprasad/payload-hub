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
"""

pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=10, open=False, kwargs={"row_factory": dict_row})


async def init():
    await pool.open(wait=True, timeout=15)
    async with pool.connection() as conn:
        await conn.execute(SCHEMA)


async def close():
    await pool.close()


async def create_transaction(txn_id, mapping_name, input_format, output_format, expression, payload):
    async with pool.connection() as conn:
        await conn.execute(
            """INSERT INTO transactions (id, status, mapping_name, input_format, output_format, expression, input_payload)
               VALUES (%s, 'RECEIVED', %s, %s, %s, %s, %s)""",
            (txn_id, mapping_name, input_format, output_format, expression, payload),
        )
        await conn.execute(
            "INSERT INTO transaction_events (txn_id, stage, detail) VALUES (%s, 'RECEIVED', %s)",
            (txn_id, f"{len(payload)} bytes of {input_format.upper()} from UI/API"),
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


async def list_transactions(limit=50, status=None, q=None, since=None, until=None):
    """Transaction log with optional search: q matches the transaction id (or its prefix), the mapping
    name, or any text in the input, output or error."""
    clauses, args = [], []
    if status:
        clauses.append("status = %s")
        args.append(status)
    if q:
        like = f"%{q.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')}%"
        text_match = "mapping_name ILIKE %s OR input_payload ILIKE %s OR output_payload ILIKE %s OR error ILIKE %s"
        args += [like] * 4
        if re.fullmatch(r"[0-9a-fA-F-]{4,36}", q):  # looks like (part of) a transaction id
            text_match = "id::text LIKE %s OR " + text_match
            args.insert(len(args) - 4, q.lower() + "%")
        clauses.append(f"({text_match})")
    if since:
        clauses.append("created_at >= %s")
        args.append(since)
    if until:
        clauses.append("created_at < %s")
        args.append(until)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"""SELECT id, created_at, status, mapping_name, input_format, output_format, error, transform_ms, total_ms,
                       count(*) OVER () AS matches
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


async def stats():
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT count(*) AS total,
                      count(*) FILTER (WHERE status = 'COMPLETED') AS completed,
                      count(*) FILTER (WHERE status = 'FAILED') AS failed,
                      count(*) FILTER (WHERE status NOT IN ('COMPLETED', 'FAILED')) AS in_flight,
                      round(avg(total_ms) FILTER (WHERE status = 'COMPLETED')::numeric, 1) AS avg_ms,
                      round((percentile_cont(0.95) WITHIN GROUP (ORDER BY total_ms)
                             FILTER (WHERE status = 'COMPLETED'))::numeric, 1) AS p95_ms
               FROM transactions"""
        )
        summary = await cur.fetchone()
        cur = await conn.execute(
            """SELECT to_char(m, 'HH24:MI') AS minute,
                      count(t.id) FILTER (WHERE t.status = 'COMPLETED') AS completed,
                      count(t.id) FILTER (WHERE t.status = 'FAILED') AS failed
               FROM generate_series(date_trunc('minute', now()) - interval '29 minutes',
                                    date_trunc('minute', now()), interval '1 minute') AS m
               LEFT JOIN transactions t ON date_trunc('minute', t.created_at) = m
               GROUP BY m ORDER BY m"""
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


async def log_agent_run(request, result, mapping_doc=None):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """INSERT INTO agent_runs (request, status, mapping_name, tool_calls, input_tokens, output_tokens,
                                       duration_ms, explanation, steps, mapping_doc_name, mapping_doc)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (request, result["status"], (result["mapping"] or {}).get("name"), len(result["steps"]),
             result["usage"]["input_tokens"], result["usage"]["output_tokens"], result["duration_ms"],
             result["explanation"], Jsonb(result["steps"]),
             mapping_doc and mapping_doc["filename"], mapping_doc and mapping_doc["text"]),
        )
        return (await cur.fetchone())["id"]


async def list_agent_runs(limit=20):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT id, created_at, request, status, mapping_name, tool_calls, input_tokens, output_tokens,
                      duration_ms, explanation, mapping_doc_name FROM agent_runs ORDER BY id DESC LIMIT %s""",
            (limit,),
        )
        return await cur.fetchall()


async def ping():
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")
