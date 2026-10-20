"""PostgreSQL transaction log: one row per transaction plus an audit trail of pipeline events."""
import os
import re

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/payloadhub")

# The schema lives in migrations/versions (Alembic) and is applied at start-up - see app/migrate.py.


USER_COLUMNS = "username, can_read, can_write, can_delete, is_admin, active, created_at, last_login_at"

PARTNER_FIELDS = ("name", "description", "contact", "input_format", "output_format", "sample_input",
                  "expected_output", "expression", "mapping_source", "queue_mode", "topic_in", "topic_out",
                  "topic_dlq", "active", "api_key", "inbound_connection", "inbound_source", "outbound_connection",
                  "outbound_target", "reply_mode", "reply_connection", "reply_path", "response_format",
                  "response_expression", "store_enabled", "store_table", "store_source", "store_row_path",
                  "store_columns", "store_connection", "reply_method", "send_format", "csv_delimiter")
CONNECTION_COLUMNS = "id, name, type, config, secret IS NOT NULL AND secret <> '' AS has_secret, " \
                     "last_test_at, last_test_ok, last_test_detail, owner_partner, secret_alias, created_at, updated_at"

pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=10, open=False, kwargs={"row_factory": dict_row})


async def init():
    import asyncio

    from . import migrate

    await asyncio.to_thread(migrate.on_startup)  # create or upgrade the schema before anything uses it
    await pool.open(wait=True, timeout=15)


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
                                       duration_ms, explanation, steps, mapping_doc_name, mapping_doc, partner_code,
                                       provider, model)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (request, result["status"], (result["mapping"] or {}).get("name"), len(result["steps"]),
             result["usage"]["input_tokens"], result["usage"]["output_tokens"], result["duration_ms"],
             result["explanation"], Jsonb(result["steps"]),
             mapping_doc and mapping_doc["filename"], mapping_doc and mapping_doc["text"], partner_code,
             result.get("provider"), result.get("model")),
        )
        return (await cur.fetchone())["id"]


async def list_agent_runs(limit=20):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT id, created_at, request, status, mapping_name, tool_calls, input_tokens, output_tokens,
                      duration_ms, explanation, mapping_doc_name, partner_code, provider, model
               FROM agent_runs ORDER BY id DESC LIMIT %s""",
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
                      p.active, p.inbound_connection, p.inbound_source, p.outbound_connection, p.outbound_target,
                      p.reply_mode, p.reply_connection, p.reply_path, p.response_format, p.send_format,
                      p.store_enabled, p.store_table, p.store_connection, p.expression IS NOT NULL AND p.expression <> '' AS has_mapping, p.mapping_source,
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
                        WHERE p.inbound_connection = c.id OR p.outbound_connection = c.id
                  OR p.reply_connection = c.id OR p.store_connection = c.id) AS used_by
                FROM connections c ORDER BY name"""
        )
        return await cur.fetchall()


async def get_connection(conn_id, with_secret=False):
    cols = "*" if with_secret else CONNECTION_COLUMNS
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT {cols} FROM connections WHERE id = %s", (conn_id,))
        return await cur.fetchone()


async def create_connection(conn_id, name, type_, config, secret, owner_partner=None, secret_alias=None):
    """secret must already be sealed (vault.seal)."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            """INSERT INTO connections (id, name, type, config, secret, owner_partner, secret_alias)
               VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING RETURNING id""",
            (conn_id, name, type_, Jsonb(config), secret, owner_partner, secret_alias),
        )
        return (await cur.fetchone()) is not None


async def set_connection_owner(conn_id, owner_partner):
    async with pool.connection() as conn:
        await conn.execute("UPDATE connections SET owner_partner = %s, updated_at = now() WHERE id = %s",
                           (owner_partner, conn_id))


async def set_connection_secret_alias(conn_id, alias):
    """Use a named secret (clears the connection's own password), or stop using one (alias None)."""
    async with pool.connection() as conn:
        if alias:
            await conn.execute("UPDATE connections SET secret_alias = %s, secret = NULL, updated_at = now() "
                               "WHERE id = %s", (alias, conn_id))
        else:
            await conn.execute("UPDATE connections SET secret_alias = NULL, updated_at = now() WHERE id = %s",
                               (conn_id,))


async def update_connection(conn_id, name, config, secret=None):
    """secret=None keeps the stored secret; a new secret must already be sealed (vault.seal)."""
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


async def set_response(txn_id, response_payload, upstream_status):
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE transactions SET response_payload = %s, upstream_status = %s, updated_at = now() WHERE id = %s",
            (response_payload, upstream_status, txn_id))


async def get_state(key):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT value FROM adapter_state WHERE key = %s", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None


async def set_state(key, value):
    async with pool.connection() as conn:
        await conn.execute(
            """INSERT INTO adapter_state (key, value) VALUES (%s, %s)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()""", (key, value))


async def set_delivery(txn_id, status):
    async with pool.connection() as conn:
        await conn.execute("UPDATE transactions SET delivery_status = %s, updated_at = now() WHERE id = %s",
                           (status, txn_id))


async def count_users():
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) AS n FROM users")
        return (await cur.fetchone())["n"]


async def list_users():
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT {USER_COLUMNS} FROM users ORDER BY lower(username)")
        return await cur.fetchall()


async def get_user(username, with_hash=False):
    cols = USER_COLUMNS + (", password_hash" if with_hash else "")
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT {cols} FROM users WHERE lower(username) = lower(%s)", (username,))
        return await cur.fetchone()


async def create_user(username, password_hash, can_read, can_write, can_delete, is_admin):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """INSERT INTO users (username, password_hash, can_read, can_write, can_delete, is_admin)
               VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING username""",
            (username, password_hash, can_read, can_write, can_delete, is_admin))
        return (await cur.fetchone()) is not None


async def update_user(username, **fields):
    allowed = {"password_hash", "can_read", "can_write", "can_delete", "is_admin", "active"}
    fields = {k: v for k, v in fields.items() if k in allowed}
    if not fields:
        return
    sets = ", ".join(f"{k} = %s" for k in fields)
    async with pool.connection() as conn:
        await conn.execute(f"UPDATE users SET {sets} WHERE lower(username) = lower(%s)", (*fields.values(), username))
        if "password_hash" in fields or fields.get("active") is False:
            await conn.execute("DELETE FROM sessions WHERE lower(username) = lower(%s)", (username,))


async def delete_user(username):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM users WHERE lower(username) = lower(%s)", (username,))


async def count_active_admins(excluding=None):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) AS n FROM users WHERE is_admin AND active AND lower(username) <> lower(coalesce(%s, ''))",
            (excluding,))
        return (await cur.fetchone())["n"]


async def touch_login(username):
    async with pool.connection() as conn:
        await conn.execute("UPDATE users SET last_login_at = now() WHERE username = %s", (username,))


async def create_session(token_hash, username, hours):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM sessions WHERE expires_at < now()")
        await conn.execute(
            "INSERT INTO sessions (token_hash, username, expires_at) VALUES (%s, %s, now() + make_interval(hours => %s))",
            (token_hash, username, hours))


async def get_session_user(token_hash):
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"""SELECT {', '.join('u.' + c.strip() for c in USER_COLUMNS.split(','))}
                FROM sessions s JOIN users u ON u.username = s.username
                WHERE s.token_hash = %s AND s.expires_at > now() AND u.active""",
            (token_hash,))
        return await cur.fetchone()


async def delete_session(token_hash):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM sessions WHERE token_hash = %s", (token_hash,))


# Columns never shown in the read-only system tables view.
SYSTEM_HIDDEN = {"users": {"password_hash"}, "sessions": {"token_hash"}, "connections": {"secret"},
                 "secrets": {"value_enc"}, "ai_providers": {"api_key_enc"},
                 "partners": {"api_key"}, "partner_documents": {"content", "text"}}


async def system_tables():
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT c.table_name, c.column_name, c.data_type FROM information_schema.columns c
               JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name
               WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE'
               ORDER BY c.table_name, c.ordinal_position""")
        tables: dict[str, dict] = {}
        for r in await cur.fetchall():
            t = tables.setdefault(r["table_name"], {"name": r["table_name"], "columns": [], "rows": 0})
            if r["column_name"] not in SYSTEM_HIDDEN.get(r["table_name"], set()):
                t["columns"].append({"name": r["column_name"], "type": r["data_type"]})
        for t in tables.values():
            cur = await conn.execute(f'SELECT count(*) AS n FROM public."{t["name"]}"')
            t["rows"] = (await cur.fetchone())["n"]
        return list(tables.values())


async def system_rows(name, limit=100):
    tables = {t["name"]: t for t in await system_tables()}
    if name not in tables:
        return None
    cols = [c["name"] for c in tables[name]["columns"]]
    order = "created_at" if "created_at" in cols else cols[0]
    async with pool.connection() as conn:
        cur = await conn.execute(
            f'SELECT {", ".join(chr(34) + c + chr(34) for c in cols)} FROM public."{name}" '
            f'ORDER BY "{order}" DESC LIMIT %s', (min(limit, 500),))
        return {"columns": cols, "rows": await cur.fetchall()}


# ---------- workflows ----------

WORKFLOW_FIELDS = ("name", "description", "trigger_type", "trigger_config", "steps", "active", "next_run_at")


async def list_workflows():
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT w.id, w.name, w.description, w.trigger_type, w.trigger_config, w.active, w.next_run_at,
                      jsonb_array_length(w.steps) AS step_count, w.updated_at,
                      (SELECT count(*) FROM workflow_runs r WHERE r.workflow_id = w.id) AS runs,
                      (SELECT count(*) FROM workflow_runs r WHERE r.workflow_id = w.id AND r.status = 'COMPLETED') AS completed,
                      (SELECT count(*) FROM workflow_runs r WHERE r.workflow_id = w.id AND r.status = 'FAILED') AS failed,
                      (SELECT max(started_at) FROM workflow_runs r WHERE r.workflow_id = w.id) AS last_run
               FROM workflows w ORDER BY w.name""")
        return await cur.fetchall()


async def get_workflow(wf_id):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM workflows WHERE id = %s", (wf_id,))
        return await cur.fetchone()


async def create_workflow(wf_id, **fields):
    fields = {k: (Jsonb(v) if k in ("trigger_config", "steps") else v) for k, v in fields.items()}
    cols = ["id", *fields]
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"INSERT INTO workflows ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
            "ON CONFLICT (id) DO NOTHING RETURNING id", (wf_id, *fields.values()))
        return (await cur.fetchone()) is not None


async def update_workflow(wf_id, **fields):
    fields = {k: (Jsonb(v) if k in ("trigger_config", "steps") else v) for k, v in fields.items()
              if k in WORKFLOW_FIELDS}
    if not fields:
        return
    sets = ", ".join(f"{k} = %s" for k in fields)
    async with pool.connection() as conn:
        await conn.execute(f"UPDATE workflows SET {sets}, updated_at = now() WHERE id = %s", (*fields.values(), wf_id))


async def delete_workflow(wf_id):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM workflows WHERE id = %s", (wf_id,))


async def create_run(run_id, wf_id, source, dry_run, input_):
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO workflow_runs (id, workflow_id, status, source, dry_run, input) VALUES (%s, %s, 'RUNNING', %s, %s, %s)",
            (run_id, wf_id, source, dry_run, Jsonb(input_)))


async def update_run(run_id, **fields):
    fields = {k: (Jsonb(v) if k in ("context", "steps_log", "input") else v) for k, v in fields.items()}
    sets = ", ".join(f"{k} = %s" for k in fields)
    async with pool.connection() as conn:
        await conn.execute(f"UPDATE workflow_runs SET {sets} WHERE id = %s", (*fields.values(), run_id))


async def get_run(run_id):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM workflow_runs WHERE id = %s", (run_id,))
        run = await cur.fetchone()
        if run:
            cur = await conn.execute(
                "SELECT id, filename, content_type, length(content) AS size, created_at FROM workflow_files "
                "WHERE run_id = %s ORDER BY id", (run_id,))
            run["files"] = await cur.fetchall()
        return run


async def list_runs(wf_id=None, limit=50):
    where, args = ("WHERE workflow_id = %s", (wf_id,)) if wf_id else ("", ())
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"""SELECT id, workflow_id, status, source, dry_run, failed_step, error, started_at, finished_at, duration_ms,
                       (SELECT count(*) FROM workflow_files f WHERE f.run_id = r.id) AS files
                FROM workflow_runs r {where} ORDER BY started_at DESC LIMIT %s""", (*args, limit))
        return await cur.fetchall()


async def add_file(run_id, filename, content_type, content):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO workflow_files (run_id, filename, content_type, content) VALUES (%s, %s, %s, %s) RETURNING id",
            (run_id, filename, content_type, content))
        return (await cur.fetchone())["id"]


async def get_file(file_id):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM workflow_files WHERE id = %s", (file_id,))
        return await cur.fetchone()


async def due_workflows():
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT * FROM workflows WHERE active AND trigger_type = 'schedule' AND (next_run_at IS NULL OR next_run_at <= now())")
        return await cur.fetchall()


# ---------- AI providers and settings ----------

async def get_ai_provider(provider):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM ai_providers WHERE provider = %s", (provider,))
        return await cur.fetchone()


async def list_ai_providers():
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT provider, key_hint, api_key_enc IS NOT NULL AS has_key, config, updated_at "
                                 "FROM ai_providers")
        return {r["provider"]: r for r in await cur.fetchall()}


async def save_ai_provider(provider, api_key_enc=None, key_hint=None, config=None, clear_key=False):
    """api_key_enc=None keeps the stored key unless clear_key is set."""
    async with pool.connection() as conn:
        await conn.execute(
            """INSERT INTO ai_providers (provider, api_key_enc, key_hint, config) VALUES (%s, %s, %s, %s)
               ON CONFLICT (provider) DO UPDATE SET
                 api_key_enc = CASE WHEN %s THEN NULL ELSE coalesce(EXCLUDED.api_key_enc, ai_providers.api_key_enc) END,
                 key_hint = CASE WHEN %s THEN NULL ELSE coalesce(EXCLUDED.key_hint, ai_providers.key_hint) END,
                 config = coalesce(%s, ai_providers.config), updated_at = now()""",
            (provider, api_key_enc, key_hint, Jsonb(config or {}), clear_key, clear_key,
             Jsonb(config) if config is not None else None))


async def get_setting(key):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT value FROM app_settings WHERE key = %s", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None


async def set_setting(key, value):
    async with pool.connection() as conn:
        await conn.execute(
            """INSERT INTO app_settings (key, value) VALUES (%s, %s)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()""", (key, Jsonb(value)))


async def overview_counts():
    async with pool.connection() as conn:
        cur = await conn.execute("""
            SELECT
              (SELECT version()) AS pg_version,
              (SELECT pg_database_size(current_database())) AS db_bytes,
              (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()) AS db_connections,
              (SELECT count(*) FROM partners) AS partners,
              (SELECT count(*) FROM partners WHERE active) AS partners_active,
              (SELECT count(*) FROM connections) AS connections,
              (SELECT count(*) FROM connections WHERE last_test_ok IS FALSE) AS connections_failing,
              (SELECT count(*) FROM workflows) AS workflows,
              (SELECT count(*) FROM workflows WHERE active AND trigger_type <> 'manual') AS workflows_automatic,
              (SELECT count(*) FROM users WHERE active) AS users,
              (SELECT count(*) FROM information_schema.tables WHERE table_schema = 'userdata') AS data_tables,
              (SELECT count(*) FROM transactions) AS txn_total,
              (SELECT count(*) FROM transactions WHERE created_at > now() - interval '1 hour') AS txn_hour,
              (SELECT count(*) FROM transactions WHERE created_at > date_trunc('day', now())) AS txn_today,
              (SELECT count(*) FROM transactions WHERE created_at > date_trunc('day', now()) AND status = 'FAILED') AS txn_failed_today,
              (SELECT round(avg(total_ms)::numeric, 1) FROM transactions
                WHERE created_at > date_trunc('day', now()) AND status = 'COMPLETED') AS txn_avg_ms_today,
              (SELECT count(*) FROM transactions WHERE created_at > date_trunc('day', now()) AND delivery_status = 'DELIVERED') AS delivered_today,
              (SELECT count(*) FROM transactions WHERE created_at > date_trunc('day', now()) AND delivery_status = 'DELIVERY_FAILED') AS delivery_failed_today,
              (SELECT count(*) FROM workflow_runs WHERE started_at > date_trunc('day', now())) AS wf_runs_today,
              (SELECT count(*) FROM workflow_runs WHERE started_at > date_trunc('day', now()) AND status = 'FAILED') AS wf_failed_today,
              (SELECT count(*) FROM agent_runs WHERE created_at > date_trunc('day', now())) AS agent_runs_today
        """)
        counts = await cur.fetchone()
        cur = await conn.execute("""
            SELECT to_char(h, 'HH24:00') AS hour,
                   count(t.id) FILTER (WHERE t.status = 'COMPLETED') AS completed,
                   count(t.id) FILTER (WHERE t.status = 'FAILED') AS failed
            FROM generate_series(date_trunc('hour', now()) - interval '23 hours', date_trunc('hour', now()), interval '1 hour') h
            LEFT JOIN transactions t ON date_trunc('hour', t.created_at) = h
            GROUP BY h ORDER BY h""")
        counts["per_hour"] = await cur.fetchall()
        cur = await conn.execute("""
            (SELECT 'transaction' AS kind, created_at AS at, coalesce(partner_code, mapping_name) AS what, error AS detail
               FROM transactions WHERE status = 'FAILED' OR delivery_status = 'DELIVERY_FAILED' ORDER BY created_at DESC LIMIT 5)
            UNION ALL
            (SELECT 'workflow', started_at, workflow_id, error FROM workflow_runs WHERE status = 'FAILED'
               ORDER BY started_at DESC LIMIT 5)
            ORDER BY at DESC LIMIT 8""")
        counts["recent_errors"] = await cur.fetchall()
        return counts


async def ping():
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")


# ---------- schedules ----------

SCHEDULE_FIELDS = ("name", "description", "enabled", "schedule", "action", "next_run_at", "last_run_at",
                   "last_status", "last_detail", "run_count", "fail_count", "running")


def _jsonb_fields(fields: dict, allowed, json_keys) -> dict:
    return {k: (Jsonb(v) if k in json_keys else v) for k, v in fields.items() if k in allowed}


async def list_schedules():
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM schedules ORDER BY name")
        return await cur.fetchall()


async def get_schedule(sid):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM schedules WHERE id = %s", (sid,))
        return await cur.fetchone()


async def create_schedule(sid, **fields):
    fields = _jsonb_fields(fields, SCHEDULE_FIELDS, ("schedule", "action"))
    cols = ["id", *fields]
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"INSERT INTO schedules ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
            "ON CONFLICT (id) DO NOTHING RETURNING id", (sid, *fields.values()))
        return (await cur.fetchone()) is not None


async def update_schedule(sid, **fields):
    fields = _jsonb_fields(fields, SCHEDULE_FIELDS, ("schedule", "action"))
    if fields:
        sets = ", ".join(f"{k} = %s" for k in fields)
        async with pool.connection() as conn:
            await conn.execute(f"UPDATE schedules SET {sets}, updated_at = now() WHERE id = %s", (*fields.values(), sid))


async def delete_schedule(sid):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM schedules WHERE id = %s", (sid,))


async def claim_due_schedules():
    """Mark due, enabled, idle tasks as running and return them (safe if several instances poll)."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            """UPDATE schedules SET running = true, updated_at = now()
               WHERE id IN (SELECT id FROM schedules WHERE enabled AND NOT running AND next_run_at <= now()
                            FOR UPDATE SKIP LOCKED)
               RETURNING *""")
        return await cur.fetchall()


async def reset_running_schedules():
    """After a restart nothing is running any more."""
    async with pool.connection() as conn:
        await conn.execute("UPDATE schedules SET running = false WHERE running")


async def start_schedule_run(sid, trigger) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute("INSERT INTO schedule_runs (schedule_id, trigger) VALUES (%s, %s) RETURNING id",
                                 (sid, trigger))
        return (await cur.fetchone())["id"]


async def finish_schedule_run(run_id, status, detail, ref, duration_ms):
    async with pool.connection() as conn:
        await conn.execute("""UPDATE schedule_runs SET finished_at = now(), status = %s, detail = %s, ref = %s,
                              duration_ms = %s WHERE id = %s""", (status, detail, ref, duration_ms, run_id))


async def list_schedule_runs(sid, limit=50):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM schedule_runs WHERE schedule_id = %s ORDER BY started_at DESC LIMIT %s",
                                 (sid, limit))
        return await cur.fetchall()


# ---------- ports ----------

PORT_FIELDS = ("name", "type", "enabled", "config", "target_type", "target_id", "last_poll_at", "last_item",
               "last_error", "ok_count", "fail_count")


async def list_ports():
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM ports ORDER BY name")
        return await cur.fetchall()


async def get_port(pid):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM ports WHERE id = %s", (pid,))
        return await cur.fetchone()


async def create_port(pid, **fields):
    fields = _jsonb_fields(fields, PORT_FIELDS, ("config",))
    cols = ["id", *fields]
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"INSERT INTO ports ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
            "ON CONFLICT (id) DO NOTHING RETURNING id", (pid, *fields.values()))
        return (await cur.fetchone()) is not None


async def update_port(pid, **fields):
    fields = _jsonb_fields(fields, PORT_FIELDS, ("config",))
    if fields:
        sets = ", ".join(f"{k} = %s" for k in fields)
        async with pool.connection() as conn:
            await conn.execute(f"UPDATE ports SET {sets}, updated_at = now() WHERE id = %s", (*fields.values(), pid))


async def delete_port(pid):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM ports WHERE id = %s", (pid,))


async def record_port_event(pid, item, status, detail=None, ref=None):
    ok = status in ("OK", "ACCEPTED")
    if not ok:
        from . import alerts

        alerts.emit("port_error", f"Port {pid} couldn't process {item}", detail or "", port=pid, key=f"port-item:{pid}")
    async with pool.connection() as conn:
        await conn.execute("INSERT INTO port_events (port_id, item, status, detail, ref) VALUES (%s, %s, %s, %s, %s)",
                           (pid, item, status, detail, ref))
        await conn.execute(
            f"""UPDATE ports SET last_item = %s, {'ok_count = ok_count + 1' if ok else 'fail_count = fail_count + 1'},
                last_error = {'last_error' if ok else '%s'} WHERE id = %s""",
            (item, pid) if ok else (item, detail, pid))


async def list_port_events(pid, limit=50):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM port_events WHERE port_id = %s ORDER BY at DESC LIMIT %s", (pid, limit))
        return await cur.fetchall()


# ---------- secrets and audit ----------

async def list_secrets():
    async with pool.connection() as conn:
        cur = await conn.execute(
            """SELECT alias, kind, description, version, created_at, created_by, updated_at, updated_by,
                      (SELECT coalesce(json_agg(c.id ORDER BY c.id), '[]') FROM connections c
                       WHERE c.secret_alias = s.alias) AS used_by
               FROM secrets s ORDER BY alias""")
        return await cur.fetchall()


async def get_secret(alias, with_value=False):
    cols = "*" if with_value else "alias, kind, description, version, created_at, created_by, updated_at, updated_by"
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT {cols} FROM secrets WHERE alias = %s", (alias,))
        return await cur.fetchone()


async def create_secret(alias, kind, description, value_enc, username):
    async with pool.connection() as conn:
        cur = await conn.execute(
            """INSERT INTO secrets (alias, kind, description, value_enc, created_by, updated_by)
               VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (alias) DO NOTHING RETURNING alias""",
            (alias, kind, description, value_enc, username, username))
        return (await cur.fetchone()) is not None


async def update_secret(alias, kind, description, value_enc, username):
    """value_enc None keeps the value; a new value bumps the version."""
    async with pool.connection() as conn:
        if value_enc is None:
            await conn.execute("""UPDATE secrets SET kind = %s, description = %s, updated_at = now(), updated_by = %s
                                  WHERE alias = %s""", (kind, description, username, alias))
        else:
            await conn.execute(
                """UPDATE secrets SET kind = %s, description = %s, value_enc = %s, version = version + 1,
                   updated_at = now(), updated_by = %s WHERE alias = %s""",
                (kind, description, value_enc, username, alias))
        # connections using it are rebuilt with the new value
        await conn.execute("UPDATE connections SET updated_at = now() WHERE secret_alias = %s", (alias,))


async def delete_secret(alias):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM secrets WHERE alias = %s", (alias,))


async def audit(username, action, object_type, object_id=None, detail=None):
    async with pool.connection() as conn:
        await conn.execute("INSERT INTO audit_log (username, action, object_type, object_id, detail) "
                           "VALUES (%s, %s, %s, %s, %s)", (username, action, object_type, object_id, detail))


async def list_audit(limit=200, object_type=None):
    async with pool.connection() as conn:
        if object_type:
            cur = await conn.execute("SELECT * FROM audit_log WHERE object_type = %s ORDER BY at DESC LIMIT %s",
                                     (object_type, limit))
        else:
            cur = await conn.execute("SELECT * FROM audit_log ORDER BY at DESC LIMIT %s", (limit,))
        return await cur.fetchall()


# ---------- alerts ----------

ALERT_CHANNEL_FIELDS = ("name", "type", "config", "secret_alias", "enabled")
ALERT_RULE_FIELDS = ("name", "enabled", "event", "filter", "threshold", "channels", "cooldown_minutes")


async def _insert(table, key, fields, json_keys):
    fields = {k: (Jsonb(v) if k in json_keys else v) for k, v in fields.items()}
    cols = ["id", *fields]
    async with pool.connection() as conn:
        cur = await conn.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
                                 "ON CONFLICT (id) DO NOTHING RETURNING id", (key, *fields.values()))
        return (await cur.fetchone()) is not None


async def _update(table, key, fields, json_keys):
    fields = {k: (Jsonb(v) if k in json_keys else v) for k, v in fields.items()}
    if fields:
        async with pool.connection() as conn:
            await conn.execute(f"UPDATE {table} SET {', '.join(f'{k} = %s' for k in fields)}, updated_at = now() "
                               "WHERE id = %s", (*fields.values(), key))


async def list_alert_channels():
    async with pool.connection() as conn:
        return await (await conn.execute("SELECT * FROM alert_channels ORDER BY name")).fetchall()


async def alert_channels(ids):
    async with pool.connection() as conn:
        return await (await conn.execute("SELECT * FROM alert_channels WHERE id = ANY(%s)", (list(ids or []),))).fetchall()


async def create_alert_channel(cid, **f):
    return await _insert("alert_channels", cid, {k: v for k, v in f.items() if k in ALERT_CHANNEL_FIELDS}, ("config",))


async def update_alert_channel(cid, **f):
    await _update("alert_channels", cid, {k: v for k, v in f.items() if k in ALERT_CHANNEL_FIELDS}, ("config",))


async def update_alert_channel_state(cid, error):
    async with pool.connection() as conn:
        await conn.execute("UPDATE alert_channels SET last_sent_at = CASE WHEN %s::text IS NULL THEN now() "
                           "ELSE last_sent_at END, last_error = %s WHERE id = %s", (error, error, cid))


async def delete_alert_channel(cid):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM alert_channels WHERE id = %s", (cid,))
        await conn.execute("UPDATE alert_rules SET channels = channels - %s", (cid,))


async def list_alert_rules():
    async with pool.connection() as conn:
        return await (await conn.execute("SELECT * FROM alert_rules ORDER BY name")).fetchall()


async def alert_rules_for(event):
    async with pool.connection() as conn:
        return await (await conn.execute("SELECT * FROM alert_rules WHERE enabled AND event = %s", (event,))).fetchall()


async def create_alert_rule(rid, **f):
    return await _insert("alert_rules", rid, {k: v for k, v in f.items() if k in ALERT_RULE_FIELDS},
                         ("filter", "threshold", "channels"))


async def update_alert_rule(rid, **f):
    await _update("alert_rules", rid, {k: v for k, v in f.items() if k in ALERT_RULE_FIELDS},
                  ("filter", "threshold", "channels"))


async def delete_alert_rule(rid):
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM alert_rules WHERE id = %s", (rid,))


async def mark_rule_fired(rid):
    async with pool.connection() as conn:
        await conn.execute("UPDATE alert_rules SET last_fired_at = now(), fired_count = fired_count + 1 WHERE id = %s",
                           (rid,))


async def record_alert(rule_id, event, key, subject, detail, status, results):
    async with pool.connection() as conn:
        await conn.execute("INSERT INTO alert_events (rule_id, event, key, subject, detail, status, results) "
                           "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           (rule_id, event, key, subject, detail, status, Jsonb(results)))


async def alert_sent_since(rule_id, key, since) -> bool:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT 1 FROM alert_events WHERE rule_id = %s AND key = %s AND at > %s "
                                 "AND status <> 'suppressed' LIMIT 1", (rule_id, key, since))
        return (await cur.fetchone()) is not None


async def list_alert_events(limit=200):
    async with pool.connection() as conn:
        return await (await conn.execute("SELECT * FROM alert_events ORDER BY at DESC LIMIT %s", (limit,))).fetchall()


async def count_failed_transactions(minutes, partner=None) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) AS n FROM transactions WHERE status = 'FAILED' AND updated_at > now() - make_interval(mins => %s)"
            + (" AND partner_code = %s" if partner else ""), (minutes, partner) if partner else (minutes,))
        return (await cur.fetchone())["n"]
