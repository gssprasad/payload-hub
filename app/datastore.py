"""User data tables: create tables, run SQL, and store partner payloads into them.

Everything runs inside the `userdata` schema as the restricted role `payloadhub_data`, which has no
rights on Payload Hub's own tables (users, partners, transactions...). Read-type SQL runs in a
read-only transaction; every statement has a time limit.
"""
import json
import logging
import re

import jmespath
from psycopg import sql
from psycopg.errors import Error as PgError

from . import db
from .transform import TransformError, _OPTIONS, parse

log = logging.getLogger("datastore")

SCHEMA = "userdata"
ROLE = "payloadhub_data"
IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
TYPES = {
    "text": "text", "integer": "integer", "bigint": "bigint", "numeric": "numeric", "boolean": "boolean",
    "date": "date", "timestamp": "timestamptz", "json": "jsonb", "uuid": "uuid",
}
READ_KINDS = {"SELECT", "WITH", "SHOW", "EXPLAIN", "VALUES", "TABLE"}
WRITE_KINDS = {"INSERT", "UPDATE", "CREATE", "ALTER", "COMMENT"}
DELETE_KINDS = {"DELETE", "DROP", "TRUNCATE"}
MAX_ROWS = 1000

role_ok = False  # True when the restricted role could be set up (needs CREATEROLE)


class DataError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


async def init():
    """Create the schema and the restricted role. Without the role, SQL console use is limited to admins."""
    global role_ok
    async with db.pool.connection() as conn:
        try:
            async with conn.transaction():
                await conn.execute(f"""DO $$ BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ROLE}') THEN
                        CREATE ROLE {ROLE} NOLOGIN;
                    END IF; END $$""")
                await conn.execute(f"GRANT {ROLE} TO current_user")
                await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} AUTHORIZATION {ROLE}")
                await conn.execute(f"REVOKE ALL ON SCHEMA public FROM {ROLE}")
            role_ok = True
        except PgError as e:
            log.warning("restricted data role unavailable (%s); SQL console limited to administrators", e)
            async with conn.transaction():
                await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            role_ok = False


async def _scope(conn, read_only: bool = False):
    """Inside a transaction: switch to the restricted role and schema, with a time limit."""
    if read_only:
        await conn.execute("SET TRANSACTION READ ONLY")
    if role_ok:
        await conn.execute(f"SET LOCAL ROLE {ROLE}")
    await conn.execute(f"SET LOCAL search_path = {SCHEMA}")
    await conn.execute("SET LOCAL statement_timeout = '15s'")


def _ident(name: str, what: str = "name") -> str:
    name = (name or "").strip().lower()
    if not IDENT.match(name):
        raise DataError(f"Invalid {what} '{name}': use lowercase letters, digits and _, starting with a letter.")
    return name


# ---------- tables ----------

async def list_tables():
    async with db.pool.connection() as conn:
        cur = await conn.execute(
            """SELECT c.table_name, c.column_name, c.data_type, c.is_nullable = 'YES' AS nullable, c.column_default
               FROM information_schema.columns c WHERE c.table_schema = %s
               ORDER BY c.table_name, c.ordinal_position""", (SCHEMA,))
        tables: dict[str, dict] = {}
        for r in await cur.fetchall():
            t = tables.setdefault(r["table_name"], {"name": r["table_name"], "columns": [], "rows": 0})
            t["columns"].append({"name": r["column_name"], "type": r["data_type"], "nullable": r["nullable"],
                                 "default": r["column_default"]})
        for t in tables.values():
            cur = await conn.execute(sql.SQL("SELECT count(*) AS n FROM {}.{}").format(
                sql.Identifier(SCHEMA), sql.Identifier(t["name"])))
            t["rows"] = (await cur.fetchone())["n"]
        return list(tables.values())


async def create_table(name: str, columns: list[dict], add_tracking: bool = True):
    name = _ident(name, "table name")
    if not columns:
        raise DataError("Add at least one column.")
    defs, seen = [], set()
    has_pk = any(c.get("primary_key") for c in columns)
    if add_tracking and not has_pk:
        defs.append(sql.SQL("id bigserial PRIMARY KEY"))
        seen.add("id")
    for c in columns:
        col = _ident(c.get("name", ""), "column name")
        if col in seen:
            raise DataError(f"Column '{col}' appears twice.")
        seen.add(col)
        type_ = TYPES.get(c.get("type", "text"))
        if not type_:
            raise DataError(f"Unknown type '{c.get('type')}' for column '{col}'.")
        parts = [sql.Identifier(col), sql.SQL(type_)]
        if c.get("primary_key"):
            parts.append(sql.SQL("PRIMARY KEY"))
        elif not c.get("nullable", True):
            parts.append(sql.SQL("NOT NULL"))
        defs.append(sql.SQL(" ").join(parts))
    if add_tracking:
        for col, type_ in (("transaction_id", "uuid"), ("partner_code", "text"),
                           ("stored_at", "timestamptz NOT NULL DEFAULT now()")):
            if col not in seen:
                defs.append(sql.SQL(f"{col} {type_}"))
    stmt = sql.SQL("CREATE TABLE {}.{} ({})").format(sql.Identifier(SCHEMA), sql.Identifier(name),
                                                     sql.SQL(", ").join(defs))
    async with db.pool.connection() as conn:
        try:
            async with conn.transaction():
                await _scope(conn)
                await conn.execute(stmt)
        except PgError as e:
            raise DataError(_pg_message(e)) from e


async def drop_table(name: str):
    name = _ident(name, "table name")
    async with db.pool.connection() as conn:
        try:
            async with conn.transaction():
                await _scope(conn)
                await conn.execute(sql.SQL("DROP TABLE {}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(name)))
        except PgError as e:
            raise DataError(_pg_message(e)) from e


async def rows(name: str, limit: int = 100):
    name = _ident(name, "table name")
    return await _query(sql.SQL("SELECT * FROM {}.{} ORDER BY 1 DESC LIMIT {}").format(
        sql.Identifier(SCHEMA), sql.Identifier(name), sql.Literal(min(limit, MAX_ROWS))), read_only=True)


# ---------- SQL console ----------

def classify(statement: str) -> tuple[str, str]:
    """Return (clean statement, permission needed: read | write | delete). One statement at a time."""
    text = re.sub(r"--[^\n]*", " ", statement)
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S).strip().rstrip(";").strip()
    if not text:
        raise DataError("Enter a SQL statement.")
    if ";" in text:
        raise DataError("Run one statement at a time (remove the extra ';').")
    kind = text.split(None, 1)[0].upper()
    if kind in READ_KINDS:
        return text, "read"
    if kind in WRITE_KINDS:
        return text, "write"
    if kind in DELETE_KINDS:
        return text, "delete"
    raise DataError(f"{kind} statements aren't allowed here. Use SELECT, INSERT, UPDATE, DELETE, "
                    "CREATE/ALTER/DROP TABLE or TRUNCATE.")


async def run(statement: str, permission: str):
    return await _query(sql.SQL(statement), read_only=(permission == "read"))


async def _query(stmt, read_only: bool):
    async with db.pool.connection() as conn:
        try:
            async with conn.transaction():
                await _scope(conn, read_only=read_only)
                cur = await conn.execute(stmt)
                columns = [d.name for d in cur.description] if cur.description else []
                data = [dict(r) for r in await cur.fetchmany(MAX_ROWS)] if columns else []
                return {"columns": columns, "rows": data, "rowcount": cur.rowcount,
                        "status": cur.statusmessage, "truncated": len(data) == MAX_ROWS}
        except PgError as e:
            raise DataError(_pg_message(e)) from e


def _pg_message(e: PgError) -> str:
    msg = (e.diag.message_primary or str(e)).strip()
    if "permission denied" in msg:
        msg += " (the data console can only use tables in its own area, not Payload Hub's own tables)"
    if "read-only transaction" in msg:
        msg = "This statement changes data, so it can't run as a read query."
    return msg


# ---------- storing partner payloads ----------

def build_rows(config: dict, payload: str, fmt: str, txn: dict) -> list[dict]:
    """Evaluate a partner's store mapping against a payload: [{column: value}, ...]."""
    data = parse(payload, fmt)
    root = data
    if config.get("row_path"):
        data = jmespath.search(config["row_path"], data, options=_OPTIONS)
    items = data if isinstance(data, list) else [data]
    specials = {"$transaction_id": str(txn.get("id", "")), "$partner": txn.get("partner_code"),
                "$received_at": str(txn.get("created_at", ""))}
    out = []
    for item in items:
        row = {}
        for col, expr in (config.get("columns") or {}).items():
            expr = (expr or "").strip()
            if not expr:
                continue
            if expr in specials:
                row[col] = specials[expr]
            elif expr.startswith("$root."):
                row[col] = jmespath.search(expr[6:], root, options=_OPTIONS)
            else:
                row[col] = jmespath.search(expr, item, options=_OPTIONS)
        out.append(row)
    return out


async def insert_rows(table: str, rows_: list[dict], txn_id: str | None, partner_code: str | None) -> int:
    table = _ident(table, "table name")
    tables = {t["name"]: t for t in await list_tables()}
    if table not in tables:
        raise DataError(f"Table '{table}' no longer exists.")
    types = {c["name"]: c["type"] for c in tables[table]["columns"]}
    count = 0
    async with db.pool.connection() as conn:
        try:
            async with conn.transaction():
                await _scope(conn)
                for row in rows_:
                    values = dict(row)
                    if "transaction_id" in types and txn_id and "transaction_id" not in values:
                        values["transaction_id"] = txn_id
                    if "partner_code" in types and partner_code and "partner_code" not in values:
                        values["partner_code"] = partner_code
                    unknown = set(values) - set(types)
                    if unknown:
                        raise DataError(f"Table '{table}' has no column(s): {', '.join(sorted(unknown))}")
                    cols = list(values)
                    stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                        sql.Identifier(SCHEMA), sql.Identifier(table),
                        sql.SQL(", ").join(sql.Identifier(c) for c in cols),
                        sql.SQL(", ").join(sql.SQL("%s::{}").format(sql.SQL(_cast(types[c]))) for c in cols))
                    await conn.execute(stmt, [_value(values[c]) for c in cols])
                    count += 1
        except PgError as e:
            raise DataError(_pg_message(e)) from e
    return count


def _cast(data_type: str) -> str:
    return {"timestamp with time zone": "timestamptz", "timestamp without time zone": "timestamp",
            "character varying": "varchar", "double precision": "float8"}.get(data_type, data_type)


def _value(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    return None if v is None else str(v)


async def store_for(partner: dict, txn: dict):
    """Store a completed transaction's data into the partner's table (if enabled). Records an event.
    The table is in the built-in data area, or in an external database connection (store_connection)."""
    from . import adapters, connectors

    cfg = {"row_path": partner.get("store_row_path"), "columns": partner.get("store_columns") or {}}
    source = partner.get("store_source") or "output"
    payload = txn["output_payload"] if source == "output" else txn["input_payload"]
    fmt = txn["output_format"] if source == "output" else txn["input_format"]
    txn_id = str(txn["id"])
    where = f"{partner['store_connection']}:{partner['store_table']}" if partner.get("store_connection") \
        else f"{SCHEMA}.{partner['store_table']}"
    try:
        data_rows = build_rows(cfg, payload, fmt, txn)
        if partner.get("store_connection"):
            adapter = await connectors.adapter_for(partner["store_connection"])
            n = await adapter.insert_rows(partner["store_table"], data_rows, txn_id, partner["code"])
        else:
            n = await insert_rows(partner["store_table"], data_rows, txn_id, partner["code"])
    except (DataError, TransformError, adapters.AdapterError, jmespath.exceptions.JMESPathError) as e:
        await db.record_event(txn_id, "STORE_FAILED", f"{where}: {e}")
        return
    await db.record_event(txn_id, "STORED", f"{n} row(s) into {where}")
