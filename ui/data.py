"""Data page: create tables, browse their rows, and run SQL - all inside the `userdata` area."""
import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

EXAMPLES = {
    "Latest rows": "SELECT * FROM {table} ORDER BY 1 DESC LIMIT 50",
    "Count per partner": "SELECT partner_code, count(*) AS rows FROM {table} GROUP BY partner_code ORDER BY rows DESC",
    "Insert a row": "INSERT INTO {table} ({first_column}) VALUES ('example')",
    "Update rows": "UPDATE {table} SET {first_column} = 'changed' WHERE {first_column} = 'example'",
    "Delete rows": "DELETE FROM {table} WHERE {first_column} = 'example'",
}


BUILTIN = ""


def _base(target: str) -> str:
    """API prefix for the chosen database: the built-in data area, or a database connection."""
    return "/api/data" if target == BUILTIN else f"/api/connections/{target}"


def page():
    common.hero("🗄️ Data", "Create tables for your integration data, see what partners stored, and query or "
                          "change it with SQL - in the built-in data area or any connected database.")
    status = api("GET", "/api/data/status")
    dbs = {c["id"]: c for c in api("GET", "/api/connections") if c["type"] == "database"}
    options = [BUILTIN, *dbs]
    target = st.selectbox(
        "Database", options, key="data_target",
        format_func=lambda t: "🏠 Payload Hub data area (built-in, schema userdata)" if t == BUILTIN else
        f"🗄️ {dbs[t]['name']} · {common.connection_address(dbs[t]).replace(' → table ', '')}",
        help="Add PostgreSQL, MySQL, SQL Server, Oracle or SQLite databases on the Adapters & Connections page.")
    if target == BUILTIN and not status["restricted_role"]:
        st.warning("This database didn't allow creating the restricted data role, so the SQL console is limited "
                   "to administrators.")
    if target != BUILTIN:
        st.caption("Changes run with this connection's database user, so that user's own rights also apply. "
                   "Read queries are always rolled back.")
    try:
        tables = api("GET", f"{_base(target)}/tables")
    except ApiError as e:
        st.error(f"Can't read this database: {e}")
        return
    c = st.columns(3)
    c[0].metric("Tables", len(tables))
    c[1].metric("Rows stored", sum(t["rows"] for t in tables))
    c[2].metric("Partners storing", sum(1 for p in common.partners() if p.get("store_enabled")))

    browse, design, modify, console = st.tabs(["📋 Tables", "🧱 Table designer", "🛠️ Modify a table", "🧮 SQL console"])
    with browse:
        _browse(target, tables)
    with design:
        _designer(target)
    with modify:
        _modify(target, tables)
    with console:
        _console(target, tables)


def _browse(target, tables):
    if not tables:
        st.info("No tables yet - create one on the **Table designer** tab.")
        return
    users_of = {}
    for p in common.partners():
        if p.get("store_enabled") and p.get("store_table"):
            users_of.setdefault(p["store_table"], []).append(p["name"])
    prefix = "userdata." if target == BUILTIN else ""
    for t in tables:
        fillers = users_of.get(t["name"], []) if target == BUILTIN else []
        with st.expander(f"**{prefix}{t['name']}** · {t['rows']} rows · {len(t['columns'])} columns"
                         + (f" · filled by {', '.join(fillers)}" if fillers else "")):
            cols, data = st.tabs(["Columns", "Rows"])
            with cols:
                st.dataframe(pd.DataFrame(t["columns"]), hide_index=True, width="stretch",
                             column_config={"name": "Column", "type": "Type", "nullable": "Nullable",
                                            "default": "Default"})
            with data:
                if t["rows"]:
                    r = api("GET", f"{_base(target)}/tables/{t['name']}/rows", params={"limit": 100})
                    st.dataframe(pd.DataFrame(r["rows"], columns=r["columns"]), hide_index=True, width="stretch")
                    if t["rows"] > 100:
                        st.caption("Showing the latest 100 rows - use the SQL console for more.")
                else:
                    st.caption("No rows yet.")
            if st.checkbox(f"I want to delete table {t['name']} and all its rows", key=f"drop_ok_{target}_{t['name']}") \
                    and st.button("Delete table", key=f"drop_{target}_{t['name']}", disabled=not common.can("delete")):
                try:
                    api("DELETE", f"{_base(target)}/tables/{t['name']}")
                except ApiError as e:
                    st.error(str(e))
                else:
                    st.toast(f"Deleted {t['name']}", icon="🗑️")
                    st.rerun()


DESIGN_TYPES = ["varchar", "text", "integer", "bigint", "numeric", "boolean", "date", "timestamp", "json", "uuid"]
STARTER = [
    {"name": "id", "type": "bigint", "length": None, "precision": None, "scale": None, "primary_key": True,
     "auto_increment": True, "nullable": False, "unique": False, "default": ""},
    {"name": "order_id", "type": "varchar", "length": 40, "precision": None, "scale": None, "primary_key": False,
     "auto_increment": False, "nullable": False, "unique": True, "default": ""},
    {"name": "amount", "type": "numeric", "length": None, "precision": 12, "scale": 2, "primary_key": False,
     "auto_increment": False, "nullable": True, "unique": False, "default": "0"},
    {"name": "created_at", "type": "timestamp", "length": None, "precision": None, "scale": None,
     "primary_key": False, "auto_increment": False, "nullable": False, "unique": False, "default": "now"},
]


def _designer_target(target: str) -> str:
    return "builtin" if target == BUILTIN else target


def _designer(target):
    st.caption("Design the columns, tick **PK** for the primary key (tick several for a composite key), and watch "
               "the SQL update below. Length applies to varchar; precision/scale to numeric. Default can be a value, "
               "`now` or `true`/`false`.")
    c1, c2 = st.columns([2, 1])
    name = c1.text_input("Table name", placeholder="e.g. customer_orders", key=f"design_name_{target}")
    tracking = c2.toggle("Add tracking columns", value=False, key=f"design_tracking_{target}",
                         help="Adds transaction_id, partner_code and stored_at (and an id key if you have none), "
                              "so rows stored by partners can be traced back to their message.")
    if f"design_cols_{target}" not in st.session_state:
        st.session_state[f"design_cols_{target}"] = pd.DataFrame(STARTER)
    edited = st.data_editor(
        st.session_state[f"design_cols_{target}"], num_rows="dynamic", hide_index=True, width="stretch",
        key=f"design_editor_{target}",
        column_order=["primary_key", "name", "type", "length", "precision", "scale", "nullable", "unique",
                      "auto_increment", "default"],
        column_config={
            "primary_key": st.column_config.CheckboxColumn("🔑 PK", help="Part of the primary key", width="small"),
            "name": st.column_config.TextColumn("Column", required=True),
            "type": st.column_config.SelectboxColumn("Type", options=DESIGN_TYPES, required=True, default="varchar"),
            "length": st.column_config.NumberColumn("Length", min_value=1, max_value=10000, step=1),
            "precision": st.column_config.NumberColumn("Precision", min_value=1, max_value=38, step=1),
            "scale": st.column_config.NumberColumn("Scale", min_value=0, max_value=30, step=1),
            "nullable": st.column_config.CheckboxColumn("Can be empty", default=True),
            "unique": st.column_config.CheckboxColumn("Unique"),
            "auto_increment": st.column_config.CheckboxColumn("Auto ++", help="Auto-numbering (integer/bigint PK)"),
            "default": st.column_config.TextColumn("Default"),
        })
    columns = []
    for row in edited.to_dict("records"):
        if not str(row.get("name") or "").strip():
            continue
        col = {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for flag in ("primary_key", "nullable", "unique", "auto_increment"):
            col[flag] = bool(col.get(flag))
        columns.append(col)
    pk = [c["name"] for c in columns if c["primary_key"]]
    st.markdown("**Primary key:** " + (" + ".join(f"`{c}`" for c in pk) if pk else
                                       "_none_" + (" (an `id` key will be added)" if tracking else "")))
    spec = {"name": name or "new_table", "columns": columns, "add_tracking": tracking}
    preview = api("POST", f"/api/db/{_designer_target(target)}/ddl", json=spec)
    st.markdown("**SQL that will run**")
    if preview.get("sql"):
        st.code(preview["sql"], language="sql")
    else:
        st.warning(preview.get("error"))
    if st.button("Create table", type="primary", disabled=not (common.can("write") and name and preview.get("sql")),
                 key=f"design_create_{target}"):
        try:
            api("POST", f"/api/db/{_designer_target(target)}/tables", json=spec)
        except ApiError as e:
            st.error(str(e))
            return
        st.session_state.pop(f"design_cols_{target}", None)
        st.toast(f"Created table {name}", icon="✅")
        st.rerun()


def _modify(target, tables):
    if not tables:
        st.info("No tables yet - create one in the Table designer.")
        return
    t = st.selectbox("Table", [x["name"] for x in tables], key=f"modify_table_{target}")
    dt = _designer_target(target)
    try:
        sc = api("GET", f"/api/db/{dt}/tables/{t}/schema")
    except ApiError as e:
        st.error(str(e))
        return
    st.dataframe([{"🔑": "🔑" if c["primary_key"] else "", "column": c["name"], "type": c["type"],
                   "can be empty": c["nullable"], "default": c["default"]} for c in sc["columns"]],
                 hide_index=True, width="stretch")
    st.caption("Primary key: " + (" + ".join(sc["primary_key"]["columns"]) or "none")
               + (" · Unique: " + "; ".join("+".join(u["columns"]) for u in sc["unique"]) if sc["unique"] else ""))
    names = [c["name"] for c in sc["columns"]]

    def run(body, ok):
        try:
            r = api("POST", f"/api/db/{dt}/tables/{t}/alter", json=body)
        except ApiError as e:
            st.error(str(e))
            return
        st.toast(ok, icon="🛠️")
        st.caption(f"Ran: `{r['ran']}`")
        st.rerun()

    a, b = st.columns(2)
    with a.form(f"add_col_{target}_{t}"):
        st.markdown("**➕ Add a column**")
        cname = st.text_input("Name")
        ctype = st.selectbox("Type", DESIGN_TYPES)
        x1, x2 = st.columns(2)
        length = x1.number_input("Length (varchar)", 1, 10000, 255)
        nullable = x2.checkbox("Can be empty", True)
        default = st.text_input("Default (optional)")
        if st.form_submit_button("Add column", disabled=not common.can("write")):
            run({"action": "add_column", "column": {"name": cname, "type": ctype, "length": length,
                                                    "nullable": nullable, "default": default}}, f"Added {cname}")
    with b.form(f"pk_{target}_{t}"):
        st.markdown("**🔑 Primary key**")
        new_pk = st.multiselect("Columns (in order)", names, default=sc["primary_key"]["columns"])
        k1, k2 = st.columns(2)
        if k1.form_submit_button("Set primary key", disabled=not common.can("write")):
            run({"action": "set_primary_key", "columns": new_pk}, "Primary key updated")
        if k2.form_submit_button("Remove primary key", disabled=not common.can("delete")):
            run({"action": "drop_primary_key"}, "Primary key removed")
    c, d = st.columns(2)
    with c.form(f"uq_{target}_{t}"):
        st.markdown("**Unique constraint**")
        uq = st.multiselect("Columns that must be unique together", names)
        if st.form_submit_button("Add unique", disabled=not common.can("write")):
            run({"action": "add_unique", "columns": uq}, "Unique constraint added")
        if sc["unique"]:
            drop_uq = st.selectbox("Remove", [u["name"] for u in sc["unique"]])
            if st.form_submit_button("Remove unique", disabled=not common.can("delete")):
                run({"action": "drop_unique", "name": drop_uq}, "Unique constraint removed")
    with d.form(f"drop_col_{target}_{t}"):
        st.markdown("**🗑️ Drop a column**")
        col = st.selectbox("Column", names)
        sure = st.checkbox("I understand the column's data is deleted")
        if st.form_submit_button("Drop column", disabled=not common.can("delete")):
            if sure:
                run({"action": "drop_column", "column": col}, f"Dropped {col}")
            else:
                st.warning("Tick the confirmation first.")


def _console(target, tables):
    perms = [p for p in ("read", "write", "delete") if common.can(p)]
    st.caption("One statement at a time, inside the userdata area. SELECT needs **read**; INSERT, UPDATE, CREATE and "
               "ALTER need **write**; DELETE, DROP and TRUNCATE need **delete**. You have: "
               + ", ".join(f"**{p}**" for p in perms) + ".")
    if tables:
        t = tables[0]
        first = next((c["name"] for c in t["columns"] if c["name"] not in ("id",)), "id")
        choice = st.pills("Examples", list(EXAMPLES), key="sql_example")
        if choice and st.session_state.get("sql_example_applied") != choice:
            st.session_state.sql_text = EXAMPLES[choice].format(table=t["name"], first_column=first)
            st.session_state.sql_example_applied = choice
    sql_text = st.text_area("SQL", key="sql_text", height=140, placeholder="SELECT * FROM my_table LIMIT 20")
    if st.button("▶ Run", type="primary"):
        try:
            r = api("POST", f"{_base(target)}/query", json={"sql": sql_text})
        except ApiError as e:
            st.error(str(e))
            return
        st.success(f"{r['status']} · ran with {r['permission']} permission"
                   + (f" · {len(r['rows'])} rows shown" if r["columns"] else "")
                   + (" (first 1000)" if r.get("truncated") else ""))
        if r["columns"]:
            st.dataframe(pd.DataFrame(r["rows"], columns=r["columns"]), hide_index=True, width="stretch")
