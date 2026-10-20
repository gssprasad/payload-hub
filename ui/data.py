"""Data page: create tables, browse their rows, and run SQL - all inside the `userdata` area."""
import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

EXAMPLES = {
    "Latest rows": "SELECT * FROM {table} ORDER BY stored_at DESC LIMIT 50",
    "Count per partner": "SELECT partner_code, count(*) AS rows FROM {table} GROUP BY partner_code ORDER BY rows DESC",
    "Insert a row": "INSERT INTO {table} ({first_column}) VALUES ('example')",
    "Update rows": "UPDATE {table} SET {first_column} = 'changed' WHERE {first_column} = 'example'",
    "Delete rows": "DELETE FROM {table} WHERE {first_column} = 'example'",
}


def page():
    common.hero("🗄️ Data", "Create tables for your integration data, see what partners stored, and query or "
                          "change it with SQL.")
    status = api("GET", "/api/data/status")
    if not status["restricted_role"]:
        st.warning("This database didn't allow creating the restricted data role, so the SQL console is limited "
                   "to administrators.")
    tables = api("GET", "/api/data/tables")
    c = st.columns(3)
    c[0].metric("Tables", len(tables))
    c[1].metric("Rows stored", sum(t["rows"] for t in tables))
    c[2].metric("Partners storing", sum(1 for p in common.partners() if p.get("store_enabled")))

    browse, create, console = st.tabs(["📋 Tables", "➕ Create a table", "🧮 SQL console"])
    with browse:
        _browse(tables)
    with create:
        _create(status["types"])
    with console:
        _console(tables)


def _browse(tables):
    if not tables:
        st.info("No tables yet - create one on the **Create a table** tab.")
        return
    users_of = {}
    for p in common.partners():
        if p.get("store_enabled") and p.get("store_table"):
            users_of.setdefault(p["store_table"], []).append(p["name"])
    for t in tables:
        with st.expander(f"**userdata.{t['name']}** · {t['rows']} rows · {len(t['columns'])} columns"
                         + (f" · filled by {', '.join(users_of[t['name']])}" if t["name"] in users_of else "")):
            cols, data = st.tabs(["Columns", "Rows"])
            with cols:
                st.dataframe(pd.DataFrame(t["columns"]), hide_index=True, width="stretch",
                             column_config={"name": "Column", "type": "Type", "nullable": "Nullable",
                                            "default": "Default"})
            with data:
                if t["rows"]:
                    r = api("GET", f"/api/data/tables/{t['name']}/rows", params={"limit": 100})
                    st.dataframe(pd.DataFrame(r["rows"], columns=r["columns"]), hide_index=True, width="stretch")
                    if t["rows"] > 100:
                        st.caption("Showing the latest 100 rows - use the SQL console for more.")
                else:
                    st.caption("No rows yet.")
            if st.checkbox(f"I want to delete table {t['name']} and all its rows", key=f"drop_ok_{t['name']}") and \
                    st.button("Delete table", key=f"drop_{t['name']}", disabled=not common.can("delete")):
                try:
                    api("DELETE", f"/api/data/tables/{t['name']}")
                except ApiError as e:
                    st.error(str(e))
                else:
                    st.toast(f"Deleted {t['name']}", icon="🗑️")
                    st.rerun()


def _create(types):
    st.caption("Names use lowercase letters, digits and _. Each table also gets id, transaction_id, partner_code "
               "and stored_at columns (unless you untick that), so stored rows can be traced back to their message.")
    name = st.text_input("Table name", placeholder="e.g. order_lines", key="new_table_name")
    if "new_table_cols" not in st.session_state:
        st.session_state.new_table_cols = pd.DataFrame(
            [{"name": "order_id", "type": "text", "nullable": False, "primary_key": False},
             {"name": "sku", "type": "text", "nullable": True, "primary_key": False},
             {"name": "quantity", "type": "integer", "nullable": True, "primary_key": False},
             {"name": "amount", "type": "numeric", "nullable": True, "primary_key": False}])
    edited = st.data_editor(
        st.session_state.new_table_cols, num_rows="dynamic", hide_index=True, width="stretch", key="new_table_editor",
        column_config={"name": st.column_config.TextColumn("Column name", required=True),
                       "type": st.column_config.SelectboxColumn("Type", options=types, required=True),
                       "nullable": st.column_config.CheckboxColumn("Can be empty"),
                       "primary_key": st.column_config.CheckboxColumn("Primary key")})
    tracking = st.checkbox("Add tracking columns (id, transaction_id, partner_code, stored_at)", value=True)
    if st.button("Create table", type="primary", disabled=not common.can("write")):
        columns = [{k: (bool(v) if k in ("nullable", "primary_key") else v) for k, v in row.items()}
                   for row in edited.to_dict("records") if str(row.get("name") or "").strip()]
        try:
            api("POST", "/api/data/tables", json={"name": name, "columns": columns, "add_tracking": tracking})
        except ApiError as e:
            st.error(str(e))
            return
        st.session_state.pop("new_table_cols", None)
        st.toast(f"Created userdata.{name.strip().lower()}", icon="✅")
        st.rerun()


def _console(tables):
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
            r = api("POST", "/api/data/query", json={"sql": sql_text})
        except ApiError as e:
            st.error(str(e))
            return
        st.success(f"{r['status']} · ran with {r['permission']} permission"
                   + (f" · {len(r['rows'])} rows shown" if r["columns"] else "")
                   + (" (first 1000)" if r.get("truncated") else ""))
        if r["columns"]:
            st.dataframe(pd.DataFrame(r["rows"], columns=r["columns"]), hide_index=True, width="stretch")
