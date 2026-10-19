"""Transactions page: search and inspect the transaction log."""
from datetime import datetime, time

import pandas as pd
import streamlit as st

from . import common
from .common import STATUSES, api, lang, show_events, status_label


def page():
    st.title("🔎 Transactions")
    partner_names = {p["code"]: p["name"] for p in common.partners()}
    with st.form("search"):
        c1, c2, c3 = st.columns([3, 1.4, 1])
        q = c1.text_input("Search", placeholder="Transaction ID, partner, mapping, or any value in input / output / error")
        partner = c2.selectbox("Partner", ["", "-", *partner_names],
                               format_func=lambda c: {"": "All partners", "-": "Ad-hoc (no partner)"}.get(c) or partner_names[c])
        status = c3.selectbox("Status", ["All"] + STATUSES)
        c4, c5, c6, c7 = st.columns(4)
        since_d = c4.date_input("From date", value=None)
        since_t = c5.time_input("From time", value=time(0, 0))
        until_d = c6.date_input("To date", value=None)
        until_t = c7.time_input("To time", value=time(23, 59))
        st.form_submit_button("Search", type="primary")

    params = {"limit": 200}
    if q.strip():
        params["q"] = q.strip()
    if partner:
        params["partner"] = partner
    if status != "All":
        params["status"] = status
    if since_d:
        params["since"] = datetime.combine(since_d, since_t).astimezone().isoformat()
    if until_d:
        params["until"] = datetime.combine(until_d, until_t).astimezone().isoformat()
    txns = api("GET", "/api/transactions", params=params)
    total = txns[0]["matches"] if txns else 0
    st.caption(f"{total} transaction{'s' if total != 1 else ''}" + (f" · showing latest {len(txns)}" if total > len(txns) else ""))
    if not txns:
        st.info("No transactions match this search.")
        return

    df = pd.DataFrame(txns)
    df["created_at"] = pd.to_datetime(df["created_at"]).dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["status"] = df["status"].map(status_label)
    df["partner"] = df["partner_code"].map(lambda c: partner_names.get(c, c) if c else "Ad-hoc")
    df["formats"] = df["input_format"] + " → " + df["output_format"]
    view = df[["created_at", "id", "partner", "topic", "mapping_name", "formats", "status", "transform_ms", "total_ms", "error"]]
    event = st.dataframe(
        view, hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row",
        column_config={
            "created_at": "Time", "id": "Transaction", "partner": "Partner", "topic": "Queue", "mapping_name": "Mapping",
            "formats": "Formats", "status": "Status", "error": "Error",
            "transform_ms": st.column_config.NumberColumn("Transform (ms)", format="%.1f"),
            "total_ms": st.column_config.NumberColumn("End-to-end (ms)", format="%.1f"),
        })
    rows = event.selection.rows
    if not rows:
        st.caption("Select a row to see its full input, output and event timeline.")
        return
    t = api("GET", f"/api/transactions/{txns[rows[0]]['id']}")
    st.subheader(f"Transaction {t['id']}")
    who = partner_names.get(t["partner_code"], t["partner_code"]) if t["partner_code"] else "Ad-hoc"
    st.markdown(f"{status_label(t['status'])} · {who} · queue `{t['topic'] or '-'}` · "
                f"{t['input_format']} → {t['output_format']}")
    show_events(t["events"])
    if t.get("error"):
        st.error(t["error"])
    a, b = st.columns(2)
    a.markdown("**Input**")
    a.code(t["input_payload"], language=lang(t["input_format"]))
    b.markdown("**Output**")
    b.code(t.get("output_payload") or "–", language=lang(t["output_format"]))
    st.markdown("**Mapping expression**")
    st.code(t["expression"], language="text")
