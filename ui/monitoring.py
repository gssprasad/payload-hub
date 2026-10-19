"""Monitoring page: overall and per-partner health, throughput, latency and Kafka queue backlog."""
from datetime import datetime

import pandas as pd
import streamlit as st

from . import common
from .common import CRITICAL, GOOD, api, status_label


def page():
    st.title("📊 Monitoring")
    partner_names = {p["code"]: p["name"] for p in common.partners()}
    c1, c2 = st.columns([2, 1])
    scope = c1.selectbox("Show", ["", "-", *partner_names],
                         format_func=lambda c: {"": "All partners", "-": "Ad-hoc (no partner)"}.get(c) or partner_names[c])
    auto = c2.toggle("Auto-refresh (5 s)", value=True)
    _dashboard_live(scope, partner_names) if auto else _dashboard(scope, partner_names)


@st.fragment(run_every=5)
def _dashboard_live(scope, partner_names):
    _dashboard(scope, partner_names)


def _dashboard(scope, partner_names):
    s = api("GET", "/api/stats", params={"partner": scope} if scope else None)
    done = s["completed"] + s["failed"]
    row1, row2 = st.columns(3), st.columns(3)
    row1[0].metric("Total", s["total"])
    row1[1].metric("Completed", s["completed"])
    row1[2].metric("Failed", s["failed"])
    row2[0].metric("Success rate", f"{s['completed'] / done * 100:.1f}%" if done else "–")
    row2[1].metric("Avg end-to-end", f"{s['avg_ms']} ms" if s["avg_ms"] is not None else "–")
    row2[2].metric("p95 end-to-end", f"{s['p95_ms']} ms" if s["p95_ms"] is not None else "–")

    st.markdown("**Transactions per minute** · last 30 minutes")
    pm = pd.DataFrame(s["per_minute"]).rename(columns={"completed": "Completed", "failed": "Failed"})
    st.bar_chart(pm, x="minute", y=["Completed", "Failed"], color=[GOOD, CRITICAL],
                 x_label="Minute", y_label="Transactions", height=240)

    st.markdown("**By partner**")
    by = pd.DataFrame(api("GET", "/api/stats/partners"))
    if not by.empty:
        by["partner"] = by.apply(lambda r: r["name"] or partner_names.get(r["partner_code"], r["partner_code"]), axis=1)
        by["queue"] = by["queue_mode"].map({"shared": "Shared", "dedicated": "Dedicated"}).fillna("Shared")
        by["success"] = by.apply(lambda r: (r["completed"] / (r["completed"] + r["failed"]) * 100)
                                 if (r["completed"] + r["failed"]) else None, axis=1)
        by["last_seen"] = pd.to_datetime(by["last_seen"]).dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S")
        st.dataframe(by[["partner", "queue", "total", "completed", "failed", "in_flight", "success", "avg_ms",
                         "delivered", "delivery_failed", "last_seen"]],
                     hide_index=True, width="stretch",
                     column_config={"partner": "Partner", "queue": "Queue", "total": "Total", "completed": "Completed",
                                    "failed": "Failed", "in_flight": "In flight",
                                    "success": st.column_config.ProgressColumn("Success rate", format="%.0f%%",
                                                                               min_value=0, max_value=100),
                                    "avg_ms": st.column_config.NumberColumn("Avg (ms)", format="%.1f"),
                                    "delivered": "Delivered", "delivery_failed": "Delivery failed",
                                    "last_seen": "Last transaction"})

    st.markdown("**Adapters**")
    listeners = api("GET", "/api/adapters/status")
    conns = api("GET", "/api/connections")
    rows = [{"Adapter": f"📥 {code} ← {l['connection']}:{l['source']}", "Status": {"running": "🟢 running",
             "error": "🔴 error"}.get(l["state"], l["state"]), "Detail": l["detail"]} for code, l in listeners.items()]
    rows += [{"Adapter": f"🔌 {c['name']} ({c['type']})",
              "Status": "⚪ not tested" if c["last_test_ok"] is None else "🟢 connected" if c["last_test_ok"] else "🔴 failing",
              "Detail": c["last_test_detail"] or ""} for c in conns]
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    else:
        st.caption("No connections or inbound listeners yet.")

    st.markdown("**Kafka queues**")
    q = pd.DataFrame(api("GET", "/api/queues"))
    if not q.empty:
        q["kind"] = q["kind"].map({"in": "Input", "out": "Output", "dlq": "Failures (DLQ)"})
        q["backlog"] = q["backlog"].map(lambda v: "–" if pd.isna(v) else f"{int(v)}")
        st.dataframe(q[["topic", "partners", "kind", "partitions", "messages", "backlog"]], hide_index=True,
                     width="stretch",
                     column_config={"topic": "Topic", "partners": "Partner(s)", "kind": "Role",
                                    "partitions": "Partitions", "messages": "Messages", "backlog": "Waiting"})
        st.caption("Messages = total kept in the topic. Waiting = messages not yet processed by the pipeline.")

    st.markdown("**Recent AI agent runs**")
    runs = api("GET", "/api/agent/runs", params={"limit": 10})
    if runs:
        rdf = pd.DataFrame(runs)
        rdf["created_at"] = pd.to_datetime(rdf["created_at"]).dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S")
        rdf["status"] = rdf["status"].map(status_label)
        rdf["partner"] = rdf["partner_code"].map(lambda c: partner_names.get(c, c) if c else "–")
        st.dataframe(rdf[["created_at", "partner", "mapping_doc_name", "status", "tool_calls", "input_tokens",
                          "output_tokens", "duration_ms"]], hide_index=True, width="stretch",
                     column_config={"created_at": "Time", "partner": "Partner", "mapping_doc_name": "Documents",
                                    "status": "Status", "tool_calls": "Tool calls", "input_tokens": "Tokens in",
                                    "output_tokens": "Tokens out", "duration_ms": "Duration (ms)"})
    else:
        st.caption("No agent runs yet.")
    st.caption(f"Updated {datetime.now():%H:%M:%S}")
