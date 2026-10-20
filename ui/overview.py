"""Overview (landing page): server health and resources, services, and today's activity."""
from datetime import datetime

import pandas as pd
import streamlit as st

from . import common
from .common import CRITICAL, GOOD, api


def _bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit in ("B", "KB") else f"{n:.1f} {unit}"
        n /= 1024


def _uptime(seconds: int) -> str:
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    return (f"{d}d " if d else "") + (f"{h}h " if h or d else "") + f"{m}m"


def page():
    common.hero("📈 Overview", "Server health, services and today's activity at a glance.")
    auto = st.toggle("Auto-refresh (10 s)", value=True, key="overview_auto")
    _live() if auto else _dashboard()


@st.fragment(run_every=10)
def _live():
    _dashboard()


def _dashboard():
    o = api("GET", "/api/overview")
    s, c, pg, kafka = o["server"], o["counts"], o["postgres"], o["kafka"]

    # ---- status strip
    consumers_ok = all(v == "running" for v in o["consumers"].values())
    checks = [("PostgreSQL", True), ("Kafka", kafka["up"]), ("Pipeline workers", consumers_ok),
              ("Inbound listeners", o["listeners"]["failing"] == 0),
              ("Adapters", c["connections_failing"] == 0)]
    st.markdown("  ".join(f"{'🟢' if ok else '🔴'} **{name}**" for name, ok in checks))

    # ---- server resources
    st.subheader("🖥️ Server")
    r = st.columns(5)
    r[0].metric("CPU", f"{s['cpu_percent']:.0f}%", help=f"{s['cpu_count']} cores"
                + (f" · load {s['load'][0]} / {s['load'][1]} / {s['load'][2]}" if s["load"] else ""))
    r[1].metric("Memory", f"{s['memory_percent']:.0f}%", help=f"{_bytes(s['memory_used'])} of {_bytes(s['memory_total'])}")
    r[2].metric("Disk", f"{s['disk_percent']:.0f}%", help=f"{_bytes(s['disk_used'])} of {_bytes(s['disk_total'])}")
    r[3].metric("App memory", _bytes(s["app_memory"]), help=f"{s['app_threads']} threads")
    r[4].metric("Uptime", _uptime(s["uptime_seconds"]))
    st.caption(f"{s['host']} · {s['os']} · Python {s['python']}")

    # ---- services
    st.subheader("🧩 Services")
    a, b = st.columns(2)
    with a.container(border=True):
        st.markdown(f"**🐘 PostgreSQL** · database `{pg['database']}`")
        st.caption(pg["version"])
        m = st.columns(2)
        m[0].metric("Size", _bytes(pg["db_bytes"]))
        m[1].metric("Connections", pg["connections"])
    with b.container(border=True):
        st.markdown(f"**🟧 Kafka** · {kafka['bootstrap']}")
        if kafka["up"]:
            m = st.columns(3)
            m[0].metric("Topics", kafka["topics"])
            m[1].metric("Messages", f"{kafka['messages'] / 1000:.1f}k" if kafka["messages"] >= 10000 else f"{kafka['messages']:,}")
            m[2].metric("Waiting", kafka["backlog"])
        else:
            st.error(kafka.get("error", "unreachable"))
    st.caption("Pipeline workers: " + " · ".join(f"{'🟢' if v == 'running' else '🔴'} {k}" for k, v in o["consumers"].items())
               + f" · inbound listeners: {o['listeners']['running']} running"
               + (f", {o['listeners']['failing']} failing" if o["listeners"]["failing"] else ""))

    # ---- platform
    st.subheader("🗂️ Platform")
    p = st.columns(5)
    p[0].metric("Partners", c["partners"], help=f"{c['partners_active']} active")
    p[1].metric("Adapters", c["connections"], help=f"{c['connections_failing']} failing their last test")
    p[2].metric("Workflows", c["workflows"], help=f"{c['workflows_automatic']} triggered automatically")
    p[3].metric("Users", c["users"])
    p[4].metric("Data tables", c["data_tables"])

    # ---- activity
    st.subheader("⚡ Today")
    t = st.columns(5)
    done = c["txn_today"]
    t[0].metric("Transactions", f"{done:,}", help=f"{c['txn_hour']:,} in the last hour · {c['txn_total']:,} all time")
    t[1].metric("Success rate", f"{(done - c['txn_failed_today']) / done * 100:.1f}%" if done else "–")
    t[2].metric("Avg end-to-end", f"{c['txn_avg_ms_today']} ms" if c["txn_avg_ms_today"] is not None else "–")
    t[3].metric("Deliveries", c["delivered_today"], help=f"{c['delivery_failed_today']} failed")
    t[4].metric("Workflow runs", c["wf_runs_today"], help=f"{c['wf_failed_today']} failed · {c['agent_runs_today']} AI agent runs")

    st.markdown("**Transactions per hour** · last 24 hours")
    ph = pd.DataFrame(c["per_hour"]).rename(columns={"completed": "Completed", "failed": "Failed"})
    st.bar_chart(ph, x="hour", y=["Completed", "Failed"], color=[GOOD, CRITICAL], x_label="Hour",
                 y_label="Transactions", height=220)

    st.markdown("**Latest errors**")
    if c["recent_errors"]:
        st.dataframe([{"Time": pd.to_datetime(e["at"]).tz_convert(None).strftime("%Y-%m-%d %H:%M:%S"),
                       "Where": "🔁 Workflow" if e["kind"] == "workflow" else "📨 Transaction",
                       "Partner / workflow": e["what"], "Error": (e["detail"] or "")[:160]}
                      for e in c["recent_errors"]], hide_index=True, width="stretch")
    else:
        st.caption("No errors. 🎉")
    st.caption(f"Updated {datetime.now():%H:%M:%S}")
