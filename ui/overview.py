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
    common.hero("📈 Overview", "Health and today's activity.")
    auto = st.toggle("Auto-refresh", value=True, key="overview_auto", help="Every 10 seconds")
    _live() if auto else _dashboard()


@st.fragment(run_every=10)
def _live():
    _dashboard()


def _dashboard():
    o = api("GET", "/api/overview")
    s, c, pg, kafka = o["server"], o["counts"], o["postgres"], o["kafka"]

    consumers_ok = all(v == "running" for v in o["consumers"].values())
    checks = [("PostgreSQL", True), ("Kafka", kafka["up"]), ("Pipeline", consumers_ok),
              ("Listeners", o["listeners"]["failing"] == 0), ("Connections", c["connections_failing"] == 0)]
    st.markdown("  ".join(f"{'🟢' if ok else '🔴'} {name}" for name, ok in checks))

    done = c["txn_today"]
    t = st.columns(4)
    t[0].metric("Transactions today", f"{done:,}", help=f"{c['txn_hour']:,} in the last hour · {c['txn_total']:,} all time")
    t[1].metric("Success rate", f"{(done - c['txn_failed_today']) / done * 100:.1f}%" if done else "–",
                help=f"{c['txn_failed_today']} failed today")
    t[2].metric("Avg end-to-end", f"{c['txn_avg_ms_today']} ms" if c["txn_avg_ms_today"] is not None else "–")
    t[3].metric("Workflow runs", c["wf_runs_today"], help=f"{c['wf_failed_today']} failed · "
                                                          f"{c['delivered_today']} deliveries today")

    ph = pd.DataFrame(c["per_hour"]).rename(columns={"completed": "Completed", "failed": "Failed"})
    st.bar_chart(ph, x="hour", y=["Completed", "Failed"], color=[GOOD, CRITICAL], x_label="Last 24 hours",
                 y_label="Transactions", height=220)

    if c["recent_errors"]:
        st.markdown("**Latest errors**")
        st.dataframe([{"Time": pd.to_datetime(e["at"]).tz_convert(common.TZ).tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"),
                       "Where": "🔁 Workflow" if e["kind"] == "workflow" else "📨 Transaction",
                       "Partner / workflow": e["what"], "Error": (e["detail"] or "")[:160]}
                      for e in c["recent_errors"]], hide_index=True, width="stretch")

    with st.expander("System details"):
        r = st.columns(5)
        r[0].metric("CPU", f"{s['cpu_percent']:.0f}%", help=f"{s['cpu_count']} cores"
                    + (f" · load {s['load'][0]} / {s['load'][1]} / {s['load'][2]}" if s["load"] else ""))
        r[1].metric("Memory", f"{s['memory_percent']:.0f}%", help=f"{_bytes(s['memory_used'])} of {_bytes(s['memory_total'])}")
        r[2].metric("Disk", f"{s['disk_percent']:.0f}%", help=f"{_bytes(s['disk_used'])} of {_bytes(s['disk_total'])}")
        r[3].metric("App memory", _bytes(s["app_memory"]), help=f"{s['app_threads']} threads")
        r[4].metric("Uptime", _uptime(s["uptime_seconds"]))
        schema = api("GET", "/api/system/schema")
        st.caption(f"{s['host']} · {s['os']} · Python {s['python']} · database schema {schema['current']}"
                   + (f" ({len(schema['pending'])} pending)" if schema["pending"] else ""))
        st.markdown(f"**PostgreSQL** `{pg['database']}` · {_bytes(pg['db_bytes'])} · {pg['connections']} connections  \n"
                    + (f"**Kafka** {kafka['bootstrap']} · {kafka['topics']} topics · {kafka['messages']:,} messages · "
                       f"{kafka['backlog']} waiting" if kafka["up"] else f"**Kafka** 🔴 {kafka.get('error', 'unreachable')}"))
        st.markdown(f"**Platform** {c['partners']} partners ({c['partners_active']} active) · {c['connections']} "
                    f"connections · {c['workflows']} workflows · {c['users']} users · {c['data_tables']} data tables")
        st.caption("Pipeline workers: " + " · ".join(f"{'🟢' if v == 'running' else '🔴'} {k}" for k, v in o["consumers"].items()))
    st.caption(f"Updated {datetime.now():%H:%M:%S}")
