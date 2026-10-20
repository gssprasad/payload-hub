"""Ports page: every way data comes in, and file polling / listener ports that feed a partner or a workflow."""
import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

STATE = {"running": "🟢 running", "error": "🔴 error", "starting": "🟡 starting", "stopped": "⚪ stopped",
         "disabled": "⏸️ disabled"}
KIND = {"http": "🌐 HTTP", "ui": "🖥️ Web", "file": "📁 File polling", "database": "🗄️ Database", "ftp": "📂 FTP/SFTP",
        "kafka": "🟧 Kafka", "rabbitmq": "🐇 RabbitMQ", "activemq": "📨 ActiveMQ", "ibmmq": "🏦 IBM MQ"}
MANAGED = {"built-in": "built in", "partner": "partner page", "workflow": "workflow trigger", "port": "this page"}


def page():
    common.hero("📡 Ports", "Every way data comes in - HTTP, queues, topics, tables and folders - and what each one "
                           "feeds. Add file polling ports and listeners here.")
    rows = api("GET", "/api/ports-overview")
    m = st.columns(4)
    m[0].metric("Ports", len(rows))
    m[1].metric("Running", sum(r["state"] == "running" for r in rows))
    m[2].metric("Errors", sum(r["state"] == "error" for r in rows))
    m[3].metric("Disabled", sum(r["state"] in ("disabled", "stopped") for r in rows))
    st.dataframe([{"Type": KIND.get(r["kind"], r["kind"]), "Port": r["name"], "Listening on": r["address"],
                   "Feeds": r["target"], "Status": STATE.get(r["state"], r["state"]),
                   "Managed on": MANAGED.get(r["managed"], r["managed"])} for r in rows],
                 hide_index=True, width="stretch")

    partners = {p["code"]: p["name"] for p in api("GET", "/api/partners")}
    workflows = {w["id"]: w["name"] for w in api("GET", "/api/workflows")["workflows"]}
    conns = api("GET", "/api/connections")
    types = api("GET", "/api/connection-types")
    inbound = {c["id"]: f"{types[c['type']]['icon']} {c['name']}" for c in conns if types[c["type"]]["inbound"]}
    ctx = {"partners": partners, "workflows": workflows, "inbound": inbound, "types": types,
           "conns": {c["id"]: c for c in conns}}

    st.subheader("Your ports")
    if common.can("admin"):
        with st.expander("➕ Add a port"):
            _port_form(None, ctx)
    else:
        st.caption("Only administrators can add or change ports, because they read folders on the server.")
    ports = api("GET", "/api/ports")
    if not ports:
        st.info("No ports yet. A **file polling port** watches a folder on this server and hands each new file to a "
                "partner or a workflow; a **listener** does the same for a queue, topic, FTP folder or table.")
    for p in ports:
        _port_card(p, ctx)


def _target_label(p, ctx) -> str:
    if p["target_type"] == "partner":
        return f"🤝 partner {ctx['partners'].get(p['target_id'], p['target_id'])}"
    return f"⚙️ workflow {ctx['workflows'].get(p['target_id'], p['target_id'])}"


def _port_card(p, ctx):
    cfg, status = p["config"], p["status"]
    with st.container(border=True):
        top = st.columns([4, 2, 2], vertical_alignment="center")
        kind = "📁 File polling" if p["type"] == "file" else "🔌 Listener"
        top[0].markdown(f"#### {p['name']}")
        top[0].caption(f"{kind} → {_target_label(p, ctx)}")
        top[1].markdown(f"**{STATE.get(status['state'] if p['enabled'] else 'disabled', status['state'])}**")
        top[2].markdown(f"✅ {p['ok_count']} · ❌ {p['fail_count']}")
        if p.get("last_poll_at"):
            top[2].caption(f"last poll {pd.to_datetime(p['last_poll_at']).tz_convert(None):%H:%M:%S}")
        if p["type"] == "file":
            st.caption(f"Watching `{cfg['monitor_dir']}` for `{cfg['filter']}` every {cfg['interval_seconds']} s "
                       f"(files at least {cfg['min_age_seconds']} s old, up to {cfg['max_files']} per poll, "
                       f"max {cfg['max_size_mb']} MB) · work `{cfg['work_dir']}` · "
                       + (f"done → `{cfg['completion_dir']}`" if cfg["after_success"] == "move" else "done → deleted")
                       + f" · errors → `{cfg['error_dir']}`")
        else:
            c = ctx["conns"].get(cfg["connection"], {})
            st.caption(f"Listening on {c.get('name', cfg['connection'])} → `{cfg['source']}`")
        if status["state"] == "error" and p["enabled"]:
            st.error(status["detail"])
        elif p.get("last_error") and p["fail_count"]:
            st.caption(f"Last failure: {p['last_error']}")
        b = st.columns([1.4, 1.4, 1.2, 2])
        admin = common.can("admin")
        if b[0].button("⏸️ Disable" if p["enabled"] else "▶️ Enable", key=f"port_en_{p['id']}", disabled=not admin,
                       width="stretch"):
            api("PUT", f"/api/ports/{p['id']}/enabled", json={"enabled": not p["enabled"]})
            st.rerun()
        if p["type"] == "file" and b[1].button("🔄 Poll now", key=f"port_poll_{p['id']}", width="stretch",
                                               disabled=not (p["enabled"] and common.can("write"))):
            api("POST", f"/api/ports/{p['id']}/poll")
            st.toast("Polling - refresh in a moment to see the files", icon="🔄")
        if b[2].button("🗑️ Delete", key=f"port_del_{p['id']}", disabled=not admin, width="stretch"):
            api("DELETE", f"/api/ports/{p['id']}")
            st.rerun()
        with st.expander("Recent files / messages"):
            events = api("GET", f"/api/ports/{p['id']}/events", params={"limit": 20})
            if events:
                st.dataframe([{"When": pd.to_datetime(e["at"]).tz_convert(None).strftime("%Y-%m-%d %H:%M:%S"),
                               "File / message": e["item"], "Result": e["status"], "Detail": e["detail"],
                               "Transaction / run": e["ref"]} for e in events], hide_index=True, width="stretch")
            else:
                st.caption("Nothing yet.")
        if admin:
            with st.expander("✏️ Edit"):
                _port_form(p, ctx)


def _port_form(p, ctx):
    """Add (p=None) or edit a port. The kind and target are chosen outside the form so the fields follow them."""
    key = p["id"] if p else "new"
    kinds = {"file": "📁 File polling - a folder on this server", "listener": "🔌 Listener - a queue, topic, "
                                                                             "FTP/SFTP folder or table"}
    kind = p["type"] if p else st.radio("Kind of port", list(kinds), format_func=kinds.get, horizontal=True,
                                        key=f"port_kind_{key}")
    targets = {"partner": "🤝 A partner (the content is its input payload)",
               "workflow": "⚙️ A workflow (the content starts a run)"}
    target_type = st.radio("Feeds", list(targets), format_func=targets.get, horizontal=True, key=f"port_tt_{key}",
                           index=list(targets).index(p["target_type"]) if p else 0)
    options = ctx["partners"] if target_type == "partner" else ctx["workflows"]
    if not options:
        st.warning(f"Create a {target_type} first.")
        return
    ids = list(options)
    cur = p["target_id"] if p and p["target_id"] in ids else ids[0]
    target_id = st.selectbox("Partner" if target_type == "partner" else "Workflow", ids, ids.index(cur),
                             format_func=options.get, key=f"port_tid_{key}_{target_type}")
    cfg = (p or {}).get("config") or {}
    with st.form(f"port_form_{key}_{kind}"):
        name = st.text_input("Name *", p["name"] if p else "", disabled=bool(p),
                             placeholder="e.g. Orders drop folder")
        if kind == "file":
            monitor = st.text_input("Monitoring folder *", cfg.get("monitor_dir", ""),
                                    placeholder="/Users/you/payload-hub/inbound",
                                    help="A full path on the server running Payload Hub.")
            c1, c2, c3 = st.columns(3)
            work = c1.text_input("Work folder", cfg.get("work_dir", ""), placeholder="<monitoring>/.work")
            done = c2.text_input("Completion folder", cfg.get("completion_dir", ""), placeholder="<monitoring>/done")
            err = c3.text_input("Error folder", cfg.get("error_dir", ""), placeholder="<monitoring>/error")
            c1, c2, c3 = st.columns(3)
            flt = c1.text_input("File name filter", cfg.get("filter", "*"),
                                help="Wildcards, several separated by ';' - e.g. *.json;*.csv")
            interval = c2.number_input("Poll every (seconds)", 2, 86400, int(cfg.get("interval_seconds", 10)))
            age = c3.number_input("Minimum file age (seconds)", 0, 3600, int(cfg.get("min_age_seconds", 5)),
                                  help="Files changed more recently are left alone - they may still be being written.")
            c1, c2, c3 = st.columns(3)
            max_files = c1.number_input("Files per poll", 1, 1000, int(cfg.get("max_files", 20)))
            max_mb = c2.number_input("Maximum size (MB)", 1, 500, int(cfg.get("max_size_mb", 10)))
            after = c3.selectbox("After success", ["move", "delete"],
                                 ["move", "delete"].index(cfg.get("after_success", "move")),
                                 format_func={"move": "Move to the completion folder", "delete": "Delete it"}.get)
            config = {"monitor_dir": monitor, "work_dir": work, "completion_dir": done, "error_dir": err,
                      "filter": flt, "interval_seconds": interval, "min_age_seconds": age, "max_files": max_files,
                      "max_size_mb": max_mb, "after_success": after}
            st.caption("JSON, XML and CSV files are read as data (CSV rows become a list). Failed files go to the "
                       "error folder with a `.error.txt` note saying why.")
        else:
            if not ctx["inbound"]:
                st.warning("Add an inbound-capable connection (MQ, Kafka, FTP/SFTP, database) first.")
            ids_c = list(ctx["inbound"]) or [""]
            conn = st.selectbox("Connection", ids_c, ids_c.index(cfg["connection"]) if cfg.get("connection") in ids_c
                                else 0, format_func=lambda c: ctx["inbound"].get(c, "—"))
            source = st.text_input("Queue / topic / folder / table:key *", cfg.get("source", ""),
                                   help="A database table is polled for new rows as table:key_column, e.g. orders:id")
            config = {"connection": conn, "source": source}
        enabled = st.toggle("Enabled", (p or {}).get("enabled", True))
        if st.form_submit_button("Save port" if p else "Create port", type="primary"):
            body = {"name": name, "type": kind, "enabled": enabled, "config": config, "target_type": target_type,
                    "target_id": target_id}
            try:
                api("PUT" if p else "POST", f"/api/ports/{p['id']}" if p else "/api/ports", json=body)
            except ApiError as e:
                st.error(str(e))
                return
            st.toast("Port saved", icon="📡")
            st.rerun()
