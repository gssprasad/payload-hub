"""Ports page: every way data comes in. Extra HTTP/HTTPS ports, file polling ports and listeners are added here."""
import html

import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

STATE = {"running": "🟢 Running", "error": "🔴 Error", "starting": "🟡 Starting", "stopped": "⚪ Stopped",
         "disabled": "⏸️ Disabled"}
KIND = {"http": "HTTP", "https": "HTTPS", "ui": "HTTP (web)", "file": "File polling", "database": "Database poll",
        "ftp": "FTP/SFTP poll", "kafka": "Kafka", "rabbitmq": "RabbitMQ", "activemq": "ActiveMQ", "ibmmq": "IBM MQ"}
NEW_KINDS = {"http": "🌐 HTTP / HTTPS port - partner endpoints on their own port number",
             "file": "📁 File polling - watch a folder on this server",
             "listener": "🔌 Listener - a queue, topic, FTP/SFTP folder or database table"}
MANAGED = {"partner": "Partners page (the partner's inbound adapter)", "workflow": "Workflows page (the trigger)",
           "built-in": "built in"}


def page():
    common.hero("📡 Ports", "Every way data comes in - HTTP ports, folders, queues, topics and tables.")
    rows = api("GET", "/api/ports-overview")
    defined = {p["id"]: p for p in api("GET", "/api/ports")}
    top = st.columns([3, 1], vertical_alignment="center")
    errors = sum(r["state"] == "error" for r in rows)
    top[0].caption(f"{len(rows)} ports · 🟢 {sum(r['state'] == 'running' for r in rows)} running"
                   + (f" · 🔴 {errors} with errors" if errors else ""))
    if top[1].button("➕ Add port", type="primary", width="stretch", disabled=not common.can("admin"),
                     help=None if common.can("admin") else "Only administrators manage ports."):
        _add_dialog()
    ids = [r.get("id") or f"_{i}" for i, r in enumerate(rows)]
    pid = common.pick_row([{"Port": r["name"], "Protocol": KIND.get(r["kind"], r["kind"]), "Listening on": r["address"],
                            "Feeds": r["target"], "Status": STATE.get(r["state"], r["state"])} for r in rows],
                          "ports", ids)
    row = rows[ids.index(pid)]
    if row.get("id") in defined:
        _detail(defined[row["id"]])
    else:
        common.detail_title(row["name"])
        where = "Built in - always on." if row["managed"] == "built-in" else \
            f"Managed on the {MANAGED.get(row['managed'], row['managed'])}."
        st.caption(f"{row['address']} → {row['target']}. {where}")


def _ctx():
    conns = api("GET", "/api/connections")
    types = api("GET", "/api/connection-types")
    return {"partners": {p["code"]: p["name"] for p in api("GET", "/api/partners")},
            "workflows": {w["id"]: w["name"] for w in api("GET", "/api/workflows")["workflows"]},
            "inbound": {c["id"]: f"{types[c['type']]['icon']} {c['name']}" for c in conns if types[c["type"]]["inbound"]},
            "conns": {c["id"]: c for c in conns},
            "aliases": [a["alias"] for a in api("GET", "/api/secret-aliases")]}


def _kv(rows):
    st.html('<div class="ph-kv">' + "".join(f"<b>{html.escape(k)}</b><span>{html.escape(str(v))}</span>"
                                            for k, v in rows) + "</div>")


def _detail(p):
    cfg, status = p["config"], p["status"]
    kind = {"http": "HTTP port", "file": "File polling port", "listener": "Listener port"}[p["type"]]
    common.detail_title(f"{p['name']} · {kind}")
    overview, settings, activity = st.tabs(["Overview", "Settings", "Activity"])
    with overview:
        state = status["state"] if p["enabled"] else "disabled"
        rows = [("Status", STATE.get(state, state) + (f" - {status['detail']}" if status.get("detail") else ""))]
        if p["type"] == "http":
            ip = {"all": "everyone", "allow": "only " + ", ".join(cfg["ip_list"]),
                  "deny": "everyone except " + ", ".join(cfg["ip_list"])}[cfg["ip_mode"]]
            rows += [("Port", f"{cfg['protocol'].upper()} {cfg['bind']}:{cfg['port']}"),
                     ("Access mode", "all partner endpoints" if cfg["access_mode"] == "all"
                      else "only " + ", ".join(cfg["partners"])),
                     ("IP access", ip), ("Max request", f"{cfg['max_body_kb']} KB")]
            if stats := status.get("stats"):
                rows.append(("Requests", f"{stats['requests']} since start, {stats['rejected']} rejected"))
        elif p["type"] == "file":
            rows += [("Folder", cfg["monitor_dir"]), ("Files", f"{cfg['filter']} · at least {cfg['min_age_seconds']} s old "
                                                             f"· up to {cfg['max_files']} per poll · max {cfg['max_size_mb']} MB"),
                     ("Polls", f"every {cfg['interval_seconds']} s"), ("Work folder", cfg["work_dir"]),
                     ("After success", f"moved to {cfg['completion_dir']}" if cfg["after_success"] == "move" else "deleted"),
                     ("Errors", cfg["error_dir"])]
            if cfg.get("cleanup_days"):
                rows.append(("Clean-up", f"completed files deleted after {cfg['cleanup_days']} days"))
        else:
            rows.append(("Listens on", f"{cfg['connection']} → {cfg['source']}"))
        if p["type"] != "http":
            rows += [("Feeds", f"{p['target_type']} {p['target_id']}"),
                     ("Processed", f"✅ {p['ok_count']} · ❌ {p['fail_count']}")]
        _kv(rows)
        if status["state"] == "error" and p["enabled"]:
            st.error(status["detail"])
        admin = common.can("admin")
        b = st.columns([1, 1, 1, 2])
        if b[0].button("⏸️ Disable" if p["enabled"] else "▶️ Enable", key=f"port_en_{p['id']}", disabled=not admin,
                       width="stretch"):
            api("PUT", f"/api/ports/{p['id']}/enabled", json={"enabled": not p["enabled"]})
            st.rerun()
        if p["type"] == "file" and b[1].button("🔄 Poll now", key=f"port_poll_{p['id']}", width="stretch",
                                               disabled=not (p["enabled"] and common.can("write"))):
            api("POST", f"/api/ports/{p['id']}/poll")
            st.toast("Polling - open Activity in a moment", icon="🔄")
        if b[2].button("🗑️ Delete", key=f"port_del_{p['id']}", disabled=not admin, width="stretch"):
            api("DELETE", f"/api/ports/{p['id']}")
            st.session_state.pop("ports_selected", None)
            st.rerun()
    with settings:
        if common.can("admin"):
            _form(p["type"], p, _ctx())
        else:
            st.caption("Only administrators can change ports.")
    with activity:
        if p["type"] == "http":
            st.caption("Requests on this port are partner transactions - see the Transactions page.")
        else:
            events = api("GET", f"/api/ports/{p['id']}/events", params={"limit": 30})
            if events:
                st.dataframe([{"When": pd.to_datetime(e["at"]).tz_convert(common.TZ).tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"),
                               "File / message": e["item"], "Result": e["status"], "Detail": e["detail"],
                               "Transaction / run": e["ref"]} for e in events], hide_index=True, width="stretch")
            else:
                st.caption("Nothing received yet.")


@st.dialog("Add a port", width="large")
def _add_dialog():
    kind = st.radio("Kind", list(NEW_KINDS), format_func=NEW_KINDS.get, key="port_new_kind")
    _form(kind, None, _ctx())


def _form(kind, p, ctx):
    """Add (p=None) or edit a port. Choices that change other fields sit outside the form."""
    key = p["id"] if p else "new"
    cfg = (p or {}).get("config") or {}
    target_type = target_id = None
    if kind != "http":
        targets = {"partner": "🤝 A partner (its input payload)", "workflow": "⚙️ A workflow (starts a run)"}
        target_type = st.radio("Feeds", list(targets), format_func=targets.get, horizontal=True, key=f"port_tt_{key}",
                               index=list(targets).index(p["target_type"]) if p else 0)
        options = ctx["partners"] if target_type == "partner" else ctx["workflows"]
        if not options:
            st.warning(f"Create a {target_type} first.")
            return
        ids = list(options)
        cur = p["target_id"] if p and p["target_id"] in ids else ids[0]
        target_id = st.selectbox(target_type.capitalize(), ids, ids.index(cur), format_func=options.get,
                                 key=f"port_tid_{key}_{target_type}")
    with st.form(f"port_form_{key}_{kind}"):
        name = st.text_input("Name (alias) *", p["name"] if p else "", disabled=bool(p),
                             placeholder={"http": "e.g. Partner gateway", "file": "e.g. Orders drop folder",
                                          "listener": "e.g. Orders queue"}[kind])
        config = {"http": _http_fields, "file": _file_fields, "listener": _listener_fields}[kind](cfg, ctx)
        enabled = st.toggle("Enabled", (p or {}).get("enabled", True))
        if st.form_submit_button("Save" if p else "Add port", type="primary"):
            body = {"name": name, "type": kind, "enabled": enabled, "config": config}
            if kind != "http":
                body.update(target_type=target_type, target_id=target_id)
            try:
                saved = api("PUT" if p else "POST", f"/api/ports/{p['id']}" if p else "/api/ports", json=body)
            except ApiError as e:
                st.error(str(e))
                return
            st.session_state["ports_selected"] = saved["id"]
            st.rerun()


def _http_fields(cfg, ctx):
    c1, c2, c3 = st.columns(3)
    port = c1.number_input("Port number *", 1, 65535, int(cfg.get("port") or 8081))
    binds = ["0.0.0.0", "127.0.0.1"]
    bind = c2.selectbox("Listen on", binds, binds.index(cfg.get("bind")) if cfg.get("bind") in binds else 0,
                        format_func={"0.0.0.0": "all networks (0.0.0.0)", "127.0.0.1": "this machine only"}.get)
    protocol = c3.selectbox("Protocol", ["http", "https"], ["http", "https"].index(cfg.get("protocol", "http")),
                            format_func=str.upper)
    c1, c2, c3 = st.columns(3)
    cert = c1.text_input("Certificate file (HTTPS)", cfg.get("cert_file", ""), placeholder="/etc/ssl/hub.crt")
    keyf = c2.text_input("Private key file (HTTPS)", cfg.get("key_file", ""), placeholder="/etc/ssl/hub.key")
    aliases = [""] + ctx["aliases"]
    key_pw = c3.selectbox("Key password", aliases, aliases.index(cfg.get("key_password_alias", ""))
                          if cfg.get("key_password_alias", "") in aliases else 0,
                          format_func=lambda a: f"🔐 {a}" if a else "none")
    st.markdown("**Access mode**")
    modes = {"all": "All partner endpoints", "selected": "Only selected partners"}
    access = st.radio("Access mode", list(modes), list(modes).index(cfg.get("access_mode", "all")),
                      format_func=modes.get, horizontal=True, label_visibility="collapsed")
    partners = st.multiselect("Partners (for 'only selected')", list(ctx["partners"]),
                              [c for c in cfg.get("partners") or [] if c in ctx["partners"]],
                              format_func=ctx["partners"].get)
    st.markdown("**IP access**")
    ip_modes = {"all": "Allow everyone", "allow": "Allow only the addresses below",
                "deny": "Deny the addresses below"}
    ip_mode = st.radio("IP access", list(ip_modes), list(ip_modes).index(cfg.get("ip_mode", "all")),
                       format_func=ip_modes.get, horizontal=True, label_visibility="collapsed")
    ips = st.text_area("Addresses or networks, one per line", "\n".join(cfg.get("ip_list") or []), height=80,
                       placeholder="10.0.0.0/8\n192.168.1.25")
    max_kb = st.number_input("Largest request (KB)", 1, 512000, int(cfg.get("max_body_kb") or 10240))
    st.caption("This port serves only the partner endpoints (POST /inbound/<code>) and /health - not the admin API.")
    return {"port": port, "bind": bind, "protocol": protocol, "cert_file": cert, "key_file": keyf,
            "key_password_alias": key_pw, "access_mode": access, "partners": partners, "ip_mode": ip_mode,
            "ip_list": [x.strip() for x in ips.splitlines() if x.strip()], "max_body_kb": max_kb}


def _file_fields(cfg, ctx):
    monitor = st.text_input("Monitoring folder *", cfg.get("monitor_dir", ""), placeholder="/data/inbound",
                            help="A full path on the server running Payload Hub.")
    c1, c2, c3 = st.columns(3)
    work = c1.text_input("Work folder", cfg.get("work_dir", ""), placeholder="<monitoring>/.work")
    done = c2.text_input("Completion folder", cfg.get("completion_dir", ""), placeholder="<monitoring>/done")
    err = c3.text_input("Error folder", cfg.get("error_dir", ""), placeholder="<monitoring>/error")
    c1, c2, c3 = st.columns(3)
    flt = c1.text_input("File name filter", cfg.get("filter", "*"), help="Wildcards; several separated by ';'")
    interval = c2.number_input("Poll every (s)", 2, 86400, int(cfg.get("interval_seconds", 10)))
    age = c3.number_input("Minimum file age (s)", 0, 3600, int(cfg.get("min_age_seconds", 5)),
                          help="Newer files may still be being written and are left for the next poll.")
    c1, c2, c3, c4 = st.columns(4)
    max_files = c1.number_input("Files per poll", 1, 1000, int(cfg.get("max_files", 20)))
    max_mb = c2.number_input("Max size (MB)", 1, 500, int(cfg.get("max_size_mb", 10)))
    after = c3.selectbox("After success", ["move", "delete"], ["move", "delete"].index(cfg.get("after_success", "move")),
                         format_func={"move": "Move to completion", "delete": "Delete"}.get)
    cleanup = c4.number_input("Clean up after (days)", 0, 3650, int(cfg.get("cleanup_days", 0)),
                              help="Delete completed files older than this. 0 = keep them.")
    return {"monitor_dir": monitor, "work_dir": work, "completion_dir": done, "error_dir": err, "filter": flt,
            "interval_seconds": interval, "min_age_seconds": age, "max_files": max_files, "max_size_mb": max_mb,
            "after_success": after, "cleanup_days": cleanup}


def _listener_fields(cfg, ctx):
    if not ctx["inbound"]:
        st.warning("Add a connection that can receive (MQ, Kafka, FTP/SFTP, database) first.")
    ids = list(ctx["inbound"]) or [""]
    conn = st.selectbox("Connection", ids, ids.index(cfg["connection"]) if cfg.get("connection") in ids else 0,
                        format_func=lambda c: ctx["inbound"].get(c, "—"))
    source = st.text_input("Queue / topic / folder / table:key *", cfg.get("source", ""),
                           help="A database table is polled for new rows as table:key_column, e.g. orders:id")
    return {"connection": conn, "source": source}
