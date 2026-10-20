"""Connections page: adapters to RabbitMQ, ActiveMQ, IBM MQ, HTTP endpoints and Kafka."""
import json

import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

PRESETS = {
    "rabbitmq": "Local RabbitMQ",
    "activemq": "Local ActiveMQ",
    "ibmmq": "IBM MQ (dev queue manager)",
    "http": "Partner webhook",
    "kafka": "Local Kafka",
    "database": "Partner database",
    "ftp": "Partner file server",
}


def page():
    common.hero("🔌 Adapters & Connections", "Connect message brokers (RabbitMQ, ActiveMQ, IBM MQ, Kafka) and HTTP "
                "APIs once; partners use them to receive messages, deliver results and forward requests.")
    types = api("GET", "/api/connection-types")
    conns = api("GET", "/api/connections")

    _summary(conns, types)
    _add_connection(types)
    st.divider()
    if not conns:
        st.info("No connections yet - add one above.")
    for c in conns:
        _connection_card(c, types)
    st.divider()
    _test_receiver()


def _summary(conns, types):
    cols = st.columns(4)
    cols[0].metric("Connections", len(conns))
    cols[1].metric("Healthy", sum(1 for c in conns if c["last_test_ok"]))
    cols[2].metric("Failing", sum(1 for c in conns if c["last_test_ok"] is False))
    cols[3].metric("In use", sum(1 for c in conns if c["used_by"]))


def _status_badge(c) -> str:
    if c["last_test_ok"] is None:
        return "⚪ Not tested"
    return "🟢 Connected" if c["last_test_ok"] else "🔴 Failing"


DB_PORTS = {"postgresql": 5432, "mysql": 3306, "sqlserver": 1433, "oracle": 1521, "sqlite": 0}


def _fields_form(type_key, spec, values, key_prefix, editing=False, fixed=None):
    """Render one input per field spec (connection fields, then the retry policy); returns (config, secret).
    fixed: values chosen outside the form (e.g. the database type), not rendered again."""
    config, secret = dict(fixed or {}), None
    main = [f for f in spec["fields"] if f.get("group") != "retry" and f["key"] not in (fixed or {})]
    retry = [f for f in spec["fields"] if f.get("group") == "retry"]
    secret = _render_fields(main, values, key_prefix, editing, config)
    if retry:
        st.markdown("**🔁 Retry policy** (outbound deliveries and API forwarding)")
        _render_fields(retry, values, key_prefix, editing, config)
    return config, secret


def _render_fields(fields, values, key_prefix, editing, config):
    secret = None
    cols = st.columns(2)
    for i, f in enumerate(fields):
        col = cols[i % 2] if f["kind"] not in ("textarea",) else st
        k = f"{key_prefix}_{f['key']}"
        current = values.get(f["key"], f["default"])
        if f["kind"] == "select":
            opts = f["options"]
            config[f["key"]] = col.selectbox(f["label"], opts, index=opts.index(current) if current in opts else 0,
                                             key=k, help=f.get("help"))
        elif f["kind"] == "int":
            config[f["key"]] = col.number_input(f["label"], value=int(current or 0), step=1, key=k, help=f.get("help"))
        elif f["kind"] == "bool":
            config[f["key"]] = col.toggle(f["label"], value=bool(current), key=k, help=f.get("help"))
        elif f["kind"] == "textarea":
            config[f["key"]] = col.text_area(f["label"], value=current or "", key=k, help=f.get("help"), height=100)
        elif f["kind"] == "secret":
            placeholder = "leave blank to keep the stored value" if editing else ""
            entered = col.text_input(f["label"], value="" if editing else (current or ""), type="password", key=k,
                                     placeholder=placeholder, help=f.get("help"))
            secret = entered if (entered or not editing) else None
        else:
            config[f["key"]] = col.text_input(f["label"], value=current or "", key=k, help=f.get("help"))
    return secret


def _add_connection(types):
    with st.expander("➕ Add a connection", expanded=False):
        options = list(types)
        type_key = st.selectbox("Type", options, key="new_conn_type",
                                format_func=lambda t: f"{types[t]['icon']} {types[t]['label']}"
                                + ("" if types[t]["available"] else "  - client not installed"))
        spec = types[type_key]
        directions = " and ".join(d for d, ok in (("inbound", spec["inbound"]), ("outbound", spec["outbound"])) if ok)
        st.caption(f"Can be used for {directions} messages.")
        if not spec["available"]:
            st.warning("IBM MQ needs IBM's MQ client libraries on the machine running Payload Hub: install the IBM MQ "
                       "redistributable client (or MQ for Developers) from IBM, then `pip install ibmmq`. You can save "
                       "the connection now; it will connect once the client is installed.")
        fixed, values = {}, {}
        if type_key == "ftp":
            proto = st.selectbox("Protocol", ["sftp", "ftp", "ftps"], key="new_ftp_protocol",
                                 format_func={"sftp": "SFTP (over SSH)", "ftp": "FTP", "ftps": "FTPS (FTP over TLS)"}.get)
            fixed = {"protocol": proto}
            values = {"port": 22 if proto == "sftp" else 21}
        if type_key == "database":
            engine = st.selectbox("Database type", list(DB_PORTS), key="new_db_engine",
                                  format_func={"postgresql": "PostgreSQL", "mysql": "MySQL / MariaDB",
                                               "sqlserver": "Microsoft SQL Server", "oracle": "Oracle",
                                               "sqlite": "SQLite (file)"}.get)
            fixed = {"engine": engine}
            values = {"port": DB_PORTS[engine]}
            if engine == "sqlite":
                st.caption("For SQLite, only the file path matters (Database / service name / SQLite file).")
        form_key = f"new_conn_{type_key}" + (f"_{next(iter(fixed.values()))}" if fixed else "")
        with st.form(form_key, clear_on_submit=False):
            name = st.text_input("Name *", value=PRESETS.get(type_key, ""), key=f"new_conn_name_{form_key}")
            config, secret = _fields_form(type_key, spec, values, f"new_{form_key}", fixed=fixed)
            if st.form_submit_button("Save and test", type="primary", disabled=not common.can("write")):
                try:
                    c = api("POST", "/api/connections",
                            json={"name": name, "type": type_key, "config": config, "secret": secret})
                except ApiError as e:
                    st.error(str(e))
                    return
                (st.success if c["test_ok"] else st.warning)(f"Saved **{c['name']}** - {c['test_detail']}")
                st.rerun()


def _connection_card(c, types):
    spec = types.get(c["type"], {"label": c["type"], "icon": "🔌", "fields": [], "target": "Target"})
    with st.container(border=True):
        top = st.columns([4, 2, 3])
        top[0].markdown(f"#### {spec['icon']} {c['name']}")
        top[0].caption(f"`{c['id']}` · {spec['label']} · {_endpoint(c)}")
        top[1].markdown(f"**{_status_badge(c)}**")
        if c["last_test_at"]:
            top[1].caption(f"tested {pd.to_datetime(c['last_test_at']).tz_convert(None):%Y-%m-%d %H:%M:%S}")
        top[2].markdown(("👤 **Dedicated to** " + c["owner_partner"]) if c.get("owner_partner") else "🌍 **Common**")
        top[2].markdown("**Used by:** " + (", ".join(c["used_by"]) if c["used_by"] else "—"))
        if c["last_test_detail"]:
            (st.caption if c["last_test_ok"] else st.error)(c["last_test_detail"])
        if spec.get("outbound"):
            cfg = c["config"]
            statuses = f" · retries on HTTP {cfg.get('retry_on_status', '408,425,429,500,502,503,504')}" \
                if c["type"] == "http" else ""
            st.caption(f"🔁 {cfg.get('retry_attempts', 3)} attempts · first retry after {cfg.get('retry_backoff', 2)} s, "
                       f"doubling up to {cfg.get('retry_backoff_max', 30)} s{statuses}")

        b = st.columns([1, 1, 1, 3])
        if b[0].button("Test", key=f"test_{c['id']}"):
            r = api("POST", f"/api/connections/{c['id']}/test")
            (st.toast if r["ok"] else st.error)(r["detail"])
            st.rerun()
        show_send = b[1].toggle("Send", key=f"send_toggle_{c['id']}", disabled=not common.can("write"), help="Send a test message through this connection")
        show_edit = b[2].toggle("Edit", key=f"edit_toggle_{c['id']}", disabled=not common.can("write"))
        if c["type"] == "ftp" and b[3].toggle("📂 Files", key=f"files_toggle_{c['id']}"):
            _file_browser(c)
        if c["type"] == "kafka" and b[3].toggle("📜 Topics", key=f"topics_toggle_{c['id']}"):
            _topic_browser(c)
        if c["type"] == "database" and "data" in common.PAGES:
            b[3].page_link(common.PAGES["data"], label="Open tables on the Data page", icon="🗄️")

        if show_send:
            with st.form(f"send_{c['id']}"):
                target = st.text_input(spec["target"], key=f"send_target_{c['id']}",
                                       placeholder="e.g. a partner's inbound queue")
                body = st.text_area("Message body", '{"hello": "from Payload Hub"}', height=120, key=f"send_body_{c['id']}")
                ctype = st.selectbox("Content type", ["application/json", "application/xml", "text/plain"],
                                     key=f"send_ct_{c['id']}")
                if st.form_submit_button("Send message", disabled=not common.can("write")):
                    r = api("POST", f"/api/connections/{c['id']}/send",
                            json={"target": target, "body": body, "content_type": ctype})
                    (st.success if r["ok"] else st.error)(r["detail"])

        if show_edit:
            with st.form(f"edit_{c['id']}"):
                name = st.text_input("Name", c["name"], key=f"edit_name_{c['id']}")
                config, secret = _fields_form(c["type"], spec, c["config"], f"edit_{c['id']}", editing=True)
                save, delete = st.columns(2)
                if save.form_submit_button("Save and test", type="primary"):
                    try:
                        r = api("PUT", f"/api/connections/{c['id']}",
                                json={"name": name, "type": c["type"], "config": config, "secret": secret})
                    except ApiError as e:
                        st.error(str(e))
                    else:
                        (st.toast if r["test_ok"] else st.error)(r["test_detail"])
                        st.rerun()
                if delete.form_submit_button("Delete connection", disabled=not common.can("delete")):
                    try:
                        api("DELETE", f"/api/connections/{c['id']}")
                    except ApiError as e:
                        st.error(str(e))
                    else:
                        common.partners.clear()
                        st.rerun()


def _topic_browser(c):
    cid = c["id"]
    try:
        topics = api("GET", f"/api/connections/{cid}/topics")
    except ApiError as e:
        st.error(str(e))
        return
    if not topics:
        st.caption("No topics yet.")
        return
    st.dataframe(topics, hide_index=True, width="stretch",
                 column_config={"topic": "Topic", "partitions": "Partitions", "messages": "Messages"})
    t1, t2 = st.columns([3, 1])
    topic = t1.selectbox("Show latest messages in", [t["topic"] for t in topics], key=f"topic_pick_{cid}")
    limit = t2.number_input("How many", 1, 200, 10, key=f"topic_limit_{cid}")
    if st.button("Show messages", key=f"topic_show_{cid}"):
        msgs = api("GET", f"/api/connections/{cid}/topics/{topic}/messages", params={"limit": limit})
        if not msgs:
            st.caption("This topic is empty.")
        for m in msgs:
            import datetime as _dt

            when = _dt.datetime.fromtimestamp(m["timestamp"] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            with st.expander(f"{when} · partition {m['partition']} · offset {m['offset']}"
                             + (f" · key {m['key'][:12]}" if m["key"] else "")):
                body = m["value"]
                st.code(body, language="xml" if body.lstrip().startswith("<") else "json")


def _file_browser(c):
    cid = c["id"]
    folder = st.text_input("Folder", key=f"files_path_{cid}", placeholder="e.g. inbound - empty = base folder")
    try:
        entries = api("GET", f"/api/connections/{cid}/files", params={"path": folder})
    except ApiError as e:
        st.error(str(e))
        return
    if not entries:
        st.caption("This folder is empty.")
    else:
        rows = [{"": "📁" if e["is_dir"] else "📄", "name": e["name"], "size": None if e["is_dir"] else e["size"]}
                for e in entries]
        event = st.dataframe(rows, hide_index=True, width="stretch", on_select="rerun",
                             selection_mode="single-row", key=f"files_table_{cid}_{folder}",
                             column_config={"size": st.column_config.NumberColumn("Bytes")})
        picked = [entries[i] for i in event.selection.rows]
        if picked and picked[0]["is_dir"]:
            st.caption(f"Type **{(folder + '/' if folder else '') + picked[0]['name']}** in Folder to open it.")
        elif picked:
            path = f"{folder}/{picked[0]['name']}" if folder else picked[0]["name"]
            f = api("GET", f"/api/connections/{cid}/file", params={"path": path})
            st.code(f["text"], language="xml" if f["text"].lstrip().startswith("<") else "json")
    with st.form(f"upload_{cid}_{folder}"):
        st.markdown(f"**Upload a file to** `{folder or '(base folder)'}`")
        name = st.text_input("File name", placeholder="e.g. PO-1001.xml", key=f"up_name_{cid}")
        content = st.text_area("Content", height=120, key=f"up_body_{cid}")
        if st.form_submit_button("Upload", disabled=not common.can("write")):
            try:
                r = api("POST", f"/api/connections/{cid}/file", json={"folder": folder, "name": name, "content": content})
            except ApiError as e:
                st.error(str(e))
            else:
                st.success(f"Uploaded {r['written']}")


def _endpoint(c) -> str:
    cfg = c["config"]
    if c["type"] == "http":
        return cfg.get("base_url", "")
    if c["type"] == "kafka":
        return cfg.get("bootstrap", "")
    if c["type"] == "ibmmq":
        return f"{cfg.get('queue_manager')} @ {cfg.get('host')}({cfg.get('port')})"
    if c["type"] == "ftp":
        return f"{cfg.get('protocol')}://{cfg.get('host')}:{cfg.get('port')}{cfg.get('base_dir') or '/'}"
    if c["type"] == "database":
        if cfg.get("engine") == "sqlite":
            return f"SQLite · {cfg.get('database')}"
        return f"{cfg.get('engine')} · {cfg.get('host')}:{cfg.get('port')}/{cfg.get('database')}"
    return f"{cfg.get('host')}:{cfg.get('port')}"


def _test_receiver():
    st.subheader("📥 Test receiver inbox")
    st.caption(f"A built-in webhook for demos: point an HTTP connection at `{common.API}/test-receiver` and use the "
               "partner code as the target path. The last 100 messages it received are shown here.")
    msgs = api("GET", "/api/test-receiver", params={"limit": 20})
    if not msgs:
        st.caption("Nothing received yet.")
        return
    for m in msgs:
        when = pd.to_datetime(m["at"]).tz_convert(None).strftime("%H:%M:%S")
        with st.expander(f"{when} · /{m['name']} · {m['headers'].get('x-partner', '')} · "
                         f"txn {m['headers'].get('x-transaction-id', '')[:8]}"):
            ctype = m["headers"].get("content-type", "")
            st.code(m["body"], language="xml" if "xml" in ctype else "json")
            st.caption(json.dumps(m["headers"]))
