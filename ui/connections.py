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
    "kafka": "External Kafka",
}


def page():
    st.title("🔌 Connections")
    st.caption("Adapters to message brokers and HTTP endpoints. Partners use them to receive messages "
               "(inbound) and to deliver transformed results (outbound).")
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


def _fields_form(type_key, spec, values, key_prefix, editing=False):
    """Render one input per field spec; returns (config, secret)."""
    config, secret = {}, None
    cols = st.columns(2)
    for i, f in enumerate(spec["fields"]):
        col = cols[i % 2] if f["kind"] not in ("textarea",) else st
        k = f"{key_prefix}_{f['key']}"
        current = values.get(f["key"], f["default"])
        if f["kind"] == "int":
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
    return config, secret


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
        with st.form(f"new_conn_{type_key}", clear_on_submit=False):
            name = st.text_input("Name *", value=PRESETS.get(type_key, ""), key=f"new_conn_name_{type_key}")
            config, secret = _fields_form(type_key, spec, {}, f"new_{type_key}")
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
        top[2].markdown("**Used by:** " + (", ".join(c["used_by"]) if c["used_by"] else "—"))
        if c["last_test_detail"]:
            (st.caption if c["last_test_ok"] else st.error)(c["last_test_detail"])

        b = st.columns([1, 1, 1, 3])
        if b[0].button("Test", key=f"test_{c['id']}"):
            r = api("POST", f"/api/connections/{c['id']}/test")
            (st.toast if r["ok"] else st.error)(r["detail"])
            st.rerun()
        show_send = b[1].toggle("Send", key=f"send_toggle_{c['id']}", disabled=not common.can("write"), help="Send a test message through this connection")
        show_edit = b[2].toggle("Edit", key=f"edit_toggle_{c['id']}", disabled=not common.can("write"))

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


def _endpoint(c) -> str:
    cfg = c["config"]
    if c["type"] == "http":
        return cfg.get("base_url", "")
    if c["type"] == "kafka":
        return cfg.get("bootstrap", "")
    if c["type"] == "ibmmq":
        return f"{cfg.get('queue_manager')} @ {cfg.get('host')}({cfg.get('port')})"
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
