"""Partner 'Flow & endpoints' tab: flow diagram, HTTP endpoint, inbound/outbound adapters, Kafka queues."""
import pandas as pd
import requests
import streamlit as st

from . import common
from .common import ApiError, api, queue_label, status_label

GREEN, RED, GREY, BLUE = "#d3f2dc", "#f9d6d5", "#eeeeee", "#dbe6fb"


def flow_diagram(p: dict, listeners: dict, conns: dict) -> str:
    """Graphviz DOT for inbound -> Kafka -> transformer -> Kafka -> outbound."""
    def node(name, label, fill, shape="box"):
        return f'"{name}" [label="{label}", shape={shape}, style="rounded,filled", fillcolor="{fill}"];'

    code = p["code"]
    lines = ['digraph G { rankdir=LR; bgcolor="transparent"; node [fontname="Helvetica", fontsize=11]; '
             'edge [color="#7a7a7a"];']
    lines.append(node("http", f"🌐 HTTP endpoint\\nPOST /inbound/{code}", BLUE))
    lines.append('"http" -> "in";')
    if p.get("inbound_connection"):
        st_ = listeners.get(code, {}).get("state")
        fill = GREEN if st_ == "running" else RED if st_ == "error" else GREY
        c = conns.get(p["inbound_connection"], {})
        lines.append(node("mqin", f"📥 {c.get('name', p['inbound_connection'])}\\n{p.get('inbound_source') or ''}", fill))
        lines.append('"mqin" -> "in";')
    lines.append(node("in", f"Kafka\\n{p['topic_in']}", GREY, "cylinder"))
    has_mapping = bool(p.get("expression"))
    lines.append(node("xf", "⚙️ Transformer\\n" + ("mapping ready" if has_mapping else "no mapping yet"),
                      GREEN if has_mapping else RED))
    lines.append(node("out", f"Kafka\\n{p['topic_out']}", GREY, "cylinder"))
    lines.append(node("dlq", f"Failures\\n{p['topic_dlq']}", RED, "cylinder"))
    lines.append('"in" -> "xf"; "xf" -> "out"; "xf" -> "dlq" [style=dashed, label="error"];')
    if p.get("reply_mode") == "forward" and p.get("reply_connection"):
        c = conns.get(p["reply_connection"], {})
        ok = c.get("last_test_ok")
        fill = GREEN if ok else RED if ok is False else GREY
        path = f"/{p['reply_path']}" if p.get("reply_path") else ""
        fmt = {"input": p["input_format"].upper(), "json": "JSON", "xml": "XML", "unchanged": "as-is"}.get(
            p.get("response_format") or "input")
        lines.append(node("api", f"↪ {c.get('name', p['reply_connection'])}{path}\n(target API)", fill))
        lines.append(f'"out" -> "api" [label="forward"]; "api" -> "http" [style=dashed, color="#4f46e5", '
                     f'fontcolor="#4f46e5", label="response as {fmt}", constraint=false];')
    else:
        lines.append('"out" -> "http" [style=dashed, color="#4f46e5", fontcolor="#4f46e5", '
                     'label="transformed payload", constraint=false];')
    if p.get("store_enabled") and p.get("store_table"):
        lines.append(node("db", f"🗄️ Table\\nuserdata.{p['store_table']}", BLUE, "cylinder"))
        lines.append('"out" -> "db" [label="store"];')
    if p.get("outbound_connection"):
        c = conns.get(p["outbound_connection"], {})
        ok = c.get("last_test_ok")
        fill = GREEN if ok else RED if ok is False else GREY
        lines.append(node("dest", f"📤 {c.get('name', p['outbound_connection'])}\\n{p.get('outbound_target') or ''}", fill))
        lines.append('"out" -> "dest" [label="deliver"]; "dest" -> "dlq" [style=dashed, label="after 3 retries"];')
    else:
        lines.append(node("store", "💾 Stored in log\\n(read via HTTP / UI)", GREY))
        lines.append('"out" -> "store";')
    lines.append("}")
    return "\n".join(lines)


def flow_tab(p: dict):
    code = p["code"]
    conns = {c["id"]: c for c in api("GET", "/api/connections")}
    listeners = api("GET", "/api/adapters/status")
    st.graphviz_chart(flow_diagram(p, listeners, conns))
    if (lst := listeners.get(code)) and lst["state"] == "error":
        st.error(f"Inbound listener problem: {lst['detail']}")
    st.markdown("**Inbound & outbound endpoints**")
    st.dataframe(common.partner_endpoints(p, conns), hide_index=True, width="stretch")
    if "connections" in common.PAGES:
        st.page_link(common.PAGES["connections"], label="Manage adapters & connections (brokers, APIs, retry policy)",
                     icon="🔌")

    _http_endpoint(p)
    st.divider()
    _response_form(p, conns)
    st.divider()
    _adapters_form(p, conns, listeners)
    st.divider()
    _store_form(p)
    st.divider()
    _kafka_queues(p)
    st.divider()
    _recent(p)


def _http_endpoint(p):
    code = p["code"]
    url = f"{common.API}/inbound/{code}"
    st.subheader("🌐 HTTP endpoint")
    if common.EMBEDDED:
        st.info("On Streamlit Cloud the endpoint runs inside the app and isn't reachable from outside - use "
                "**Try it** below for the demo. On your laptop it's a real URL you can call with curl or Postman.")
    c1, c2 = st.columns([3, 2])
    c1.text_input("Endpoint URL", url, disabled=True, key=f"url_{code}")
    show = c2.toggle("Show API key", key=f"showkey_{code}", disabled=not common.can("write"), help="Needs write permission.")
    c2.text_input("API key (header X-API-Key)", p["api_key"] if show and common.can("write") else "•" * 20, disabled=True, key=f"key_{code}_{show}")
    if c2.button("Rotate API key", key=f"rotate_{code}", disabled=not common.can("write"), help="Issue a new key; the old one stops working at once."):
        api("POST", f"/api/partners/{code}/api-key")
        st.toast("New API key issued", icon="🔑")
        st.rerun()
    ctype = "application/xml" if p["input_format"] == "xml" else "application/json"
    sample_file = "payload.xml" if p["input_format"] == "xml" else "payload.json"
    st.code(f'curl -X POST "{url}" \\\n  -H "X-API-Key: <api key>" \\\n  -H "Content-Type: {ctype}" \\\n'
            f'  --data-binary @{sample_file}', language="bash")
    st.caption("Waits for the result and returns it (200), or the error (422). Add `?wait=false` to get a "
               "transaction id at once (202) and poll `/inbound/<code>/transactions/<id>`.")

    with st.expander("▶️ Try it", expanded=False):
        payload = st.text_area("Request body", p["sample_input"] or "", height=200, key=f"try_body_{code}")
        wait = st.toggle("Wait for the result", value=True, key=f"try_wait_{code}")
        if st.button("Send request", type="primary", key=f"try_send_{code}", disabled=not common.can("write")):
            try:
                r = requests.post(url, data=payload.encode(), timeout=30, params={"wait": str(wait).lower()},
                                  headers={"X-API-Key": p["api_key"], "Content-Type": ctype})
            except requests.RequestException as e:
                st.error(f"Request failed: {e}")
                return
            ok = r.status_code < 300
            extra = f" · upstream HTTP {r.headers['X-Upstream-Status']}" if r.headers.get("X-Upstream-Status") else ""
            (st.success if ok else st.error)(f"HTTP {r.status_code} · {r.headers.get('X-Status', '')}{extra} · "
                                             f"transaction {r.headers.get('X-Transaction-Id', '-')}")
            is_xml = "xml" in r.headers.get("content-type", "")
            st.code(r.text, language="xml" if is_xml else "json")


RESPONSE_FORMATS = {
    "input": "Same as the request (the caller's agreed format)",
    "json": "JSON",
    "xml": "XML",
    "unchanged": "Unchanged (whatever the API returns)",
}


def _response_form(p, conns):
    code = p["code"]
    st.subheader("↩️ Response to the caller")
    http_conns = [c for c in conns if conns[c]["type"] == "http"]
    modes = {"transformed": "Return the transformed payload",
             "forward": "Forward to another API and return its response"}
    mode = st.radio("When a caller sends a message", list(modes), format_func=modes.get, horizontal=True,
                    index=list(modes).index(p.get("reply_mode") or "transformed"), key=f"reply_mode_{code}")
    if mode == "transformed":
        st.caption(f"The caller gets the transformed {p['output_format'].upper()} straight back.")
        if (p.get("reply_mode") or "transformed") != "transformed" and \
                st.button("Save", type="primary", key=f"reply_save_t_{code}", disabled=not common.can("write")):
            _save_reply(code, {"reply_mode": "transformed"})
        return
    if not http_conns:
        st.warning("Add an HTTP connection for the target API on the **Connections** page first.")
        return
    with st.form(f"reply_{code}"):
        c1, c2 = st.columns(2)
        cur = p.get("reply_connection") if p.get("reply_connection") in http_conns else http_conns[0]
        conn = c1.selectbox("Target API (HTTP connection)", http_conns, http_conns.index(cur),
                            format_func=lambda c: f"🌐 {conns[c]['name']} · {conns[c]['config'].get('base_url', '')}")
        path = c2.text_input("Path (appended to the base URL)", p.get("reply_path") or "",
                             placeholder="e.g. orders/v1/submit")
        fmt = st.selectbox("Send the API's response back as", list(RESPONSE_FORMATS),
                           list(RESPONSE_FORMATS).index(p.get("response_format") or "input"),
                           format_func=lambda f: RESPONSE_FORMATS[f].replace(
                               "the caller's agreed format", f"{p['input_format'].upper()}, the caller's agreed format"))
        expr = st.text_area("Response mapping (optional, JMESPath on the API's response)",
                            p.get("response_expression") or "", height=90,
                            placeholder="e.g. {confirmation: {reference: reference, state: status}}")
        st.caption("Flow: caller → transform → POST the transformed payload to the target API → convert the API's "
                   "response → caller. The API's HTTP status is passed through; if it can't be reached the caller "
                   "gets 502. Messages from MQ or Kafka are forwarded too, and the response is kept on the "
                   "transaction.")
        if st.form_submit_button("Save response settings", type="primary", disabled=not common.can("write")):
            _save_reply(code, {"reply_mode": "forward", "reply_connection": conn, "reply_path": path,
                               "response_format": fmt, "response_expression": expr})


def _save_reply(code, body):
    try:
        api("PUT", f"/api/partners/{code}/reply", json=body)
    except ApiError as e:
        st.error(str(e))
        return
    common.partners.clear()
    st.toast("Response settings saved", icon="↩️")
    st.rerun()


def _adapters_form(p, conns, listeners):
    code = p["code"]
    types = api("GET", "/api/connection-types")
    st.subheader("🔀 Inbound & outbound adapters")
    in_opts = [""] + [c for c in conns if types[conns[c]["type"]]["inbound"]]
    out_opts = [""] + [c for c in conns if types[conns[c]["type"]]["outbound"]]
    label = lambda c: f"{types[conns[c]['type']]['icon']} {conns[c]['name']}" if c else None  # noqa: E731
    with st.form(f"adapters_{code}"):
        left, right = st.columns(2)
        with left:
            st.markdown("**Inbound** (besides HTTP)")
            cur = p.get("inbound_connection") or ""
            inbound = st.selectbox("Receive from", in_opts, in_opts.index(cur) if cur in in_opts else 0,
                                   format_func=lambda c: label(c) or "HTTP endpoint only", key=f"in_conn_{code}")
            source = st.text_input("Queue / topic to listen on", p.get("inbound_source") or f"{code}.in",
                                   key=f"in_src_{code}")
            if lst := listeners.get(code):
                icon = {"running": "🟢", "error": "🔴"}.get(lst["state"], "⚪")
                st.caption(f"{icon} Listener {lst['state']}: {lst['detail']}")
        with right:
            st.markdown("**Outbound**")
            cur = p.get("outbound_connection") or ""
            outbound = st.selectbox("Deliver to", out_opts, out_opts.index(cur) if cur in out_opts else 0,
                                    format_func=lambda c: label(c) or "Store only", key=f"out_conn_{code}")
            target = st.text_input("Queue / topic / path", p.get("outbound_target") or f"{code}.out",
                                   key=f"out_tgt_{code}")
            st.caption("Retries 3 times, then parks the message on the partner's failure queue.")
        if st.form_submit_button("Save adapters", type="primary", disabled=not common.can("write")):
            try:
                api("PUT", f"/api/partners/{code}/adapters",
                    json={"inbound_connection": inbound or None, "inbound_source": source if inbound else None,
                          "outbound_connection": outbound or None, "outbound_target": target if outbound else None})
            except ApiError as e:
                st.error(str(e))
                return
            common.partners.clear()
            st.toast("Adapters saved", icon="🔀")
            st.rerun()
    if not conns:
        st.caption("No connections yet - add RabbitMQ, ActiveMQ, IBM MQ, HTTP or Kafka on the **Connections** page.")
    elif p.get("inbound_connection") and p.get("inbound_source"):
        with st.expander("📨 Put a test message on the inbound queue"):
            body = st.text_area("Message", p["sample_input"] or "", height=160, key=f"mq_body_{code}")
            if st.button("Send to inbound queue", key=f"mq_send_{code}", disabled=not common.can("write")):
                ctype = "application/xml" if p["input_format"] == "xml" else "application/json"
                r = api("POST", f"/api/connections/{p['inbound_connection']}/send",
                        json={"target": p["inbound_source"], "body": body, "content_type": ctype})
                (st.success if r["ok"] else st.error)(r["detail"] + (" - the listener will pick it up; see "
                                                                     "Recent activity below." if r["ok"] else ""))


def _store_form(p):
    code = p["code"]
    st.subheader("🗄️ Store in the database")
    enabled = st.toggle("Store each completed message in a data table", value=bool(p.get("store_enabled")),
                        key=f"store_on_{code}", disabled=not common.can("write"))
    if not enabled:
        st.caption("Off: messages are kept in the transaction log only.")
        if p.get("store_enabled") and st.button("Save (turn storing off)", key=f"store_off_{code}"):
            _save_store(code, {"store_enabled": False})
        return
    tables = {t["name"]: t for t in api("GET", "/api/data/tables")}
    if not tables:
        st.warning("No data tables yet - create one on the **Data** page first.")
        if "data" in common.PAGES:
            st.page_link(common.PAGES["data"], label="Open the Data page", icon="🗄️")
        return
    names = list(tables)
    c1, c2 = st.columns(2)
    table = c1.selectbox("Table", names, names.index(p["store_table"]) if p.get("store_table") in names else 0,
                         key=f"store_table_{code}", format_func=lambda t: f"userdata.{t} ({tables[t]['rows']} rows)")
    source = c2.selectbox("Take values from", ["output", "input"],
                          ["output", "input"].index(p.get("store_source") or "output"), key=f"store_src_{code}",
                          format_func={"output": f"Transformed payload ({p['output_format'].upper()})",
                                       "input": f"Original payload ({p['input_format'].upper()})"}.get)
    row_path = st.text_input("One row per item in (optional JMESPath to a list)", p.get("store_row_path") or "",
                             key=f"store_rows_{code}", placeholder="e.g. salesOrder.lines - leave empty for one row per message")
    current = p.get("store_columns") or {}
    auto = {"id", "transaction_id", "partner_code", "stored_at"}
    editor = st.data_editor(
        [{"column": c["name"], "type": c["type"], "value from": current.get(c["name"], "")}
         for c in tables[table]["columns"] if c["name"] not in auto],
        key=f"store_map_{code}_{table}", hide_index=True, width="stretch", disabled=["column", "type"],
        column_config={"value from": st.column_config.TextColumn(
            "Value from (JMESPath)", help="Evaluated on each row item. Use $root.<path> for the whole payload, or "
                                          "$transaction_id, $partner, $received_at. Empty = leave the column empty.")})
    st.caption("transaction_id, partner_code and stored_at are filled automatically when the table has them.")
    body = {"store_enabled": True, "store_table": table, "store_source": source, "store_row_path": row_path,
            "store_columns": {r["column"]: r["value from"] or "" for r in editor}}
    b1, b2 = st.columns([1, 1])
    if b1.button("Preview rows from the sample", key=f"store_preview_{code}"):
        try:
            rows = api("POST", f"/api/partners/{code}/store/preview", json=body)["rows"]
        except ApiError as e:
            st.error(str(e))
        else:
            st.dataframe(rows, width="stretch")
    if b2.button("Save storage settings", type="primary", key=f"store_save_{code}", disabled=not common.can("write")):
        _save_store(code, body)


def _save_store(code, body):
    try:
        api("PUT", f"/api/partners/{code}/store", json=body)
    except ApiError as e:
        st.error(str(e))
        return
    common.partners.clear()
    st.toast("Storage settings saved", icon="🗄️")
    st.rerun()


def _kafka_queues(p):
    code = p["code"]
    st.subheader("🟧 Kafka queues")
    st.markdown(f"**{queue_label(p)}**: `{p['topic_in']}` → `{p['topic_out']}` · failures `{p['topic_dlq']}`")
    target = "shared" if p["queue_mode"] == "dedicated" else "dedicated"
    if st.button(f"Move to {target} queues", key=f"move_{code}", disabled=not common.can("write"),
                 help="Messages already queued finish on the current queue; new ones use the new queue."):
        try:
            api("POST", f"/api/partners/{code}/queue", data={"queue_mode": target})
        except ApiError as e:
            st.error(str(e))
        else:
            common.partners.clear()
            st.rerun()


def _recent(p):
    st.subheader("Recent activity")
    txns = api("GET", "/api/transactions", params={"partner": p["code"], "limit": 8})
    if not txns:
        st.caption("No transactions yet.")
        return
    df = pd.DataFrame(txns)
    df["created_at"] = pd.to_datetime(df["created_at"]).dt.tz_convert(None).dt.strftime("%H:%M:%S")
    df["status"] = df["status"].map(status_label)
    df["delivery"] = df["delivery_status"].map({"DELIVERED": "📤 Delivered", "DELIVERY_FAILED": "⚠️ Failed"}).fillna("–")
    st.dataframe(df[["created_at", "source", "status", "delivery", "total_ms", "error"]], hide_index=True,
                 width="stretch", column_config={"created_at": "Time", "source": "Source", "status": "Status",
                                                 "delivery": "Delivery", "error": "Error",
                                                 "total_ms": st.column_config.NumberColumn("ms", format="%.0f")})
