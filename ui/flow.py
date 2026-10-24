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
        kind = {"http": "API", "ftp": "file", "database": "table"}.get(c.get("type"), "queue")
        csv = {"csv": " · CSV", "query": " · URL query parameters"}.get(p.get("send_format"), "")
        answer = "response" if c.get("type") == "http" else "receipt"
        lines.append(node("api", f"🎯 Target system: {c.get('name', p['reply_connection'])}\\n{kind} {path.lstrip('/')}{csv}", fill))
        lines.append(f'"out" -> "api" [label="send"]; "api" -> "http" [style=dashed, color="#4f46e5", '
                     f'fontcolor="#4f46e5", label="{answer} as {fmt}", constraint=false];')
    else:
        lines.append('"out" -> "http" [style=dashed, color="#4f46e5", fontcolor="#4f46e5", '
                     'label="transformed payload", constraint=false];')
    if p.get("store_enabled") and p.get("store_table"):
        where = conns.get(p.get("store_connection") or "", {}).get("name") or "userdata"
        lines.append(node("db", f"🗄️ {where}\\n{p['store_table']}", BLUE, "cylinder"))
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


def overview_tab(p: dict):
    """The partner's flow at a glance: diagram, every endpoint, and its Kafka queues."""
    code = p["code"]
    conns = {c["id"]: c for c in api("GET", "/api/connections")}
    listeners = api("GET", "/api/adapters/status")
    st.graphviz_chart(flow_diagram(p, listeners, conns))
    if (lst := listeners.get(code)) and lst["state"] == "error":
        st.error(f"Inbound listener problem: {lst['detail']}")
    st.dataframe(common.partner_endpoints(p, conns), hide_index=True, width="stretch")
    with st.expander("🟧 Kafka queues"):
        _kafka_queues(p)


def route_tab(p: dict):
    conns = {c["id"]: c for c in api("GET", "/api/connections")}
    _route_form(p, conns, api("GET", "/api/connection-types"))


def endpoint_tab(p: dict):
    """How data reaches the partner: its HTTP endpoint, plus an optional queue/folder/table listener and an outbound
    copy."""
    conns = {c["id"]: c for c in api("GET", "/api/connections")}
    _http_endpoint(p)
    st.divider()
    _adapters_form(p, conns, api("GET", "/api/adapters/status"))


def storage_tab(p: dict):
    _store_form(p)


def activity_tab(p: dict):
    _recent(p)
    st.markdown("**Change history**")
    history = api("GET", f"/api/partners/{p['code']}/history", params={"limit": 30})
    if history:
        import pandas as pd

        st.dataframe([{"When": pd.to_datetime(e["at"]).tz_convert(common.TZ).tz_localize(None).strftime("%Y-%m-%d %H:%M"), "Who": e["username"],
                       "What": e["action"], "Changes": e["detail"]} for e in history], hide_index=True, width="stretch")
    else:
        st.caption("No changes recorded yet.")


def _http_endpoint(p):
    code = p["code"]
    url = f"{common.API}/inbound/{code}"
    if common.EMBEDDED:
        st.info("On Streamlit Cloud the endpoint runs inside the app and isn't reachable from outside - use "
                "**Try it** below for the demo. On your laptop it's a real URL you can call with curl or Postman.")
    c1, c2 = st.columns([3, 2])
    c1.text_input("Endpoint URL", url, disabled=True, key=f"url_{code}")
    show = c2.toggle("Show API key", key=f"showkey_{code}", disabled=not common.can("write"), help="Needs write permission.")
    shown = common.partner_key(code) if show and common.can("write") else p.get("api_key_hint") or "•" * 20
    c2.text_input("API key (header X-API-Key)", shown, disabled=True, key=f"key_{code}_{show}")
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
                                  headers={"X-API-Key": common.partner_key(code), "Content-Type": ctype})
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
    "input": "Translated into the caller's format",
    "json": "Translated into JSON",
    "xml": "Translated into XML",
    "unchanged": "As is - exactly what the target system answered",
}
TARGET_LABEL = {"http": "Path (added to the API's base URL)", "ftp": "Folder", "database": "Table",
                "kafka": "Topic", "rabbitmq": "Queue", "activemq": "Queue", "ibmmq": "Queue"}
SCOPES = {"own": "👤 This partner's endpoints", "common": "🌍 Common endpoints (shared)",
          "new": "➕ New endpoint for this partner"}


def visible_endpoints(conns: dict, code: str) -> dict:
    """Common endpoints plus this partner's own; other partners' dedicated endpoints are hidden."""
    return {cid: c for cid, c in conns.items() if c.get("owner_partner") in (None, code)}


def _route_form(p, conns, types):
    code = p["code"]
    st.caption("The **source system** calls this partner's endpoint → the payload is transformed → sent to the **target system** "
               "(an API, a CSV/JSON/XML file on FTP/SFTP, a database table or a queue) → the target system's answer goes back "
               "to A, translated or as is.")
    if pending := st.session_state.pop(f"route_pending_{code}", None):  # a just-created endpoint
        st.session_state[f"route_scope_{code}"] = "own"
        st.session_state[f"route_conn_{code}"] = pending
        st.session_state[f"reply_mode_{code}"] = "forward"
    modes = {"transformed": "Reply at once with the transformed payload (no target system)",
             "forward": "Send to the target system and reply with its answer"}
    if f"reply_mode_{code}" not in st.session_state:
        st.session_state[f"reply_mode_{code}"] = p.get("reply_mode") or "transformed"
    mode = st.radio("When a caller sends a message", list(modes), format_func=modes.get, key=f"reply_mode_{code}")
    if mode == "transformed":
        st.caption(f"The caller gets the transformed {p['output_format'].upper()} straight back.")
        if (p.get("reply_mode") or "transformed") != "transformed" and \
                st.button("Save", type="primary", key=f"reply_save_t_{code}", disabled=not common.can("write")):
            _save_reply(code, {"reply_mode": "transformed"})
        return

    outs = {cid: c for cid, c in visible_endpoints(conns, code).items() if types[c["type"]]["outbound"]}
    own = [cid for cid, c in outs.items() if c.get("owner_partner") == code]
    shared = [cid for cid, c in outs.items() if not c.get("owner_partner")]
    if f"route_scope_{code}" not in st.session_state:
        cur = p.get("reply_connection")
        st.session_state[f"route_scope_{code}"] = "own" if cur in own else "common" if cur in shared \
            else "own" if own else "common" if shared else "new"
    scope = st.radio("Target system endpoint", list(SCOPES), horizontal=True, key=f"route_scope_{code}",
                     format_func=lambda x: SCOPES[x] + {"own": f" ({len(own)})", "common": f" ({len(shared)})",
                                                        "new": ""}[x])
    if scope == "new":
        _new_endpoint(p, types)
        return
    choices = own if scope == "own" else shared
    if not choices:
        st.info("No endpoints here yet - choose **➕ New endpoint for this partner**, or add a common one on the "
                "Adapters & Connections page.")
        return
    key = f"route_conn_{code}"
    if st.session_state.get(key) not in choices:
        st.session_state[key] = p.get("reply_connection") if p.get("reply_connection") in choices else choices[0]
    conn_id = st.selectbox("Target system", choices, key=key, format_func=lambda c: f"{types[outs[c]['type']]['icon']} "
                           f"{outs[c]['name']} · {common.connection_address(outs[c])}")
    c = outs[conn_id]
    t = c["type"]
    used = [x for x in c.get("used_by") or [] if x != code]
    sc1, sc2 = st.columns([2, 1], vertical_alignment="center")
    if c.get("owner_partner"):
        sc1.caption(f"👤 Dedicated to **{code}** - other partners can't see or use it.")
        if sc2.button("🌍 Make it common", key=f"own_common_{code}", disabled=not common.can("write")):
            _set_owner(conn_id, None)
    else:
        sc1.caption("🌍 Common endpoint" + (f" - also used by {', '.join(used)}." if used else " - not used by other partners yet."))
        if not used and sc2.button("👤 Dedicate to this partner", key=f"own_ded_{code}", disabled=not common.can("write")):
            _set_owner(conn_id, code)
    same = p.get("reply_connection") == conn_id
    with st.form(f"reply_{code}_{conn_id}"):
        if t == "http":
            m1, m2 = st.columns([1, 3])
            methods = ["POST", "PUT", "PATCH", "GET", "DELETE"]
            cur_m = (p.get("reply_method") if same else None) or c["config"].get("method") or "POST"
            method = m1.selectbox("Method", methods, methods.index(cur_m) if cur_m in methods else 0)
            target = m2.text_input(TARGET_LABEL[t], (p.get("reply_path") if same else "") or "",
                                   placeholder="e.g. orders/v1/submit")
        else:
            method = None
            target = st.text_input(TARGET_LABEL[t] + " *", (p.get("reply_path") if same else "") or "",
                                   placeholder={"ftp": "e.g. outbound/orders", "database": "e.g. orders_in"}.get(t, f"e.g. {code}.out"))
        f1, f2 = st.columns([3, 1])
        formats = {"output": f"As transformed ({p['output_format'].upper()})", "csv": "CSV"}
        if t == "http":
            formats["query"] = "URL query parameters (mapped JSON fields)"
        if t == "database":
            formats = {"output": "Rows (JSON fields → columns)"}
        cur_f = p.get("send_format") or "output"
        send_format = f1.radio("Target system receives", list(formats), list(formats).index(cur_f) if cur_f in formats else 0,
                               format_func=formats.get, horizontal=True,
                               help="Query parameters: map the request to a JSON object; its fields become URL parameters. "
                                    "Null values are omitted and lists become repeated parameters. "
                                    "CSV: one row per item of the transformed list (or one row for an object); "
                                    "nested fields become columns like customer.name.")
        delims = {",": "comma", ";": "semicolon", "|": "pipe", "\t": "tab"}
        delim = f2.selectbox("CSV separator", list(delims), list(delims).index(p.get("csv_delimiter") or ",")
                             if (p.get("csv_delimiter") or ",") in delims else 0, format_func=delims.get,
                             disabled=t == "database")
        fmt = st.selectbox("The source system gets the target system's answer", list(RESPONSE_FORMATS),
                           list(RESPONSE_FORMATS).index(p.get("response_format") or "input"),
                           format_func=lambda f: RESPONSE_FORMATS[f].replace(
                               "the caller's format", f"the caller's format ({p['input_format'].upper()})"))
        expr = st.text_area("Translate the answer (optional JMESPath mapping on the target system's answer)",
                            p.get("response_expression") or "", height=90,
                            placeholder="e.g. {confirmation: {reference: reference, state: status}}")
        if t == "http":
            st.caption("The target system's HTTP status is passed back to the caller; if it can't be reached the caller gets 502. "
                       f"Retries follow the endpoint's policy ({common.retry_summary(c)}).")
        else:
            st.caption("A file, table or queue doesn't answer, so the caller gets a **delivery receipt** - "
                       "`{status: DELIVERED, system, type, target, detail, format, transactionId}` - which you can "
                       "translate with the mapping above. If delivery fails the caller gets 502.")
        if st.form_submit_button("Save route", type="primary", disabled=not common.can("write")):
            _save_reply(code, {"reply_mode": "forward", "reply_connection": conn_id, "reply_path": target,
                               "reply_method": method, "send_format": send_format,
                               "csv_delimiter": delim,
                               "response_format": fmt, "response_expression": expr})


def _set_owner(conn_id, owner):
    try:
        api("PUT", f"/api/connections/{conn_id}/owner", json={"owner_partner": owner})
    except ApiError as e:
        st.error(str(e))
        return
    st.toast("Endpoint is now " + ("dedicated to this partner" if owner else "common"), icon="🔌")
    st.rerun()


def _new_endpoint(p, types):
    """Create a target system endpoint dedicated to this partner, without leaving the page."""
    from .connections import DB_PORTS, _fields_form

    code = p["code"]
    kinds = {"http": "🌐 API (HTTP)", "ftp": "📁 File on FTP / SFTP (CSV, JSON, XML)", "database": "🗄️ Database table",
             "rabbitmq": "🐇 RabbitMQ queue", "activemq": "📨 ActiveMQ queue", "ibmmq": "🏦 IBM MQ queue",
             "kafka": "🟧 Kafka topic"}
    kinds = {k: v for k, v in kinds.items() if k in types and types[k]["outbound"]}
    t = st.selectbox("Kind of target system", list(kinds), format_func=kinds.get, key=f"newep_type_{code}")
    fixed, values = {}, {}
    if t == "ftp":
        proto = st.selectbox("Protocol", ["sftp", "ftp", "ftps"], key=f"newep_proto_{code}",
                             format_func={"sftp": "SFTP (over SSH)", "ftp": "FTP", "ftps": "FTPS (FTP over TLS)"}.get)
        fixed, values = {"protocol": proto}, {"port": 22 if proto == "sftp" else 21}
    if t == "database":
        engine = st.selectbox("Database type", list(DB_PORTS), key=f"newep_engine_{code}")
        fixed, values = {"engine": engine}, {"port": DB_PORTS[engine]}
    fk = f"newep_{code}_{t}" + (f"_{next(iter(fixed.values()))}" if fixed else "")
    with st.form(fk):
        name = st.text_input("Name *", f"{p['name']} {kinds[t].split(' ', 1)[1].split(' (')[0]}", key=f"{fk}_name")
        config, secret = _fields_form(t, types[t], values, fk, fixed=fixed)
        if st.form_submit_button("Create endpoint and test", type="primary", disabled=not common.can("write")):
            try:
                c = api("POST", "/api/connections", json={"name": name, "type": t, "config": config, "secret": secret,
                                                          "owner_partner": code})
            except ApiError as e:
                st.error(str(e))
                return
            (st.toast if c["test_ok"] else st.warning)(f"Created {c['name']} - {c['test_detail']}")
            st.session_state[f"route_pending_{code}"] = c["id"]
            st.rerun()
    st.caption("The endpoint is dedicated to this partner. You can make it common later.")


def _save_reply(code, body):
    try:
        api("PUT", f"/api/partners/{code}/reply", json=body)
    except ApiError as e:
        st.error(str(e))
        return
    common.partners.clear()
    st.toast("Route saved", icon="🔁")
    st.rerun()


def _adapters_form(p, conns, listeners):
    code = p["code"]
    types = api("GET", "/api/connection-types")
    st.markdown("**More ways in and out**")
    mine = visible_endpoints(conns, code)
    in_opts = [""] + [c for c in mine if types[conns[c]["type"]]["inbound"]]
    out_opts = [""] + [c for c in mine if types[conns[c]["type"]]["outbound"]]
    label = lambda c: (f"{types[conns[c]['type']]['icon']} {conns[c]['name']}"  # noqa: E731
                       + (" · 👤 this partner" if conns[c].get("owner_partner") else " · 🌍 common")) if c else None
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
            st.markdown("**Outbound copy** (fire and forget, no reply)")
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
    enabled = st.toggle("Store each completed message in a data table", value=bool(p.get("store_enabled")),
                        key=f"store_on_{code}", disabled=not common.can("write"))
    if not enabled:
        st.caption("Off: messages are kept in the transaction log only.")
        if p.get("store_enabled") and st.button("Save (turn storing off)", key=f"store_off_{code}"):
            _save_store(code, {"store_enabled": False})
        return
    dbs = {c["id"]: c for c in api("GET", "/api/connections") if c["type"] == "database"}
    db_opts = ["", *dbs]
    where = st.selectbox("Database", db_opts, db_opts.index(p.get("store_connection") or "")
                         if (p.get("store_connection") or "") in db_opts else 0, key=f"store_db_{code}",
                         format_func=lambda d: "🏠 Payload Hub data area (built-in)" if not d else f"🗄️ {dbs[d]['name']}")
    try:
        tables = {t["name"]: t for t in api("GET", f"/api/connections/{where}/tables" if where else "/api/data/tables")}
    except ApiError as e:
        st.error(f"Can't read that database: {e}")
        return
    if not tables:
        st.warning("That database has no tables yet - create one on the **Data** page first.")
        if "data" in common.PAGES:
            st.page_link(common.PAGES["data"], label="Open the Data page", icon="🗄️")
        return
    names = list(tables)
    c1, c2 = st.columns(2)
    prefix = "" if where else "userdata."
    table = c1.selectbox("Table", names, names.index(p["store_table"]) if p.get("store_table") in names else 0,
                         key=f"store_table_{code}_{where}", format_func=lambda t: f"{prefix}{t} ({tables[t]['rows']} rows)")
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
    body = {"store_enabled": True, "store_connection": where or None, "store_table": table, "store_source": source,
            "store_row_path": row_path,
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
    txns = api("GET", "/api/transactions", params={"partner": p["code"], "limit": 8})
    if not txns:
        st.caption("No transactions yet.")
        return
    df = pd.DataFrame(txns)
    df["created_at"] = pd.to_datetime(df["created_at"]).dt.tz_convert(common.TZ).dt.tz_localize(None).dt.strftime("%H:%M:%S")
    df["status"] = df["status"].map(status_label)
    df["delivery"] = df["delivery_status"].map({"DELIVERED": "📤 Delivered", "DELIVERY_FAILED": "⚠️ Failed"}).fillna("–")
    st.dataframe(df[["created_at", "source", "status", "delivery", "total_ms", "error"]], hide_index=True,
                 width="stretch", column_config={"created_at": "Time", "source": "Source", "status": "Status",
                                                 "delivery": "Delivery", "error": "Error",
                                                 "total_ms": st.column_config.NumberColumn("ms", format="%.0f")})
