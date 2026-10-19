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

    _http_endpoint(p)
    st.divider()
    _adapters_form(p, conns, listeners)
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
            (st.success if ok else st.error)(f"HTTP {r.status_code} · transaction {r.headers.get('X-Transaction-Id', '-')}")
            is_xml = "xml" in r.headers.get("content-type", "")
            st.code(r.text, language="xml" if is_xml else "json")


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
