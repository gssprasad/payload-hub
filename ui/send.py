"""Send & Test page: push a payload through Kafka as a partner, or with an ad-hoc mapping."""
import streamlit as st

from . import common
from .common import ApiError, api, lang, queue_label, show_events, status_label


def page():
    common.hero("▶️ Send & Test", "Send a payload through the Kafka pipeline and see the transformed result.")
    mode = st.radio("Send as", ["Partner", "Ad-hoc mapping"], horizontal=True,
                    help="Partner uses the partner's mapping and its shared or dedicated queue.")
    if mode == "Partner":
        _partner_send()
    else:
        _adhoc_send()

    if t := st.session_state.get("last_txn"):
        st.divider()
        _show_result(t)


def _partner_send():
    ready = [p for p in common.partners() if p["active"] and p["has_mapping"]]
    if not ready:
        st.info("No active partner with a mapping yet - set one up on the Partners page.")
        return
    code = st.selectbox("Partner", [p["code"] for p in ready],
                        format_func=lambda c: next(f"{p['name']} · {queue_label(p).lower()} queue · "
                                                   f"{p['input_format'].upper()} → {p['output_format'].upper()}"
                                                   for p in ready if p["code"] == c))
    p = api("GET", f"/api/partners/{code}")
    st.caption(f"Goes to **{p['topic_in']}** → transformer → **{p['topic_out']}** (failures: {p['topic_dlq']})")
    payload = st.text_area("Payload", p["sample_input"] or "", height=280, key=f"send_payload_{code}")
    if st.button("Send via Kafka", type="primary", disabled=not common.can("write")):
        _submit({"partner_code": code, "payload": payload})


def _adhoc_send():
    samples = api("GET", "/api/samples")
    ss = st.session_state
    if "adhoc_mapping" not in ss:
        first = next(iter(samples))
        ss.update(adhoc_mapping=first, adhoc_payload=samples[first]["input"], adhoc_expr=samples[first]["expression"],
                  adhoc_in=samples[first]["input_format"], adhoc_out=samples[first]["output_format"])

    def on_change():
        s = samples[ss.adhoc_mapping]
        ss.adhoc_expr, ss.adhoc_in, ss.adhoc_out = s["expression"], s["input_format"], s["output_format"]
        if s.get("input"):
            ss.adhoc_payload = s["input"]

    labels = {k: ("🤖 " if v.get("saved_by_agent") else "") + v["label"] for k, v in samples.items()}
    c1, c2, c3 = st.columns([2, 1, 1])
    c1.selectbox("Mapping", list(labels), format_func=labels.get, key="adhoc_mapping", on_change=on_change)
    c2.selectbox("Input format", ["json", "xml"], key="adhoc_in")
    c3.selectbox("Output format", ["json", "xml"], key="adhoc_out")
    st.caption("Ad-hoc tests use the shared queues: payload.in → transformer → payload.out")
    left, right = st.columns(2)
    left.text_area("Payload", key="adhoc_payload", height=280)
    right.text_area("Mapping expression (JMESPath)", key="adhoc_expr", height=280)
    if st.button("Send via Kafka", type="primary", disabled=not common.can("write")):
        same = samples.get(ss.adhoc_mapping, {}).get("expression") == ss.adhoc_expr
        _submit({"payload": ss.adhoc_payload, "input_format": ss.adhoc_in, "output_format": ss.adhoc_out,
                 "expression": ss.adhoc_expr, "mapping_name": ss.adhoc_mapping if same else "custom"})


def _submit(body):
    with st.spinner("Publishing to Kafka…"):
        try:
            st.session_state.last_txn = api("POST", "/api/transform", json=body)
        except ApiError as e:
            st.error(str(e))


def _show_result(t):
    who = f"partner **{t['partner_code']}**" if t.get("partner_code") else "ad-hoc mapping"
    st.subheader("Result")
    st.caption(f"Transaction {t['id']} · {who} · queue {t.get('topic') or '-'}")
    m1, m2, m3 = st.columns(3)
    m1.metric("Status", status_label(t["status"]))
    m2.metric("Transform", f"{t['transform_ms']:.1f} ms" if t["transform_ms"] is not None else "–")
    m3.metric("End-to-end", f"{t['total_ms']:.1f} ms" if t["total_ms"] is not None else "–")
    if reply := t.get("reply"):
        if reply.get("ok") or reply.get("body"):
            st.markdown(f"**↩️ Response for the caller** - from the target API (HTTP {reply['status']}), "
                        f"as {reply.get('format', '').upper()}")
            body = reply.get("body") or ""
            st.code(body, language="xml" if body.lstrip().startswith("<") else "json")
        else:
            st.error(reply.get("error", "Forwarding failed"))
        with st.expander("Transformed payload sent to the target API"):
            st.code(t.get("output_payload") or "", language=lang(t["output_format"]))
    elif t.get("output_payload"):
        st.code(t["output_payload"], language=lang(t["output_format"]))
    elif t.get("error"):
        st.error(t["error"])
    else:
        st.warning("Still processing - check the Transactions page.")
    show_events(t.get("events", []))
