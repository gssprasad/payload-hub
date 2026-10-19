"""Step-by-step partner onboarding wizard."""
import streamlit as st

from . import common
from .common import ApiError, api

STEPS = ["Basics", "Payloads & mapping", "Kafka queues", "Inbound", "Outbound", "Review"]
QUEUE_HELP = {
    "shared": "**Shared** - uses the common queues `payload.in` / `payload.out` / `payload.dlq`. "
              "Quick to set up; fine for low volume.",
    "dedicated": "**Dedicated** - gets its own queues `partner.<code>.in` / `.out` / `.dlq`, created automatically. "
                 "Isolates this partner's traffic, failures and backlog.",
}


DEFAULTS = {"name": "", "code": "", "description": "", "contact": "", "input_format": "json", "output_format": "json",
            "sample_input": "", "expected_output": "", "expression": "", "queue_mode": "shared",
            "inbound_connection": "", "inbound_source": "", "outbound_connection": "", "outbound_target": ""}


def _k(field: str) -> str:
    """Session key for a wizard field; values live in session state so they survive moving between steps."""
    key = f"wizf_{field}"
    if key not in st.session_state:
        st.session_state[key] = DEFAULTS[field]
    return key


class _Values(dict):
    """Read-through view of the wizard fields."""

    def __getitem__(self, field):
        return st.session_state[_k(field)]

    def get(self, field, default=None):
        return st.session_state.get(f"wizf_{field}", default) or default


def wizard():
    ss = st.session_state
    for key in [k for k in ss if k.startswith("wizf_")]:
        ss[key] = ss[key]  # keep values of widgets not shown on this step
    wiz = _Values()
    step = ss.setdefault("wiz_step", 0)

    st.progress((step + 1) / len(STEPS), text=f"Step {step + 1} of {len(STEPS)} · **{STEPS[step]}**")
    [_basics, _payloads, _queues, _inbound, _outbound, _review][step](wiz)

    back, nxt, cancel = st.columns([1, 1, 4])
    if step > 0 and back.button("← Back", key=f"wiz_back_{step}"):
        ss.wiz_step -= 1
        st.rerun()
    if step < len(STEPS) - 1 and nxt.button("Next →", type="primary", key=f"wiz_next_{step}"):
        problem = _validate(step, wiz)
        if problem:
            st.error(problem)
        else:
            ss.wiz_step += 1
            st.rerun()
    if cancel.button("Cancel", key=f"wiz_cancel_{step}"):
        _reset()
        st.rerun()


def _reset():
    for k in [k for k in st.session_state if k.startswith("wizf_")] + ["wiz_step", "wiz_doc"]:
        st.session_state.pop(k, None)
    st.session_state.onboarding = False


def _code(wiz) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "-", (wiz.get("code") or wiz.get("name") or "").lower()).strip("-")[:40]


def _validate(step, wiz):
    if step == 0 and len((wiz.get("name") or "").strip()) < 2:
        return "Enter the partner name."
    if step == 3 and wiz.get("inbound_connection") and not (wiz.get("inbound_source") or "").strip():
        return "Enter the queue or topic to listen on, or choose 'HTTP endpoint only'."
    return None


def _basics(wiz):
    c1, c2 = st.columns(2)
    c1.text_input("Partner name *", key=_k("name"), placeholder="e.g. Zenith Pharma")
    c2.text_input("Code", key=_k("code"), placeholder=f"auto: {_code(wiz) or 'zenith-pharma'}")
    st.text_input("Description", key=_k("description"), placeholder="e.g. Sends purchase orders, receives sales orders")
    st.text_input("Contact", key=_k("contact"), placeholder="e.g. integration team email")


def _payloads(wiz):
    c1, c2 = st.columns(2)
    c1.selectbox("Input format", ["json", "xml"], key=_k("input_format"))
    c2.selectbox("Output format", ["json", "xml"], key=_k("output_format"))
    c3, c4 = st.columns(2)
    c3.text_area("Sample input", key=_k("sample_input"), height=220, help="Used to test the mapping and by the AI agent.")
    c4.text_area("Expected output (optional)", key=_k("expected_output"), height=220)
    st.text_area("Mapping expression (optional)", key=_k("expression"), height=140,
                 help="JMESPath. Leave empty to generate it later with the AI agent.")
    doc = st.file_uploader("Mapping document (optional, saved with the partner)", type=["xlsx", "xlsm", "csv"],
                           key="wiz_doc_upload")
    if doc is not None:
        st.session_state.wiz_doc = (doc.name, doc.getvalue())
    if st.session_state.get("wiz_doc"):
        st.caption(f"📄 Will attach {st.session_state.wiz_doc[0]}")


def _queues(wiz):
    code = _code(wiz)
    st.radio("Kafka queues", ["shared", "dedicated"], key=_k("queue_mode"),
             format_func=lambda m: QUEUE_HELP[m].replace("<code>", code))


def _inbound(wiz):
    st.markdown(f"🌐 **HTTP endpoint** - always on: `POST {common.API}/inbound/{_code(wiz)}` with an API key "
                "generated for this partner.")
    conns = [c for c in api("GET", "/api/connections") if _types()[c["type"]]["inbound"]]
    options = [""] + [c["id"] for c in conns]
    names = {c["id"]: f"{_types()[c['type']]['icon']} {c['name']} ({_types()[c['type']]['label']})" for c in conns}
    if st.session_state[_k("inbound_connection")] not in options:
        st.session_state[_k("inbound_connection")] = ""
    st.selectbox("Also receive messages from", options, key=_k("inbound_connection"),
                 format_func=lambda c: names.get(c, "HTTP endpoint only"), help="Add brokers on the Connections page.")
    if wiz["inbound_connection"]:
        label = _types()[next(c["type"] for c in conns if c["id"] == wiz["inbound_connection"])]["target"]
        if not st.session_state[_k("inbound_source")]:
            st.session_state[_k("inbound_source")] = f"{_code(wiz)}.in"
        st.text_input(f"{label} to listen on", key=_k("inbound_source"))


def _outbound(wiz):
    conns = [c for c in api("GET", "/api/connections") if _types()[c["type"]]["outbound"]]
    options = [""] + [c["id"] for c in conns]
    names = {c["id"]: f"{_types()[c['type']]['icon']} {c['name']} ({_types()[c['type']]['label']})" for c in conns}
    if st.session_state[_k("outbound_connection")] not in options:
        st.session_state[_k("outbound_connection")] = ""
    st.selectbox("Deliver transformed results to", options, key=_k("outbound_connection"),
                 format_func=lambda c: names.get(c, "Nowhere - store only (callers read results via HTTP / the UI)"))
    if wiz["outbound_connection"]:
        type_ = next(c["type"] for c in conns if c["id"] == wiz["outbound_connection"])
        if not st.session_state[_k("outbound_target")]:
            st.session_state[_k("outbound_target")] = _code(wiz) if type_ == "http" else f"{_code(wiz)}.out"
        st.text_input(_types()[type_]["target"], key=_k("outbound_target"))
        st.caption("Failed deliveries are retried 3 times, then parked on the partner's failure queue.")


def _review(wiz):
    from .flow import flow_diagram

    code = _code(wiz)
    preview = {**{f: wiz[f] for f in DEFAULTS}, "code": code,
               "topic_in": f"partner.{code}.in" if wiz["queue_mode"] == "dedicated" else "payload.in",
               "topic_out": f"partner.{code}.out" if wiz["queue_mode"] == "dedicated" else "payload.out",
               "topic_dlq": f"partner.{code}.dlq" if wiz["queue_mode"] == "dedicated" else "payload.dlq",
               "expression": wiz.get("expression")}
    st.graphviz_chart(flow_diagram(preview, {}, {c["id"]: c for c in api("GET", "/api/connections")}))
    c1, c2 = st.columns(2)
    c1.markdown(f"**{wiz['name']}** (`{code}`)  \n{wiz.get('description') or ''}  \n"
                f"{wiz['input_format'].upper()} → {wiz['output_format'].upper()} · "
                f"{'mapping set' if wiz.get('expression') else 'no mapping yet'}"
                + (f" · 📄 {st.session_state.wiz_doc[0]}" if st.session_state.get("wiz_doc") else ""))
    c2.markdown(f"**Kafka:** {wiz['queue_mode']} queues  \n"
                f"**Inbound:** HTTP" + (f" + `{wiz['inbound_connection']}` → `{wiz['inbound_source']}`"
                                         if wiz.get("inbound_connection") else "") + "  \n"
                f"**Outbound:** " + (f"`{wiz['outbound_connection']}` → `{wiz.get('outbound_target', '')}`"
                                     if wiz.get("outbound_connection") else "store only"))
    if st.button("✅ Create partner", type="primary"):
        body = {k: (wiz.get(k) or None) for k in ("name", "code", "description", "contact", "sample_input",
                                                  "expected_output", "expression", "inbound_connection",
                                                  "inbound_source", "outbound_connection", "outbound_target")}
        body.update(input_format=wiz["input_format"], output_format=wiz["output_format"], queue_mode=wiz["queue_mode"])
        try:
            p = api("POST", "/api/partners", json=body)
            if st.session_state.get("wiz_doc"):
                name, data = st.session_state.wiz_doc
                api("POST", f"/api/partners/{p['code']}/documents", files={"file": (name, data)})
        except ApiError as e:
            st.error(str(e))
            return
        _reset()
        common.partners.clear()
        st.session_state.select_partner = p["code"]
        st.session_state.onboarding = False
        st.toast(f"Onboarded {p['name']} - its HTTP endpoint is live", icon="✅")
        st.rerun()


@st.cache_data(ttl=60)
def _types():
    return api("GET", "/api/connection-types")
