"""AI Agent page: build mappings with Claude, Gemini, OpenAI, GitHub Models or Azure OpenAI from a mapping
document and/or instructions; administrators paste provider API keys here."""
import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api, lang, show_agent_result, show_agent_steps, status_label


def page():
    common.hero("🤖 AI Agent", "The agent reads your mapping document and instructions, inspects a sample payload, writes the mapping, tests it with the real transformer, and saves it.")

    info = api("GET", "/api/ai/providers")
    providers = {p["id"]: p for p in info["providers"]}
    if common.can("admin"):
        _providers_panel(providers, info["active"])
    choice = _model_picker(providers, info["active"])
    ready = choice["ready"]
    st.session_state.agent_choice = {"provider": choice["provider"], "model": choice["model"]}

    target = st.radio("Build a mapping", ["For a partner", "New standalone mapping"], horizontal=True,
                      help="For a partner: uses the partner's sample input and documents and saves the mapping on the "
                           "partner. Standalone: saved as a reusable mapping you can pick on Send & Test (ad-hoc).")
    if target == "For a partner":
        _for_partner(ready)
    else:
        _standalone(ready)

    st.divider()
    _history()


def _model_picker(providers: dict, active: dict) -> dict:
    """Provider and model for the next run (defaults to the active choice)."""
    usable = [pid for pid, p in providers.items() if p["has_key"] or p["from_env"]]
    with st.container(border=True):
        c1, c2 = st.columns([1, 1.4])
        options = list(providers)
        default = active["provider"] if active["provider"] in options else options[0]
        pid = c1.selectbox("AI provider", options, options.index(default), key="agent_provider",
                           format_func=lambda i: f"{providers[i]['icon']} {providers[i]['label']}"
                           + ("" if i in usable else "  - no key"))
        p = providers[pid]
        loaded = st.session_state.get(f"models_{pid}") or []
        suggestions = list(dict.fromkeys(([active["model"]] if pid == active["provider"] else []) + p["models"] + loaded))
        if suggestions:
            model = c2.selectbox("Model", suggestions + ["(type another…)"], key=f"agent_model_{pid}")
            if model == "(type another…)":
                model = c2.text_input("Model name", key=f"agent_model_typed_{pid}")
        else:
            model = c2.text_input("Model", key=f"agent_model_typed_{pid}",
                                  placeholder="e.g. a model id - or Load models under AI providers")
        if pid not in usable:
            st.warning(f"{p['label']} has no API key yet." + (" Paste one under **🔑 AI providers** above." if
                                                             common.can("admin") else " Ask an administrator to add one."))
        elif not model:
            st.info("Choose or type a model.")
        else:
            st.caption(f"🟢 The agent will use **{p['label']}** · `{model}`"
                       + ("  (the default)" if pid == active["provider"] and model == active["model"] else ""))
    return {"provider": pid, "model": model, "ready": bool(pid in usable and model)}


def _providers_panel(providers: dict, active: dict):
    with st.expander("🔑 AI providers - paste API keys, load models, choose the default"):
        st.caption("Keys are encrypted before they're stored and never shown again - only their last 4 characters. "
                   "Only administrators see this panel.")
        for pid, p in providers.items():
            with st.container(border=True):
                head, state = st.columns([3, 2])
                head.markdown(f"**{p['icon']} {p['label']}**")
                if p["has_key"]:
                    state.markdown(f"🟢 Key saved `{p['key_hint']}`")
                elif p["from_env"]:
                    state.markdown("🟢 Key from environment variable")
                else:
                    state.markdown("⚪ No key")
                st.caption(p["key_help"])
                with st.form(f"ai_key_{pid}", clear_on_submit=True):
                    key = st.text_input("API key" if pid != "github" else "GitHub token", type="password",
                                        placeholder="paste to add or replace - leave empty to keep the saved key")
                    config = {}
                    for f in p["fields"]:
                        config[f["key"]] = st.text_input(f["label"], p["config"].get(f["key"], f["default"]),
                                                         key=f"ai_cfg_{pid}_{f['key']}")
                    if st.form_submit_button("Save and test", type="primary"):
                        try:
                            api("PUT", f"/api/ai/providers/{pid}", json={"api_key": key or None,
                                                                         "config": config if p["fields"] else None})
                            r = api("POST", f"/api/ai/providers/{pid}/test", timeout=60)
                        except ApiError as e:
                            st.error(str(e))
                        else:
                            if r.get("models"):
                                st.session_state[f"models_{pid}"] = r["models"]
                            st.session_state[f"ai_test_{pid}"] = r
                            st.rerun()
                if r := st.session_state.get(f"ai_test_{pid}"):
                    (st.success if r["ok"] else st.error)(r["detail"])
                b1, b2, b3 = st.columns(3)
                if b1.button("Load models", key=f"ai_models_{pid}", disabled=not (p["has_key"] or p["from_env"])):
                    try:
                        st.session_state[f"models_{pid}"] = api("GET", f"/api/ai/providers/{pid}/models", timeout=60)["models"]
                        st.toast(f"{len(st.session_state[f'models_{pid}'])} models loaded", icon="📋")
                    except ApiError as e:
                        st.error(str(e))
                if p["has_key"] and b2.button("Remove key", key=f"ai_rm_{pid}"):
                    api("DELETE", f"/api/ai/providers/{pid}/key")
                    st.session_state.pop(f"ai_test_{pid}", None)
                    st.rerun()
                if pid == active["provider"]:
                    b3.markdown(f"⭐ Default · `{active['model']}`")
        st.markdown("**Default for all agent runs**")
        d1, d2, d3 = st.columns([1, 1.3, 0.7], vertical_alignment="bottom")
        dp = d1.selectbox("Provider", list(providers), list(providers).index(active["provider"]), key="ai_default_p",
                          format_func=lambda i: f"{providers[i]['icon']} {providers[i]['label']}")
        opts = list(dict.fromkeys(providers[dp]["models"] + (st.session_state.get(f"models_{dp}") or [])))
        dm = d2.selectbox("Model", opts, key=f"ai_default_m_{dp}") if opts else \
            d2.text_input("Model", active["model"] if dp == active["provider"] else "", key=f"ai_default_mt_{dp}")
        if d3.button("Set default", key="ai_set_default"):
            try:
                api("PUT", "/api/ai/active", json={"provider": dp, "model": dm})
            except ApiError as e:
                st.error(str(e))
            else:
                st.toast("Default model saved", icon="⭐")
                st.rerun()


def _for_partner(ready: bool):
    partners = common.partners()
    if not partners:
        st.info("No partners yet - onboard one on the Partners page, or build a standalone mapping.")
        return
    code = st.selectbox("Partner", [p["code"] for p in partners],
                        format_func=lambda c: next(f"{p['name']} · {p['input_format'].upper()} → "
                                                   f"{p['output_format'].upper()} · {p['documents']} document(s)"
                                                   for p in partners if p["code"] == c))
    p = api("GET", f"/api/partners/{code}")
    if not p["sample_input"]:
        st.warning("This partner has no sample input. Add one on Partners → Mapping - the agent tests against it.")
        return
    with st.expander("Sample input the agent will test with"):
        st.code(p["sample_input"], language=lang(p["input_format"]))
    if p["expected_output"]:
        with st.expander("Expected output the agent will aim for"):
            st.code(p["expected_output"], language=lang(p["output_format"]))

    docs = p.get("documents", [])
    upload = st.file_uploader("Add a mapping document to this partner (optional)", type=["xlsx", "xlsm", "csv"],
                              key=f"agent_upload_{code}")
    if upload is not None and st.button("Save document to partner", key=f"agent_save_doc_{code}", disabled=not common.can("write")):
        try:
            api("POST", f"/api/partners/{code}/documents", files={"file": (upload.name, upload.getvalue())})
        except ApiError as e:
            st.error(str(e))
        else:
            common.partners.clear()
            st.rerun()
    chosen = st.multiselect("Mapping documents to use", [d["id"] for d in docs], default=[d["id"] for d in docs],
                            format_func=lambda i: next(d["filename"] for d in docs if d["id"] == i),
                            placeholder="None - instructions only")
    instructions = st.text_area("Instructions", height=110, key=f"agent_page_inst_{code}",
                                placeholder="Optional with documents, e.g. 'Dates as YYYY-MM-DD. Skip internal notes.'")
    if p["expression"]:
        st.caption("This partner already has a mapping; a successful run replaces it.")
    if st.button("Run agent", type="primary", disabled=not (ready and common.can("write"))):
        if not chosen and len(instructions.strip()) < 3:
            st.warning("Choose a mapping document or write instructions.")
            return
        with st.spinner("Agent is reading the documents, writing and testing the mapping… (usually 15–60 s)"):
            try:
                r = api("POST", f"/api/partners/{code}/agent", timeout=600,
                        json={"instructions": instructions, "document_ids": chosen, **st.session_state.agent_choice})
            except ApiError as e:
                st.error(str(e))
                return
        common.partners.clear()
        st.session_state[f"agent_page_result_{code}"] = r
    if r := st.session_state.get(f"agent_page_result_{code}"):
        show_agent_result(r)


def _standalone(ready: bool):
    ss = st.session_state
    doc_file = st.file_uploader("Mapping document (Excel .xlsx or CSV, optional)", type=["xlsx", "xlsm", "csv"])
    doc = None
    if doc_file is not None:
        key = (doc_file.name, doc_file.size)
        if ss.get("agent_doc_key") != key:
            try:
                ss.agent_doc = api("POST", "/api/mapping-doc", files={"file": (doc_file.name, doc_file.getvalue())})
                ss.agent_doc_key = key
            except ApiError as e:
                ss.agent_doc, ss.agent_doc_key = None, None
                st.error(str(e))
        doc = ss.get("agent_doc")
        if doc:
            st.caption(" · ".join(f"{s['name']}: {len(s['rows'])} rows" for s in doc["sheets"]))
            for sheet_tab, sheet in zip(st.tabs([s["name"] for s in doc["sheets"]]), doc["sheets"]):
                with sheet_tab:
                    rows = sheet["rows"]
                    df = pd.DataFrame(rows[1:], columns=[c or f"col{i + 1}" for i, c in enumerate(rows[0])])
                    df.index = range(2, len(rows) + 1)
                    st.dataframe(df, width="stretch", height=min(320, 38 + 35 * len(df)))

    c1, _ = st.columns([1, 3])
    input_format = c1.selectbox("Sample payload format", ["json", "xml"])
    payload = st.text_area("Sample payload *", height=220, placeholder="Paste a real example of the input message")
    instructions = st.text_area("Instructions", height=110,
                                placeholder="Optional with a document, e.g. 'Turn this order into a shipment as XML…'")
    if st.button("Run agent", type="primary", disabled=not (ready and common.can("write"))):
        if not payload.strip():
            st.warning("Paste a sample payload - the agent tests its mapping against it.")
            return
        if not doc and len(instructions.strip()) < 3:
            st.warning("Upload a mapping document or write instructions.")
            return
        with st.spinner("Agent is inspecting the payload, writing and testing a mapping… (usually 15–60 s)"):
            try:
                r = api("POST", "/api/agent", timeout=600, json={
                    "request": instructions, "payload": payload, "input_format": input_format,
                    **st.session_state.agent_choice,
                    "mapping_doc": {"filename": doc["filename"], "text": doc["text"]} if doc else None})
            except ApiError as e:
                st.error(str(e))
                return
        ss.agent_standalone_result = r
    if r := ss.get("agent_standalone_result"):
        show_agent_result(r)
        if r.get("mapping"):
            st.caption("Use it on **Send & Test → Ad-hoc mapping** (marked 🤖), or paste the expression into a "
                       "partner's Mapping tab.")


def _history():
    st.subheader("Run history")
    runs = api("GET", "/api/agent/runs", params={"limit": 50})
    if not runs:
        st.caption("No agent runs yet.")
        return
    names = {p["code"]: p["name"] for p in common.partners()}
    df = pd.DataFrame(runs)
    df["created_at"] = pd.to_datetime(df["created_at"]).dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["status"] = df["status"].map(status_label)
    df["partner"] = df["partner_code"].map(lambda c: names.get(c, c) if c else "Standalone")
    df["seconds"] = df["duration_ms"] / 1000
    event = st.dataframe(
        df[["created_at", "partner", "provider", "model", "mapping_doc_name", "status", "mapping_name", "tool_calls",
            "input_tokens",
            "output_tokens", "seconds"]],
        hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row",
        column_config={"created_at": "Time", "partner": "For", "provider": "Provider", "model": "Model", "mapping_doc_name": "Documents", "status": "Status",
                       "mapping_name": "Saved as", "tool_calls": "Tool calls", "input_tokens": "Tokens in",
                       "output_tokens": "Tokens out",
                       "seconds": st.column_config.NumberColumn("Duration (s)", format="%.1f")})
    if not event.selection.rows:
        st.caption("Select a run to see what the agent did.")
        return
    run = api("GET", f"/api/agent/runs/{runs[event.selection.rows[0]]['id']}")
    st.markdown(f"**Run {run['id']}** · {status_label(run['status'])}")
    if run["request"]:
        with st.expander("Request sent to the agent"):
            st.text(run["request"])
    if run["explanation"]:
        st.info(run["explanation"])
    show_agent_steps(run["steps"])
