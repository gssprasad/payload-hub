"""Partners page: onboarding wizard, and each partner's flow, endpoints, mapping, documents and details."""
import pandas as pd
import streamlit as st

from . import common, flow, onboarding
from .common import ApiError, api, lang, queue_label

def page():
    common.hero("🤝 Partners", "Onboard partners, set up each one's mapping, documents, HTTP endpoint, MQ adapters and Kafka queues.")
    partners = common.partners()

    if st.session_state.get("onboarding") or not partners:
        with st.container(border=True):
            st.subheader("➕ Onboard a new partner")
            onboarding.wizard()
        if not partners:
            return
    elif st.button("➕ Onboard a new partner", type="primary", disabled=not common.can("write")):
        st.session_state.onboarding = True
        st.rerun()

    clicked = _partner_table(partners)
    st.divider()
    codes = [p["code"] for p in partners]
    requested = st.session_state.pop("select_partner", None) or clicked  # just onboarded, or clicked in the table
    if requested in codes:
        st.session_state.manage_partner = requested
    elif st.session_state.get("manage_partner") not in codes:
        st.session_state.manage_partner = codes[0]
    pick, access = st.columns([4, 1], vertical_alignment="bottom")
    code = pick.selectbox("Manage partner", codes, key="manage_partner",
                          format_func=lambda c: next(f"{p['name']} ({c})" for p in partners if p["code"] == c))
    p = api("GET", f"/api/partners/{code}")
    if clicked or access.button("🔑 API access", width="stretch"):
        api_access_dialog(p)
    flow_tab_, mapping, documents, details = st.tabs(["Flow & endpoints", "Mapping", "Mapping documents", "Details"])
    with flow_tab_:
        flow.flow_tab(p)
    with mapping:
        _mapping_tab(p)
    with documents:
        _documents_tab(p)
    with details:
        _details_tab(p)


def _partner_table(partners):
    if not partners:
        return
    df = pd.DataFrame(partners)
    df["queue"] = df.apply(lambda r: f"{queue_label(r)} · {r['topic_in'].rsplit('.', 1)[0]}", axis=1)
    df["formats"] = df["input_format"].str.upper() + " → " + df["output_format"].str.upper()
    df["mapping"] = df.apply(lambda r: ("🤖 AI" if r["mapping_source"] == "agent" else "✍️ Manual")
                             if r["has_mapping"] else "— none", axis=1)
    df["status"] = df["active"].map({True: "Active", False: "Inactive"})
    df["inbound"] = df.apply(lambda r: "HTTP" + (f" + {r['inbound_connection']}:{r['inbound_source']}"
                                                 if r["inbound_connection"] else ""), axis=1)
    df["outbound"] = df.apply(lambda r: f"{r['outbound_connection']}:{r['outbound_target'] or ''}"
                              if r["outbound_connection"] else "store only", axis=1)
    df["reply"] = df.apply(lambda r: f"↪ {r['reply_connection']}" if r["reply_mode"] == "forward"
                           else "transformed", axis=1)
    st.caption("💡 Click a partner to see its API access details.")
    event = st.dataframe(
        df[["name", "code", "status", "formats", "inbound", "queue", "reply", "outbound", "mapping", "documents",
            "transactions"]],
        hide_index=True, width="stretch", on_select="rerun", selection_mode=["single-row", "single-cell"],
        key="partner_table",
        column_config={"name": "Partner", "code": "Code", "status": "Status", "formats": "Formats",
                       "inbound": "Inbound", "queue": "Kafka queue", "reply": "Response", "outbound": "Outbound",
                       "mapping": "Mapping", "documents": "Docs", "transactions": "Transactions"})
    rows = list(event.selection.rows) + [cell[0] for cell in getattr(event.selection, "cells", [])]
    selected = partners[rows[0]]["code"] if rows else None
    # open the dialog only when the selection changes, not on every rerun while a row stays selected
    previous = st.session_state.get("partner_table_last")
    st.session_state.partner_table_last = selected
    return selected if selected and selected != previous else None


@st.dialog("🔑 API access", width="large")
def api_access_dialog(p):
    code = p["code"]
    url = f"{common.API}/inbound/{code}"
    ctype = "application/xml" if p["input_format"] == "xml" else "application/json"
    status = "🟢 Active" if p["active"] else "⏸️ Inactive"
    if p.get("reply_mode") == "forward":
        fmt = {"input": p["input_format"].upper() + " (same as the request)", "json": "JSON", "xml": "XML",
               "unchanged": "the API's own format"}[p.get("response_format") or "input"]
        reply = f"Forwards to **{p['reply_connection']}**{('/' + p['reply_path']) if p.get('reply_path') else ''} " \
                f"and returns that API's response as {fmt}"
    else:
        reply = f"Returns the transformed payload ({p['output_format'].upper()})"
    st.markdown(f"### {p['name']} &nbsp; <span class='ph-pill'>{code}</span>", unsafe_allow_html=True)
    st.html(f"""<div class="ph-kv">
      <b>Status</b><span>{status}</span>
      <b>Method</b><span>POST</span>
      <b>Request body</b><span>{p['input_format'].upper()} ({ctype})</span>
      <b>Authentication</b><span>Header <code>X-API-Key</code></span>
      <b>Response</b><span>{reply.replace('**', '')}</span>
      <b>Async option</b><span><code>?wait=false</code> → 202 + transaction id, then
        <code>GET /inbound/{code}/transactions/&lt;id&gt;</code></span>
    </div>""")
    st.markdown("**Inbound & outbound endpoints**")
    conns = {c["id"]: c for c in api("GET", "/api/connections")}
    st.dataframe(common.partner_endpoints(p, conns), hide_index=True, width="stretch")
    st.markdown("**Endpoint URL**")
    st.code(url, language="text")
    reveal = common.can("write") and st.toggle("Show API key", key=f"dlg_reveal_{code}")
    st.markdown("**API key**")
    st.code(p["api_key"] if reveal else "•" * 24 + "  (needs write permission to reveal)" * (not common.can("write")),
            language="text")
    key = p["api_key"] if reveal else "<api key>"
    sample = "payload.xml" if p["input_format"] == "xml" else "payload.json"
    st.markdown("**Try it from a terminal**")
    st.code("\n".join([f'curl -X POST "{url}" \\',
                       f'  -H "X-API-Key: {key}" \\',
                       f'  -H "Content-Type: {ctype}" \\',
                       f'  --data-binary @{sample}']), language="bash")
    if common.EMBEDDED:
        st.info("On Streamlit Cloud this endpoint isn't reachable from outside - use Flow & endpoints → Try it.")



def _details_tab(p):
    with st.form(f"details_{p['code']}"):
        name = st.text_input("Partner name", p["name"])
        description = st.text_input("Description", p["description"] or "")
        contact = st.text_input("Contact", p["contact"] or "")
        active = st.toggle("Active", p["active"], help="Inactive partners can't send transactions.")
        if st.form_submit_button("Save details", type="primary", disabled=not common.can("write")):
            _update(p["code"], {"name": name, "description": description or None, "contact": contact or None,
                                "active": active}, "Details saved")
    st.caption(f"Code `{p['code']}` · onboarded {pd.to_datetime(p['created_at']):%Y-%m-%d %H:%M}")
    with st.expander("Danger zone"):
        st.write("Removing a partner deletes its details and documents. Its transactions stay in the log, "
                 "and dedicated Kafka topics are left in place.")
        if st.checkbox(f"I want to remove {p['name']}", key=f"confirm_del_{p['code']}"):
            if st.button("Remove partner", type="primary", key=f"del_{p['code']}", disabled=not common.can("delete")):
                api("DELETE", f"/api/partners/{p['code']}")
                common.partners.clear()
                st.rerun()


def _mapping_tab(p):
    code = p["code"]
    with st.form(f"mapping_{code}"):
        c1, c2 = st.columns(2)
        input_format = c1.selectbox("Input format", ["json", "xml"], index=["json", "xml"].index(p["input_format"]))
        output_format = c2.selectbox("Output format", ["json", "xml"], index=["json", "xml"].index(p["output_format"]))
        c3, c4 = st.columns(2)
        sample_input = c3.text_area("Sample input", p["sample_input"] or "", height=260)
        expected_output = c4.text_area("Expected output (optional)", p["expected_output"] or "", height=260)
        source = {"agent": "generated by the AI agent", "manual": "written by hand"}.get(p["mapping_source"], "")
        expression = st.text_area(f"Mapping expression (JMESPath){' - ' + source if source else ''}",
                                  p["expression"] or "", height=200)
        saved = st.form_submit_button("Save mapping", type="primary", disabled=not common.can("write"))
    if saved:
        _update(code, {"input_format": input_format, "output_format": output_format,
                       "sample_input": sample_input or None, "expected_output": expected_output or None,
                       "expression": expression or None}, "Mapping saved")

    if st.button("Test mapping on the sample input", disabled=not (p["expression"] and p["sample_input"]),
                 key=f"test_{code}"):
        try:
            r = api("POST", f"/api/partners/{code}/test")
        except ApiError as e:
            st.error(str(e))
        else:
            if not r["ok"]:
                st.error(r["error"])
            else:
                if r["matches_expected"] is True:
                    st.success("Output matches the expected output.")
                elif r["matches_expected"] is False:
                    st.warning("Output differs from the expected output.")
                st.code(r["output"], language=lang(p["output_format"]))

    st.subheader("Generate with the AI agent")
    docs = p.get("documents", [])
    if not p["sample_input"]:
        st.info("Add a sample input above first - the agent tests its mapping against it.")
        return
    chosen = st.multiselect("Mapping documents to use", [d["id"] for d in docs], default=[d["id"] for d in docs],
                            format_func=lambda i: next(d["filename"] for d in docs if d["id"] == i),
                            key=f"agent_docs_{code}", placeholder="None - upload on the Mapping documents tab")
    instructions = st.text_area("Instructions (optional with documents)", key=f"agent_inst_{code}", height=80,
                                placeholder="e.g. Use carrier BLUEDART. Dates as YYYY-MM-DD.")
    if st.button("Generate mapping", type="primary", key=f"agent_{code}", disabled=not common.can("write")):
        with st.spinner("Agent is reading the documents, writing and testing the mapping… (usually 15–60 s)"):
            try:
                r = api("POST", f"/api/partners/{code}/agent", timeout=600,
                        json={"instructions": instructions, "document_ids": chosen})
            except ApiError as e:
                st.error(str(e))
                return
        st.session_state[f"agent_result_{code}"] = r
        common.partners.clear()
        st.rerun()
    if r := st.session_state.get(f"agent_result_{code}"):
        common.show_agent_result(r)


def _documents_tab(p):
    code = p["code"]
    st.caption("Excel (.xlsx) or CSV mapping specifications for this partner. The AI agent uses them to build the mapping.")
    upload = st.file_uploader("Upload mapping document", type=["xlsx", "xlsm", "csv"], key=f"upload_{code}")
    if upload is not None and st.button("Save document", type="primary", key=f"save_doc_{code}", disabled=not common.can("write")):
        try:
            api("POST", f"/api/partners/{code}/documents", files={"file": (upload.name, upload.getvalue())})
        except ApiError as e:
            st.error(str(e))
        else:
            common.partners.clear()
            st.toast(f"Saved {upload.name}", icon="📄")
            st.rerun()

    docs = p.get("documents", [])
    if not docs:
        st.info("No documents yet.")
        return
    for d in docs:
        with st.expander(f"📄 {d['filename']} · {d['size'] / 1024:.1f} KB · "
                         f"uploaded {pd.to_datetime(d['uploaded_at']):%Y-%m-%d %H:%M}"):
            full = api("GET", f"/api/partners/{code}/documents/{d['id']}")
            sheets = full["sheets"]
            for sheet_tab, sheet in zip(st.tabs([s["name"] for s in sheets]), sheets):
                with sheet_tab:
                    rows = sheet["rows"]
                    df = pd.DataFrame(rows[1:], columns=[c or f"col{i + 1}" for i, c in enumerate(rows[0])])
                    df.index = range(2, len(rows) + 1)  # spreadsheet row numbers
                    st.dataframe(df, width="stretch", height=min(400, 38 + 35 * len(df)))
            c1, c2 = st.columns([1, 1])
            c1.download_button("Download", api("GET", f"/api/partners/{code}/documents/{d['id']}/download"),
                               file_name=d["filename"], key=f"dl_{d['id']}")
            if c2.button("Delete", key=f"deldoc_{d['id']}", disabled=not common.can("delete")):
                api("DELETE", f"/api/partners/{code}/documents/{d['id']}")
                common.partners.clear()
                st.rerun()



def _update(code, fields, message):
    try:
        api("PUT", f"/api/partners/{code}", json=fields)
    except ApiError as e:
        st.error(str(e))
        return
    common.partners.clear()
    st.toast(message, icon="✅")
    st.rerun()
