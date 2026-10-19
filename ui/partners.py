"""Partners page: onboard partners and manage each partner's mapping, documents and queues."""
import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api, lang, queue_label

QUEUE_HELP = {
    "shared": "Shared - uses the common queues payload.in / payload.out / payload.dlq",
    "dedicated": "Dedicated - gets its own queues partner.<code>.in / .out / .dlq",
}


def page():
    st.title("🤝 Partners")
    st.caption("Onboard partners, keep each partner's mapping documents, and choose shared or dedicated Kafka queues.")

    partners = common.partners()
    _partner_table(partners)
    _onboard_form()

    if not partners:
        st.info("No partners yet - onboard one above.")
        return
    st.divider()
    codes = [p["code"] for p in partners]
    default = st.session_state.pop("select_partner", None)
    code = st.selectbox("Manage partner", codes, index=codes.index(default) if default in codes else 0,
                        format_func=lambda c: next(f"{p['name']} ({c})" for p in partners if p["code"] == c))
    p = api("GET", f"/api/partners/{code}")
    details, mapping, documents, queue = st.tabs(["Details", "Mapping", "Mapping documents", "Queue"])
    with details:
        _details_tab(p)
    with mapping:
        _mapping_tab(p)
    with documents:
        _documents_tab(p)
    with queue:
        _queue_tab(p)


def _partner_table(partners):
    if not partners:
        return
    df = pd.DataFrame(partners)
    df["queue"] = df.apply(lambda r: f"{queue_label(r)} · {r['topic_in'].rsplit('.', 1)[0]}", axis=1)
    df["formats"] = df["input_format"].str.upper() + " → " + df["output_format"].str.upper()
    df["mapping"] = df.apply(lambda r: ("🤖 AI" if r["mapping_source"] == "agent" else "✍️ Manual")
                             if r["has_mapping"] else "— none", axis=1)
    df["status"] = df["active"].map({True: "Active", False: "Inactive"})
    st.dataframe(df[["name", "code", "status", "formats", "queue", "mapping", "documents", "transactions"]],
                 hide_index=True, width="stretch",
                 column_config={"name": "Partner", "code": "Code", "status": "Status", "formats": "Formats",
                                "queue": "Queue", "mapping": "Mapping", "documents": "Docs",
                                "transactions": "Transactions"})


def _onboard_form():
    with st.expander("➕ Onboard a new partner", expanded=not common.partners()):
        with st.form("onboard", clear_on_submit=True):
            c1, c2 = st.columns(2)
            name = c1.text_input("Partner name *", placeholder="e.g. Acme Retail")
            code = c2.text_input("Code", placeholder="auto from name, e.g. acme-retail")
            description = st.text_input("Description", placeholder="e.g. Sends purchase orders, receives invoices")
            contact = st.text_input("Contact", placeholder="e.g. integration team email")
            c3, c4 = st.columns(2)
            input_format = c3.selectbox("Input format", ["json", "xml"])
            output_format = c4.selectbox("Output format", ["json", "xml"])
            queue_mode = st.radio("Queues", ["shared", "dedicated"], format_func=QUEUE_HELP.get)
            sample_input = st.text_area("Sample input", height=150,
                                        help="Used to test the mapping and by the AI agent.")
            expected_output = st.text_area("Expected output (optional)", height=120,
                                           help="An example of what the partner expects back; tests compare against it.")
            expression = st.text_area("Mapping expression (optional)", height=120,
                                      help="JMESPath. Leave empty to build it later, e.g. with the AI agent from a mapping document.")
            if st.form_submit_button("Onboard partner", type="primary"):
                if len(name.strip()) < 2:
                    st.error("Enter the partner name.")
                    return
                body = {"name": name.strip(), "code": code.strip() or None, "description": description or None,
                        "contact": contact or None, "input_format": input_format, "output_format": output_format,
                        "queue_mode": queue_mode, "sample_input": sample_input or None,
                        "expected_output": expected_output or None, "expression": expression or None}
                try:
                    p = api("POST", "/api/partners", json=body)
                except ApiError as e:
                    st.error(str(e))
                    return
                common.partners.clear()
                st.session_state.select_partner = p["code"]
                st.toast(f"Onboarded {p['name']} on {queue_label(p).lower()} queues ({p['topic_in']})", icon="✅")
                st.rerun()


def _details_tab(p):
    with st.form(f"details_{p['code']}"):
        name = st.text_input("Partner name", p["name"])
        description = st.text_input("Description", p["description"] or "")
        contact = st.text_input("Contact", p["contact"] or "")
        active = st.toggle("Active", p["active"], help="Inactive partners can't send transactions.")
        if st.form_submit_button("Save details", type="primary"):
            _update(p["code"], {"name": name, "description": description or None, "contact": contact or None,
                                "active": active}, "Details saved")
    st.caption(f"Code `{p['code']}` · onboarded {pd.to_datetime(p['created_at']):%Y-%m-%d %H:%M}")
    with st.expander("Danger zone"):
        st.write("Removing a partner deletes its details and documents. Its transactions stay in the log, "
                 "and dedicated Kafka topics are left in place.")
        if st.checkbox(f"I want to remove {p['name']}", key=f"confirm_del_{p['code']}"):
            if st.button("Remove partner", type="primary", key=f"del_{p['code']}"):
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
        saved = st.form_submit_button("Save mapping", type="primary")
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
    if st.button("Generate mapping", type="primary", key=f"agent_{code}"):
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
        icon = "✅" if r["status"] == "SAVED" else "❌"
        st.markdown(f"{icon} **{r['status']}** · {len(r['steps'])} tool calls · {r['duration_ms'] / 1000:.1f} s · "
                    f"{r['usage']['input_tokens']:,} in / {r['usage']['output_tokens']:,} out tokens")
        if r.get("explanation"):
            st.info(r["explanation"])
        if r.get("mapping"):
            st.success("Saved as this partner's mapping (shown above).")
            st.code(r["mapping"]["preview"], language=lang(r["mapping"]["output_format"]))


def _documents_tab(p):
    code = p["code"]
    st.caption("Excel (.xlsx) or CSV mapping specifications for this partner. The AI agent uses them to build the mapping.")
    upload = st.file_uploader("Upload mapping document", type=["xlsx", "xlsm", "csv"], key=f"upload_{code}")
    if upload is not None and st.button("Save document", type="primary", key=f"save_doc_{code}"):
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
            if c2.button("Delete", key=f"deldoc_{d['id']}"):
                api("DELETE", f"/api/partners/{code}/documents/{d['id']}")
                common.partners.clear()
                st.rerun()


def _queue_tab(p):
    code = p["code"]
    st.markdown(f"**Current:** {queue_label(p)} queues")
    st.dataframe(pd.DataFrame([{"Role": "Input", "Topic": p["topic_in"]}, {"Role": "Output", "Topic": p["topic_out"]},
                               {"Role": "Failures (DLQ)", "Topic": p["topic_dlq"]}]),
                 hide_index=True, width="stretch")
    target = "shared" if p["queue_mode"] == "dedicated" else "dedicated"
    st.markdown(f"**Move to {target} queues**  \n{QUEUE_HELP[target].replace('<code>', code)}")
    st.caption("Messages already queued finish on the current queue; new messages use the new one.")
    if st.button(f"Move {p['name']} to {target} queues", key=f"move_{code}"):
        try:
            api("POST", f"/api/partners/{code}/queue", data={"queue_mode": target})
        except ApiError as e:
            st.error(str(e))
        else:
            common.partners.clear()
            st.toast(f"{p['name']} now uses {target} queues", icon="🔀")
            st.rerun()

    base = p["topic_in"].rsplit(".", 1)[0]
    rows = [q for q in api("GET", "/api/queues") if q["topic"].rsplit(".", 1)[0] == base]
    if rows:
        st.markdown("**Queue activity**")
        st.dataframe(pd.DataFrame(rows)[["topic", "partitions", "messages", "backlog"]], hide_index=True,
                     width="stretch", column_config={"topic": "Topic", "partitions": "Partitions",
                                                     "messages": "Messages", "backlog": "Waiting"})
        if p["queue_mode"] == "shared":
            st.caption("Shared queues carry traffic for every shared partner and ad-hoc tests.")


def _update(code, fields, message):
    try:
        api("PUT", f"/api/partners/{code}", json=fields)
    except ApiError as e:
        st.error(str(e))
        return
    common.partners.clear()
    st.toast(message, icon="✅")
    st.rerun()
