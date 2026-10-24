"""Partners page: onboarding wizard, and each partner's flow, endpoints, mapping, documents and details."""
import pandas as pd
import streamlit as st

from . import common, flow, onboarding
from .common import ApiError, api, lang

def page():
    common.hero("🤝 Partners", "Each partner's endpoint, mapping, route to its target system, and activity.")
    partners = common.partners()
    if st.session_state.get("onboarding") or not partners:
        with st.container(border=True):
            head = st.columns([4, 1], vertical_alignment="center")
            head[0].markdown("**➕ Onboard a new partner**")
            if partners and head[1].button("Cancel", width="stretch"):
                st.session_state.onboarding = False
                st.rerun()
            onboarding.wizard()
        return
    top = st.columns([3, 1], vertical_alignment="center")
    active = sum(p["active"] for p in partners)
    top[0].caption(f"{len(partners)} partners · {active} active · click a partner to open it")
    if top[1].button("➕ New partner", type="primary", width="stretch", disabled=not common.can("write")):
        st.session_state.onboarding = True
        st.rerun()
    codes = [p["code"] for p in partners]
    if (just_added := st.session_state.pop("select_partner", None)) in codes:
        st.session_state["partners_selected"] = just_added
    code = common.pick_row([_row(p) for p in partners], "partners", codes)
    p = api("GET", f"/api/partners/{code}")
    head = st.columns([4, 1], vertical_alignment="bottom")
    with head[0]:
        common.detail_title(f"{p['name']} · {p['input_format'].upper()} → {p['output_format'].upper()}")
    if head[1].button("📇 Profile", width="stretch", help="Everything about this partner on one card: endpoints, "
                                                         "content types, keys and credentials."):
        api_access_dialog(p)
    tabs = st.tabs(["Overview", "Endpoint", "Mapping", "Route", "Storage", "Documents", "Activity", "Details"])
    views = [flow.overview_tab, flow.endpoint_tab, _mapping_tab, flow.route_tab, flow.storage_tab, _documents_tab,
             flow.activity_tab, _details_tab]
    for tab, view in zip(tabs, views):
        with tab:
            view(p)


def _row(p) -> dict:
    route = "→ target system" if p.get("reply_mode") == "forward" and p.get("reply_connection") else "reply directly"
    return {"Partner": p["name"], "Code": p["code"], "Formats": f"{p['input_format'].upper()} → {p['output_format'].upper()}",
            "Mapping": ("🤖 AI" if p.get("mapping_source") == "agent" else "✍️ manual") if p.get("has_mapping") else "— none",
            "Route": route, "Transactions": p["transactions"], "Status": "🟢 active" if p["active"] else "⏸️ inactive"}


MIME = {"json": "application/json", "xml": "application/xml"}


@st.dialog("📇 Partner profile", width="large")
def api_access_dialog(p):
    """Everything about one partner: identity, formats and content types, inbound and outbound addresses,
    keys and credentials, retry policies, response, mapping, storage, documents and statistics."""
    code = p["code"]
    conns = {c["id"]: c for c in api("GET", "/api/connections")}
    types = api("GET", "/api/connection-types")
    admin, writer = common.can("admin"), common.can("write")
    url = f"{common.API}/inbound/{code}"
    reveal = admin and st.toggle("🔓 Show keys and passwords", key=f"prof_reveal_{code}",
                                 help="Administrators only. Values are fetched when you switch this on.")
    st.markdown(f"### {p['name']} &nbsp; <span class='ph-pill'>{code}</span> "
                f"{'🟢 Active' if p['active'] else '⏸️ Inactive'}", unsafe_allow_html=True)
    if p.get("description"):
        st.caption(p["description"])

    ident, inbound, outbound, mapping, more = st.tabs(
        ["🪪 Overview", "⬇️ Inbound", "⬆️ Outbound & response", "🔀 Mapping & data", "📊 Activity & files"])

    with ident:
        _kv([("Code", f"<code>{code}</code>"), ("Status", "Active" if p["active"] else "Inactive"),
             ("Contact", p.get("contact") or "—"),
             ("Onboarded", str(p["created_at"])[:19].replace("T", " ")),
             ("Input", f"{p['input_format'].upper()} · Content-Type <code>{MIME[p['input_format']]}</code>"),
             ("Output", f"{p['output_format'].upper()} · Content-Type <code>{MIME[p['output_format']]}</code>"),
             ("Kafka queues", f"{'Dedicated' if p['queue_mode'] == 'dedicated' else 'Shared'}: "
                              f"<code>{p['topic_in']}</code> → <code>{p['topic_out']}</code> · failures <code>{p['topic_dlq']}</code>")])
        st.markdown("**All endpoints**")
        st.dataframe(common.partner_endpoints(p, conns), hide_index=True, width="stretch")

    with inbound:
        st.markdown("**🌐 HTTP endpoint** (always on)")
        _kv([("URL", f"<code>POST {url}</code>"),
             ("Content-Type", f"<code>{MIME[p['input_format']]}</code> ({p['input_format'].upper()} body)"),
             ("Authentication", "Header <code>X-API-Key</code>"),
             ("Wait for result", "default; returns 200 with the result, 422 on mapping errors"),
             ("Async", f"<code>?wait=false</code> → 202, then <code>GET {url}/transactions/&lt;id&gt;</code>")])
        key = common.partner_key(code) if (reveal or (writer and st.toggle("Show API key", key=f"prof_key_{code}"))) \
            else "•" * 24
        st.code(key, language="text")
        st.code("\n".join([f'curl -X POST "{url}" \\', f'  -H "X-API-Key: {key if key[0] != "•" else "<api key>"}" \\',
                           f'  -H "Content-Type: {MIME[p["input_format"]]}" \\',
                           f'  --data-binary @payload.{p["input_format"]}']), language="bash")
        if p.get("inbound_connection") in conns:
            st.markdown("**📥 Also listening on**")
            _connection_details(conns[p["inbound_connection"]], types, p.get("inbound_source"), reveal,
                                role="queue / topic / folder / table to read")

    with outbound:
        st.markdown("**🔁 Route: Source → Target system → Source**")
        if p.get("reply_mode") == "forward" and p.get("reply_connection") in conns:
            rc = conns[p["reply_connection"]]
            fmt = {"input": p["input_format"], "json": "json", "xml": "xml", "unchanged": None}[p.get("response_format") or "input"]
            http = rc["type"] == "http"
            sends = "text/csv" if p.get("send_format") == "csv" else MIME[p["output_format"]]
            _kv([("Mode", "Send the transformed payload to the target system and return its "
                          + ("response" if http else "delivery receipt")),
                 ("Target system", f"{rc['name']} ({rc['type']}) · "
                              + ("👤 dedicated to this partner" if rc.get("owner_partner") else "🌍 common endpoint")),
                 ("Sends to", f"<code>{((p.get('reply_method') or rc['config'].get('method', 'POST')) + ' ') if http else ''}"
                              f"{common.connection_address(rc, p.get('reply_path'))}</code>"),
                 ("Sends", "URL query parameters (no request body)" if p.get("send_format") == "query"
                  else f"Content-Type <code>{sends}</code>"),
                 ("Returns to caller", f"Content-Type <code>{MIME[fmt]}</code> (translated)" if fmt
                  else "the target system's answer as is"),
                 ("Response mapping", f"<code>{p['response_expression']}</code>" if p.get("response_expression") else "none")])
            _connection_details(rc, types, p.get("reply_path"), reveal,
                                role="path" if rc["type"] == "http" else "folder / table / queue / topic")
        else:
            _kv([("Mode", "Return the transformed payload"),
                 ("Returns to caller", f"Content-Type <code>{MIME[p['output_format']]}</code>")])
        st.markdown("**📤 Outbound copy (asynchronous, no reply)**")
        if p.get("outbound_connection") in conns:
            oc = conns[p["outbound_connection"]]
            _kv([("Sends", f"Content-Type <code>{MIME[p['output_format']]}</code>"),
                 ("Headers", "<code>X-Transaction-Id</code>, <code>X-Partner</code>"),
                 ("On failure", f"retries, then <code>{p['topic_dlq']}</code>")])
            _connection_details(oc, types, p.get("outbound_target"), reveal, role="queue / topic / path / folder / table")
        else:
            st.caption("None - results are stored and returned to callers only.")

    with mapping:
        _kv([("Mapping", {"agent": "🤖 generated by the AI agent", "manual": "✍️ written by hand"}.get(
                 p.get("mapping_source"), "—") if p.get("expression") else "— none yet")])
        if p.get("expression"):
            st.code(p["expression"], language="text")
        e1, e2 = st.columns(2)
        with e1.expander("Sample input"):
            st.code(p.get("sample_input") or "—", language=lang(p["input_format"]))
        with e2.expander("Expected output"):
            st.code(p.get("expected_output") or "—", language=lang(p["output_format"]))
        st.markdown("**🗄️ Store in the database**")
        if p.get("store_enabled"):
            sc = conns.get(p.get("store_connection") or "")
            _kv([("Database", sc["name"] if sc else "Built-in data area (userdata)"),
                 ("Table", f"<code>{p['store_table']}</code>"),
                 ("From", f"the {p.get('store_source') or 'output'} payload"
                          + (f", one row per <code>{p['store_row_path']}</code>" if p.get("store_row_path") else ""))])
            st.dataframe([{"Column": k, "Value from": v} for k, v in (p.get("store_columns") or {}).items()],
                         hide_index=True, width="stretch")
            if sc:
                _connection_details(sc, types, p["store_table"], reveal, role="table")
        else:
            st.caption("Off.")
        docs = p.get("documents") or []
        st.markdown(f"**📄 Mapping documents** ({len(docs)})")
        for d in docs:
            st.caption(f"{d['filename']} · {d['size'] / 1024:.1f} KB · {str(d['uploaded_at'])[:16].replace('T', ' ')}")

    with more:
        stats = next((r for r in api("GET", "/api/stats/partners") if r["partner_code"] == code), None)
        if stats:
            m = st.columns(4)
            m[0].metric("Transactions", stats["total"])
            done = stats["completed"] + stats["failed"]
            m[1].metric("Success", f"{stats['completed'] / done * 100:.0f}%" if done else "–")
            m[2].metric("Avg ms", stats["avg_ms"] if stats["avg_ms"] is not None else "–")
            m[3].metric("Delivered", stats["delivered"], help=f"{stats['delivery_failed']} failed")
            st.caption(f"Last transaction: {str(stats['last_seen'])[:19].replace('T', ' ') if stats['last_seen'] else 'never'}")
        export = {k: v for k, v in p.items() if k not in ("api_key", "documents")}
        export["connections"] = {cid: {k: v for k, v in conns[cid].items() if k not in ("used_by",)}
                                 for cid in {p.get("inbound_connection"), p.get("outbound_connection"),
                                             p.get("reply_connection"), p.get("store_connection")} if cid in conns}
        st.download_button("⬇️ Download configuration (JSON, without keys or passwords)",
                           __import__("json").dumps(export, indent=2, default=str), file_name=f"partner-{code}.json",
                           mime="application/json", key=f"prof_export_{code}")
    if common.EMBEDDED:
        st.info("On Streamlit Cloud the HTTP endpoint isn't reachable from outside - use Flow & endpoints → Try it.")


def _kv(rows):
    st.html('<div class="ph-kv">' + "".join(f"<b>{k}</b><span>{v}</span>" for k, v in rows) + "</div>")


def _connection_details(c, types, target, reveal, role):
    """A connection's address, credentials and retry policy, as used by this partner."""
    spec = types.get(c["type"], {})
    cfg = c["config"]
    secret = ""
    if reveal and c.get("has_secret"):
        secret = api("GET", f"/api/connections/{c['id']}/secret")["secret"]
    rows = [("Adapter", f"{spec.get('icon', '')} {c['name']} <code>{c['id']}</code> · {spec.get('label', c['type'])}"),
            ("Address", f"<code>{common.connection_address(c, target)}</code>"),
            (role[0].upper() + role[1:], f"<code>{target or '—'}</code>")]
    if cfg.get("username"):
        rows.append(("Username", f"<code>{cfg['username']}</code>"))
    secret_label = "Auth header" if c["type"] == "http" else "Password"
    if c.get("has_secret"):
        rows.append((secret_label, f"<code>{secret}</code>" if reveal else "•••••••• (stored)"))
    elif c["type"] != "http":
        rows.append((secret_label, "none"))
    extra = {"http": [("Method", cfg.get("method")), ("Timeout", f"{cfg.get('timeout', 15)} s")],
             "ftp": [("Protocol", cfg.get("protocol")), ("File pattern", cfg.get("file_pattern")),
                     ("After reading", cfg.get("after_read")), ("Output file name", cfg.get("filename_pattern"))],
             "database": [("Database type", cfg.get("engine")), ("Poll interval", f"{cfg.get('poll_interval', 10)} s")],
             "kafka": [("Security", cfg.get("security_protocol"))],
             "activemq": [("Queue prefix", cfg.get("destination_prefix"))],
             "rabbitmq": [("Virtual host", cfg.get("vhost"))]}.get(c["type"], [])
    rows += [(k, f"<code>{v}</code>") for k, v in extra if v]
    if spec.get("outbound"):
        rows.append(("Retry policy", common.retry_summary(c)))
    status = "🟢 connected" if c["last_test_ok"] else "🔴 failing" if c["last_test_ok"] is False else "⚪ not tested"
    rows.append(("Last test", f"{status} · {c.get('last_test_detail') or ''}"))
    with st.container(border=True):
        _kv(rows)


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
