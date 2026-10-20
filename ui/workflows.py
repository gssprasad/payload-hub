"""Workflows page: build multi-step workflows (database, API, transform, condition, PDF, deliver) and run them."""
import json

import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

STEP_TYPES = {
    "db_query": "🗄️ Database query",
    "db_update": "✏️ Database update",
    "api_call": "🌐 API call",
    "transform": "🔀 Transform",
    "condition": "❓ Condition",
    "pdf": "📄 Create PDF",
    "deliver": "📤 Deliver file",
}
STATUS = {"COMPLETED": "✅", "FAILED": "❌", "STOPPED": "⏹️", "RUNNING": "⏳", "OK": "✅", "SKIPPED": "⏭️",
          "REUSED": "♻️"}
NEW_STEP = {
    "db_query": {"connection": "builtin", "mode": "one", "required": True, "sql": "SELECT * FROM my_table WHERE id = :id",
                 "params": {"id": "{{ trigger.id }}"}},
    "db_update": {"connection": "builtin", "sql": "UPDATE my_table SET status = 'DONE' WHERE id = :id",
                  "params": {"id": "{{ trigger.id }}"}},
    "api_call": {"method": "GET", "path": "", "content_type": "application/json", "fail_on_error": True},
    "transform": {"expression": "{total: sum(lines[].amount)}"},
    "condition": {"expression": "length(lines) > `0`", "message": "Nothing to do."},
    "pdf": {"filename": "document_{{ trigger.id }}.pdf",
            "template": "<html><body><h1>Document</h1><p>{{ trigger }}</p></body></html>"},
    "deliver": {"target": ""},
}


def page():
    common.hero("⚙️ Workflows", "Chain database queries, API calls, updates and PDF creation - each step can use "
                                "the results of the steps before it.")
    data = api("GET", "/api/workflows")
    wfs, triggers = data["workflows"], data["triggers"]
    m = st.columns(4)
    m[0].metric("Workflows", len(wfs))
    m[1].metric("Runs", sum(w["runs"] for w in wfs))
    m[2].metric("Completed", sum(w["completed"] for w in wfs))
    m[3].metric("Failed", sum(w["failed"] for w in wfs))

    _new_workflow()
    if not wfs:
        st.info("No workflows yet - create one above.")
        return
    selected = _table(wfs, triggers)
    ids = [w["id"] for w in wfs]
    if selected:
        st.session_state.wf_pick = selected
    if st.session_state.get("wf_pick") not in ids:
        st.session_state.wf_pick = ids[0]
    wf_id = st.selectbox("Workflow", ids, key="wf_pick",
                         format_func=lambda i: next(w["name"] for w in wfs if w["id"] == i))
    wf = api("GET", f"/api/workflows/{wf_id}")
    if wf.get("description"):
        st.caption(wf["description"])
    run_tab, steps_tab, trigger_tab, runs_tab = st.tabs(["▶️ Run", "🧩 Steps", "⏱️ Trigger", "📜 Runs"])
    with run_tab:
        _run(wf)
    with steps_tab:
        _steps(wf)
    with trigger_tab:
        _trigger(wf, triggers.get(wf_id))
    with runs_tab:
        _runs(wf)


# ---------- list ----------

def _table(wfs, triggers):
    rows = []
    for w in wfs:
        trig = {"manual": "▶️ Manual / API", "schedule": f"⏱️ Every {w['trigger_config'].get('interval_minutes', '?')} min",
                "database": f"🗄️ New rows in {w['trigger_config'].get('source', '?')}"}[w["trigger_type"]]
        if w["trigger_type"] == "database" and (t := triggers.get(w["id"])):
            trig += " 🟢" if t["state"] == "running" else " 🔴"
        done = w["completed"] + w["failed"]
        rows.append({"Workflow": w["name"], "Trigger": trig, "Steps": w["step_count"],
                     "Active": "✅" if w["active"] else "⏸️", "Runs": w["runs"],
                     "Success": (w["completed"] / done * 100) if done else None,
                     "Last run": pd.to_datetime(w["last_run"]).tz_convert(None).strftime("%Y-%m-%d %H:%M")
                     if w["last_run"] else "never"})
    st.caption("💡 Click a workflow to open it.")
    event = st.dataframe(rows, hide_index=True, width="stretch", on_select="rerun",
                         selection_mode=["single-row", "single-cell"], key="wf_table",
                         column_config={"Success": st.column_config.ProgressColumn("Success", format="%.0f%%",
                                                                                   min_value=0, max_value=100)})
    picked = list(event.selection.rows) + [c[0] for c in getattr(event.selection, "cells", [])]
    return wfs[picked[0]]["id"] if picked else None


def _new_workflow():
    with st.expander("➕ New workflow"):
        with st.form("new_wf", clear_on_submit=True):
            name = st.text_input("Name *", placeholder="e.g. Order invoice")
            desc = st.text_input("Description", placeholder="What it does, in one line")
            if st.form_submit_button("Create", type="primary", disabled=not common.can("write")):
                try:
                    w = api("POST", "/api/workflows", json={"name": name, "description": desc, "steps": []})
                except ApiError as e:
                    st.error(str(e))
                    return
                st.session_state.wf_pick = w["id"]
                st.toast(f"Created {w['name']} - add its steps on the Steps tab", icon="⚙️")
                st.rerun()


# ---------- run ----------

def _run(wf):
    hint = _input_hint(wf)
    st.markdown("**Input** - the record that starts the run, available to every step as `trigger`")
    text = st.text_area("Input (JSON)", json.dumps(hint, indent=2), height=110, key=f"run_input_{wf['id']}",
                        label_visibility="collapsed")
    c1, c2 = st.columns([1, 3])
    dry = c1.toggle("Dry run", value=True, key=f"dry_{wf['id']}",
                    help="Runs everything except database updates and deliveries, so nothing changes.")
    if c2.button("▶️ Run now", type="primary", disabled=not (common.can("write") and wf["steps"]),
                 key=f"run_btn_{wf['id']}"):
        try:
            payload = json.loads(text or "{}")
        except json.JSONDecodeError as e:
            st.error(f"The input isn't valid JSON: {e}")
            return
        with st.spinner("Running…"):
            try:
                st.session_state[f"last_run_{wf['id']}"] = api(
                    "POST", f"/api/workflows/{wf['id']}/run", json={"input": payload, "dry_run": dry}, timeout=300)
            except ApiError as e:
                st.error(str(e))
    if not wf["steps"]:
        st.info("This workflow has no steps yet - add them on the **Steps** tab.")
    if run := st.session_state.get(f"last_run_{wf['id']}"):
        _show_run(run)


def _input_hint(wf):
    """A sensible starting input: the trigger fields the steps refer to, with values from the last run."""
    import re

    names = set()
    for s in wf["steps"]:
        for text in [s.get("sql", ""), s.get("path", ""), json.dumps(s.get("params", {})), s.get("template", "")]:
            names.update(re.findall(r"trigger\.([A-Za-z_][A-Za-z0-9_]*)", text or ""))
    runs = api("GET", f"/api/workflows/{wf['id']}/runs", params={"limit": 1})
    last = api("GET", f"/api/workflow-runs/{runs[0]['id']}")["input"] if runs else {}
    return {n: (last or {}).get(n, "") for n in sorted(names)} or (last or {})


def _show_run(run):
    icon = STATUS.get(run["status"], "")
    dry = " · dry run (no updates or deliveries)" if run["dry_run"] else ""
    st.markdown(f"#### {icon} {run['status']} · {run['duration_ms']} ms{dry}")
    if run.get("error"):
        (st.warning if run["status"] == "STOPPED" else st.error)(run["error"])
    st.dataframe([{"": STATUS.get(s["status"], ""), "Step": s["label"], "Status": s["status"],
                   "Saved as": s.get("save_as") or "", "ms": s.get("ms"), "Error": s.get("error", "")}
                  for s in run["steps_log"]], hide_index=True, width="stretch")
    for s in run["steps_log"]:
        if s.get("output") is not None:
            with st.expander(f"{STATUS.get(s['status'], '')} {s['label']} → `{s.get('save_as') or '-'}`"):
                st.code(s["output"] if isinstance(s["output"], str) else json.dumps(s["output"], indent=1),
                        language="json")
    for f in run.get("files", []):
        content = api("GET", f"/api/workflow-files/{f['id']}")
        st.download_button(f"⬇️ Download {f['filename']} ({f['size'] / 1024:.1f} KB)", content,
                           file_name=f["filename"], mime=f["content_type"], key=f"dl_{run['id']}_{f['id']}")
        _show_pdf(content)
    if run["status"] == "FAILED" and st.button("🔁 Rerun from the failed step", key=f"rerun_{run['id']}",
                                               disabled=not common.can("write")):
        try:
            new = api("POST", f"/api/workflow-runs/{run['id']}/rerun", timeout=300)
        except ApiError as e:
            st.error(str(e))
            return
        st.session_state[f"last_run_{run['workflow_id']}"] = new
        st.rerun()


def _show_pdf(content: bytes):
    try:
        st.pdf(content, height=520)
    except Exception:  # noqa: BLE001 - the PDF viewer component may not be installed
        st.caption("Inline preview unavailable here - use the download button.")


# ---------- steps ----------

def _connections():
    conns = api("GET", "/api/connections")
    dbs = {"builtin": "🏠 Built-in data area"} | {c["id"]: f"🗄️ {c['name']}" for c in conns if c["type"] == "database"}
    https = {c["id"]: f"🌐 {c['name']}" for c in conns if c["type"] == "http"}
    outs = {c["id"]: f"{c['name']} ({c['type']})" for c in conns}
    return dbs, https, outs


def _steps(wf):
    key = f"wf_steps_{wf['id']}"
    if key not in st.session_state or st.session_state.get(f"{key}_ver") != wf["updated_at"]:
        import uuid

        # each step gets a stable id for its widgets, so moving steps never mixes up their fields
        st.session_state[key] = [{**x, "_uid": uuid.uuid4().hex[:8]} for x in json.loads(json.dumps(wf["steps"]))]
        st.session_state[f"{key}_ver"] = wf["updated_at"]
    steps = st.session_state[key]
    dbs, https, outs = _connections()
    st.caption("Values from earlier steps: `{{ trigger.order_id }}` (the starting record) or `{{ order.customer_id }}` "
               "(a step saved as *order*). SQL uses `:name` placeholders filled from Parameters - values are never "
               "pasted into the SQL text. Transform and Condition use JMESPath over all results.")
    for i, s in enumerate(steps):
        title = f"**{i + 1}. {s.get('label') or STEP_TYPES[s['type']]}** · {STEP_TYPES[s['type']]}" \
                + (f" → `{s['save_as']}`" if s.get("save_as") else "")
        with st.expander(title, expanded=False):
            uid = s["_uid"]
            _step_form(wf["id"], uid, s, steps, dbs, https, outs)
            b = st.columns(4)
            if b[0].button("⬆️ Up", key=f"up_{uid}", disabled=i == 0):
                steps[i - 1], steps[i] = steps[i], steps[i - 1]
                st.rerun()
            if b[1].button("⬇️ Down", key=f"down_{uid}", disabled=i == len(steps) - 1):
                steps[i + 1], steps[i] = steps[i], steps[i + 1]
                st.rerun()
            if b[2].button("🗑️ Remove", key=f"rm_{uid}"):
                steps.pop(i)
                st.rerun()
    a1, a2 = st.columns([2, 1], vertical_alignment="bottom")
    new_type = a1.selectbox("Add a step", list(STEP_TYPES), format_func=STEP_TYPES.get, key=f"add_type_{wf['id']}")
    if a2.button("➕ Add step", key=f"add_{wf['id']}", width="stretch"):
        base = json.loads(json.dumps(NEW_STEP[new_type]))
        used = {s.get("save_as") for s in steps}
        name = new_type if new_type not in ("condition", "deliver", "db_update") else None
        while name and name in used:
            name += "_2"
        import uuid

        steps.append({"type": new_type, "label": STEP_TYPES[new_type][2:], "save_as": name, **base,
                      "_uid": uuid.uuid4().hex[:8]})
        st.rerun()
    st.divider()
    s1, s2 = st.columns(2)
    if s1.button("💾 Save workflow", type="primary", disabled=not common.can("write"), key=f"save_{wf['id']}"):
        body = {k: wf[k] for k in ("name", "description", "trigger_type", "trigger_config", "active")}
        try:
            api("PUT", f"/api/workflows/{wf['id']}",
                json={**body, "steps": [{k: v for k, v in x.items() if k != "_uid"} for x in steps]})
        except ApiError as e:
            st.error(str(e))
            return
        st.toast("Workflow saved", icon="💾")
        st.rerun()
    if s2.button("↩️ Discard changes", key=f"discard_{wf['id']}"):
        st.session_state.pop(key, None)
        st.rerun()
    with st.expander("Danger zone"):
        if st.checkbox(f"I want to delete the workflow '{wf['name']}' and its run history", key=f"del_ok_{wf['id']}") \
                and st.button("Delete workflow", key=f"del_{wf['id']}", disabled=not common.can("delete")):
            api("DELETE", f"/api/workflows/{wf['id']}")
            st.session_state.pop("wf_pick", None)
            st.rerun()


def _params_text(params: dict) -> str:
    return "\n".join(f"{k} = {v}" for k, v in (params or {}).items())


def _parse_params(text: str) -> dict:
    out = {}
    for line in (text or "").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            if k.strip():
                out[k.strip()] = v.strip()
    return out


def _pick(label, options: dict, current, key):
    keys = list(options)
    if not keys:
        st.warning(f"No {label.lower()} available - add one on the Adapters & Connections page.")
        return current
    return st.selectbox(label, keys, keys.index(current) if current in keys else 0, format_func=options.get, key=key)


def _step_form(wf_id, uid, s, steps, dbs, https, outs):
    k = f"{wf_id}_{uid}"
    c1, c2 = st.columns(2)
    s["label"] = c1.text_input("Label", s.get("label", ""), key=f"label_{k}")
    if s["type"] not in ("condition", "deliver", "db_update"):
        s["save_as"] = c2.text_input("Save result as", s.get("save_as") or "", key=f"save_as_{k}",
                                     help="Later steps refer to it as {{ name.field }}")
    t = s["type"]
    if t in ("db_query", "db_update"):
        s["connection"] = _pick("Database", dbs, s.get("connection"), f"conn_{k}")
        if t == "db_query":
            m1, m2 = st.columns(2)
            s["mode"] = m1.radio("Returns", ["one", "all"], ["one", "all"].index(s.get("mode") or "one"),
                                 horizontal=True, key=f"mode_{k}", format_func={"one": "One row", "all": "All rows"}.get)
            if s["mode"] == "one":
                s["required"] = m2.toggle("Fail if no row", s.get("required", True), key=f"req_{k}")
        s["sql"] = st.text_area("SQL", s.get("sql", ""), height=100, key=f"sql_{k}")
        s["params"] = _parse_params(st.text_area("Parameters (one per line: name = value)", _params_text(s.get("params")),
                                                 height=70, key=f"params_{k}",
                                                 help="e.g. order_id = {{ trigger.order_id }}"))
    elif t == "api_call":
        s["connection"] = _pick("API (HTTP connection)", https, s.get("connection"), f"conn_{k}")
        m1, m2 = st.columns([1, 3])
        s["method"] = m1.selectbox("Method", ["GET", "POST", "PUT", "PATCH", "DELETE"],
                                   ["GET", "POST", "PUT", "PATCH", "DELETE"].index(s.get("method") or "GET"), key=f"method_{k}")
        s["path"] = m2.text_input("Path", s.get("path", ""), key=f"path_{k}", placeholder="customers/{{ order.customer_id }}")
        if s["method"] != "GET":
            s["body"] = st.text_area("Body (template)", s.get("body", ""), height=90, key=f"body_{k}",
                                     placeholder='{"orderId": "{{ order.order_id }}"}')
            s["content_type"] = st.selectbox("Content type", ["application/json", "application/xml", "text/plain"],
                                             key=f"ctype_{k}")
        s["fail_on_error"] = st.toggle("Fail the run on HTTP 4xx/5xx", s.get("fail_on_error", True), key=f"foe_{k}")
        st.caption("The result is `{status, data}` - e.g. `{{ " + (s.get("save_as") or "name") + ".data.field }}`. "
                   "The connection's retry policy applies.")
    elif t == "transform":
        s["expression"] = st.text_area("JMESPath expression", s.get("expression", ""), height=80, key=f"expr_{k}")
    elif t == "condition":
        s["expression"] = st.text_input("Continue only if (JMESPath)", s.get("expression", ""), key=f"expr_{k}")
        s["message"] = st.text_input("Message when it stops", s.get("message", ""), key=f"msg_{k}")
    elif t == "pdf":
        s["filename"] = st.text_input("File name", s.get("filename", ""), key=f"fname_{k}")
        s["template"] = st.text_area("HTML template (Jinja2)", s.get("template", ""), height=260, key=f"tpl_{k}",
                                     help="Use {{ step.field }}, {% for l in lines %}…{% endfor %}, and | money.")
        if st.button("👁️ Preview with the last run's data", key=f"preview_{k}"):
            runs = api("GET", f"/api/workflows/{wf_id}/runs", params={"limit": 20})
            ctx = next((api("GET", f"/api/workflow-runs/{r['id']}")["context"] for r in runs
                        if r["status"] in ("COMPLETED", "STOPPED")), None)
            if not ctx:
                st.warning("No successful run yet - run the workflow once (a dry run is fine) to get sample data.")
            else:
                try:
                    _show_pdf(api("POST", "/api/workflows/pdf-preview", json={"template": s["template"], "context": ctx}))
                except ApiError as e:
                    st.error(str(e))
    elif t == "deliver":
        pdfs = {x["save_as"]: x["save_as"] for x in steps if x["type"] == "pdf" and x.get("save_as")}
        s["file"] = _pick("File (a PDF step)", pdfs, s.get("file"), f"file_{k}")
        s["connection"] = _pick("Send through", outs, s.get("connection"), f"conn_{k}")
        s["target"] = st.text_input("Folder / queue / path", s.get("target", ""), key=f"target_{k}")


# ---------- trigger ----------

def _trigger(wf, status):
    dbs, _, _ = _connections()
    cfg = dict(wf["trigger_config"] or {})
    kinds = {"manual": "▶️ Manual - the Run button or the API",
             "schedule": "⏱️ Schedule - every N minutes, optionally one run per row of a query",
             "database": "🗄️ Database - one run per new row in a table"}
    kind = st.radio("Start runs", list(kinds), list(kinds).index(wf["trigger_type"]), format_func=kinds.get,
                    key=f"trig_{wf['id']}")
    if kind == "manual":
        st.code(f'curl -X POST "{common.API}/api/workflows/{wf["id"]}/run" \\\n  -H "Authorization: Bearer <session token>" '
                f'\\\n  -H "Content-Type: application/json" -d \'{{"input": {{...}}}}\'', language="bash")
    elif kind == "schedule":
        cfg["interval_minutes"] = st.number_input("Every (minutes)", 1, 10080, int(cfg.get("interval_minutes") or 60),
                                                  key=f"every_{wf['id']}")
        cfg["connection"] = _pick("Database for the query", dbs, cfg.get("connection"), f"tconn_{wf['id']}")
        cfg["query"] = st.text_area("Query (optional) - one run per row", cfg.get("query", ""), height=80,
                                    key=f"tquery_{wf['id']}", placeholder="SELECT order_id FROM orders WHERE status = 'NEW'")
        cfg["max_rows"] = st.number_input("At most rows per schedule", 1, 10000, int(cfg.get("max_rows") or 100),
                                          key=f"tmax_{wf['id']}")
        if wf.get("next_run_at") and wf["trigger_type"] == "schedule":
            st.caption(f"Next run: {pd.to_datetime(wf['next_run_at']).tz_convert(None):%Y-%m-%d %H:%M}")
    else:
        cfg["connection"] = _pick("Database", {k: v for k, v in dbs.items() if k != "builtin"}, cfg.get("connection"),
                                  f"tconn_{wf['id']}")
        cfg["source"] = st.text_input("Table and key column (table:key)", cfg.get("source", ""), key=f"tsrc_{wf['id']}",
                                      placeholder="invoice_requests:id",
                                      help="New rows (key greater than the last one seen) each start a run; "
                                           "the row is the run's `trigger`.")
        if status and wf["trigger_type"] == "database":
            (st.success if status["state"] == "running" else st.error)(status["detail"])
    active = st.toggle("Active", wf["active"], key=f"active_{wf['id']}",
                       help="Inactive workflows can still be run by hand but aren't triggered automatically.")
    if st.button("Save trigger", type="primary", disabled=not common.can("write"), key=f"save_trig_{wf['id']}"):
        body = {k: wf[k] for k in ("name", "description", "steps")}
        try:
            api("PUT", f"/api/workflows/{wf['id']}", json={**body, "trigger_type": kind, "trigger_config": cfg,
                                                           "active": active})
        except ApiError as e:
            st.error(str(e))
            return
        st.toast("Trigger saved", icon="⏱️")
        st.rerun()


# ---------- run history ----------

def _runs(wf):
    runs = api("GET", f"/api/workflows/{wf['id']}/runs", params={"limit": 100})
    if not runs:
        st.caption("No runs yet.")
        return
    rows = [{"": STATUS.get(r["status"], ""), "Started": pd.to_datetime(r["started_at"]).tz_convert(None).strftime(
        "%Y-%m-%d %H:%M:%S"), "Status": r["status"], "Source": r["source"], "Dry run": "yes" if r["dry_run"] else "",
             "ms": r["duration_ms"], "Files": r["files"], "Error": r["error"] or ""} for r in runs]
    event = st.dataframe(rows, hide_index=True, width="stretch", on_select="rerun",
                         selection_mode="single-row", key=f"runs_table_{wf['id']}")
    if event.selection.rows:
        _show_run(api("GET", f"/api/workflow-runs/{runs[event.selection.rows[0]]['id']}"))
    else:
        st.caption("Select a run to see each step, its results and any PDF.")
