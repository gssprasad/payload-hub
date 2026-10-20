"""Workflows page: build multi-step workflows (database, API, transform, condition, PDF, deliver) and run them."""
import json

import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

STEP_TYPES = {
    "db": "🧱 DB connector",
    "db_query": "🗄️ SQL query",
    "db_update": "✏️ SQL update",
    "api_call": "🌐 API call",
    "transform": "🔀 Transform",
    "branch": "🔱 Branch (if / else)",
    "condition": "❓ Stop unless",
    "pdf": "📄 Create PDF",
    "deliver": "📤 Deliver file",
}
STATUS = {"COMPLETED": "✅", "FAILED": "❌", "STOPPED": "⏹️", "RUNNING": "⏳", "OK": "✅", "SKIPPED": "⏭️",
          "REUSED": "♻️", "FAILED_CONTINUED": "⚠️"}
NO_RESULT = ("condition", "deliver", "db_update", "branch")
RETRYABLE = ("db", "db_query", "db_update", "api_call", "pdf", "deliver")
DB_OPS = {"select_one": "🔎 Get one row", "select_all": "📋 Get rows", "insert": "➕ Insert a row",
          "update": "✏️ Update rows", "delete": "🗑️ Delete rows"}
WHERE_OPS = ["=", "!=", ">", ">=", "<", "<=", "like", "in", "is null", "is not null"]
NEW_STEP = {
    "db": {"connection": "builtin", "table": "", "operation": "select_one", "columns": [], "where": [], "values": {},
           "required": True, "limit": 100},
    "db_query": {"connection": "builtin", "mode": "one", "required": True, "sql": "SELECT * FROM my_table WHERE id = :id",
                 "params": {"id": "{{ trigger.id }}"}},
    "db_update": {"connection": "builtin", "sql": "UPDATE my_table SET status = 'DONE' WHERE id = :id",
                  "params": {"id": "{{ trigger.id }}"}},
    "api_call": {"method": "GET", "path": "", "content_type": "application/json", "fail_on_error": True},
    "transform": {"expression": "{total: sum(lines[].amount)}"},
    "branch": {"expression": "order.total > `1000`", "then": [], "else": []},
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
    for s in _walk(wf["steps"]):
        for text in [s.get("sql", ""), s.get("path", ""), json.dumps(s.get("params", {})), s.get("template", ""),
                     json.dumps(s.get("where", [])), json.dumps(s.get("values", {})), s.get("body", "")]:
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
    st.dataframe([{"": STATUS.get(s["status"], ""), "#": s.get("path") or str(s["index"] + 1),
                   "Step": "    " * s.get("depth", 0) + ("↳ " if s.get("depth") else "") + s["label"],
                   "Status": s["status"], "Tries": s.get("attempts"), "Saved as": s.get("save_as") or "",
                   "ms": s.get("ms"), "Error": s.get("error", "")}
                  for s in run["steps_log"]], hide_index=True, width="stretch")
    for s in run["steps_log"]:
        if s.get("output") is not None or s.get("retries"):
            with st.expander(f"{STATUS.get(s['status'], '')} {s.get('path') or ''} {s['label']} → `{s.get('save_as') or '-'}`"):
                for r in s.get("retries") or []:
                    st.caption(f"🔁 {r}")
                if s.get("output") is not None:
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


# ---------- steps: the visual builder ----------

def _uid() -> str:
    import uuid

    return uuid.uuid4().hex[:8]


def _walk(steps):
    """Every step, including the steps inside branches."""
    for x in steps or []:
        yield x
        if x.get("type") == "branch":
            yield from _walk(x.get("then"))
            yield from _walk(x.get("else"))


def _with_uids(steps):
    # each step gets a stable id for its widgets, so moving steps never mixes up their fields
    out = []
    for x in steps or []:
        x = {**x, "_uid": _uid()}
        if x["type"] == "branch":
            x["then"], x["else"] = _with_uids(x.get("then")), _with_uids(x.get("else"))
        out.append(x)
    return out


def _clean(steps):
    out = []
    for x in steps:
        x = {k: v for k, v in x.items() if not k.startswith("_")}
        if x["type"] == "branch":
            x["then"], x["else"] = _clean(x.get("then") or []), _clean(x.get("else") or [])
        out.append(x)
    return out


def _free_name(base: str, all_steps) -> str:
    used = {x.get("save_as") for x in _walk(all_steps)}
    name, n = base, 2
    while name in used:
        name, n = f"{base}_{n}", n + 1
    return name


def _new_step(t: str, all_steps) -> dict:
    step = {"type": t, "label": STEP_TYPES[t].split(" ", 1)[1], **json.loads(json.dumps(NEW_STEP[t])),
            "save_as": None if t in NO_RESULT else _free_name(t, all_steps), "_uid": _uid()}
    st.session_state[f"open_{step['_uid']}"] = True  # new steps open for editing
    return step


def _copy(step: dict, all_steps) -> dict:
    new = _with_uids([json.loads(json.dumps(_clean([step])[0]))])[0]
    for x in _walk([new]):
        if x.get("save_as"):
            x["save_as"] = _free_name(x["save_as"], all_steps)
            all_steps = [*all_steps, x]
    new["label"] = (new.get("label") or "") + " (copy)"
    return new


def _connections():
    conns = api("GET", "/api/connections")
    dbs = {"builtin": "🏠 Built-in data area"} | {c["id"]: f"🗄️ {c['name']}" for c in conns if c["type"] == "database"}
    https = {c["id"]: f"🌐 {c['name']}" for c in conns if c["type"] == "http"}
    outs = {c["id"]: f"{c['name']} ({c['type']})" for c in conns}
    return dbs, https, outs


@st.cache_data(ttl=60, show_spinner=False)
def _tables(conn: str, session_key: str | None) -> list[dict]:  # cached per signed-in session
    try:
        return api("GET", "/api/data/tables" if conn == "builtin" else f"/api/connections/{conn}/tables")
    except ApiError:
        return []


def _diagram(steps) -> str:
    """Graphviz flow chart of the steps, with branches drawn as diamonds."""
    out = ['digraph { rankdir=TB; bgcolor="transparent"; nodesep=0.35; ranksep=0.3;',
           'node [shape=box style="rounded,filled" fillcolor="#eef2ff" color="#6366f1" fontname="Helvetica" '
           'fontsize=11 margin="0.15,0.06"]; edge [color="#94a3b8" fontname="Helvetica" fontsize=10 arrowsize=0.7];',
           'start [label="▶ trigger" shape=oval fillcolor="#dcfce7" color="#16a34a"];']
    seq = [0]

    def esc(text):
        return str(text).replace("\\", "\\\\").replace('"', '\\"')

    def edge(a, b, label=None):
        out.append(f'{a} -> {b}' + (f' [label="{label}"]' if label else '') + ';')

    def chain(items, prev, first_label=None, prefix=""):
        for i, x in enumerate(items):
            seq[0] += 1
            n, num = f"n{seq[0]}", f"{prefix}{i + 1}"
            extra = []
            if (x.get("retry_attempts") or 1) > 1:
                extra.append(f"🔁 {x['retry_attempts']} tries")
            if x.get("on_error") == "continue":
                extra.append("↷ continues on error")
            if x.get("type") == "db":
                extra.append(f"{DB_OPS.get(x.get('operation'), '')[2:]} · {x.get('table') or '?'}")
            lines = [f"{num}. {x.get('label') or STEP_TYPES[x['type']]}", *extra]
            attrs = 'label="' + "\\n".join(esc(line) for line in lines) + '"'
            if x["type"] == "branch":
                attrs += ' shape=diamond fillcolor="#fef3c7" color="#d97706"'
            elif x["type"] == "condition":
                attrs += ' shape=hexagon fillcolor="#fee2e2" color="#dc2626"'
            if x.get("disabled"):
                attrs += ' style="rounded,dashed" fillcolor="#f1f5f9" fontcolor="#94a3b8"'
            out.append(f"{n} [{attrs}];")
            for p in prev:
                edge(p, n, first_label)
            first_label = None
            if x["type"] == "branch":
                seq[0] += 1
                join = f"j{seq[0]}"
                out.append(f'{join} [shape=point width=0.08 color="#94a3b8"];')
                for side, lab in (("then", "yes"), ("else", "no")):
                    if x.get(side):
                        for e in chain(x[side], [n], lab, f"{num}{'✓' if side == 'then' else '✗'}."):
                            edge(e, join)
                    else:
                        edge(n, join, lab)
                prev = [join]
            else:
                prev = [n]
        return prev

    for e in chain(steps, ["start"]):
        edge(e, "finish")
    out.append('finish [label="■ done" shape=oval fillcolor="#e2e8f0" color="#64748b"];}')
    return "\n".join(out)


def _steps(wf):
    key = f"wf_steps_{wf['id']}"
    if key not in st.session_state or st.session_state.get(f"{key}_ver") != wf["updated_at"]:
        st.session_state[key] = _with_uids(json.loads(json.dumps(wf["steps"])))
        st.session_state[f"{key}_ver"] = wf["updated_at"]
    steps = st.session_state[key]
    ctx = {"wf_id": wf["id"], "all": steps, "conns": _connections(), "write": common.can("write")}
    saved = _clean(steps) == json.loads(json.dumps(wf["steps"]))

    view = st.segmented_control("View", ["🧩 Builder", "🗺️ Flow diagram", "🧾 JSON"], default="🧩 Builder",
                                key=f"wf_view_{wf['id']}", label_visibility="collapsed") or "🧩 Builder"
    if view == "🗺️ Flow diagram":
        st.graphviz_chart(_diagram(steps), width="content")
        st.caption("Diamonds are branches (yes / no paths), red hexagons stop the run, dashed steps are switched off.")
    elif view == "🧾 JSON":
        st.code(json.dumps(_clean(steps), indent=2), language="json")
    else:
        st.caption("Values from earlier steps: `{{ trigger.order_id }}` (the starting record) or `{{ order.customer_id }}` "
                   "(a step saved as *order*). Branches and Transform use JMESPath over all results, e.g. "
                   "``order.total > `1000` ``.")
        if not steps:
            st.info("No steps yet - pick one from the palette below to start.")
        _step_list(steps, ctx, "")
        st.markdown("**➕ Add a step**")
        _palette(steps, ctx, f"top_{wf['id']}")

    st.divider()
    s1, s2, s3 = st.columns([1, 1, 2], vertical_alignment="center")
    if s1.button("💾 Save workflow", type="primary", disabled=not ctx["write"] or saved, key=f"save_{wf['id']}"):
        body = {k: wf[k] for k in ("name", "description", "trigger_type", "trigger_config", "active")}
        try:
            api("PUT", f"/api/workflows/{wf['id']}", json={**body, "steps": _clean(steps)})
        except ApiError as e:
            st.error(str(e))
            return
        st.toast("Workflow saved", icon="💾")
        st.rerun()
    if s2.button("↩️ Discard changes", key=f"discard_{wf['id']}", disabled=saved):
        st.session_state.pop(key, None)
        st.rerun()
    s3.caption("✅ All changes saved" if saved else "✏️ Unsaved changes")
    with st.expander("Danger zone"):
        if st.checkbox(f"I want to delete the workflow '{wf['name']}' and its run history", key=f"del_ok_{wf['id']}") \
                and st.button("Delete workflow", key=f"del_{wf['id']}", disabled=not common.can("delete")):
            api("DELETE", f"/api/workflows/{wf['id']}")
            st.session_state.pop("wf_pick", None)
            st.rerun()


def _palette(target: list, ctx, key: str, compact: bool = False):
    """Buttons (or a compact picker inside branches) that append a new step to `target`."""
    if compact:
        a1, a2 = st.columns([3, 1], vertical_alignment="bottom")
        t = a1.selectbox("Add a step here", list(STEP_TYPES), format_func=STEP_TYPES.get, key=f"pal_{key}",
                         label_visibility="collapsed")
        if a2.button("➕ Add", key=f"paladd_{key}", width="stretch"):
            target.append(_new_step(t, ctx["all"]))
            st.rerun()
        return
    cols = st.columns(5)
    for i, t in enumerate(STEP_TYPES):
        if cols[i % 5].button(STEP_TYPES[t], key=f"pal_{key}_{t}", width="stretch"):
            target.append(_new_step(t, ctx["all"]))
            st.rerun()


def _step_list(steps: list, ctx, prefix: str):
    for i, s in enumerate(steps):
        _card(steps, i, s, ctx, f"{prefix}{i + 1}")


def _card(steps: list, i: int, s: dict, ctx, num: str):
    uid, t = s["_uid"], s["type"]
    k = f"{ctx['wf_id']}_{uid}"
    with st.container(border=True):
        badges = []
        if s.get("disabled"):
            badges.append("⏸️ *switched off*")
        if (s.get("retry_attempts") or 1) > 1:
            badges.append(f"🔁 {s['retry_attempts']} tries")
        if s.get("on_error") == "continue":
            badges.append("↷ continues on error")
        if t == "db" and s.get("table"):
            badges.append(f"`{DB_OPS.get(s.get('operation'), '')[2:]}` on `{s['table']}`")
        if t == "branch":
            badges.append(f"if `{s.get('expression', '')}`")
        h1, h2 = st.columns([6, 1], vertical_alignment="center")
        h1.markdown(f"**{num}. {s.get('label') or STEP_TYPES[t]}** · {STEP_TYPES[t]}"
                    + (f" → `{s['save_as']}`" if s.get("save_as") else "")
                    + ("  \n" + " · ".join(badges) if badges else ""))
        editing = h2.toggle("Edit", key=f"open_{uid}")
        if editing:
            _step_form(ctx, k, s)
            if t in RETRYABLE:
                _retry_form(k, s)
        b = st.columns(5)
        if b[0].button("⬆️", key=f"up_{uid}", disabled=i == 0, help="Move up", width="stretch"):
            steps[i - 1], steps[i] = steps[i], steps[i - 1]
            st.rerun()
        if b[1].button("⬇️", key=f"down_{uid}", disabled=i == len(steps) - 1, help="Move down", width="stretch"):
            steps[i + 1], steps[i] = steps[i], steps[i + 1]
            st.rerun()
        if b[2].button("⧉", key=f"dup_{uid}", help="Duplicate", width="stretch"):
            steps.insert(i + 1, _copy(s, ctx["all"]))
            st.rerun()
        if b[3].button("▶️" if s.get("disabled") else "⏸️", key=f"off_{uid}", width="stretch",
                       help="Switch on" if s.get("disabled") else "Switch off (skipped when the workflow runs)"):
            s["disabled"] = not s.get("disabled")
            st.rerun()
        if b[4].button("🗑️", key=f"rm_{uid}", help="Remove", width="stretch"):
            steps.pop(i)
            st.rerun()
        if t == "branch":
            for side, title in (("then", "✅ **Yes** - when the condition is true"),
                                ("else", "❌ **No** - otherwise")):
                s.setdefault(side, [])
                with st.container(border=True):
                    st.markdown(title)
                    if not s[side]:
                        st.caption("No steps - the run carries on after the branch.")
                    _step_list(s[side], ctx, f"{num}{'✓' if side == 'then' else '✗'}.")
                    _palette(s[side], ctx, f"{uid}_{side}", compact=True)


def _retry_form(k: str, s: dict):
    st.markdown("**🔁 If this step fails**")
    r = st.columns(2, vertical_alignment="bottom") + st.columns(2, vertical_alignment="bottom")
    s["retry_attempts"] = r[0].number_input("Attempts", 1, 10, int(s.get("retry_attempts") or 1), key=f"ra_{k}",
                                            help="1 = no retry")
    retry = s["retry_attempts"] > 1
    s["retry_delay"] = r[1].number_input("Wait (seconds)", 0, 3600, int(s.get("retry_delay") or 5), key=f"rd_{k}",
                                         disabled=not retry)
    s["retry_backoff"] = r[2].toggle("Double the wait each time", s.get("retry_backoff", True), key=f"rb_{k}",
                                     disabled=not retry)
    s["on_error"] = r[3].selectbox("Then", ["stop", "continue"], ["stop", "continue"].index(s.get("on_error") or "stop"),
                                   key=f"oe_{k}", format_func={"stop": "⛔ Stop the run",
                                                               "continue": "↷ Carry on (result = null)"}.get)
    if retry:
        waits = [s["retry_delay"] * (2 ** n if s["retry_backoff"] else 1) for n in range(s["retry_attempts"] - 1)]
        st.caption("Waits between tries: " + ", ".join(f"{w} s" for w in waits))


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


def _step_form(ctx, k, s):
    dbs, https, outs = ctx["conns"]
    wf_id, t = ctx["wf_id"], s["type"]
    c1, c2 = st.columns(2)
    s["label"] = c1.text_input("Label", s.get("label", ""), key=f"label_{k}")
    if t not in NO_RESULT:
        optional = t == "db" and s.get("operation") in ("insert", "update", "delete")
        s["save_as"] = c2.text_input("Save result as" + (" (optional)" if optional else ""), s.get("save_as") or "",
                                     key=f"save_as_{k}", help="Later steps refer to it as {{ name.field }}") or None
    if t == "db":
        _db_form(k, s, dbs)
    elif t in ("db_query", "db_update"):
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
        st.caption("SQL uses `:name` placeholders filled from Parameters - values are never pasted into the SQL text. "
                   "Prefer the 🧱 DB connector if you don't want to write SQL.")
    elif t == "api_call":
        s["connection"] = _pick("API (HTTP connection)", https, s.get("connection"), f"conn_{k}")
        m1, m2 = st.columns([1, 3])
        methods = ["GET", "POST", "PUT", "PATCH", "DELETE"]
        s["method"] = m1.selectbox("Method", methods, methods.index(s.get("method") or "GET"), key=f"method_{k}")
        s["path"] = m2.text_input("Path", s.get("path", ""), key=f"path_{k}", placeholder="customers/{{ order.customer_id }}")
        if s["method"] != "GET":
            s["body"] = st.text_area("Body (template)", s.get("body", ""), height=90, key=f"body_{k}",
                                     placeholder='{"orderId": "{{ order.order_id }}"}')
            types = ["application/json", "application/xml", "text/plain"]
            s["content_type"] = st.selectbox("Content type", types, types.index(s.get("content_type") or types[0])
                                             if s.get("content_type") in types else 0, key=f"ctype_{k}")
        s["fail_on_error"] = st.toggle("Fail the step on HTTP 4xx/5xx", s.get("fail_on_error", True), key=f"foe_{k}")
        st.caption("The result is `{status, data}` - e.g. `{{ " + (s.get("save_as") or "name") + ".data.field }}`. "
                   "The connection's own retry policy applies too.")
    elif t == "transform":
        s["expression"] = st.text_area("JMESPath expression", s.get("expression", ""), height=80, key=f"expr_{k}")
    elif t == "branch":
        s["expression"] = st.text_input("Condition (JMESPath) - true takes the Yes path, otherwise No",
                                        s.get("expression", ""), key=f"expr_{k}",
                                        help="Examples: order.total > `1000` · customer.country == 'IN' · "
                                             "length(lines) > `0` · api.status == `200`")
    elif t == "condition":
        s["expression"] = st.text_input("Continue only if (JMESPath)", s.get("expression", ""), key=f"expr_{k}")
        s["message"] = st.text_input("Message when it stops", s.get("message", ""), key=f"msg_{k}")
    elif t == "pdf":
        s["filename"] = st.text_input("File name", s.get("filename", ""), key=f"fname_{k}")
        s["template"] = st.text_area("HTML template (Jinja2)", s.get("template", ""), height=260, key=f"tpl_{k}",
                                     help="Use {{ step.field }}, {% for l in lines %}…{% endfor %}, and | money.")
        if st.button("👁️ Preview with the last run's data", key=f"preview_{k}"):
            runs = api("GET", f"/api/workflows/{wf_id}/runs", params={"limit": 20})
            run_ctx = next((api("GET", f"/api/workflow-runs/{r['id']}")["context"] for r in runs
                            if r["status"] in ("COMPLETED", "STOPPED")), None)
            if not run_ctx:
                st.warning("No successful run yet - run the workflow once (a dry run is fine) to get sample data.")
            else:
                try:
                    _show_pdf(api("POST", "/api/workflows/pdf-preview", json={"template": s["template"], "context": run_ctx}))
                except ApiError as e:
                    st.error(str(e))
    elif t == "deliver":
        pdfs = {x["save_as"]: x["save_as"] for x in _walk(ctx["all"]) if x["type"] == "pdf" and x.get("save_as")}
        s["file"] = _pick("File (a PDF step)", pdfs, s.get("file"), f"file_{k}")
        s["connection"] = _pick("Send through", outs, s.get("connection"), f"conn_{k}")
        s["target"] = st.text_input("Folder / queue / path", s.get("target", ""), key=f"target_{k}")


def _db_form(k: str, s: dict, dbs: dict):
    """The DB connector: pick a table and an action; conditions and values are filled in tables, no SQL."""
    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        s["connection"] = _pick("Database", dbs, s.get("connection") or "builtin", f"conn_{k}")
    tables = _tables(s["connection"], common.token())
    by_name = {x["name"]: x for x in tables}
    if by_name:
        names = list(by_name)
        s["table"] = c2.selectbox("Table", names, names.index(s["table"]) if s.get("table") in names else 0,
                                  key=f"table_{k}_{s['connection']}")
    else:
        s["table"] = c2.text_input("Table", s.get("table", ""), key=f"table_{k}",
                                   help="Couldn't list the tables of this database - type the name.")
    ops = list(DB_OPS)
    s["operation"] = c3.selectbox("Action", ops, ops.index(s.get("operation") or "select_one"),
                                  format_func=DB_OPS.get, key=f"op_{k}")
    op = s["operation"]
    cols = [c["name"] for c in (by_name.get(s["table"]) or {}).get("columns", [])]
    if cols:
        st.caption("Columns: " + ", ".join(f"`{c['name']}` {c['type']}" for c in by_name[s["table"]]["columns"]))
    tk = f"{k}_{s['connection']}_{s['table']}"  # the editors start fresh when the table changes

    if op.startswith("select"):
        s["columns"] = st.multiselect("Columns to return (none = all)", cols, [c for c in s.get("columns") or []
                                                                               if c in cols], key=f"cols_{tk}")
    if op in ("insert", "update"):
        st.markdown("**Values** " + ("for the new row" if op == "insert" else "to set"))
        init = st.session_state.setdefault(f"vals_init_{tk}", [{"Column": c, "Value": v}
                                                              for c, v in (s.get("values") or {}).items()])
        rows = st.data_editor(pd.DataFrame(init, columns=["Column", "Value"]), num_rows="dynamic", hide_index=True,
                              width="stretch", key=f"vals_{tk}", column_config={
                                  "Column": st.column_config.SelectboxColumn("Column", options=cols, required=True)
                                  if cols else st.column_config.TextColumn("Column", required=True),
                                  "Value": st.column_config.TextColumn(
                                      "Value", help="Fixed text, or a template like {{ trigger.id }}")})
        s["values"] = {r["Column"]: ("" if pd.isna(r["Value"]) else str(r["Value"]))
                       for r in rows.to_dict("records") if isinstance(r["Column"], str) and r["Column"]}
    else:
        s.pop("values", None)
    if op != "insert":
        st.markdown("**Conditions** (all must match)")
        init = st.session_state.setdefault(f"where_init_{tk}", [
            {"Column": w.get("column"), "Condition": w.get("op") or "=", "Value": w.get("value", "")}
            for w in s.get("where") or []])
        rows = st.data_editor(pd.DataFrame(init, columns=["Column", "Condition", "Value"]), num_rows="dynamic",
                              hide_index=True, width="stretch", key=f"where_{tk}", column_config={
                                  "Column": st.column_config.SelectboxColumn("Column", options=cols, required=True)
                                  if cols else st.column_config.TextColumn("Column", required=True),
                                  "Condition": st.column_config.SelectboxColumn("Condition", options=WHERE_OPS,
                                                                                default="=", required=True),
                                  "Value": st.column_config.TextColumn(
                                      "Value", help="e.g. {{ trigger.customer_id }}; for 'in' a comma list; "
                                                    "'like' accepts % wildcards")})
        s["where"] = [{"column": r["Column"], "op": r["Condition"] or "=",
                       "value": "" if pd.isna(r["Value"]) else str(r["Value"])}
                      for r in rows.to_dict("records") if isinstance(r["Column"], str) and r["Column"]]
    else:
        s.pop("where", None)
    if op.startswith("select"):
        o = st.columns(3, vertical_alignment="bottom")
        order = ["(none)"] + cols
        picked = o[0].selectbox("Order by", order, order.index(s["order_by"]) if s.get("order_by") in order else 0,
                                key=f"order_{tk}")
        s["order_by"] = None if picked == "(none)" else picked
        s["order_desc"] = o[1].toggle("Newest / largest first", s.get("order_desc", False), key=f"desc_{tk}",
                                      disabled=not s["order_by"])
        if op == "select_all":
            s["limit"] = o[2].number_input("At most (rows)", 1, 10000, int(s.get("limit") or 100), key=f"limit_{k}")
        else:
            s["required"] = o[2].toggle("Fail if no row", s.get("required", True), key=f"req_{k}",
                                        help="Off: the result is null and the run carries on (use a Branch to check).")
    if op in ("update", "delete") and not s.get("where"):
        s["allow_all_rows"] = st.checkbox(f"⚠️ No conditions - {op} **every row** in the table", False,
                                          key=f"all_{tk}")
    else:
        s.pop("allow_all_rows", None)
    hint = {"select_one": "The result is the row, e.g. `{{ name.column }}`.",
            "select_all": "The result is a list of rows - loop over it in a PDF template or Transform.",
            "insert": "The result is `{rowcount, key}` with the new row's key.",
            "update": "The result is `{rowcount}`.", "delete": "The result is `{rowcount}`."}[op]
    st.caption(hint + (" Dry runs don't change the database." if op in ("insert", "update", "delete") else ""))
    if s.get("table") and st.button("🧾 Show the SQL", key=f"sqlprev_{k}"):
        r = api("POST", f"/api/db/{s['connection']}/build-sql",
                json={x: v for x, v in s.items() if not x.startswith("_")})
        if r.get("sql"):
            st.code(r["sql"], language="sql")
            st.caption("Values are sent separately as parameters - never pasted into the SQL.")
        else:
            st.error(r.get("error") or "Couldn't build the SQL.")


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
