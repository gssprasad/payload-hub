"""Schedulers page: tasks that run a workflow, send a message to a partner or call an API on a schedule."""
import json
from datetime import datetime, timedelta
from zoneinfo import available_timezones

import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

RESULT = {"OK": "✅", "FAILED": "❌", "SKIPPED": "⏭️", "ACCEPTED": "📨", "RUNNING": "⏳"}
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
KINDS = {"interval": "🔁 Every N seconds / minutes / hours", "daily": "📅 Daily at a time",
         "cron": "🧮 Cron expression", "once": "1️⃣ Once"}
ACTIONS = {"workflow": "⚙️ Run a workflow", "partner": "🤝 Send a message to a partner", "http": "🌐 Call an API",
           "backup": "💾 Back up the database"}


def _when(value) -> str:
    return pd.to_datetime(value).tz_convert(common.TZ).tz_localize(None).strftime("%Y-%m-%d %H:%M:%S") if value else "—"


def page():
    common.hero("⏰ Schedulers", "Run workflows, partner messages or API calls on a schedule.")
    data = api("GET", "/api/schedules")
    tasks = data["tasks"]
    ctx = {"partners": {p["code"]: p["name"] for p in api("GET", "/api/partners")},
           "workflows": {w["id"]: w["name"] for w in api("GET", "/api/workflows")["workflows"]},
           "https": {c["id"]: c["name"] for c in api("GET", "/api/connections") if c["type"] == "http"},
           "tz": data["local_timezone"]}
    top = st.columns([3, 1], vertical_alignment="center")
    failed = sum(t["last_status"] == "FAILED" for t in tasks)
    top[0].caption(f"{len(tasks)} tasks · {sum(t['enabled'] for t in tasks)} active"
                   + (f" · ❌ {failed} failed last time" if failed else "") + f" · times in {data['local_timezone']}")
    if top[1].button("➕ New task", type="primary", width="stretch", disabled=not common.can("write")):
        _new_task_dialog(ctx)
    if tasks:
        sid = common.pick_row(
            [{"Task": t["name"], "Runs": t["schedule_text"], "Does": t["action_text"],
              "Next run": _when(t["next_run_at"]) if t["enabled"] else "⏸️ paused",
              "Last run": f"{RESULT.get(t['last_status'], '')} {_when(t['last_run_at'])}" if t["last_run_at"] else "never"}
             for t in tasks], "schedules", [t["id"] for t in tasks])
        _detail(next(t for t in tasks if t["id"] == sid), ctx)
    else:
        st.info("No scheduled tasks yet.")
    if data["workflow_schedules"]:
        with st.expander(f"Workflow schedules ({len(data['workflow_schedules'])}) - set on each workflow's Trigger tab"):
            st.dataframe([{"Workflow": w["name"], "Every": f"{w['every_minutes']} min",
                           "Per row of a query": "yes" if w["per_row"] else "no",
                           "Next run": _when(w["next_run_at"]) if w["active"] else "⏸️ paused"}
                          for w in data["workflow_schedules"]], hide_index=True, width="stretch")


@st.dialog("New scheduled task", width="large")
def _new_task_dialog(ctx):
    _task_form(None, ctx)


def _detail(t, ctx):
    common.detail_title(t["name"])
    if t.get("description"):
        st.caption(t["description"])
    overview, history, settings = st.tabs(["Overview", "History", "Settings"])
    with overview:
        st.html('<div class="ph-kv">' + "".join(
            f"<b>{k}</b><span>{v}</span>" for k, v in [
                ("Does", t["action_text"]), ("Runs", t["schedule_text"]),
                ("Next run", _when(t["next_run_at"]) if t["enabled"] else "⏸️ paused"),
                ("Last run", f"{RESULT.get(t['last_status'], '')} {_when(t['last_run_at'])} - {t['last_detail'] or ''}"
                 if t["last_run_at"] else "never"),
                ("Totals", f"✅ {t['run_count'] - t['fail_count']} · ❌ {t['fail_count']}")]) + "</div>")
        write = common.can("write")
        b = st.columns([1, 1, 1, 2])
        if b[0].button("▶️ Run now", key=f"sch_run_{t['id']}", disabled=not write, width="stretch"):
            with st.spinner("Running…"):
                r = api("POST", f"/api/schedules/{t['id']}/run", timeout=300)
            (st.success if r["status"] in ("OK", "ACCEPTED") else st.error)(
                f"{RESULT.get(r['status'], '')} {r['status']} · {r['detail']}")
        if b[1].button("⏸️ Pause" if t["enabled"] else "▶️ Resume", key=f"sch_en_{t['id']}", disabled=not write,
                       width="stretch"):
            try:
                api("PUT", f"/api/schedules/{t['id']}/enabled", json={"enabled": not t["enabled"]})
            except ApiError as e:
                st.error(str(e))
            else:
                st.rerun()
        if b[2].button("🗑️ Delete", key=f"sch_del_{t['id']}", disabled=not common.can("delete"), width="stretch"):
            api("DELETE", f"/api/schedules/{t['id']}")
            st.session_state.pop("schedules_selected", None)
            st.rerun()
    with history:
        runs = api("GET", f"/api/schedules/{t['id']}/runs", params={"limit": 30})
        if runs:
            st.dataframe([{"Started": _when(r["started_at"]), "": RESULT.get(r["status"], ""), "Result": r["status"],
                           "How": r["trigger"], "ms": r["duration_ms"], "Detail": r["detail"],
                           "Transaction / run": r["ref"]} for r in runs], hide_index=True, width="stretch")
        else:
            st.caption("Not run yet.")
    with settings:
        if common.can("write"):
            _task_form(t, ctx)
        else:
            st.caption("You need write permission to change tasks.")


def _task_form(t, ctx):
    key = t["id"] if t else "new"
    action, spec = (t or {}).get("action") or {}, (t or {}).get("schedule") or {}
    a_type = st.radio("The task", list(ACTIONS), format_func=ACTIONS.get, horizontal=True, key=f"sch_at_{key}",
                      index=list(ACTIONS).index(action["type"]) if action else 0)
    kind = st.radio("Runs", list(KINDS), format_func=KINDS.get, horizontal=True, key=f"sch_kind_{key}",
                    index=list(KINDS).index(spec["kind"]) if spec else 0)
    with st.form(f"sch_form_{key}_{a_type}_{kind}"):
        name = st.text_input("Name *", (t or {}).get("name", ""), disabled=bool(t), placeholder="e.g. Nightly invoices")
        desc = st.text_input("Description", (t or {}).get("description") or "")
        st.markdown("**What it does**")
        body = _action_fields(a_type, action, ctx)
        st.markdown("**When**")
        schedule = _schedule_fields(kind, spec, ctx)
        enabled = st.toggle("Active", (t or {}).get("enabled", True))
        if st.form_submit_button("Save task" if t else "Create task", type="primary"):
            if body is None:
                return
            payload = {"name": name, "description": desc, "enabled": enabled, "schedule": schedule, "action": body}
            try:
                api("PUT" if t else "POST", f"/api/schedules/{t['id']}" if t else "/api/schedules", json=payload)
            except ApiError as e:
                st.error(str(e))
                return
            st.session_state["schedules_selected"] = _slug(name) if not t else t["id"]
            st.rerun()


def _slug(text: str) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:40]


def _pick(label, options: dict, current):
    ids = list(options) or [""]
    return st.selectbox(label, ids, ids.index(current) if current in ids else 0,
                        format_func=lambda i: options.get(i, "— none available —"))


def _action_fields(a_type, action, ctx):
    if a_type == "workflow":
        wf = _pick("Workflow", ctx["workflows"], action.get("workflow_id"))
        text = st.text_area("Input (JSON) - available to the steps as `trigger`",
                            json.dumps(action.get("input") or {}, indent=2), height=90)
        try:
            data = json.loads(text or "{}")
        except json.JSONDecodeError as e:
            st.error(f"The input isn't valid JSON: {e}")
            return None
        return {"type": "workflow", "workflow_id": wf, "input": data}
    if a_type == "backup":
        st.caption("Takes a database backup into the backup folder (see the Backups page). Administrators only.")
        return {"type": "backup"}
    if a_type == "partner":
        code = _pick("Partner", ctx["partners"], action.get("partner_code"))
        payload = st.text_area("Message (empty = the partner's sample input)", action.get("payload") or "", height=120)
        st.caption("It goes through the partner's mapping, queues, target-system route and deliveries like any "
                   "other message.")
        return {"type": "partner", "partner_code": code, "payload": payload}
    conn = _pick("API (HTTP connection)", ctx["https"], action.get("connection"))
    c1, c2 = st.columns([1, 3])
    methods = ["GET", "POST", "PUT", "PATCH", "DELETE"]
    method = c1.selectbox("Method", methods, methods.index(action.get("method") or "GET"))
    path = c2.text_input("Path", action.get("path") or "", placeholder="e.g. health or reports/run")
    body = st.text_area("Body (for POST / PUT / PATCH)", action.get("body") or "", height=80)
    return {"type": "http", "connection": conn, "method": method, "path": path, "body": body,
            "content_type": "application/json"}


def _schedule_fields(kind, spec, ctx):
    zones = sorted(available_timezones())
    tz_cur = spec.get("timezone") or ctx["tz"]
    if kind == "interval":
        c1, c2 = st.columns(2)
        every = c1.number_input("Every", 1, 100000, int(spec.get("every") or 15))
        units = ["seconds", "minutes", "hours"]
        unit = c2.selectbox("Unit", units, units.index(spec.get("unit") or "minutes"))
        st.caption("The first run is one interval after saving.")
        return {"kind": "interval", "every": every, "unit": unit, "timezone": tz_cur}
    tz = st.selectbox("Time zone", zones, zones.index(tz_cur) if tz_cur in zones else 0)
    if kind == "daily":
        c1, c2 = st.columns([1, 3])
        at = c1.time_input("At", datetime.strptime(spec.get("at") or "09:00", "%H:%M").time(), step=300)
        days = c2.multiselect("On", DAYS, spec.get("days") or DAYS, format_func=str.capitalize)
        return {"kind": "daily", "at": at.strftime("%H:%M"), "days": days, "timezone": tz}
    if kind == "cron":
        expr = st.text_input("Cron expression", spec.get("cron") or "0 9 * * mon-fri",
                             help="minute hour day-of-month month day-of-week. Examples: '*/15 * * * *' every 15 "
                                  "minutes · '0 9 * * mon-fri' weekdays at 09:00 · '30 2 1 * *' 02:30 on the 1st")
        return {"kind": "cron", "cron": expr, "timezone": tz}
    default = datetime.fromisoformat(spec["run_at"]) if spec.get("run_at") else datetime.now() + timedelta(hours=1)
    c1, c2 = st.columns(2)
    d = c1.date_input("On", default.date())
    t = c2.time_input("At", default.time().replace(second=0, microsecond=0), step=300)
    return {"kind": "once", "run_at": datetime.combine(d, t).isoformat(timespec="minutes"), "timezone": tz}
