"""Deployments page (administrators): move configuration between environments with packages."""
import json

import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

PLURAL = {"partners": "Partners", "workflows": "Workflows", "schedules": "Scheduled tasks", "ports": "Ports",
          "connections": "Connections", "alert_rules": "Alert rules", "alert_channels": "Alert channels",
          "secrets": "Secrets (names only)"}
ACTION = {"create": "➕ create", "update": "✏️ update", "unchanged": "✔️ unchanged", "present": "🔐 present",
          "missing": "❌ missing"}


def page():
    common.hero("🚚 Deployments", "Move partners, workflows, connections and more to the next environment.")
    inv = api("GET", "/api/deploy/inventory")
    st.caption(f"This environment: **{inv['environment']}** · version `{inv['version']}` · see "
               "docs/DEPLOYMENT.md for the full path from DEV to production.")
    export_tab, import_tab, history_tab = st.tabs(["Export a package", "Deploy a package", "History"])
    with export_tab:
        _export(inv)
    with import_tab:
        _import(inv)
    with history_tab:
        _history()


def _export(inv):
    st.caption("Pick what to move. Connections, secrets (names only), workflows and alert channels they use are "
               "added automatically. Secret values never leave this environment.")
    selection = {}
    cols = st.columns(2)
    order = ["partners", "workflows", "schedules", "ports", "connections", "alert_rules", "alert_channels", "secrets"]
    for i, kind in enumerate(order):
        options = {a["id"]: a["name"] for a in inv["assets"].get(kind, [])}
        selection[kind] = cols[i % 2].multiselect(PLURAL[kind], list(options), format_func=options.get,
                                                  key=f"exp_{kind}", placeholder="none")
    c1, c2, c3 = st.columns([2, 1.2, 1], vertical_alignment="bottom")
    name = c1.text_input("Package name", "release", key="exp_name")
    deps = c2.toggle("Dependencies", True, key="exp_deps", help="Add the connections, workflows, alert channels and "
                                                                   "secret names the chosen items use.")
    if c3.button("📦 Create", type="primary", width="stretch", disabled=not any(selection.values())):
        try:
            st.session_state.exp_pkg = api("POST", "/api/deploy/export",
                                           json={"name": name, "selection": selection, "with_dependencies": deps})
        except ApiError as e:
            st.error(str(e))
    if pkg := st.session_state.get("exp_pkg"):
        counts = ", ".join(f"{len(v)} {inv['labels'][k].lower()}{'s' if len(v) != 1 else ''}"
                           for k, v in pkg["assets"].items())
        st.success(f"Package **{pkg['name']}**: {counts}.")
        st.download_button("⬇️ Download package", json.dumps(pkg, indent=2, ensure_ascii=False),
                           file_name=f"{pkg['name']}.json", mime="application/json")
        st.caption("Keep packages in git (e.g. deploy/packages/) so every environment gets exactly the same thing, "
                   "and so the release pipeline can deploy them.")


def _import(inv):
    st.caption(f"Deploying into **{inv['environment']}**. Check first - nothing changes until you deploy, and a "
               "database backup is taken right before.")
    c1, c2 = st.columns(2)
    pkg_file = c1.file_uploader("Package (.json)", type=["json"], key="imp_pkg")
    ovr_file = c2.file_uploader("Overrides for this environment (optional)", type=["json"], key="imp_ovr",
                                help="What differs here: hosts, URLs, folders, secret aliases, enabled schedules. "
                                     "See deploy/environments/test.example.json.")
    if not pkg_file:
        return
    try:
        package = json.loads(pkg_file.getvalue())
        overrides = json.loads(ovr_file.getvalue()) if ovr_file else None
    except json.JSONDecodeError as e:
        st.error(f"Not valid JSON: {e}")
        return
    try:
        plan = api("POST", "/api/deploy/plan", json={"package": package, "overrides": overrides})
    except ApiError as e:
        st.error(str(e))
        return
    p = plan["package"]
    st.markdown(f"**{p.get('name')}** from **{p.get('source_environment')}** (version `{p.get('app_version')}`, "
                f"exported {str(p.get('created_at', ''))[:16].replace('T', ' ')} by {p.get('created_by') or '?'}) "
                f"→ **{plan['target_environment']}**")
    c = plan["counts"]
    m = st.columns(4)
    m[0].metric("Create", c["create"])
    m[1].metric("Update", c["update"])
    m[2].metric("Unchanged", c["unchanged"])
    m[3].metric("Missing secrets", c["missing"])
    st.dataframe([{"Action": ACTION.get(i["action"], i["action"]), "Type": i["label"], "Name": i["name"],
                   "Changes": ", ".join(i["changes"]), "Problems": " · ".join(i["problems"])} for i in plan["items"]],
                 hide_index=True, width="stretch")
    for prob in plan["problems"]:
        (st.error if prob["blocking"] else st.warning)(f"{prob['id']}: {prob['problem']}")
    if not plan["can_apply"]:
        st.info("Fix the problems above (e.g. create the missing secrets on the Secrets page), then check again.")
        return
    if c["create"] + c["update"] == 0:
        st.success("Everything is already up to date here.")
        return
    sure = st.checkbox(f"Deploy {c['create'] + c['update']} change(s) to {plan['target_environment']}", key="imp_sure")
    if st.button("🚚 Deploy", type="primary", disabled=not sure):
        with st.spinner("Backing up and deploying…"):
            try:
                r = api("POST", "/api/deploy/apply", json={"package": package, "overrides": overrides}, timeout=900)
            except ApiError as e:
                st.error(str(e))
                return
        if r["status"] == "DEPLOYED":
            st.success(f"Deployed - {len(r['applied'])} item(s). Backup taken first: {r['backup']}.")
        else:
            st.error(f"Stopped part-way: {r['error']}. {len(r['applied'])} item(s) were applied; the backup "
                     f"{r['backup']} has the state from before (Backups page).")


def _history():
    rows = api("GET", "/api/deploy/history")
    if not rows:
        st.caption("Nothing deployed into this environment yet.")
        return
    st.dataframe([{"When": pd.to_datetime(d["at"]).tz_convert(common.TZ).strftime("%Y-%m-%d %H:%M"),
                   "Package": d["package_name"], "From": d["source_environment"], "Version": d["app_version"],
                   "By": d["username"], "Result": "✅ deployed" if d["status"] == "DEPLOYED" else "❌ " + d["status"],
                   "Applied": len((d["summary"] or {}).get("applied", [])), "Backup": d["backup_file"],
                   "Error": d["error"] or ""} for d in rows], hide_index=True, width="stretch")
