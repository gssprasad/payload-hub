"""Secrets page (administrators): named secrets, the master key, and the audit log."""
import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

KINDS = {"password": "🔑 Password", "api_key": "🗝️ API key", "token": "🎫 Token", "certificate": "📜 Certificate",
         "other": "🔒 Other"}


def _when(v) -> str:
    return pd.to_datetime(v).tz_convert(None).strftime("%Y-%m-%d %H:%M") if v else "—"


def page():
    common.hero("🔐 Secrets", "Encrypted credentials, the master key, and the audit log.")
    data = api("GET", "/api/secrets")
    secrets_tab, key_tab, audit_tab = st.tabs(["Secrets", "Master key", "Audit log"])
    with secrets_tab:
        _secrets(data["secrets"])
    with key_tab:
        _master_key(data["key"], data["check"])
    with audit_tab:
        _audit()


def _secrets(secrets):
    top = st.columns([3, 1])
    top[0].caption("Values are write-only: they're encrypted with the master key and never shown again. "
                   "A connection can use a secret instead of its own password - change it here once and every "
                   "connection using it picks it up.")
    with top[1].popover("➕ New secret", width="stretch"):
        _secret_form(None)
    if not secrets:
        st.info("No secrets yet.")
        return
    alias = common.pick_row(
        [{"Alias": s["alias"], "Kind": KINDS.get(s["kind"], s["kind"]), "Used by": ", ".join(s["used_by"]) or "—",
          "Version": s["version"], "Changed": f"{_when(s['updated_at'])} by {s['updated_by'] or '—'}"} for s in secrets],
        "secrets", [s["alias"] for s in secrets])
    s = next(x for x in secrets if x["alias"] == alias)
    common.detail_title(f"{KINDS.get(s['kind'], '')} {s['alias']}")
    if s.get("description"):
        st.caption(s["description"])
    left, right = st.columns(2)
    with left, st.container(border=True):
        st.markdown("**Change**")
        _secret_form(s)
    with right, st.container(border=True):
        st.markdown("**Delete**")
        if s["used_by"]:
            st.caption(f"Used by {', '.join(s['used_by'])} - switch them to another secret first.")
        elif st.button("🗑️ Delete secret", key=f"sec_del_{alias}"):
            api("DELETE", f"/api/secrets/{alias}")
            st.session_state.pop("secrets_selected", None)
            st.rerun()


def _secret_form(s):
    key = s["alias"] if s else "new"
    with st.form(f"secret_{key}", clear_on_submit=True):
        alias = st.text_input("Alias *", s["alias"] if s else "", disabled=bool(s), placeholder="e.g. erp.db.password")
        kinds = list(KINDS)
        kind = st.selectbox("Kind", kinds, kinds.index(s["kind"]) if s else 0, format_func=KINDS.get)
        desc = st.text_input("Description", (s or {}).get("description") or "")
        value = st.text_input("New value" if s else "Value *", type="password",
                              placeholder="leave empty to keep the current value" if s else "")
        if st.form_submit_button("Save" if s else "Create", type="primary"):
            body = {"alias": alias, "kind": kind, "description": desc, "value": value or None}
            try:
                api("PUT" if s else "POST", f"/api/secrets/{s['alias']}" if s else "/api/secrets", json=body)
            except ApiError as e:
                st.error(str(e))
                return
            st.toast("Secret saved", icon="🔐")
            st.session_state["secrets_selected"] = alias
            st.rerun()


def _master_key(key, check):
    c = st.columns(3)
    c[0].metric("Encrypted credentials", check["total"])
    c[1].metric("Unreadable", len(check["unreadable"]))
    c[2].metric("Key fingerprint", key["fingerprint"][:8])
    st.markdown(f"**Key source:** {key['source']}" + (f" · created {key['created']}" if key.get("created") else ""))
    if check["unreadable"]:
        st.error("These can't be decrypted with the current key and must be entered again: "
                 + ", ".join(check["unreadable"]))
    st.caption("Connection passwords, AI provider keys and named secrets are all encrypted with this key.")
    if not key["rotatable"]:
        st.info("The key comes from the PAYLOAD_HUB_SECRET_KEY environment variable - rotate it where that is set.")
        return
    with st.container(border=True):
        st.markdown("**Rotate the master key**")
        st.caption("Creates a new key and re-encrypts every stored credential in one step. The old key is kept as a "
                   "timestamped backup next to the key file. Back up the new key file afterwards.")
        if st.checkbox("I understand - rotate the key now", key="rotate_ok") and \
                st.button("🔄 Rotate master key", type="primary"):
            try:
                r = api("POST", "/api/secrets/master-key/rotate")
            except ApiError as e:
                st.error(str(e))
                return
            st.success(f"Done - {r['reencrypted']} credential(s) re-encrypted. New fingerprint {r['fingerprint'][:8]}; "
                       f"old key saved as {r['backup']}.")


def _audit():
    types = ["all", "secret", "master key", "connection", "partner", "port", "schedule", "workflow", "user"]
    c1, c2 = st.columns([1, 3])
    kind = c1.selectbox("Show", types, key="audit_type")
    rows = api("GET", "/api/audit", params={"limit": 300, **({"object_type": kind} if kind != "all" else {})})
    if not rows:
        st.caption("Nothing recorded yet.")
        return
    st.dataframe([{"When": pd.to_datetime(r["at"]).tz_convert(None).strftime("%Y-%m-%d %H:%M:%S"),
                   "Who": r["username"] or "system", "Did": r["action"], "What": r["object_type"],
                   "Which": r["object_id"] or "", "Detail": r["detail"] or ""} for r in rows],
                 hide_index=True, width="stretch", height=480)
