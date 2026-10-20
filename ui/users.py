"""Users page (administrators): add users and set read / write / delete permissions."""
import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

PERMISSIONS = {
    "can_read": ("Read", "View partners, transactions, monitoring, connections"),
    "can_write": ("Write", "Onboard and edit partners, mappings, connections; send messages; run the AI agent"),
    "can_delete": ("Delete", "Remove partners, documents and connections"),
    "is_admin": ("Admin", "Manage users"),
}


def page():
    common.hero("👥 Users", "Who can sign in, and what each person may do.")
    users = api("GET", "/api/users")
    top = st.columns([3, 1], vertical_alignment="center")
    top[0].caption(f"{len(users)} users · {sum(u['active'] for u in users)} active · click a user to edit")
    if top[1].button("➕ New user", type="primary", width="stretch"):
        _add_user_dialog()
    name = common.pick_row([_row(u) for u in users], "users", [u["username"] for u in users])
    _edit_user(next(u for u in users if u["username"] == name))


def _locked(u) -> bool:
    return bool(u.get("locked_until")) and pd.to_datetime(u["locked_until"]) > pd.Timestamp.now(tz="UTC")


def _row(u) -> dict:
    status = "🔒 locked" if _locked(u) else "🟢 active" if u["active"] else "⏸️ disabled"
    last = u.get("last_login_at")
    return {"User": u["username"], "Status": status,
            "Signs in with": "🔐 single sign-on" if u.get("auth_source") == "sso" else "🔑 password",
            **{label: "✅" if u[k] else "—" for k, (label, _) in PERMISSIONS.items()},
            "Last sign-in": pd.to_datetime(last).tz_convert(common.TZ).strftime("%Y-%m-%d %H:%M") if last else "never"}


def _permission_inputs(prefix, current=None):
    current = current or {"can_read": True, "can_write": False, "can_delete": False, "is_admin": False}
    cols = st.columns(4)
    return {k: cols[i].checkbox(label, value=current[k], help=help_, key=f"{prefix}_{k}")
            for i, (k, (label, help_)) in enumerate(PERMISSIONS.items())}


@st.dialog("New user")
def _add_user_dialog():
    with st.form("add_user", clear_on_submit=True):
        c1, c2 = st.columns(2)
        username = c1.text_input("Username *", help="Letters, digits and . _ @ -")
        password = c2.text_input("Initial password *", type="password", help="At least 6 characters.")
        perms = _permission_inputs("new")
        if st.form_submit_button("Add user", type="primary"):
            try:
                api("POST", "/api/users", json={"username": username.strip(), "password": password, **perms})
            except ApiError as e:
                st.error(_friendly(str(e)))
                return
            st.toast(f"Added {username}", icon="👤")
            st.rerun()


def _edit_user(u):
    name = u["username"]
    common.detail_title(name)
    me = common.user().get("username", "").lower() == name.lower()
    if _locked(u):
        c = st.columns([3, 1], vertical_alignment="center")
        c[0].warning(f"Locked after too many wrong passwords, until "
                     f"{pd.to_datetime(u['locked_until']).tz_convert(common.TZ):%H:%M}.")
        if c[1].button("🔓 Unlock", width="stretch"):
            api("PUT", f"/api/users/{name}", json={"unlock": True})
            st.rerun()
    with st.form(f"edit_user_{name}"):
        perms = _permission_inputs(f"edit_{name}", u)
        active = st.toggle("Account active", value=u["active"], disabled=me,
                           help="Disabled users can't sign in; their sessions end at once.")
        new_password = "" if u.get("auth_source") == "sso" else \
            st.text_input("Set a new password (optional)", type="password", key=f"pw_{name}")
        if st.form_submit_button("Save changes", type="primary"):
            body = {**perms, "active": active}
            if new_password:
                body["password"] = new_password
            try:
                api("PUT", f"/api/users/{name}", json=body)
            except ApiError as e:
                st.error(_friendly(str(e)))
                return
            if me and new_password:
                common.sign_out("Password changed - please sign in again.")
            elif me:
                st.session_state.auth_user = api("GET", "/api/auth/me")  # refresh my own permissions
            st.toast(f"Saved {name}", icon="✅")
            st.rerun()
    if not me:
        with st.expander("Delete user"):
            if st.checkbox(f"I want to delete {name}", key=f"confirm_del_user_{name}") and \
                    st.button("Delete user", key=f"del_user_{name}"):
                try:
                    api("DELETE", f"/api/users/{name}")
                except ApiError as e:
                    st.error(_friendly(str(e)))
                    return
                st.rerun()


def _friendly(detail: str) -> str:
    if "string_pattern_mismatch" in detail or "pattern" in detail:
        return "Usernames may only contain letters, digits and . _ @ -"
    if "string_too_short" in detail:
        return "Username needs at least 2 characters and the password at least 6."
    return detail
