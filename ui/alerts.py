"""Alerts page: what fired, the rules (which events go where) and the channels (webhook, email)."""
import pandas as pd
import streamlit as st

from . import common
from .common import ApiError, api

STATUS = {"sent": "✅ sent", "partial": "⚠️ partly sent", "failed": "❌ not delivered", "suppressed": "🔕 suppressed",
          "no_channel": "— no channel"}
FORMATS = {"generic": "Generic JSON", "slack": "Slack", "teams": "Microsoft Teams"}


def _when(v) -> str:
    return pd.to_datetime(v).tz_convert(common.TZ).tz_localize(None).strftime("%Y-%m-%d %H:%M:%S") if v else "—"


def page():
    common.hero("🔔 Alerts", "Get told when something fails - by webhook (Slack, Teams) or email.")
    data = api("GET", "/api/alerts")
    down = [k for k, v in data["watch"].items() if v == "down"]
    if down:
        st.error("Down right now: " + ", ".join(k.split(":", 1)[1] for k in down))
    log_tab, rules_tab, channels_tab = st.tabs(["Alert log", f"Rules ({len(data['rules'])})",
                                                f"Channels ({len(data['channels'])})"])
    with log_tab:
        _log(data)
    with rules_tab:
        _rules(data)
    with channels_tab:
        _channels(data)


def _log(data):
    if not data["rules"]:
        st.info("No alert rules yet. Add a channel (where alerts go), then a rule (what to alert on).")
    rules = {r["id"]: r["name"] for r in data["rules"]}
    if not data["log"]:
        st.caption("Nothing has fired yet.")
        return
    st.dataframe([{"When": _when(e["at"]), "Rule": rules.get(e["rule_id"], e["rule_id"] or "—"), "Alert": e["subject"],
                   "Status": STATUS.get(e["status"], e["status"]), "Detail": (e["detail"] or "")[:200]}
                  for e in data["log"]], hide_index=True, width="stretch", height=420)
    st.caption("Suppressed = the same thing already alerted within the rule's cool-down.")


def _rules(data):
    top = st.columns([3, 1])
    top[0].caption("A rule sends an event to one or more channels. Repeats within the cool-down are suppressed; "
                   "'down' events also tell you when things recover.")
    if top[1].button("➕ New rule", type="primary", width="stretch", disabled=not (common.can("write") and data["channels"]),
                     help=None if data["channels"] else "Add a channel first"):
        _rule_dialog(None, data)
    if not data["rules"]:
        return
    rid = common.pick_row([{"Rule": r["name"], "Alerts on": r["text"],
                            "Sends to": ", ".join(r["channels"]), "Cool-down": f"{r['cooldown_minutes']} min",
                            "Fired": r["fired_count"], "Last": _when(r["last_fired_at"]),
                            "Active": "✅" if r["enabled"] else "⏸️"} for r in data["rules"]],
                          "alert_rules", [r["id"] for r in data["rules"]])
    r = next(x for x in data["rules"] if x["id"] == rid)
    common.detail_title(r["name"])
    if common.can("write"):
        _rule_form(r, data)
        if st.button("🗑️ Delete rule", key=f"rule_del_{rid}", disabled=not common.can("delete")):
            api("DELETE", f"/api/alerts/rules/{rid}")
            st.session_state.pop("alert_rules_selected", None)
            st.rerun()


@st.dialog("New alert rule", width="large")
def _rule_dialog(r, data):
    _rule_form(r, data)


def _rule_form(r, data):
    key = r["id"] if r else "new"
    events = data["events"]
    event = st.selectbox("Alert when", list(events), list(events).index(r["event"]) if r else 0,
                         format_func=events.get, key=f"rule_event_{key}")
    field = data["filters"].get(event)
    options = {}
    if field == "partner":
        options = {p["code"]: p["name"] for p in api("GET", "/api/partners")}
    elif field == "workflow":
        options = {w["id"]: w["name"] for w in api("GET", "/api/workflows")["workflows"]}
    elif field == "task":
        options = {t["id"]: t["name"] for t in api("GET", "/api/schedules")["tasks"]}
    elif field == "port":
        options = {p["id"]: p["name"] for p in api("GET", "/api/ports")}
    with st.form(f"rule_form_{key}_{event}"):
        name = st.text_input("Name *", r["name"] if r else "", disabled=bool(r), placeholder="e.g. Acme failures")
        scope = None
        if field:
            ids = [""] + list(options)
            cur = (r or {}).get("filter", {}).get(field, "")
            scope = st.selectbox(f"Only for this {field}", ids, ids.index(cur) if cur in ids else 0,
                                 format_func=lambda i: options.get(i, f"any {field}"))
        threshold = {}
        if event == "error_rate":
            t = (r or {}).get("threshold") or {}
            c1, c2 = st.columns(2)
            threshold = {"count": c1.number_input("Failed transactions", 1, 100000, int(t.get("count", 5))),
                         "minutes": c2.number_input("Within (minutes)", 1, 1440, int(t.get("minutes", 10)))}
        channels = {c["id"]: c["name"] for c in data["channels"]}
        chosen = st.multiselect("Send to", list(channels), [c for c in (r or {}).get("channels", []) if c in channels],
                                format_func=channels.get)
        cooldown = st.number_input("Cool-down (minutes)", 0, 1440, int((r or {}).get("cooldown_minutes", 15)),
                                   help="Don't repeat the same alert within this time. 0 = every time.")
        enabled = st.toggle("Active", (r or {}).get("enabled", True))
        if st.form_submit_button("Save" if r else "Create rule", type="primary"):
            body = {"name": name, "event": event, "filter": {field: scope} if field and scope else {},
                    "threshold": threshold, "channels": chosen, "cooldown_minutes": cooldown, "enabled": enabled}
            try:
                saved = api("PUT" if r else "POST", f"/api/alerts/rules/{r['id']}" if r else "/api/alerts/rules", json=body)
            except ApiError as e:
                st.error(str(e))
                return
            st.session_state["alert_rules_selected"] = saved["id"]
            st.rerun()


def _channels(data):
    admin = common.can("admin")
    top = st.columns([3, 1])
    top[0].caption("Where alerts go. A webhook URL that contains a token (Slack, Teams) is best kept as a stored "
                   "secret; email passwords always are.")
    if top[1].button("➕ New channel", type="primary", width="stretch", disabled=not admin,
                     help=None if admin else "Only administrators manage channels."):
        _channel_dialog(None)
    if not data["channels"]:
        return
    cid = common.pick_row([{"Channel": c["name"], "Type": "🌐 webhook" if c["type"] == "webhook" else "✉️ email",
                            "Sends to": _target(c), "Last sent": _when(c["last_sent_at"]),
                            "Status": "❌ " + c["last_error"] if c.get("last_error") else "✅ ok",
                            "Active": "✅" if c["enabled"] else "⏸️"} for c in data["channels"]],
                          "alert_channels", [c["id"] for c in data["channels"]])
    c = next(x for x in data["channels"] if x["id"] == cid)
    common.detail_title(c["name"])
    if st.button("📨 Send a test alert", key=f"ch_test_{cid}", disabled=not admin):
        r = api("POST", f"/api/alerts/channels/{cid}/test")
        (st.success if r["ok"] else st.error)(r["detail"])
    if admin:
        _channel_form(c)
        if st.button("🗑️ Delete channel", key=f"ch_del_{cid}"):
            api("DELETE", f"/api/alerts/channels/{cid}")
            st.session_state.pop("alert_channels_selected", None)
            st.rerun()


def _target(c) -> str:
    cfg = c["config"]
    if c["type"] == "email":
        return cfg.get("to", "")
    return (f"🔐 {c['secret_alias']}" if c.get("secret_alias") else cfg.get("url", "")[:50]) + \
        f" ({FORMATS.get(cfg.get('format'), '')})"


@st.dialog("New alert channel", width="large")
def _channel_dialog(c):
    _channel_form(c)


def _channel_form(c):
    key = c["id"] if c else "new"
    kind = c["type"] if c else st.radio("Type", ["webhook", "email"], horizontal=True, key="ch_new_type",
                                        format_func={"webhook": "🌐 Webhook (Slack, Teams, any JSON)",
                                                     "email": "✉️ Email (SMTP)"}.get)
    cfg = (c or {}).get("config") or {}
    aliases = [""] + [a["alias"] for a in api("GET", "/api/secret-aliases")]
    with st.form(f"ch_form_{key}_{kind}"):
        name = st.text_input("Name *", c["name"] if c else "", disabled=bool(c), placeholder="e.g. Ops on-call")
        if kind == "webhook":
            url = st.text_input("Webhook URL", cfg.get("url", ""), placeholder="https://hooks.slack.com/services/…")
            alias = st.selectbox("…or the URL from a stored secret", aliases,
                                 aliases.index(c.get("secret_alias") or "") if c and (c.get("secret_alias") or "") in aliases
                                 else 0, format_func=lambda a: f"🔐 {a}" if a else "— use the URL above —")
            fmts = list(FORMATS)
            fmt = st.selectbox("Format", fmts, fmts.index(cfg.get("format", "generic")), format_func=FORMATS.get)
            config = {"url": url, "format": fmt}
        else:
            c1, c2, c3 = st.columns([3, 1, 1])
            host = c1.text_input("SMTP server *", cfg.get("host", ""), placeholder="smtp.office365.com")
            port = c2.number_input("Port", 1, 65535, int(cfg.get("port", 587)))
            secs = ["starttls", "ssl", "none"]
            security = c3.selectbox("Security", secs, secs.index(cfg.get("security", "starttls")), format_func=str.upper)
            c1, c2 = st.columns(2)
            user = c1.text_input("Username", cfg.get("username", ""))
            alias = c2.selectbox("Password (stored secret)", aliases,
                                 aliases.index(c.get("secret_alias") or "") if c and (c.get("secret_alias") or "") in aliases
                                 else 0, format_func=lambda a: f"🔐 {a}" if a else "none")
            sender = st.text_input("From", cfg.get("from", ""), placeholder="payload-hub@company.com")
            to = st.text_input("To *", cfg.get("to", ""), placeholder="ops@company.com, oncall@company.com")
            config = {"host": host, "port": port, "security": security, "username": user, "from": sender, "to": to}
        enabled = st.toggle("Active", (c or {}).get("enabled", True))
        if st.form_submit_button("Save" if c else "Create channel", type="primary"):
            body = {"name": name, "type": kind, "config": config, "secret_alias": alias or None, "enabled": enabled}
            try:
                saved = api("PUT" if c else "POST", f"/api/alerts/channels/{c['id']}" if c else "/api/alerts/channels",
                            json=body)
            except ApiError as e:
                st.error(str(e))
                return
            st.session_state["alert_channels_selected"] = saved["id"]
            st.rerun()
