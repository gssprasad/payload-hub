"""Helpers shared by the Streamlit pages."""
import json

import requests
import streamlit as st

API = "http://localhost:8000"  # set by streamlit_app.py before any page runs
EMBEDDED = False  # True on Streamlit Cloud, where the backend runs inside the Streamlit process
PAGES: dict = {}  # name -> st.Page, set by streamlit_app.py so pages can link to each other
STATUSES = ["COMPLETED", "FAILED", "QUEUED", "TRANSFORMING", "TRANSFORMED"]
STATUS_ICON = {"COMPLETED": "✅", "FAILED": "❌", "SAVED": "✅", "NOT_SAVED": "⚠️"}
GOOD, CRITICAL = "#0ca30c", "#d03b3b"


class ApiError(Exception):
    """The backend rejected a request; the message is safe to show to the user."""


def token() -> str | None:
    return st.session_state.get("auth_token")


def user() -> dict:
    return st.session_state.get("auth_user") or {}


def can(permission: str) -> bool:
    """permission: read | write | delete | admin"""
    u = user()
    return bool(u.get("is_admin") if permission == "admin" else u.get(f"can_{permission}"))


def sign_out(message: str | None = None):
    for k in ("auth_token", "auth_user"):
        st.session_state.pop(k, None)
    st.query_params.pop("session", None)
    if message:
        st.session_state.login_message = message


def api(method: str, path: str, fatal: bool = True, **kwargs):
    """Call the backend as the signed-in user. If it is unreachable, stop the page with a readable message
    (or return None when fatal=False, e.g. for the sidebar health check)."""
    headers = {**kwargs.pop("headers", {}), **({"Authorization": f"Bearer {token()}"} if token() else {})}
    try:
        r = requests.request(method, f"{API}{path}", timeout=kwargs.pop("timeout", 30), headers=headers, **kwargs)
    except requests.ConnectionError:
        if not fatal:
            return None
        st.error(f"Can't reach the Payload Hub backend at {API}. Locally, start it with "
                 "`.venv/bin/uvicorn app.main:app --port 8000`. On Streamlit Cloud, check the app's secrets.")
        st.stop()
    if r.status_code == 401 and path.startswith("/api/") and path != "/api/auth/login":
        sign_out("Your session has ended - please sign in again.")
        st.rerun()
    if r.status_code == 403:
        st.error(f"🔒 {r.json().get('detail', 'Not allowed.')} Ask an administrator if you need it.")
        st.stop()
    if r.status_code == 204:
        return None
    is_json = r.headers.get("content-type", "").startswith("application/json")
    if r.status_code >= 400 and r.status_code != 503:
        detail = r.json().get("detail", r.text) if is_json else r.text
        raise ApiError(detail if isinstance(detail, str) else json.dumps(detail))
    return r.json() if is_json else r.content


def status_label(s: str) -> str:
    return f"{STATUS_ICON.get(s, '⏳')} {s}"


def lang(fmt: str) -> str:
    return "xml" if fmt == "xml" else "json"


def queue_label(p: dict) -> str:
    return "Dedicated" if p.get("queue_mode") == "dedicated" else "Shared"


def partners() -> list[dict]:
    return _partners(token())


@st.cache_data(ttl=5)
def _partners(session_key: str | None) -> list[dict]:  # cached per signed-in session
    return api("GET", "/api/partners")


partners.clear = _partners.clear


def show_events(events: list[dict]):
    import pandas as pd

    if events:
        df = pd.DataFrame(events)
        df["at"] = pd.to_datetime(df["at"]).dt.tz_convert(None).dt.strftime("%H:%M:%S.%f").str[:-3]
        st.dataframe(df, hide_index=True, width="stretch",
                     column_config={"at": "Time", "stage": "Stage", "detail": "Detail"})


def show_agent_result(r: dict):
    """Status line, explanation, saved mapping with preview, and the agent's tool calls."""
    icon = {"SAVED": "✅", "NOT_SAVED": "⚠️"}.get(r["status"], "❌")
    st.markdown(f"{icon} **{r['status']}** · {len(r['steps'])} tool calls · {r['duration_ms'] / 1000:.1f} s · "
                f"{r['usage']['input_tokens']:,} in / {r['usage']['output_tokens']:,} out tokens")
    if r.get("explanation"):
        st.info(r["explanation"])
    if m := r.get("mapping"):
        st.success(f"Saved as **{m['label']}** (`{m['name']}`)")
        a, b = st.columns(2)
        a.markdown("**Mapping expression**")
        a.code(m["expression"], language="text")
        b.markdown(f"**Preview output** ({m['output_format'].upper()})")
        b.code(m["preview"], language=lang(m["output_format"]))
    show_agent_steps(r["steps"])


def show_agent_steps(steps: list[dict]):
    if not steps:
        return
    with st.expander(f"What the agent did ({len(steps)} steps)"):
        for i, s in enumerate(steps, 1):
            detail = {"test_mapping": "output OK" if s["ok"] else s.get("result"),
                      "save_mapping": f"saved “{s.get('name')}”" if s["ok"] else s.get("result"),
                      "inspect_input": f"parsed the payload as {s.get('input_format')}"
                                       + ("" if s["ok"] else f": {s.get('result')}")}.get(s["tool"], "")
            st.markdown(f"{i}. {'✅' if s['ok'] else '❌'} `{s['tool']}` - {detail}")
            if s["tool"] == "test_mapping" and s.get("expression"):
                st.code(s["expression"], language="text")


_CSS = """<style>
/* Page header */
.ph-hero {padding: 1.15rem 1.4rem; border-radius: 1rem; margin: 0 0 .6rem 0; color: #fff;
  background: linear-gradient(120deg, #4f46e5 0%, #7c3aed 55%, #0891b2 100%);
  box-shadow: 0 10px 30px -12px rgba(79, 70, 229, .55);}
.ph-hero h1 {margin: 0; padding: 0; color: #fff; font-size: 1.65rem; font-weight: 700; letter-spacing: -.01em;}
.ph-hero p {margin: .25rem 0 0 0; color: rgba(255,255,255,.85); font-size: .92rem;}
/* Metric cards */
[data-testid="stMetric"] {background: rgba(99,102,241,.06); border: 1px solid rgba(99,102,241,.16);
  border-radius: .9rem; padding: .8rem 1rem;}
[data-testid="stMetricLabel"] p {font-size: .74rem; text-transform: uppercase; letter-spacing: .04em; opacity: .75;}
[data-testid="stMetricValue"] {font-size: 1.45rem !important;}
[data-testid="stMetricValue"] > div {overflow: visible !important; text-overflow: clip !important;}
/* Bordered containers as cards */
[data-testid="stVerticalBlockBorderWrapper"] {border-radius: 1rem;}
/* Tabs */
.stTabs [data-baseweb="tab-list"] {gap: .35rem;}
.stTabs [data-baseweb="tab"] {padding: .45rem .9rem; border-radius: .6rem .6rem 0 0; font-weight: 500;}
/* Tables and buttons */
[data-testid="stDataFrame"] {border-radius: .8rem; overflow: hidden;}
.stButton > button, .stDownloadButton > button, [data-testid="stFormSubmitButton"] > button {font-weight: 600;
  transition: transform .08s ease, box-shadow .15s ease;}
.stButton > button:hover, [data-testid="stFormSubmitButton"] > button:hover {transform: translateY(-1px);
  box-shadow: 0 6px 16px -8px rgba(79,70,229,.6);}
/* Sidebar navigation */
[data-testid="stSidebarNav"] a {border-radius: .6rem; font-weight: 500;}
/* API access card */
.ph-kv {display: grid; grid-template-columns: 9rem 1fr; gap: .35rem .8rem; font-size: .9rem; align-items: baseline;}
.ph-kv b {opacity: .7; font-weight: 600;}
.ph-pill {display: inline-block; padding: .1rem .55rem; border-radius: 999px; font-size: .78rem; font-weight: 600;
  background: rgba(99,102,241,.12); color: #4f46e5;}
</style>"""


def style():
    """Apply the Payload Hub look. Call once per run, before any page renders."""
    st.html(_CSS)


def hero(title: str, subtitle: str = ""):
    """Gradient page header. title may start with an emoji."""
    import html

    sub = f"<p>{html.escape(subtitle)}</p>" if subtitle else ""
    st.html(f'<div class="ph-hero"><h1>{html.escape(title)}</h1>{sub}</div>')


def connection_address(c: dict, target: str = "") -> str:
    """Human-readable address of a connection plus queue/topic/path."""
    cfg, t = c.get("config", {}), (target or "").strip()
    if c["type"] == "http":
        base = cfg.get("base_url", "").rstrip("/")
        return f"{base}/{t.lstrip('/')}" if t else base
    if c["type"] == "rabbitmq":
        vhost = cfg.get("vhost", "/")
        return f"amqp{'s' if cfg.get('tls') else ''}://{cfg.get('host')}:{cfg.get('port')}{vhost if vhost.startswith('/') else '/' + vhost} → queue {t}"
    if c["type"] == "activemq":
        return f"stomp://{cfg.get('host')}:{cfg.get('port')} → {cfg.get('destination_prefix', '/queue/')}{t}"
    if c["type"] == "ibmmq":
        return f"{cfg.get('queue_manager')} @ {cfg.get('host')}({cfg.get('port')}) channel {cfg.get('channel')} → queue {t}"
    if c["type"] == "kafka":
        return f"kafka://{cfg.get('bootstrap')} → topic {t}"
    if c["type"] == "ftp":
        import posixpath

        return f"{cfg.get('protocol')}://{cfg.get('host')}:{cfg.get('port')}" \
               f"{posixpath.join(cfg.get('base_dir') or '/', t)} (files {cfg.get('file_pattern') or '*'})"
    if c["type"] == "database":
        where = cfg.get("database") if cfg.get("engine") == "sqlite" else \
            f"{cfg.get('host')}:{cfg.get('port')}/{cfg.get('database')}"
        return f"{cfg.get('engine')}://{where} → table {t}"
    return t


def retry_summary(c: dict) -> str:
    cfg = c.get("config", {})
    extra = f", on HTTP {cfg.get('retry_on_status', '408,425,429,500,502,503,504')}" if c["type"] == "http" else ""
    return (f"{cfg.get('retry_attempts', 3)} attempts, first retry after {cfg.get('retry_backoff', 2)} s, "
            f"doubling to max {cfg.get('retry_backoff_max', 30)} s{extra}")


def partner_endpoints(p: dict, conns: dict) -> list[dict]:
    """Every inbound and outbound address of a partner, for the API access view and the flow tab."""
    rows = [{"Direction": "⬇️ Inbound", "Type": "HTTP endpoint", "Address": f"POST {API}/inbound/{p['code']}",
             "Details": f"header X-API-Key · body {p['input_format'].upper()}"}]
    if p.get("inbound_connection") and p["inbound_connection"] in conns:
        c = conns[p["inbound_connection"]]
        rows.append({"Direction": "⬇️ Inbound", "Type": f"{c['type'].upper()} listener ({c['name']})",
                     "Address": connection_address(c, p.get("inbound_source")), "Details": "messages are consumed and transformed"})
    rows.append({"Direction": "⚙️ Internal", "Type": "Kafka", "Address": f"{p['topic_in']} → {p['topic_out']}",
                 "Details": f"failures → {p['topic_dlq']}"})
    if p.get("reply_mode") == "forward" and p.get("reply_connection") in conns:
        c = conns[p["reply_connection"]]
        http = c["type"] == "http"
        method = (p.get("reply_method") or c.get("config", {}).get("method") or "POST") + " " if http else ""
        rows.append({"Direction": "🎯 System B (sync)",
                     "Type": f"{c['type'].upper()} ({c['name']}) · " + ("👤 dedicated" if c.get("owner_partner") else "🌍 common"),
                     "Address": method + connection_address(c, p.get("reply_path")),
                     "Details": ("sends CSV · " if p.get("send_format") == "csv" else "")
                                + ("response" if http else "delivery receipt") + " returned to caller "
                                + ("as is" if p.get("response_format") == "unchanged" else "translated")
                                + f" · retry: {retry_summary(c)}"})
    if p.get("outbound_connection") and p["outbound_connection"] in conns:
        c = conns[p["outbound_connection"]]
        rows.append({"Direction": "⬆️ Outbound copy (async)", "Type": f"Deliver via {c['type'].upper()} ({c['name']}) · "
                     + ("👤 dedicated" if c.get("owner_partner") else "🌍 common"),
                     "Address": connection_address(c, p.get("outbound_target")), "Details": f"retry: {retry_summary(c)}"})
    if p.get("store_enabled") and p.get("store_table"):
        sc = conns.get(p.get("store_connection") or "")
        rows.append({"Direction": "🗄️ Store", "Type": f"Database table ({sc['name'] if sc else 'built-in data area'})",
                     "Address": connection_address(sc, p["store_table"]) if sc else f"userdata.{p['store_table']}",
                     "Details": f"from the {p.get('store_source') or 'output'} payload"})
    return rows
