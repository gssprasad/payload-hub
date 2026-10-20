"""Streamlit UI for Payload Hub.

The Kafka pipeline, PostgreSQL log and mapping agent run in the FastAPI backend (app/main.py);
this app is a front end that calls its REST API. Pages live in the ui/ package.

Locally, start the backend first, then:

    .venv/bin/streamlit run streamlit_app.py

On Streamlit Community Cloud there is no separate backend, so set EMBED_BACKEND = "true" in the
app's secrets: the backend (and with it the Kafka pipeline) then starts inside this process.
All settings - Kafka, DATABASE_URL, ANTHROPIC_API_KEY, ADMIN_PASSWORD - can come from secrets.
Sign-in uses the users stored in PostgreSQL; the first start creates Administrator (password from
ADMIN_PASSWORD, default "manage").
"""
import asyncio
import os
import threading
import time as clock
from pathlib import Path

import streamlit as st

st.set_page_config(page_title="Payload Hub", page_icon="🔀", layout="wide")


def _load_secrets():
    """Expose Streamlit secrets (UPPER_CASE keys) as environment variables for the backend modules."""
    try:
        items = dict(st.secrets)
    except Exception:  # noqa: BLE001 - no secrets file locally
        return
    for k, v in items.items():
        if k.isupper() and isinstance(v, (str, int, float, bool)):
            os.environ.setdefault(k, str(v))


_load_secrets()


# Streamlit Community Cloud checks repositories out under /mount/src; there is no separate backend
# there, so the pipeline always runs in-process. Elsewhere EMBED_BACKEND decides.
ON_STREAMLIT_CLOUD = Path(__file__).resolve().as_posix().startswith("/mount/src/")
_embed = os.getenv("EMBED_BACKEND", "").strip().lower()
EMBED_BACKEND = _embed in ("1", "true", "yes") if _embed else ON_STREAMLIT_CLOUD
EMBED_PORT = int(os.getenv("EMBED_PORT", "8765"))
REQUIRED_SECRETS = {
    "DATABASE_URL": "PostgreSQL connection string, e.g. from Neon",
    "KAFKA_BOOTSTRAP": "Kafka host:port, e.g. from Aiven",
}
SASL_SECRETS = ("KAFKA_USERNAME", "KAFKA_PASSWORD", "KAFKA_CA_CERT")


def _setup_problem() -> str | None:
    """Explain missing settings before trying to connect (Streamlit Cloud only)."""
    if not ON_STREAMLIT_CLOUD:
        return None
    missing = [f"- `{k}` - {why}" for k, why in REQUIRED_SECRETS.items() if not os.getenv(k)]
    if os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper().startswith("SASL"):
        missing += [f"- `{k}` - needed for SASL Kafka" for k in SASL_SECRETS if not os.getenv(k)]
    if not missing:
        return None
    return ("**The app's secrets are missing settings it needs on Streamlit Cloud:**\n\n" + "\n".join(missing) +
            "\n\nOpen **Manage app → ⋮ → Settings → Secrets**, paste the template from "
            "`.streamlit/secrets.toml.example` with your values, and save. The app restarts by itself.")


def _preflight() -> list[str]:
    """Check the database and Kafka connections, so a bad setting gets a specific message."""
    problems = []
    try:
        import psycopg

        from app import db

        with psycopg.connect(db.DATABASE_URL, connect_timeout=10):
            pass
    except Exception as e:  # noqa: BLE001
        problems.append(f"**PostgreSQL** - can't connect with `DATABASE_URL`: {e}")

    async def kafka_check():
        from aiokafka.admin import AIOKafkaAdminClient

        from app import pipeline

        admin = AIOKafkaAdminClient(**pipeline._connection(), request_timeout_ms=15000)
        await admin.start()
        try:
            await admin.list_topics()
        finally:
            await admin.close()

    try:
        asyncio.run(asyncio.wait_for(kafka_check(), 30))
    except Exception as e:  # noqa: BLE001
        detail = str(e) or type(e).__name__
        problems.append(f"**Kafka** - can't connect to `{os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')}`: {detail}. "
                        "Check KAFKA_BOOTSTRAP, KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM, KAFKA_USERNAME, "
                        "KAFKA_PASSWORD and KAFKA_CA_CERT.")
    return problems


@st.cache_resource(show_spinner="Starting the pipeline (Kafka, database)…")
def _start_backend():
    """Run the FastAPI backend in a background thread of this process (Streamlit Cloud mode)."""
    import uvicorn

    from app.main import app as backend  # imported here so it sees the settings loaded from secrets

    problems = _preflight()
    if problems:
        raise RuntimeError("\n\n".join(problems))
    server = uvicorn.Server(uvicorn.Config(backend, host="127.0.0.1", port=EMBED_PORT, log_level="info"))
    thread = threading.Thread(target=server.run, name="payload-hub-backend", daemon=True)
    thread.start()
    deadline = clock.monotonic() + 90
    while not server.started:
        if not thread.is_alive() or clock.monotonic() > deadline:
            raise RuntimeError("The pipeline failed to start although the database and Kafka are reachable. "
                               "See **Manage app → Logs** for the error.")
        clock.sleep(0.25)
    return server


from ui import (agent, common, connections, data, monitoring, overview, partners, ports, schedulers, send,  # noqa: E402
                transactions, users, workflows)

common.style()

if EMBED_BACKEND:
    if problem := _setup_problem():
        st.title("🔀 Payload Hub - setup needed")
        st.error(problem)
        st.stop()
    try:
        _start_backend()
    except RuntimeError as e:
        _start_backend.clear()  # retry on the next run instead of caching the failure
        st.title("🔀 Payload Hub - can't start the pipeline")
        st.error(str(e))
        st.button("Retry")
        st.stop()
    common.API = f"http://127.0.0.1:{EMBED_PORT}"
    common.EMBEDDED = True
else:
    common.API = os.getenv("PAYLOAD_HUB_API", "http://localhost:8000")


def _restore_session():
    """Stay signed in across browser refreshes: the session token is kept in the page URL (?session=...)."""
    token = st.query_params.get("session")
    if not token or common.token():
        return
    st.session_state.auth_token = token
    try:
        st.session_state.auth_user = common.api("GET", "/api/auth/me")
    except common.ApiError:
        common.sign_out()
        st.query_params.pop("session", None)


def _login_page():
    with st.sidebar:
        st.markdown("### 🔀 Payload Hub")
        st.info("Sign in to see the menu: Partners, Connections, AI Agent, Send & Test, Transactions, "
                "Monitoring and Users.")
    st.markdown("<div style='height:6vh'></div>", unsafe_allow_html=True)
    _, middle, _ = st.columns([1, 1.7, 1])
    with middle:
        common.hero("🔀 Payload Hub", "Kafka payload transformation · partner adapters · AI mapping agent")
        if message := st.session_state.pop("login_message", None):
            st.info(message)
        with st.form("login"):
            username = st.text_input("Username", autocomplete="username")
            password = st.text_input("Password", type="password", autocomplete="current-password")
            if st.form_submit_button("Sign in", type="primary", width="stretch"):
                try:
                    r = common.api("POST", "/api/auth/login", json={"username": username, "password": password})
                except common.ApiError as e:
                    st.error(str(e))
                else:
                    st.session_state.auth_token, st.session_state.auth_user = r["token"], r["user"]
                    st.query_params["session"] = r["token"]
                    st.rerun()
        if EMBEDDED_DEFAULT_ADMIN:
            st.caption("⚠️ Using the built-in Administrator account? Change its default password after signing in "
                       "(sidebar → My account).")
    st.stop()


EMBEDDED_DEFAULT_ADMIN = EMBED_BACKEND and not os.getenv("ADMIN_PASSWORD")
_restore_session()
if not common.token():
    st.query_params.pop("session", None)
    _login_page()
st.query_params["session"] = common.token()  # keep it in the URL after page changes, so a refresh stays signed in


def _account():
    u = common.user()
    perms = [p for p, ok in (("read", u.get("can_read")), ("write", u.get("can_write")),
                             ("delete", u.get("can_delete")), ("admin", u.get("is_admin"))) if ok]
    st.markdown(f"**👤 {u.get('username')}**")
    st.caption(" · ".join(perms) or "no permissions")
    with st.expander("My account"):
        with st.form("change_password", clear_on_submit=True):
            current = st.text_input("Current password", type="password")
            new = st.text_input("New password", type="password", help="At least 6 characters.")
            if st.form_submit_button("Change password"):
                try:
                    common.api("POST", "/api/auth/password", json={"current_password": current, "new_password": new})
                except common.ApiError as e:
                    st.error("New password must be at least 6 characters." if "string_too_short" in str(e) else str(e))
                else:
                    common.sign_out("Password changed - please sign in with the new one.")
                    st.rerun()
    if st.button("Sign out", width="stretch"):
        try:
            common.api("POST", "/api/auth/logout")
        finally:
            common.sign_out("You've signed out.")
        st.rerun()


@st.fragment(run_every=10)
def _health():
    h = common.api("GET", "/health", fatal=False)
    if h is None:
        st.caption(f"🔴 Backend unreachable at {common.API}")
        return
    items = [("PostgreSQL", h["database"]), ("Kafka", h["kafka"]), *h.get("consumers", {}).items()]
    st.caption("  \n".join(f"{'🟢' if v in ('up', 'running') else '🔴'} {k}: {v}" for k, v in items))


pages = [
    st.Page(overview.page, title="Overview", icon="📈", url_path="overview", default=True),
    st.Page(partners.page, title="Partners", icon="🤝", url_path="partners"),
    st.Page(connections.page, title="Adapters & Connections", icon="🔌", url_path="connections"),
    st.Page(ports.page, title="Ports", icon="📡", url_path="ports"),
    st.Page(workflows.page, title="Workflows", icon="⚙️", url_path="workflows"),
    st.Page(schedulers.page, title="Schedulers", icon="⏰", url_path="schedulers"),
    st.Page(agent.page, title="AI Agent", icon="🤖", url_path="agent"),
    st.Page(send.page, title="Send & Test", icon="▶️", url_path="send"),
    st.Page(transactions.page, title="Transactions", icon="🔎", url_path="transactions"),
    st.Page(data.page, title="Data", icon="🗄️", url_path="data"),
    st.Page(monitoring.page, title="Monitoring", icon="📊", url_path="monitoring"),
]
if common.can("admin"):
    pages.append(st.Page(users.page, title="Users", icon="👥", url_path="users"))
common.PAGES = {pg.url_path: pg for pg in pages}
nav = st.navigation(pages)

with st.sidebar:
    st.markdown("### 🔀 Payload Hub")
    st.caption("Integration hub · Kafka · MQ · HTTP")
    st.markdown("**Health**")
    _health()
    if not EMBED_BACKEND:
        st.caption(f"[API docs]({common.API}/docs) · [Prometheus metrics]({common.API}/metrics) · [Classic page]({common.API}/classic)")
    st.divider()
    _account()

nav.run()
