"""Streamlit UI for Payload Hub.

The Kafka pipeline, PostgreSQL log and mapping agent run in the FastAPI backend (app/main.py);
this app is a front end that calls its REST API. Pages live in the ui/ package.

Locally, start the backend first, then:

    .venv/bin/streamlit run streamlit_app.py

On Streamlit Community Cloud there is no separate backend, so set EMBED_BACKEND = "true" in the
app's secrets: the backend (and with it the Kafka pipeline) then starts inside this process.
All settings - Kafka, DATABASE_URL, ANTHROPIC_API_KEY, APP_PASSWORD - can come from secrets.
"""
import hmac
import os
import threading
import time as clock

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


def _require_password():
    password = os.getenv("APP_PASSWORD")
    if not password or st.session_state.get("authed"):
        return
    st.title("🔀 Payload Hub")
    with st.form("login"):
        entered = st.text_input("Password", type="password")
        if st.form_submit_button("Sign in", type="primary"):
            if hmac.compare_digest(entered.encode(), password.encode()):
                st.session_state.authed = True
                st.rerun()
            st.error("Wrong password.")
    st.stop()


_require_password()

EMBED_BACKEND = os.getenv("EMBED_BACKEND", "").lower() in ("1", "true", "yes")
EMBED_PORT = int(os.getenv("EMBED_PORT", "8765"))


@st.cache_resource(show_spinner="Starting the pipeline (Kafka, database)…")
def _start_backend():
    """Run the FastAPI backend in a background thread of this process (Streamlit Cloud mode)."""
    import uvicorn

    from app.main import app as backend  # imported here so it sees the settings loaded from secrets

    server = uvicorn.Server(uvicorn.Config(backend, host="127.0.0.1", port=EMBED_PORT, log_level="info"))
    thread = threading.Thread(target=server.run, name="payload-hub-backend", daemon=True)
    thread.start()
    deadline = clock.monotonic() + 90
    while not server.started:
        if not thread.is_alive() or clock.monotonic() > deadline:
            raise RuntimeError("The backend failed to start - check the Kafka and DATABASE_URL settings "
                               "in the app's secrets, and the app logs.")
        clock.sleep(0.25)
    return server


from ui import common, monitoring, partners, send, transactions  # noqa: E402

if EMBED_BACKEND:
    try:
        _start_backend()
    except RuntimeError as e:
        st.error(str(e))
        st.stop()
    common.API = f"http://127.0.0.1:{EMBED_PORT}"
else:
    common.API = os.getenv("PAYLOAD_HUB_API", "http://localhost:8000")


@st.fragment(run_every=10)
def _health():
    h = common.api("GET", "/health")
    items = [("PostgreSQL", h["database"]), ("Kafka", h["kafka"]), *h.get("consumers", {}).items()]
    st.caption("  \n".join(f"{'🟢' if v in ('up', 'running') else '🔴'} {k}: {v}" for k, v in items))


nav = st.navigation([
    st.Page(partners.page, title="Partners", icon="🤝", url_path="partners", default=True),
    st.Page(send.page, title="Send & Test", icon="▶️", url_path="send"),
    st.Page(transactions.page, title="Transactions", icon="🔎", url_path="transactions"),
    st.Page(monitoring.page, title="Monitoring", icon="📊", url_path="monitoring"),
])

with st.sidebar:
    st.caption("Kafka payload transformation · PostgreSQL log · AI mapping agent")
    st.markdown("**Health**")
    _health()
    if not EMBED_BACKEND:
        st.caption(f"[Prometheus metrics]({common.API}/metrics) · [HTML UI]({common.API}/)")
    if os.getenv("APP_PASSWORD") and st.button("Sign out"):
        st.session_state.authed = False
        st.rerun()

nav.run()
