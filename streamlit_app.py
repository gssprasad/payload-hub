"""Streamlit UI for Payload Hub.

The Kafka pipeline, PostgreSQL log and mapping agent run in the FastAPI backend (app/main.py);
this app is a front end that calls its REST API. Pages live in the ui/ package.

Locally, start the backend first, then:

    .venv/bin/streamlit run streamlit_app.py

On Streamlit Community Cloud there is no separate backend, so set EMBED_BACKEND = "true" in the
app's secrets: the backend (and with it the Kafka pipeline) then starts inside this process.
All settings - Kafka, DATABASE_URL, ANTHROPIC_API_KEY, APP_PASSWORD - can come from secrets.
"""
import asyncio
import hmac
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


from ui import common, monitoring, partners, send, transactions  # noqa: E402

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
else:
    common.API = os.getenv("PAYLOAD_HUB_API", "http://localhost:8000")


@st.fragment(run_every=10)
def _health():
    h = common.api("GET", "/health", fatal=False)
    if h is None:
        st.caption(f"🔴 Backend unreachable at {common.API}")
        return
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
