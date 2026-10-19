"""Helpers shared by the Streamlit pages."""
import json

import requests
import streamlit as st

API = "http://localhost:8000"  # set by streamlit_app.py before any page runs
STATUSES = ["COMPLETED", "FAILED", "QUEUED", "TRANSFORMING", "TRANSFORMED"]
STATUS_ICON = {"COMPLETED": "✅", "FAILED": "❌", "SAVED": "✅", "NOT_SAVED": "⚠️"}
GOOD, CRITICAL = "#0ca30c", "#d03b3b"


class ApiError(Exception):
    """The backend rejected a request; the message is safe to show to the user."""


def api(method: str, path: str, fatal: bool = True, **kwargs):
    """Call the backend. If it is unreachable, stop the page with a readable message
    (or return None when fatal=False, e.g. for the sidebar health check)."""
    try:
        r = requests.request(method, f"{API}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    except requests.ConnectionError:
        if not fatal:
            return None
        st.error(f"Can't reach the Payload Hub backend at {API}. Locally, start it with "
                 "`.venv/bin/uvicorn app.main:app --port 8000`. On Streamlit Cloud, check the app's secrets.")
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


@st.cache_data(ttl=5)
def partners() -> list[dict]:
    return api("GET", "/api/partners")


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
