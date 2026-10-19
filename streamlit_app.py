"""Streamlit UI for Payload Hub.

The Kafka pipeline, PostgreSQL log and mapping agent run in the FastAPI backend (app/main.py);
this app is a front end that calls its REST API.

Locally, start the backend first, then:

    .venv/bin/streamlit run streamlit_app.py

On Streamlit Community Cloud there is no separate backend, so set EMBED_BACKEND = "true" in the
app's secrets: the backend (and with it the Kafka pipeline) then starts inside this process.
All settings - Kafka, DATABASE_URL, ANTHROPIC_API_KEY, APP_PASSWORD - can come from secrets.
"""
import hmac
import json
import os
import threading
import time as clock
from datetime import datetime, time

import pandas as pd
import requests
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


if EMBED_BACKEND:
    try:
        _start_backend()
    except RuntimeError as e:
        st.error(str(e))
        st.stop()
    API = f"http://127.0.0.1:{EMBED_PORT}"
else:
    API = os.getenv("PAYLOAD_HUB_API", "http://localhost:8000")

STATUSES = ["COMPLETED", "FAILED", "QUEUED", "TRANSFORMING", "TRANSFORMED"]
STATUS_ICON = {"COMPLETED": "✅", "FAILED": "❌", "SAVED": "✅", "NOT_SAVED": "⚠️"}
GOOD, CRITICAL = "#0ca30c", "#d03b3b"


def api(method: str, path: str, **kwargs):
    """Call the backend; show a readable error instead of a traceback."""
    try:
        r = requests.request(method, f"{API}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    except requests.ConnectionError:
        st.error(f"Can't reach the Payload Hub backend at {API}. Start it with "
                 "`.venv/bin/uvicorn app.main:app --port 8000`.")
        st.stop()
    if r.status_code >= 400 and r.status_code != 503:
        detail = r.json().get("detail", r.text) if r.headers.get("content-type", "").startswith("application/json") else r.text
        raise RuntimeError(detail if isinstance(detail, str) else json.dumps(detail))
    return r.json()


@st.cache_data
def _template(api_url: str) -> bytes:
    return requests.get(f"{api_url}/samples/mapping-template.xlsx", timeout=10).content


def status_label(s: str) -> str:
    return f"{STATUS_ICON.get(s, '⏳')} {s}"


def lang(fmt: str) -> str:
    return "xml" if fmt == "xml" else "json"


# ---------- shared state: the payload and mapping used by Transform and the agent ----------

samples = api("GET", "/api/samples")
ss = st.session_state
if "mapping" not in ss:
    first = next(iter(samples))
    ss.update(mapping=first, payload=samples[first]["input"], expr=samples[first]["expression"],
              in_fmt=samples[first]["input_format"], out_fmt=samples[first]["output_format"])
if pending := ss.pop("pending", None):  # set by the agent after it saves a mapping; applied before widgets draw
    ss.update(pending)


def on_mapping_change():
    s = samples.get(ss.mapping)
    if s:
        ss.expr, ss.in_fmt, ss.out_fmt = s["expression"], s["input_format"], s["output_format"]
        if s.get("input"):
            ss.payload = s["input"]


# ---------- sidebar: health ----------

with st.sidebar:
    st.title("🔀 Payload Hub")
    st.caption("Kafka payload transformation · PostgreSQL log · AI mapping agent")

    @st.fragment(run_every=5)
    def health():
        h = api("GET", "/health")
        st.subheader("Health")
        for name, value in [("PostgreSQL", h["database"]), ("Kafka", h["kafka"]),
                            *[(k, v) for k, v in h.get("consumers", {}).items()]]:
            ok = value in ("up", "running")
            st.markdown(f"{'🟢' if ok else '🔴'} **{name}**: {value}")

    health()
    st.divider()
    if not EMBED_BACKEND:
        st.caption(f"Backend: {API}  \n[Prometheus metrics]({API}/metrics) · [HTML UI]({API}/)")
    if os.getenv("APP_PASSWORD") and st.button("Sign out"):
        st.session_state.authed = False
        st.rerun()

tab_transform, tab_agent, tab_txns, tab_monitor = st.tabs(
    ["Transform", "Mapping agent", "Transactions", "Monitoring"])

# ---------- Transform ----------

with tab_transform:
    labels = {k: ("🤖 " if v.get("saved_by_agent") else "") + v["label"] for k, v in samples.items()}
    labels["custom"] = "Custom expression"
    c1, c2, c3 = st.columns([2, 1, 1])
    c1.selectbox("Mapping", list(labels), format_func=labels.get, key="mapping", on_change=on_mapping_change)
    c2.selectbox("Input format", ["json", "xml"], key="in_fmt")
    c3.selectbox("Output format", ["json", "xml"], key="out_fmt")

    left, right = st.columns(2)
    with left:
        st.text_area("Payload", key="payload", height=300)
        st.text_area("Mapping expression (JMESPath)", key="expr", height=200)
        run = st.button("Transform via Kafka", type="primary")
        st.caption("payload.in → transformer → payload.out → PostgreSQL")
    with right:
        st.markdown("**Output**")
        if run:
            with st.spinner("Publishing to Kafka…"):
                try:
                    ss.last_txn = api("POST", "/api/transform", json={
                        "payload": ss.payload, "input_format": ss.in_fmt, "output_format": ss.out_fmt,
                        "expression": ss.expr,
                        "mapping_name": ss.mapping if samples.get(ss.mapping, {}).get("expression") == ss.expr else "custom",
                    })
                    ss.pop("agent_preview", None)
                except RuntimeError as e:
                    st.error(str(e))
        if ss.get("agent_preview"):
            st.info("Agent preview - click **Transform via Kafka** to run it through the pipeline.")
            st.code(ss.agent_preview, language=lang(ss.out_fmt))
        elif t := ss.get("last_txn"):
            m1, m2, m3 = st.columns(3)
            m1.metric("Status", status_label(t["status"]))
            m2.metric("Transform", f"{t['transform_ms']:.1f} ms" if t["transform_ms"] is not None else "–")
            m3.metric("End-to-end", f"{t['total_ms']:.1f} ms" if t["total_ms"] is not None else "–")
            if t.get("output_payload"):
                st.code(t["output_payload"], language=lang(t["output_format"]))
            elif t.get("error"):
                st.error(t["error"])
            else:
                st.warning("Still processing - check the Transactions tab.")
            st.caption(f"Transaction {t['id']}")
            st.dataframe(pd.DataFrame(t.get("events", [])), hide_index=True, width="stretch")
        else:
            st.caption("Run a transformation to see the result.")

# ---------- Mapping agent ----------

with tab_agent:
    st.markdown("Attach a **mapping document** and/or describe the transformation. The agent inspects the "
                "payload from the Transform tab, writes the mapping, tests it and saves it.")
    doc_file = st.file_uploader("Mapping document (Excel .xlsx or CSV)", type=["xlsx", "xlsm", "csv"])
    st.download_button("Download example template", _template(API), file_name="order-to-shipment-mapping.xlsx")

    doc = None
    if doc_file is not None:
        key = (doc_file.name, doc_file.size)
        if ss.get("doc_key") != key:
            try:
                ss.doc = api("POST", "/api/mapping-doc", files={"file": (doc_file.name, doc_file.getvalue())})
                ss.doc_key = key
            except RuntimeError as e:
                ss.doc, ss.doc_key = None, None
                st.error(str(e))
        doc = ss.get("doc")
        if doc:
            st.caption(" · ".join(f"{s['name']}: {len(s['rows'])} rows" for s in doc["sheets"]))
            for sheet_tab, sheet in zip(st.tabs([s["name"] for s in doc["sheets"]]), doc["sheets"]):
                with sheet_tab:
                    rows = sheet["rows"]
                    df = pd.DataFrame(rows[1:], columns=[c or f"col{i + 1}" for i, c in enumerate(rows[0])])
                    df.index = range(2, len(rows) + 1)  # match spreadsheet row numbers
                    st.dataframe(df, width="stretch", height=min(400, 38 + 35 * len(df)))

    instructions = st.text_area("Instructions (optional with a mapping document)", height=100,
                                placeholder="e.g. Output XML. Use carrier BLUEDART. Skip prices.")
    with st.expander("Sample payload the agent will use (edit it on the Transform tab)"):
        st.code(ss.payload, language=lang(ss.in_fmt))

    if st.button("Ask agent", type="primary"):
        if not instructions.strip() and not doc:
            st.warning("Attach a mapping document or describe the transformation.")
        else:
            with st.spinner("Agent is inspecting the payload, writing and testing a mapping… (usually 15–60 s)"):
                try:
                    r = api("POST", "/api/agent", timeout=600, json={
                        "request": instructions, "payload": ss.payload, "input_format": ss.in_fmt,
                        "mapping_doc": {"filename": doc["filename"], "text": doc["text"]} if doc else None,
                    })
                except RuntimeError as e:
                    st.error(str(e))
                    r = None
            if r:
                ss.agent_result = r
                if r.get("mapping"):
                    m = r["mapping"]
                    ss.pending = {"mapping": m["name"], "expr": m["expression"],
                                  "in_fmt": m["input_format"], "out_fmt": m["output_format"]}
                    ss.agent_preview = m["preview"]
                st.rerun()

    if r := ss.get("agent_result"):
        st.markdown(f"**{status_label(r['status'])}** · {len(r['steps'])} tool calls · "
                    f"{r['duration_ms'] / 1000:.1f} s · {r['usage']['input_tokens']:,} in / "
                    f"{r['usage']['output_tokens']:,} out tokens")
        if r.get("explanation"):
            st.info(r["explanation"])
        if r.get("mapping"):
            st.success(f"Saved as **{r['mapping']['label']}** - selected on the Transform tab.")
            st.code(r["mapping"]["expression"], language="text")
        for s in r["steps"]:
            detail = {"test_mapping": "output OK" if s["ok"] else s.get("result"),
                      "save_mapping": f"saved “{s.get('name')}”" if s["ok"] else s.get("result"),
                      "inspect_input": f"parsed as {s.get('input_format')}"}.get(s["tool"], "")
            st.markdown(f"{'✅' if s['ok'] else '❌'} `{s['tool']}` {detail}")

# ---------- Transactions (search) ----------

with tab_txns:
    with st.form("search"):
        c1, c2 = st.columns([3, 1])
        q = c1.text_input("Search", placeholder="Transaction ID, mapping, or any value in input / output / error "
                                                "(e.g. 1042, INV-1042, Asha)")
        status = c2.selectbox("Status", ["All"] + STATUSES)
        c3, c4, c5, c6 = st.columns(4)
        since_d = c3.date_input("From date", value=None)
        since_t = c4.time_input("From time", value=time(0, 0))
        until_d = c5.date_input("To date", value=None)
        until_t = c6.time_input("To time", value=time(23, 59))
        st.form_submit_button("Search", type="primary")

    params = {"limit": 200}
    if q.strip():
        params["q"] = q.strip()
    if status != "All":
        params["status"] = status
    if since_d:
        params["since"] = datetime.combine(since_d, since_t).astimezone().isoformat()
    if until_d:
        params["until"] = datetime.combine(until_d, until_t).astimezone().isoformat()
    txns = api("GET", "/api/transactions", params=params)
    total = txns[0]["matches"] if txns else 0
    st.caption(f"{total} transaction{'s' if total != 1 else ''}" + (f" · showing latest {len(txns)}" if total > len(txns) else ""))

    if txns:
        df = pd.DataFrame(txns)
        df["created_at"] = pd.to_datetime(df["created_at"]).dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S")
        df["status"] = df["status"].map(status_label)
        df["formats"] = df["input_format"] + " → " + df["output_format"]
        view = df[["created_at", "id", "mapping_name", "formats", "status", "transform_ms", "total_ms", "error"]]
        event = st.dataframe(
            view, hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row",
            column_config={
                "created_at": "Time", "id": "Transaction", "mapping_name": "Mapping", "formats": "Formats",
                "status": "Status", "error": "Error",
                "transform_ms": st.column_config.NumberColumn("Transform (ms)", format="%.1f"),
                "total_ms": st.column_config.NumberColumn("End-to-end (ms)", format="%.1f"),
            })
        rows = event.selection.rows
        if rows:
            t = api("GET", f"/api/transactions/{txns[rows[0]]['id']}")
            st.subheader(f"Transaction {t['id']}")
            st.markdown(f"{status_label(t['status'])} · {t['mapping_name']} · {t['input_format']} → {t['output_format']}")
            st.dataframe(pd.DataFrame(t["events"]), hide_index=True, width="stretch")
            if t.get("error"):
                st.error(t["error"])
            a, b = st.columns(2)
            a.markdown("**Input**")
            a.code(t["input_payload"], language=lang(t["input_format"]))
            b.markdown("**Output**")
            b.code(t.get("output_payload") or "–", language=lang(t["output_format"]))
            st.markdown("**Mapping expression**")
            st.code(t["expression"], language="text")
        else:
            st.caption("Select a row to see its full input, output and event timeline.")

# ---------- Monitoring ----------

with tab_monitor:
    @st.fragment(run_every=3)
    def monitoring():
        s = api("GET", "/api/stats")
        done = s["completed"] + s["failed"]
        row1, row2 = st.columns(3), st.columns(3)
        row1[0].metric("Total", s["total"])
        row1[1].metric("Completed", s["completed"])
        row1[2].metric("Failed", s["failed"])
        row2[0].metric("Success rate", f"{s['completed'] / done * 100:.1f}%" if done else "–")
        row2[1].metric("Avg end-to-end", f"{s['avg_ms']} ms" if s["avg_ms"] is not None else "–")
        row2[2].metric("p95 end-to-end", f"{s['p95_ms']} ms" if s["p95_ms"] is not None else "–")

        st.markdown("**Transactions per minute** · last 30 minutes")
        pm = pd.DataFrame(s["per_minute"]).rename(columns={"completed": "Completed", "failed": "Failed"})
        st.bar_chart(pm, x="minute", y=["Completed", "Failed"], color=[GOOD, CRITICAL],
                     x_label="Minute", y_label="Transactions", height=260)

        st.markdown("**Recent agent runs**")
        runs = api("GET", "/api/agent/runs", params={"limit": 10})
        if runs:
            rdf = pd.DataFrame(runs)
            rdf["created_at"] = pd.to_datetime(rdf["created_at"]).dt.tz_convert(None).dt.strftime("%H:%M:%S")
            rdf["status"] = rdf["status"].map(status_label)
            st.dataframe(rdf[["created_at", "request", "mapping_doc_name", "status", "mapping_name", "tool_calls",
                              "input_tokens", "output_tokens", "duration_ms"]],
                         hide_index=True, width="stretch",
                         column_config={"created_at": "Time", "request": "Request", "mapping_doc_name": "Document",
                                        "status": "Status", "mapping_name": "Mapping", "tool_calls": "Tool calls",
                                        "input_tokens": "Tokens in", "output_tokens": "Tokens out",
                                        "duration_ms": "Duration (ms)"})
        else:
            st.caption("No agent runs yet.")
        st.caption(f"Refreshes every 3 s · updated {datetime.now():%H:%M:%S}")

    monitoring()
