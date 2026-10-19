"""FastAPI app: REST API, monitoring endpoints and the web UI."""
import logging
import os
import re
import secrets
import uuid
from collections import deque
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from . import adapters, agent, auth, connectors, db, mapping_doc, pipeline
from .transform import SAMPLES, TransformError, parse, transform

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_app):
    await db.init()
    await auth.ensure_admin()
    await pipeline.start()
    await connectors.start()
    yield
    await connectors.stop()
    await pipeline.stop()
    await db.close()


app = FastAPI(title="Payload Hub", lifespan=lifespan)


@app.middleware("http")
async def require_login(request: Request, call_next):
    """Every /api call needs a signed-in user with the right permission (read / write / delete / admin).
    Partner endpoints (/inbound), the test receiver, /health and /metrics are outside /api."""
    path = request.url.path
    if not path.startswith("/api/") or path == "/api/auth/login":
        return await call_next(request)
    header = request.headers.get("authorization", "")
    token = header[7:] if header.lower().startswith("bearer ") else None
    user = await auth.user_for_token(token)
    if user is None:
        return JSONResponse({"detail": "Sign in required."}, status_code=401)
    permission = auth.required_permission(request.method, path)
    if not auth.allowed(user, permission):
        return JSONResponse({"detail": f"Your account doesn't have {permission} permission."}, status_code=403)
    request.state.user = user
    request.state.token = token
    return await call_next(request)


class LoginIn(BaseModel):
    username: str
    password: str


@app.post("/api/auth/login")
async def login(req: LoginIn):
    result = await auth.login(req.username.strip(), req.password)
    if result is None:
        raise HTTPException(401, "Wrong username or password, or the account is disabled.")
    token, user = result
    return {"token": token, "user": user}


@app.post("/api/auth/logout", status_code=204)
async def logout(request: Request):
    await db.delete_session(auth.token_hash(request.state.token))


@app.get("/api/auth/me")
async def me(request: Request):
    return request.state.user


class PasswordChange(BaseModel):
    current_password: str
    new_password: str = Field(min_length=6, max_length=200)


@app.post("/api/auth/password", status_code=204)
async def change_password(req: PasswordChange, request: Request):
    username = request.state.user["username"]
    stored = await db.get_user(username, with_hash=True)
    if not auth.verify_password(req.current_password, stored["password_hash"]):
        raise HTTPException(400, "Current password is wrong.")
    await db.update_user(username, password_hash=auth.hash_password(req.new_password))


# ---------- users (admins only) ----------

class UserIn(BaseModel):
    username: str = Field(min_length=2, max_length=60, pattern=r"^[A-Za-z0-9._@-]+$")
    password: str = Field(min_length=6, max_length=200)
    can_read: bool = True
    can_write: bool = False
    can_delete: bool = False
    is_admin: bool = False


class UserUpdate(BaseModel):
    password: str | None = Field(default=None, min_length=6, max_length=200)
    can_read: bool | None = None
    can_write: bool | None = None
    can_delete: bool | None = None
    is_admin: bool | None = None
    active: bool | None = None


@app.get("/api/users")
async def users():
    return await db.list_users()


@app.post("/api/users", status_code=201)
async def add_user(req: UserIn):
    if not await db.create_user(req.username, auth.hash_password(req.password), req.can_read, req.can_write,
                                req.can_delete, req.is_admin):
        raise HTTPException(409, f"User '{req.username}' already exists.")
    return await db.get_user(req.username)


@app.put("/api/users/{username}")
async def update_user(username: str, req: UserUpdate, request: Request):
    target = await db.get_user(username)
    if not target:
        raise HTTPException(404, "User not found.")
    fields = req.model_dump(exclude_unset=True, exclude={"password"})
    removing_admin = (fields.get("is_admin") is False or fields.get("active") is False) and target["is_admin"]
    if removing_admin and await db.count_active_admins(excluding=target["username"]) == 0:
        raise HTTPException(409, "There must be at least one active administrator.")
    if target["username"].lower() == request.state.user["username"].lower() and fields.get("active") is False:
        raise HTTPException(409, "You can't disable your own account.")
    if req.password:
        fields["password_hash"] = auth.hash_password(req.password)
    await db.update_user(target["username"], **fields)
    return await db.get_user(target["username"])


@app.delete("/api/users/{username}", status_code=204)
async def delete_user(username: str, request: Request):
    target = await db.get_user(username)
    if not target:
        raise HTTPException(404, "User not found.")
    if target["username"].lower() == request.state.user["username"].lower():
        raise HTTPException(409, "You can't delete your own account.")
    if target["is_admin"] and await db.count_active_admins(excluding=target["username"]) == 0:
        raise HTTPException(409, "There must be at least one active administrator.")
    await db.delete_user(target["username"])


class TransformRequest(BaseModel):
    """Either partner_code (uses the partner's mapping and queue) or an ad-hoc expression + formats."""
    payload: str = Field(min_length=1)
    partner_code: str | None = None
    input_format: Literal["json", "xml"] | None = None
    output_format: Literal["json", "xml"] | None = None
    expression: str | None = None
    mapping_name: str = "custom"


APP_URL = os.getenv("PAYLOAD_HUB_APP_URL", "http://localhost:8501")

LANDING = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Payload Hub</title>
<style>
:root {{ --bg:#f6f7f9; --panel:#fff; --border:#e2e5ea; --text:#1d2330; --muted:#667085; --accent:#ff4b4b; --link:#3b5bdb; }}
@media (prefers-color-scheme: dark) {{ :root {{ --bg:#12151b; --panel:#1a1e26; --border:#2b313d; --text:#e6e9ef;
  --muted:#98a2b3; --link:#7b93ff; }} }}
body {{ margin:0; background:var(--bg); color:var(--text); font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }}
main {{ max-width:640px; margin:10vh auto; padding:0 16px; }}
.card {{ background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:28px; }}
h1 {{ margin:0 0 4px; font-size:26px; }} .muted {{ color:var(--muted); }}
.btn {{ display:inline-block; margin:18px 0 8px; background:var(--accent); color:#fff; text-decoration:none;
  font-weight:600; padding:11px 20px; border-radius:8px; }}
ul {{ padding-left:18px; }} a {{ color:var(--link); }} code {{ font-size:13px; }}
.ok {{ color:#1f8a4c; font-weight:600; }} .bad {{ color:#c2362f; font-weight:600; }}
</style></head><body><main><div class="card">
<h1>🔀 Payload Hub</h1>
<div class="muted">Backend and API server · status: <span class="{status_class}">{status}</span></div>
<a class="btn" href="{app_url}">Open Payload Hub →</a>
<div class="muted">The full application (partners, connections, AI agent, transactions, monitoring, users) runs at
<a href="{app_url}">{app_url}</a>. This port serves the API behind it.</div>
<h3>On this server</h3>
<ul>
<li><a href="/docs">API documentation</a> (try the API in the browser)</li>
<li><a href="/health">Health check</a> · <a href="/metrics">Prometheus metrics</a></li>
<li>Partner endpoints: <code>POST /inbound/&lt;partner-code&gt;</code> with header <code>X-API-Key</code></li>
<li><a href="/classic">Classic quick-test page</a> (the original single-page tester; sign-in required)</li>
</ul></div></main></body></html>"""


@app.get("/", response_class=Response)
async def index():
    """Start page for the backend port: points people to the full app and to the API tools."""
    healthy = True
    try:
        await db.ping()
        await pipeline.producer.client.fetch_all_metadata()
    except Exception:  # noqa: BLE001
        healthy = False
    html = LANDING.format(app_url=APP_URL, status="running" if healthy else "degraded - see /health",
                          status_class="ok" if healthy else "bad")
    return Response(html, media_type="text/html")


@app.get("/classic")
async def classic():
    """The original single-page UI; it needs a signed-in session (it shows a sign-in box)."""
    return FileResponse(STATIC / "index.html")


@app.get("/samples/mapping-template.xlsx")
async def mapping_template():
    """Example Excel mapping document (Order JSON -> Shipment XML)."""
    return FileResponse(
        Path(__file__).parent.parent / "samples" / "order-to-shipment-mapping.xlsx",
        filename="order-to-shipment-mapping.xlsx",
    )


@app.get("/api/samples")
async def samples():
    """Built-in sample mappings plus every mapping the agent has saved."""
    saved = {
        m["name"]: {**m, "input": None, "saved_by_agent": True} for m in await db.list_mappings()
    }
    return {**SAMPLES, **saved}


class MappingDoc(BaseModel):
    filename: str
    text: str = Field(min_length=1, max_length=mapping_doc.MAX_TEXT)


class AgentRequest(BaseModel):
    request: str = Field(default="", max_length=4000)
    payload: str = Field(min_length=1)
    input_format: Literal["json", "xml"] | None = None
    mapping_doc: MappingDoc | None = None


@app.post("/api/mapping-doc")
async def upload_mapping_doc(file: UploadFile = File(...)):
    """Read an Excel/CSV mapping specification and return its sheets for preview and for the agent."""
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(413, "File is larger than 10 MB.")
    try:
        return mapping_doc.read(file.filename or "mapping.xlsx", content)
    except mapping_doc.MappingDocError as e:
        raise HTTPException(400, str(e)) from e


@app.post("/api/agent")
async def run_agent(req: AgentRequest):
    """Ask the mapping agent to build, test and save a mapping from a request and/or mapping document."""
    if len(req.request.strip()) < 3 and not req.mapping_doc:
        raise HTTPException(422, "Describe the transformation or attach a mapping document.")
    doc = req.mapping_doc.model_dump() if req.mapping_doc else None
    return await agent.run(req.request.strip(), req.payload, req.input_format, doc)


@app.get("/api/agent/runs")
async def agent_runs(limit: int = 20):
    return await db.list_agent_runs(min(limit, 200))


@app.get("/api/agent/runs/{run_id}")
async def agent_run(run_id: int):
    run = await db.get_agent_run(run_id)
    if not run:
        raise HTTPException(404, "Agent run not found.")
    return run


@app.get("/api/agent/status")
async def agent_status():
    """Whether the agent can call Claude. Never returns the key itself."""
    configured = bool(os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_AUTH_TOKEN"))
    return {"configured": configured, "model": agent.MODEL}


@app.post("/api/transform")
async def submit_transform(req: TransformRequest):
    if req.partner_code:
        p = await db.get_partner(req.partner_code)
        if not p:
            raise HTTPException(404, f"Partner '{req.partner_code}' not found.")
        try:
            txn_id, result = await connectors.ingest(p, req.payload, "UI")
        except connectors.IngestError as e:
            raise HTTPException(e.status, str(e)) from e
        return result or await db.get_transaction(txn_id)
    else:
        if not (req.input_format and req.output_format and req.expression):
            raise HTTPException(422, "Give a partner_code, or input_format, output_format and expression.")
        msg = {"mapping_name": req.mapping_name, "input_format": req.input_format, "output_format": req.output_format,
               "expression": req.expression, "topic_in": pipeline.TOPIC_IN, "topic_out": pipeline.TOPIC_OUT,
               "topic_dlq": pipeline.TOPIC_DLQ, "partner_code": None}
    txn_id = str(uuid.uuid4())
    await db.create_transaction(txn_id, msg["mapping_name"], msg["input_format"], msg["output_format"],
                                msg["expression"], req.payload, msg["partner_code"], msg["topic_in"], "UI")
    result = await pipeline.submit(txn_id, {"id": txn_id, "payload": req.payload, **msg})
    return result or await db.get_transaction(txn_id)


# ---------- partners ----------

class PartnerIn(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    code: str | None = Field(default=None, max_length=40)
    description: str | None = None
    contact: str | None = None
    input_format: Literal["json", "xml"] = "json"
    output_format: Literal["json", "xml"] = "json"
    sample_input: str | None = None
    expected_output: str | None = None
    expression: str | None = None
    queue_mode: Literal["shared", "dedicated"] = "shared"
    inbound_connection: str | None = None
    inbound_source: str | None = None
    outbound_connection: str | None = None
    outbound_target: str | None = None


class PartnerUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=2, max_length=120)
    description: str | None = None
    contact: str | None = None
    input_format: Literal["json", "xml"] | None = None
    output_format: Literal["json", "xml"] | None = None
    sample_input: str | None = None
    expected_output: str | None = None
    expression: str | None = None
    active: bool | None = None


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:40]


async def _topics_for(code: str, mode: str) -> dict:
    if mode == "dedicated":
        topics = pipeline.partner_topics(code)
        await pipeline.ensure_topics(topics)
    else:
        topics = pipeline.SHARED_TOPICS
    return dict(zip(("topic_in", "topic_out", "topic_dlq"), topics))


async def _partner_or_404(code: str):
    p = await db.get_partner(code)
    if not p:
        raise HTTPException(404, f"Partner '{code}' not found.")
    return p


@app.get("/api/partners")
async def partners():
    return await db.list_partners()


@app.post("/api/partners", status_code=201)
async def onboard_partner(req: PartnerIn):
    """Onboard a partner onto the shared queues or onto its own dedicated queues."""
    code = _slug(req.code or req.name)
    if len(code) < 2:
        raise HTTPException(422, "Partner code must have at least 2 letters or digits.")
    topics = await _topics_for(code, req.queue_mode)
    fields = req.model_dump(exclude={"code", "inbound_connection", "inbound_source", "outbound_connection",
                                     "outbound_target"})
    fields["mapping_source"] = "manual" if req.expression else None
    fields["api_key"] = _new_api_key()
    await _check_adapters(req.inbound_connection, req.inbound_source, req.outbound_connection)
    fields.update(inbound_connection=req.inbound_connection or None, inbound_source=req.inbound_source or None,
                  outbound_connection=req.outbound_connection or None, outbound_target=req.outbound_target or None)
    if not await db.create_partner(code, **fields, **topics):
        raise HTTPException(409, f"A partner with code '{code}' already exists.")
    await connectors.sync()
    return await db.get_partner(code)


@app.get("/api/partners/{code}")
async def partner(code: str):
    p = await _partner_or_404(code)
    p["documents"] = await db.list_partner_documents(code)
    return p


@app.put("/api/partners/{code}")
async def update_partner(code: str, req: PartnerUpdate):
    await _partner_or_404(code)
    fields = req.model_dump(exclude_unset=True)
    if "expression" in fields:
        fields["mapping_source"] = "manual"
    await db.update_partner(code, **fields)
    await connectors.sync()  # 'active' may have changed
    return await db.get_partner(code)


@app.post("/api/partners/{code}/queue")
async def change_partner_queue(code: str, queue_mode: Literal["shared", "dedicated"] = Form(...)):
    """Move a partner between the shared queues and its own dedicated queues. Messages already
    queued finish on the old queue; new messages use the new one."""
    await _partner_or_404(code)
    topics = await _topics_for(code, queue_mode)
    await db.update_partner(code, queue_mode=queue_mode, **topics)
    return await db.get_partner(code)


@app.delete("/api/partners/{code}", status_code=204)
async def delete_partner(code: str):
    """Remove a partner and its documents. Its transactions stay in the log; dedicated Kafka
    topics are left in place (delete them in Kafka if no longer needed)."""
    await _partner_or_404(code)
    await db.delete_partner(code)
    await connectors.sync()  # stop its inbound listener


@app.post("/api/partners/{code}/test")
async def test_partner_mapping(code: str, payload: str | None = Form(None)):
    """Run the partner's mapping on a payload (default: its sample input) without Kafka, and compare
    with the expected output if one is stored."""
    p = await _partner_or_404(code)
    payload = payload or p["sample_input"]
    if not payload:
        raise HTTPException(422, "No payload given and the partner has no sample input.")
    if not p["expression"]:
        raise HTTPException(409, "The partner has no mapping yet.")
    try:
        output = transform(payload, p["input_format"], p["expression"], p["output_format"])
    except TransformError as e:
        return {"ok": False, "error": str(e)}
    matches = None
    if p["expected_output"]:
        try:
            matches = parse(output, p["output_format"]) == parse(p["expected_output"], p["output_format"])
        except TransformError:
            matches = False
    return {"ok": True, "output": output, "matches_expected": matches}


@app.post("/api/partners/{code}/documents", status_code=201)
async def upload_partner_document(code: str, file: UploadFile = File(...)):
    await _partner_or_404(code)
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(413, "File is larger than 10 MB.")
    try:
        doc = mapping_doc.read(file.filename or "mapping.xlsx", content)
    except mapping_doc.MappingDocError as e:
        raise HTTPException(400, str(e)) from e
    return await db.add_partner_document(code, doc["filename"], content, doc["text"], doc["sheets"])


@app.get("/api/partners/{code}/documents/{doc_id}")
async def partner_document(code: str, doc_id: int):
    d = await db.get_partner_document(code, doc_id)
    if not d:
        raise HTTPException(404, "Document not found.")
    return {k: d[k] for k in ("id", "filename", "uploaded_at", "sheets", "text")}


@app.get("/api/partners/{code}/documents/{doc_id}/download")
async def download_partner_document(code: str, doc_id: int):
    d = await db.get_partner_document(code, doc_id)
    if not d:
        raise HTTPException(404, "Document not found.")
    media = "text/csv" if d["filename"].lower().endswith(".csv") else \
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return Response(bytes(d["content"]), media_type=media,
                    headers={"Content-Disposition": f'attachment; filename="{d["filename"]}"'})


@app.delete("/api/partners/{code}/documents/{doc_id}", status_code=204)
async def delete_partner_document(code: str, doc_id: int):
    await db.delete_partner_document(code, doc_id)


class PartnerAgentRequest(BaseModel):
    instructions: str = Field(default="", max_length=4000)
    document_ids: list[int] | None = None  # default: all of the partner's documents


@app.post("/api/partners/{code}/agent")
async def partner_agent(code: str, req: PartnerAgentRequest):
    """Have the agent build the partner's mapping from its documents and sample input, and save it."""
    p = await _partner_or_404(code)
    if not p["sample_input"]:
        raise HTTPException(422, "Add a sample input to the partner first - the agent tests against it.")
    docs = await db.list_partner_documents(code, with_text=True)
    if req.document_ids is not None:
        docs = [d for d in docs if d["id"] in req.document_ids]
    if not docs and len(req.instructions.strip()) < 3:
        raise HTTPException(422, "Upload a mapping document or write instructions.")
    combined = None
    if docs:
        combined = {"filename": ", ".join(d["filename"] for d in docs),
                    "text": "\n\n".join(f'<document filename="{d["filename"]}">\n{d["text"]}\n</document>' for d in docs)}
    instructions = req.instructions.strip()
    context = (f"This mapping is for partner '{p['name']}'. Input format: {p['input_format']}. "
               f"Required output format: {p['output_format']}.")
    if p["expected_output"]:
        context += f"\nThe partner expects output like this example:\n{p['expected_output']}"
    return await agent.run(f"{context}\n\n{instructions}".strip(), p["sample_input"], p["input_format"],
                           combined, partner_code=code)


def _new_api_key() -> str:
    return "ph_" + secrets.token_urlsafe(24)


async def _check_adapters(inbound_connection, inbound_source, outbound_connection):
    if inbound_connection:
        c = await db.get_connection(inbound_connection)
        if not c:
            raise HTTPException(422, f"Connection '{inbound_connection}' not found.")
        if not adapters.TYPES[c["type"]]["inbound"]:
            raise HTTPException(422, f"{adapters.TYPES[c['type']]['label']} connections can't receive inbound messages.")
        if not (inbound_source or "").strip():
            raise HTTPException(422, "Give the queue or topic to listen on.")
    if outbound_connection and not await db.get_connection(outbound_connection):
        raise HTTPException(422, f"Connection '{outbound_connection}' not found.")


class PartnerAdapters(BaseModel):
    inbound_connection: str | None = None
    inbound_source: str | None = None
    outbound_connection: str | None = None
    outbound_target: str | None = None


@app.put("/api/partners/{code}/adapters")
async def set_partner_adapters(code: str, req: PartnerAdapters):
    """Choose where a partner's messages come from (besides its HTTP endpoint) and where results go."""
    await _partner_or_404(code)
    await _check_adapters(req.inbound_connection, req.inbound_source, req.outbound_connection)
    await db.update_partner(code, inbound_connection=req.inbound_connection or None,
                            inbound_source=(req.inbound_source or "").strip() or None,
                            outbound_connection=req.outbound_connection or None,
                            outbound_target=(req.outbound_target or "").strip() or None)
    await connectors.sync()
    return await db.get_partner(code)


@app.post("/api/partners/{code}/api-key")
async def rotate_partner_api_key(code: str):
    await _partner_or_404(code)
    await db.update_partner(code, api_key=_new_api_key())
    return {"api_key": (await db.get_partner(code))["api_key"]}


# ---------- connections (adapters) ----------

class ConnectionIn(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    type: Literal["rabbitmq", "activemq", "ibmmq", "http", "kafka"]
    config: dict = Field(default_factory=dict)
    secret: str | None = None  # write-only; None on update keeps the stored one


def _clean_config(type_: str, config: dict) -> dict:
    out = {}
    for f in adapters.TYPES[type_]["fields"]:
        if f["kind"] == "secret":
            continue
        value = config.get(f["key"], f["default"])
        if f["kind"] == "int":
            try:
                value = int(value)
            except (TypeError, ValueError) as e:
                raise HTTPException(422, f"{f['label']} must be a number.") from e
        elif f["kind"] == "bool":
            value = bool(value)
        out[f["key"]] = value
    return out


@app.get("/api/connection-types")
async def connection_types():
    return {k: {**v, "available": adapters.ibmmq_available() if k == "ibmmq" else True}
            for k, v in adapters.TYPES.items()}


@app.get("/api/connections")
async def list_connections():
    return await db.list_connections()


@app.post("/api/connections", status_code=201)
async def create_connection(req: ConnectionIn):
    conn_id = _slug(req.name)
    if len(conn_id) < 2:
        raise HTTPException(422, "Give the connection a name with letters or digits.")
    if not await db.create_connection(conn_id, req.name, req.type, _clean_config(req.type, req.config), req.secret):
        raise HTTPException(409, f"A connection called '{conn_id}' already exists.")
    ok, detail = await connectors.test_connection(conn_id)
    return {**(await db.get_connection(conn_id)), "test_ok": ok, "test_detail": detail}


@app.put("/api/connections/{conn_id}")
async def update_connection(conn_id: str, req: ConnectionIn):
    existing = await db.get_connection(conn_id)
    if not existing:
        raise HTTPException(404, "Connection not found.")
    if req.type != existing["type"]:
        raise HTTPException(422, "A connection's type can't change - create a new connection instead.")
    await db.update_connection(conn_id, req.name, _clean_config(req.type, req.config), req.secret)
    ok, detail = await connectors.test_connection(conn_id)
    await connectors.sync()
    return {**(await db.get_connection(conn_id)), "test_ok": ok, "test_detail": detail}


@app.post("/api/connections/{conn_id}/test")
async def test_connection(conn_id: str):
    if not await db.get_connection(conn_id):
        raise HTTPException(404, "Connection not found.")
    ok, detail = await connectors.test_connection(conn_id)
    return {"ok": ok, "detail": detail}


class SendTest(BaseModel):
    target: str = ""
    body: str = '{"hello": "from Payload Hub"}'
    content_type: str = "application/json"


@app.post("/api/connections/{conn_id}/send")
async def send_test_message(conn_id: str, req: SendTest):
    """Send one message through a connection, e.g. to put a test message on a partner's inbound queue."""
    if not await db.get_connection(conn_id):
        raise HTTPException(404, "Connection not found.")
    try:
        adapter = await connectors.adapter_for(conn_id)
        detail = await adapter.send(req.target, req.body.encode(), {"X-Sent-By": "payload-hub"}, req.content_type)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "detail": str(e) or type(e).__name__}
    return {"ok": True, "detail": detail}


@app.delete("/api/connections/{conn_id}", status_code=204)
async def delete_connection(conn_id: str):
    conns = {c["id"]: c for c in await db.list_connections()}
    if conn_id not in conns:
        raise HTTPException(404, "Connection not found.")
    if conns[conn_id]["used_by"]:
        raise HTTPException(409, "Still used by partner(s): " + ", ".join(conns[conn_id]["used_by"]))
    await db.delete_connection(conn_id)


@app.get("/api/adapters/status")
async def adapters_status():
    return connectors.listener_status()


# ---------- partner HTTP endpoints ----------

async def _authorised_partner(code: str, api_key: str | None):
    p = await db.get_partner(code)
    if not p or not api_key or not secrets.compare_digest(api_key, p["api_key"] or ""):
        raise HTTPException(401, "Unknown partner or wrong API key.", headers={"WWW-Authenticate": "X-API-Key"})
    return p


@app.post("/inbound/{code}", tags=["partner endpoints"])
async def inbound(code: str, request: Request, wait: bool = True,
                  x_api_key: str | None = Header(default=None)):
    """Partner HTTP endpoint. Send the raw payload (JSON or XML) as the body with header X-API-Key.
    wait=true (default) returns the transformed output; wait=false returns 202 with a transaction id."""
    p = await _authorised_partner(code, x_api_key or request.query_params.get("api_key"))
    body = (await request.body()).decode("utf-8", errors="replace")
    if not body.strip():
        raise HTTPException(422, "Empty body - send the payload as the request body.")
    try:
        txn_id, result = await connectors.ingest(p, body, "HTTP", wait=15 if wait else None)
    except connectors.IngestError as e:
        raise HTTPException(e.status, str(e)) from e
    headers = {"X-Transaction-Id": txn_id}
    if result and result["status"] == "COMPLETED":
        media = "application/xml" if p["output_format"] == "xml" else "application/json"
        return Response(result["output_payload"], media_type=media, headers={**headers, "X-Status": "COMPLETED"})
    if result and result["status"] == "FAILED":
        return JSONResponse({"transaction_id": txn_id, "status": "FAILED", "error": result["error"]},
                            status_code=422, headers=headers)
    return JSONResponse({"transaction_id": txn_id, "status": "ACCEPTED",
                         "status_url": f"/inbound/{code}/transactions/{txn_id}"}, status_code=202, headers=headers)


@app.get("/inbound/{code}/transactions/{txn_id}", tags=["partner endpoints"])
async def inbound_status(code: str, txn_id: uuid.UUID, request: Request,
                         x_api_key: str | None = Header(default=None)):
    await _authorised_partner(code, x_api_key or request.query_params.get("api_key"))
    t = await db.get_transaction(txn_id)
    if not t or t["partner_code"] != code:
        raise HTTPException(404, "Transaction not found.")
    return {k: t[k] for k in ("id", "status", "delivery_status", "output_payload", "error", "created_at")}


# ---------- built-in test receiver (a webhook target for demos) ----------

_received: deque = deque(maxlen=100)


@app.api_route("/test-receiver/{name:path}", methods=["POST", "PUT"], tags=["test receiver"])
async def test_receiver(name: str, request: Request):
    body = (await request.body()).decode("utf-8", errors="replace")
    _received.appendleft({"at": _now_iso(), "name": name, "method": request.method,
                          "headers": {k: v for k, v in request.headers.items()
                                      if k.lower().startswith(("x-", "content-type"))},
                          "body": body})
    return {"received": True}


@app.get("/api/test-receiver")
async def test_receiver_messages(limit: int = 50):
    return list(_received)[:limit]


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


@app.get("/api/queues")
async def queues():
    """Kafka queues (topics) with message counts and consumer backlog, labelled by partner."""
    rows = await pipeline.queue_stats()
    owners = {}
    for p in await db.list_partners():
        owners.setdefault(p["topic_in"].rsplit(".", 1)[0], []).append(p["name"])
    for r in rows:
        r["partners"] = ", ".join(owners.get(r["topic"].rsplit(".", 1)[0], [])) or "-"
    return rows


@app.get("/api/transactions")
async def transactions(limit: int = 50, status: str | None = None, q: str | None = None,
                       since: datetime | None = None, until: datetime | None = None, partner: str | None = None):
    """Transaction log. q searches transaction id, partner, mapping name, input, output and error text;
    partner filters to one partner ("-" = ad-hoc traffic with no partner)."""
    return await db.list_transactions(min(limit, 500), status, (q or "").strip() or None, since, until, partner)


@app.get("/api/transactions/{txn_id}")
async def transaction(txn_id: uuid.UUID):
    txn = await db.get_transaction(txn_id)
    if not txn:
        raise HTTPException(404, "Transaction not found")
    return txn


@app.get("/api/stats")
async def stats(partner: str | None = None):
    return await db.stats(partner)


@app.get("/api/stats/partners")
async def stats_by_partner():
    return await db.partner_stats()


@app.get("/health")
async def health():
    checks = {}
    try:
        await db.ping()
        checks["database"] = "up"
    except Exception as e:  # noqa: BLE001
        checks["database"] = f"down: {e}"
    try:
        await pipeline.producer.client.fetch_all_metadata()
        checks["kafka"] = "up"
    except Exception as e:  # noqa: BLE001
        checks["kafka"] = f"down: {e}"
    checks["consumers"] = {t.get_name(): ("running" if not t.done() else "stopped") for t in pipeline._tasks}
    ok = checks["database"] == "up" and checks["kafka"] == "up" and all(
        v == "running" for v in checks["consumers"].values()
    )
    return JSONResponse({"status": "UP" if ok else "DOWN", **checks}, status_code=200 if ok else 503)


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
