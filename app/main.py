"""FastAPI app: REST API, monitoring endpoints and the web UI."""
import asyncio
import json
import logging
import os
import re
import secrets
import uuid
from collections import deque
from datetime import date, datetime
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from psycopg.types.json import Jsonb

from . import (adapters, agent, ai, alerts, auth, backup, cluster, connectors, cron, datastore, db, mapping_doc,
               migrate, pipeline, ports, scheduler, vault, workflows)
from .transform import SAMPLES, TransformError, parse, transform

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_app):
    await db.init()
    await vault.migrate()
    cluster.on_txn_done = pipeline.resolve_waiter
    await cluster.start()
    await auth.ensure_admin()
    await datastore.init()
    await pipeline.start()
    await connectors.start()
    await workflows.start()
    await ports.start()
    await scheduler.start()
    await alerts.start()
    yield
    await alerts.stop()
    await scheduler.stop()
    await ports.stop()
    await workflows.stop()
    await connectors.stop()
    await pipeline.stop()
    await cluster.stop()
    await db.close()


app = FastAPI(title="Payload Hub", lifespan=lifespan)


@app.middleware("http")
async def require_login(request: Request, call_next):
    """Every /api call needs a signed-in user with the right permission (read / write / delete / admin).
    Partner endpoints (/inbound), the test receiver, /health and /metrics are outside /api."""
    path = request.url.path
    if not path.startswith("/api/") or path in ("/api/auth/login", "/api/auth/sso"):
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
    audited = permission != "read" and request.method in ("POST", "PUT", "DELETE") and not NOT_AUDITED.match(path)
    parts = path.split("/")[2:]  # /api/<type>/<id>/<sub>
    before = await _snapshot(parts) if audited and request.method != "POST" else None
    response = await call_next(request)
    if audited and response.status_code < 400:
        action = {"POST": "created", "PUT": "changed", "DELETE": "deleted"}[request.method]
        if len(parts) > 2:
            action = f"{action} {'/'.join(parts[2:])}"
        try:
            detail = f"{request.method} {path}"
            if before is not None:
                after = await _snapshot(parts) if request.method != "DELETE" else None
                detail = _diff(before, after) or detail
            await db.audit(user["username"], action, parts[0].rstrip("s") if parts else "?",
                           parts[1] if len(parts) > 1 else None, detail)
        except Exception:  # noqa: BLE001 - never fail a request because of the audit log
            logging.getLogger("audit").exception("audit write failed")
    return response


# What an audited object looked like, so the audit log can say what changed (never secrets or counters).
_SNAPSHOTS = {"partners": lambda i: db.get_partner(i), "connections": lambda i: db.get_connection(i),
              "ports": lambda i: db.get_port(i), "schedules": lambda i: db.get_schedule(i),
              "workflows": lambda i: db.get_workflow(i), "users": lambda i: db.get_user(i)}
_NOT_DIFFED = {"updated_at", "created_at", "api_key_hash", "api_key_enc", "password_hash", "secret", "has_secret", "last_poll_at",
               "last_item", "last_error", "ok_count", "fail_count", "next_run_at", "last_run_at", "last_status",
               "last_detail", "run_count", "running", "last_test_at", "last_test_ok", "last_test_detail",
               "last_login_at", "used_by"}


async def _snapshot(parts):
    fetch = _SNAPSHOTS.get(parts[0]) if len(parts) > 1 else None
    if not fetch:
        return None
    try:
        row = await fetch(parts[1])
    except Exception:  # noqa: BLE001
        return None
    return {k: v for k, v in (row or {}).items() if k not in _NOT_DIFFED}


def _short(v) -> str:
    text = "—" if v in (None, "", [], {}) else json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v)
    return text if len(text) <= 60 else "(changed)"


def _diff(before: dict, after: dict | None) -> str:
    if after is None:
        return f"deleted (was: {before.get('name') or before.get('username') or ''})"
    changes = [f"{k}: {_short(before.get(k))} → {_short(after.get(k))}" for k in sorted(set(before) | set(after))
               if before.get(k) != after.get(k)]
    return "; ".join(changes)[:2000]


# Calls that change nothing worth auditing (runs, sends, tests, polls) or that write their own audit entries.
NOT_AUDITED = re.compile(r"^/api/(auth/|transform$|secrets|agent|workflows/[^/]+/run$|workflow-runs/|schedules/[^/]+/run$|"
                         r"ports/[^/]+/poll$|connections/[^/]+/(test|send|query|file)$|partners/[^/]+/test$)")


class LoginIn(BaseModel):
    username: str
    password: str


def _networks(text: str):
    import ipaddress

    return [ipaddress.ip_network(a.strip(), strict=False) for a in text.split(",") if a.strip()]


# Callers allowed to pass on the browser's address (the web console): addresses or networks, e.g. 172.16.0.0/12
TRUSTED_PROXIES = _networks(os.getenv("PAYLOAD_HUB_TRUSTED_PROXIES", "127.0.0.1,::1"))


def client_address(request: Request) -> str | None:
    """The browser's address. The web console passes it on in X-Client-Address; only trusted callers may do that."""
    import ipaddress

    direct = request.client.host if request.client else None
    forwarded = request.headers.get("x-client-address")
    try:
        trusted = direct is not None and any(ipaddress.ip_address(direct) in n for n in TRUSTED_PROXIES)
    except ValueError:
        trusted = False
    return forwarded if forwarded and trusted else direct


@app.post("/api/auth/login")
async def login(req: LoginIn, request: Request):
    try:
        result = await auth.login(req.username.strip(), req.password, client_address(request))
    except auth.LoginBlocked as e:
        raise HTTPException(429, str(e)) from e
    if result is None:
        raise HTTPException(401, "Wrong username or password, or the account is disabled.")
    token, user = result
    return {"token": token, "user": user}


class SSOIn(BaseModel):
    email: str = Field(min_length=3, max_length=200)
    name: str | None = None


@app.post("/api/auth/sso")
async def sso_login(req: SSOIn, request: Request):
    """Called by the web console after the company identity provider signed someone in. Trusted only with the
    shared PAYLOAD_HUB_SSO_SECRET; disabled when that isn't set."""
    import hmac

    secret = os.getenv("PAYLOAD_HUB_SSO_SECRET")
    if not secret:
        raise HTTPException(404, "Single sign-on isn't enabled.")
    if not hmac.compare_digest(request.headers.get("x-sso-secret", ""), secret):
        raise HTTPException(401, "Not allowed.")
    result = await auth.sso_login(req.email)
    if result is None:
        raise HTTPException(403, f"{req.email} has no Payload Hub account yet - ask an administrator to add it.")
    token, user = result
    await db.audit(user["username"], "signed in", "user", user["username"], "single sign-on")
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
    unlock: bool | None = None  # clear a lockout after too many wrong passwords


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
    fields = req.model_dump(exclude_unset=True, exclude={"password", "unlock"})
    if req.unlock:
        fields.update(locked_until=None, failed_logins=0)
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
    provider: str | None = None  # default: the active provider and model
    model: str | None = None


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
    return await agent.run(req.request.strip(), req.payload, req.input_format, doc,
                           provider=req.provider, model=req.model)


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
    """Whether the agent can run with the active provider and model. Never returns a key."""
    provider, model = await ai.active()
    key, _ = await ai.credentials(provider)
    return {"configured": bool(key and model), "provider": provider, "model": model,
            "label": ai.PROVIDERS[provider]["label"]}


# ---------- AI providers (keys pasted in the UI, stored encrypted) ----------

class AIProviderIn(BaseModel):
    api_key: str | None = Field(default=None, max_length=1000)  # None keeps the stored key
    config: dict | None = None


class AIActiveIn(BaseModel):
    provider: str
    model: str = Field(min_length=1, max_length=200)


def _provider_or_404(provider: str):
    if provider not in ai.PROVIDERS:
        raise HTTPException(404, "Unknown AI provider.")


@app.get("/api/ai/providers")
async def ai_providers():
    saved = await db.list_ai_providers()
    provider, model = await ai.active()
    out = []
    for pid, spec in ai.PROVIDERS.items():
        row = saved.get(pid) or {}
        from_env = bool(os.getenv(spec["env"]) or (pid == "gemini" and os.getenv("GOOGLE_API_KEY")))
        out.append({"id": pid, **{k: spec[k] for k in ("label", "icon", "key_help", "fields", "models", "default_model")},
                    "has_key": bool(row.get("has_key")), "key_hint": row.get("key_hint"),
                    "from_env": from_env and not row.get("has_key"), "config": row.get("config") or {}})
    return {"providers": out, "active": {"provider": provider, "model": model}}


@app.put("/api/ai/providers/{provider}")
async def ai_save_provider(provider: str, req: AIProviderIn):
    _provider_or_404(provider)
    key = (req.api_key or "").strip()
    await db.save_ai_provider(provider, vault.encrypt(key) if key else None, f"…{key[-4:]}" if key else None,
                              req.config)
    return {"saved": True}


@app.delete("/api/ai/providers/{provider}/key", status_code=204)
async def ai_remove_key(provider: str):
    _provider_or_404(provider)
    await db.save_ai_provider(provider, clear_key=True)


@app.post("/api/ai/providers/{provider}/test")
async def ai_test_provider(provider: str):
    """Check the key by listing the provider's models (no tokens are used)."""
    _provider_or_404(provider)
    try:
        models = await ai.list_models(provider)
    except ai.AIError as e:
        if provider == "azure" and "deployment" in str(e):
            key, cfg = await ai.credentials(provider)
            return {"ok": bool(key and cfg.get("endpoint")), "detail": "Key and endpoint saved. Azure is checked on the "
                                                                     "first agent run with your deployment name."}
        return {"ok": False, "detail": str(e)}
    return {"ok": True, "detail": f"Key works - {len(models)} models available.", "models": models[:200]}


@app.get("/api/ai/providers/{provider}/models")
async def ai_models(provider: str):
    _provider_or_404(provider)
    try:
        return {"models": await ai.list_models(provider)}
    except ai.AIError as e:
        raise HTTPException(422, str(e)) from e


@app.put("/api/ai/active")
async def ai_set_active(req: AIActiveIn):
    _provider_or_404(req.provider)
    await db.set_setting("agent_model", {"provider": req.provider, "model": req.model.strip()})
    return {"provider": req.provider, "model": req.model.strip()}


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
        reply = await connectors.reply_for(p, txn_id, result)
        txn = await db.get_transaction(txn_id)
        if reply:
            txn["reply"] = reply
        return txn
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
    reply_mode: Literal["transformed", "forward"] = "transformed"
    reply_connection: str | None = None
    reply_path: str | None = None
    response_format: Literal["input", "json", "xml", "unchanged"] = "input"
    response_expression: str | None = None


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
                                     "outbound_target", "reply_connection"})
    if req.reply_mode == "forward":
        c = await db.get_connection(req.reply_connection or "")
        if not c or c["type"] != "http":
            raise HTTPException(422, "Forwarding needs an HTTP connection for the target API.")
    fields["reply_connection"] = req.reply_connection if req.reply_mode == "forward" else None
    fields["mapping_source"] = "manual" if req.expression else None
    fields.update(_key_fields(_new_api_key()))
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
    return {k: v for k, v in p.items() if k not in ("api_key_hash", "api_key_enc")}


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
    provider: str | None = None
    model: str | None = None


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
                           combined, partner_code=code, provider=req.provider, model=req.model)


def _new_api_key() -> str:
    return "ph_" + secrets.token_urlsafe(24)


def _key_fields(key: str) -> dict:
    """A partner API key is stored as a hash (to check calls) and encrypted (to show to people who may write)."""
    import hashlib

    return {"api_key_hash": hashlib.sha256(key.encode()).hexdigest(), "api_key_enc": vault.seal(key),
            "api_key_hint": f"{key[:5]}…{key[-4:]}"}


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


class PartnerReply(BaseModel):
    reply_mode: Literal["transformed", "forward"] = "transformed"
    reply_connection: str | None = None  # the target system
    reply_path: str | None = None  # API path, folder, table, queue or topic
    reply_method: Literal["POST", "PUT", "PATCH", "GET", "DELETE"] | None = None
    send_format: Literal["output", "csv"] = "output"
    csv_delimiter: str | None = Field(default=None, max_length=1)
    response_format: Literal["input", "json", "xml", "unchanged"] = "input"
    response_expression: str | None = None


@app.put("/api/partners/{code}/reply")
async def set_partner_reply(code: str, req: PartnerReply):
    """The the target system route: send the transformed payload to the target system (API, file, database, queue) and return its
    answer to the caller - translated or as is. 'transformed' returns the transformed payload directly."""
    await _partner_or_404(code)
    if req.reply_mode == "forward":
        c = await db.get_connection(req.reply_connection or "")
        if not c:
            raise HTTPException(422, "Choose the target system (an outbound connection).")
        if not adapters.TYPES[c["type"]]["outbound"]:
            raise HTTPException(422, f"A {c['type']} connection can't receive messages.")
        if c.get("owner_partner") and c["owner_partner"] != code:
            raise HTTPException(422, f"'{c['name']}' belongs to partner {c['owner_partner']}.")
        if c["type"] != "http" and not (req.reply_path or "").strip():
            raise HTTPException(422, "Give the folder, table, queue or topic to send to.")
        if c["type"] == "database" and req.send_format == "csv":
            raise HTTPException(422, "A database table takes rows, not CSV - choose 'as transformed' (JSON).")
    await db.update_partner(code, reply_mode=req.reply_mode,
                            reply_connection=req.reply_connection if req.reply_mode == "forward" else None,
                            reply_path=(req.reply_path or "").strip() or None,
                            reply_method=req.reply_method, send_format=req.send_format,
                            csv_delimiter=req.csv_delimiter or None,
                            response_format=req.response_format,
                            response_expression=(req.response_expression or "").strip() or None)
    return await db.get_partner(code)


class PartnerStore(BaseModel):
    store_enabled: bool = False
    store_connection: str | None = None  # None = the built-in data area
    store_table: str | None = None
    store_source: Literal["output", "input"] = "output"
    store_row_path: str | None = None
    store_columns: dict[str, str] = Field(default_factory=dict)


@app.put("/api/partners/{code}/store")
async def set_partner_store(code: str, req: PartnerStore):
    """Whether (and how) each completed message is also written into a data table."""
    await _partner_or_404(code)
    if req.store_enabled:
        tables = {t["name"] for t in await _tables_of(req.store_connection)}
        if req.store_table not in tables:
            raise HTTPException(422, "Choose an existing table (create one on the Data page).")
        if not any(v.strip() for v in req.store_columns.values()):
            raise HTTPException(422, "Map at least one column to a field.")
    await db.update_partner(code, store_enabled=req.store_enabled, store_table=req.store_table,
                            store_connection=req.store_connection or None,
                            store_source=req.store_source, store_row_path=(req.store_row_path or "").strip() or None,
                            store_columns=Jsonb({k: v for k, v in req.store_columns.items() if v and v.strip()}))
    return await db.get_partner(code)


@app.post("/api/partners/{code}/store/preview")
async def preview_partner_store(code: str, req: PartnerStore):
    """Show the rows a mapping would store for the partner's sample, without writing anything."""
    p = await _partner_or_404(code)
    if not p["sample_input"]:
        raise HTTPException(422, "The partner needs a sample input to preview.")
    try:
        if req.store_source == "output":
            if not p["expression"]:
                raise HTTPException(422, "The partner has no mapping yet, so there is no output to preview.")
            payload, fmt = transform(p["sample_input"], p["input_format"], p["expression"], p["output_format"]), \
                p["output_format"]
        else:
            payload, fmt = p["sample_input"], p["input_format"]
        rows = datastore.build_rows({"row_path": req.store_row_path, "columns": req.store_columns}, payload, fmt,
                                    {"id": "00000000-0000-0000-0000-000000000000", "partner_code": code,
                                     "created_at": "(now)"})
    except (TransformError, datastore.DataError) as e:
        raise HTTPException(422, str(e)) from e
    except Exception as e:  # noqa: BLE001 - JMESPath syntax errors etc.
        raise HTTPException(422, f"Mapping error: {e}") from e
    return {"rows": rows}


# ---------- data tables & SQL console ----------

class TableIn(BaseModel):
    name: str
    columns: list[dict]
    add_tracking: bool = True


class QueryIn(BaseModel):
    sql: str = Field(min_length=1, max_length=20000)


@app.get("/api/data/status")
async def data_status():
    return {"schema": datastore.SCHEMA, "restricted_role": datastore.role_ok, "types": list(datastore.TYPES)}


@app.get("/api/data/tables")
async def data_tables():
    return await datastore.list_tables()


@app.post("/api/data/tables", status_code=201)
async def data_create_table(req: TableIn):
    try:
        await datastore.create_table(req.name, req.columns, req.add_tracking)
    except datastore.DataError as e:
        raise HTTPException(e.status, str(e)) from e
    return {"created": req.name.strip().lower()}


@app.delete("/api/data/tables/{name}", status_code=204)
async def data_drop_table(name: str):
    in_use = [p["code"] for p in await db.list_partners() if p.get("store_enabled") and p.get("store_table") == name]
    if in_use:
        raise HTTPException(409, "Partners store into this table: " + ", ".join(in_use))
    try:
        await datastore.drop_table(name)
    except datastore.DataError as e:
        raise HTTPException(e.status, str(e)) from e


@app.get("/api/data/tables/{name}/rows")
async def data_rows(name: str, limit: int = 100):
    try:
        return await datastore.rows(name, limit)
    except datastore.DataError as e:
        raise HTTPException(e.status, str(e)) from e


@app.post("/api/data/query")
async def data_query(req: QueryIn, request: Request):
    """Run one SQL statement in the data area. SELECT needs read, INSERT/UPDATE/CREATE/ALTER need write,
    DELETE/DROP/TRUNCATE need delete permission."""
    user = request.state.user
    try:
        statement, needed = datastore.classify(req.sql)
    except datastore.DataError as e:
        raise HTTPException(e.status, str(e)) from e
    if not auth.allowed(user, needed):
        raise HTTPException(403, f"This statement needs {needed} permission.")
    if not datastore.role_ok and not user["is_admin"]:
        raise HTTPException(403, "The SQL console is limited to administrators on this database "
                                 "(the restricted data role couldn't be created).")
    try:
        return {**await datastore.run(statement, needed), "permission": needed}
    except datastore.DataError as e:
        raise HTTPException(e.status, str(e)) from e


# ---------- external databases (database connections) ----------

async def _database(conn_id: str) -> adapters.Database:
    c = await db.get_connection(conn_id)
    if not c:
        raise HTTPException(404, "Connection not found.")
    if c["type"] != "database":
        raise HTTPException(422, "That connection isn't a database.")
    return await connectors.adapter_for(conn_id)


async def _tables_of(conn_id: str | None):
    if not conn_id:
        return await datastore.list_tables()
    try:
        return await (await _database(conn_id)).tables()
    except adapters.AdapterError as e:
        raise HTTPException(502, str(e)) from e


@app.get("/api/connections/{conn_id}/tables")
async def db_tables(conn_id: str):
    return await _tables_of(conn_id)


@app.get("/api/connections/{conn_id}/tables/{name}/rows")
async def db_table_rows(conn_id: str, name: str, limit: int = 100):
    try:
        return await (await _database(conn_id)).table_rows(name, min(limit, 1000))
    except adapters.AdapterError as e:
        raise HTTPException(502, str(e)) from e


@app.post("/api/connections/{conn_id}/tables", status_code=201)
async def db_create_table(conn_id: str, req: TableIn):
    name = req.name.strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", name) or \
            any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", str(c.get("name", ""))) for c in req.columns):
        raise HTTPException(422, "Table and column names: letters, digits and _, starting with a letter.")
    try:
        await (await _database(conn_id)).create_table(name, req.columns, req.add_tracking)
    except adapters.AdapterError as e:
        raise HTTPException(422, str(e)) from e
    return {"created": name}


@app.delete("/api/connections/{conn_id}/tables/{name}", status_code=204)
async def db_drop_table(conn_id: str, name: str):
    in_use = [p["code"] for p in await db.list_partners()
              if p.get("store_enabled") and p.get("store_connection") == conn_id and p.get("store_table") == name]
    if in_use:
        raise HTTPException(409, "Partners store into this table: " + ", ".join(in_use))
    try:
        await (await _database(conn_id)).drop_table(name)
    except adapters.AdapterError as e:
        raise HTTPException(422, str(e)) from e


@app.post("/api/connections/{conn_id}/query")
async def db_query(conn_id: str, req: QueryIn, request: Request):
    """Run one SQL statement on an external database, checked against the user's permissions.
    Reads are always rolled back. The database's own user rights apply on top."""
    try:
        statement, needed = adapters.classify_sql(req.sql)
    except adapters.AdapterError as e:
        raise HTTPException(400, str(e)) from e
    if not auth.allowed(request.state.user, needed):
        raise HTTPException(403, f"This statement needs {needed} permission.")
    try:
        return {**await (await _database(conn_id)).query(statement, needed), "permission": needed}
    except adapters.AdapterError as e:
        raise HTTPException(400, str(e)) from e


# ---------- files on FTP / SFTP connections ----------

async def _files(conn_id: str) -> adapters.FileTransfer:
    c = await db.get_connection(conn_id)
    if not c:
        raise HTTPException(404, "Connection not found.")
    if c["type"] != "ftp":
        raise HTTPException(422, "That connection isn't an FTP/SFTP connection.")
    return await connectors.adapter_for(conn_id)


@app.get("/api/connections/{conn_id}/files")
async def ftp_list(conn_id: str, path: str = ""):
    try:
        return await (await _files(conn_id)).list_dir(path)
    except adapters.AdapterError as e:
        raise HTTPException(502, str(e)) from e


@app.get("/api/connections/{conn_id}/file")
async def ftp_read(conn_id: str, path: str):
    try:
        data = await (await _files(conn_id)).read_file(path, max_bytes=1_000_000)
    except adapters.AdapterError as e:
        raise HTTPException(502, str(e)) from e
    return {"path": path, "size": len(data), "text": data.decode("utf-8", errors="replace")}


class FileWrite(BaseModel):
    folder: str = ""
    name: str = Field(min_length=1, max_length=200, pattern=r"^[^/\\]+$")
    content: str


@app.post("/api/connections/{conn_id}/file", status_code=201)
async def ftp_write(conn_id: str, req: FileWrite):
    """Upload a file, e.g. to drop a test message into a partner's inbound folder."""
    try:
        path = await (await _files(conn_id)).write_file(req.folder, req.name, req.content.encode())
    except adapters.AdapterError as e:
        raise HTTPException(502, str(e)) from e
    return {"written": path}


# ---------- Kafka connections: topics and messages ----------

async def _kafka(conn_id: str) -> adapters.Kafka:
    c = await db.get_connection(conn_id)
    if not c:
        raise HTTPException(404, "Connection not found.")
    if c["type"] != "kafka":
        raise HTTPException(422, "That connection isn't Kafka.")
    return await connectors.adapter_for(conn_id)


@app.get("/api/connections/{conn_id}/topics")
async def kafka_topics(conn_id: str):
    try:
        return await (await _kafka(conn_id)).topics()
    except adapters.AdapterError as e:
        raise HTTPException(502, str(e)) from e


@app.get("/api/connections/{conn_id}/topics/{topic}/messages")
async def kafka_messages(conn_id: str, topic: str, limit: int = 20):
    try:
        return await (await _kafka(conn_id)).peek(topic, min(max(limit, 1), 200))
    except adapters.AdapterError as e:
        raise HTTPException(422 if not e.retryable else 502, str(e)) from e


# ---------- workflows ----------

class WorkflowIn(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    description: str | None = None
    trigger_type: Literal["manual", "schedule", "database"] = "manual"
    trigger_config: dict = Field(default_factory=dict)
    steps: list[dict] = Field(default_factory=list)
    active: bool = True


class RunIn(BaseModel):
    input: dict = Field(default_factory=dict)
    dry_run: bool = False
    wait: bool = True


async def _workflow_or_404(wf_id: str):
    wf = await db.get_workflow(wf_id)
    if not wf:
        raise HTTPException(404, "Workflow not found.")
    return wf


def _clean_workflow(req: WorkflowIn) -> dict:
    try:
        steps = workflows.validate(req.steps)
    except workflows.StepError as e:
        raise HTTPException(422, str(e)) from e
    return {**req.model_dump(), "steps": steps}


@app.get("/api/workflows")
async def list_workflows():
    return {"workflows": await db.list_workflows(), "triggers": workflows.trigger_status()}


@app.post("/api/workflows", status_code=201)
async def create_workflow(req: WorkflowIn):
    wf_id = _slug(req.name)
    if not await db.create_workflow(wf_id, **_clean_workflow(req)):
        raise HTTPException(409, f"A workflow called '{wf_id}' already exists.")
    await workflows.sync()
    return await db.get_workflow(wf_id)


@app.get("/api/workflows/{wf_id}")
async def get_workflow(wf_id: str):
    return await _workflow_or_404(wf_id)


@app.put("/api/workflows/{wf_id}")
async def update_workflow(wf_id: str, req: WorkflowIn):
    await _workflow_or_404(wf_id)
    await db.update_workflow(wf_id, **_clean_workflow(req), next_run_at=None)
    await workflows.sync()
    return await db.get_workflow(wf_id)


@app.delete("/api/workflows/{wf_id}", status_code=204)
async def delete_workflow(wf_id: str):
    await _workflow_or_404(wf_id)
    await db.delete_workflow(wf_id)
    await workflows.sync()


@app.post("/api/workflows/{wf_id}/run")
async def run_workflow(wf_id: str, req: RunIn, request: Request):
    """Start a run. wait=true returns the finished run; wait=false returns at once with its id."""
    wf = await _workflow_or_404(wf_id)
    source = f"manual by {request.state.user['username']}"
    if req.wait:
        return await workflows.run(wf, req.input, source, req.dry_run)
    task = asyncio.create_task(workflows.run(wf, req.input, source, req.dry_run))
    await asyncio.sleep(0.2)
    return {"status": "STARTED", "running": not task.done()}


@app.get("/api/workflows/{wf_id}/runs")
async def workflow_runs(wf_id: str, limit: int = 50):
    return await db.list_runs(wf_id, min(limit, 500))


@app.get("/api/workflow-runs")
async def all_workflow_runs(limit: int = 50):
    return await db.list_runs(None, min(limit, 500))


@app.get("/api/workflow-runs/{run_id}")
async def workflow_run(run_id: uuid.UUID):
    run = await db.get_run(run_id)
    if not run:
        raise HTTPException(404, "Run not found.")
    return run


@app.post("/api/workflow-runs/{run_id}/rerun")
async def rerun_workflow(run_id: uuid.UUID):
    try:
        return await workflows.rerun(str(run_id))
    except workflows.StepError as e:
        raise HTTPException(404, str(e)) from e


@app.get("/api/workflow-files/{file_id}")
async def workflow_file(file_id: int):
    f = await db.get_file(file_id)
    if not f:
        raise HTTPException(404, "File not found.")
    return Response(bytes(f["content"]), media_type=f["content_type"],
                    headers={"Content-Disposition": f'attachment; filename="{f["filename"]}"'})


class PdfPreviewIn(BaseModel):
    template: str
    context: dict = Field(default_factory=dict)


@app.post("/api/workflows/pdf-preview")
async def pdf_preview(req: PdfPreviewIn):
    """Render a PDF template with sample data (e.g. a previous run's results), without storing anything."""
    try:
        html = workflows._html_env.from_string(req.template).render(**req.context)
        data = await asyncio.to_thread(workflows.make_pdf, html)
    except Exception as e:  # noqa: BLE001 - template errors are shown to the user
        raise HTTPException(422, f"Template error: {e}") from e
    return Response(data, media_type="application/pdf")


@app.get("/mock/customers/{customer_id}", tags=["mock external system"])
async def mock_customer_api(customer_id: str):
    """A stand-in 'other system' for workflow demos: a customer credit profile."""
    seed = sum(ord(ch) for ch in customer_id)
    segments = ["GOLD", "SILVER", "STANDARD"]
    return {"customer_id": customer_id, "segment": segments[seed % 3], "credit_limit": 25000 + (seed % 7) * 5000,
            "discount_percent": [10, 5, 0][seed % 3], "payment_terms_days": [45, 30, 15][seed % 3],
            "account_manager": ["Priya Nair", "Karthik Rao", "Sara Thomas"][seed % 3]}


# ---------- server overview (landing page) ----------

_STARTED_AT = __import__("time").time()


@app.get("/api/overview")
async def overview():
    """Server and platform statistics for the landing page."""
    import platform
    import socket
    import time as _time

    import psutil

    proc = psutil.Process()
    vm, disk = psutil.virtual_memory(), psutil.disk_usage("/")
    try:
        load = [round(x, 2) for x in os.getloadavg()]
    except OSError:
        load = None
    counts = await db.overview_counts()
    try:
        queues = await pipeline.queue_stats()
        kafka = {"up": True, "topics": len(queues), "messages": sum(q["messages"] for q in queues),
                 "backlog": sum(q["backlog"] or 0 for q in queues), "bootstrap": pipeline.BOOTSTRAP}
    except Exception as e:  # noqa: BLE001
        kafka = {"up": False, "error": str(e) or type(e).__name__, "bootstrap": pipeline.BOOTSTRAP}
    listeners = connectors.listener_status()
    return {
        "server": {"host": socket.gethostname(), "os": f"{platform.system()} {platform.release()}",
                   "python": platform.python_version(), "cpu_count": psutil.cpu_count(),
                   "cpu_percent": psutil.cpu_percent(interval=0.2), "load": load,
                   "memory_used": vm.used, "memory_total": vm.total, "memory_percent": vm.percent,
                   "disk_used": disk.used, "disk_total": disk.total, "disk_percent": disk.percent,
                   "app_memory": proc.memory_info().rss, "app_threads": proc.num_threads(),
                   "uptime_seconds": round(_time.time() - _STARTED_AT)},
        "postgres": {"version": counts.pop("pg_version").split(" on ")[0], "db_bytes": counts.pop("db_bytes"),
                     "connections": counts.pop("db_connections"), "database": db.DATABASE_URL.rsplit("/", 1)[-1].split("?")[0]},
        "kafka": kafka,
        "consumers": {t.get_name(): ("running" if not t.done() else "stopped") for t in pipeline._tasks},
        "listeners": {"running": sum(1 for v in listeners.values() if v["state"] == "running"),
                      "failing": sum(1 for v in listeners.values() if v["state"] == "error")},
        "workflow_triggers": workflows.trigger_status(),
        "counts": counts,
    }


@app.get("/api/connections/{conn_id}/secret")
async def connection_secret(conn_id: str, request: Request):
    """Administrators only: a connection's own password, for the partner profile (recorded in the audit log).
    Named secrets are never shown - only their alias."""
    c = await db.get_connection(conn_id, with_secret=True)
    if not c:
        raise HTTPException(404, "Connection not found.")
    if c.get("secret_alias"):
        return {"secret": None, "alias": c["secret_alias"]}
    await db.audit(request.state.user["username"], "revealed password", "connection", conn_id)
    return {"secret": vault.unseal(c.get("secret")) or ""}


# ---------- secrets (administrators) ----------

ALIAS = re.compile(r"^[A-Za-z0-9_.-]{2,80}$")


async def _check_alias(alias: str | None):
    if alias and not await db.get_secret(alias):
        raise HTTPException(422, f"There's no stored secret called '{alias}'.")


class SecretIn(BaseModel):
    alias: str = Field(min_length=2, max_length=80)
    kind: Literal["password", "api_key", "token", "certificate", "other"] = "password"
    description: str | None = None
    value: str | None = None  # write-only; None on update keeps the stored value


@app.get("/api/secret-aliases")
async def secret_aliases():
    """Names and kinds only, so anyone who edits connections can pick a stored secret."""
    return [{"alias": x["alias"], "kind": x["kind"]} for x in await db.list_secrets()]


@app.get("/api/secrets")
async def list_secrets():
    return {"secrets": await db.list_secrets(), "key": vault.key_info(), "check": await vault.check()}


@app.post("/api/secrets", status_code=201)
async def create_secret(req: SecretIn, request: Request):
    if not ALIAS.match(req.alias):
        raise HTTPException(422, "An alias uses letters, digits, '.', '_' or '-' (2-80 characters).")
    if not req.value:
        raise HTTPException(422, "Enter the secret value.")
    user = request.state.user["username"]
    if not await db.create_secret(req.alias, req.kind, req.description, vault.encrypt(req.value), user):
        raise HTTPException(409, f"A secret called '{req.alias}' already exists.")
    await db.audit(user, "created", "secret", req.alias, req.kind)
    return await db.get_secret(req.alias)


@app.put("/api/secrets/{alias}")
async def update_secret(alias: str, req: SecretIn, request: Request):
    if not await db.get_secret(alias):
        raise HTTPException(404, "Secret not found.")
    user = request.state.user["username"]
    await db.update_secret(alias, req.kind, req.description, vault.encrypt(req.value) if req.value else None, user)
    await db.audit(user, "changed value" if req.value else "changed details", "secret", alias)
    return await db.get_secret(alias)


@app.delete("/api/secrets/{alias}", status_code=204)
async def delete_secret(alias: str, request: Request):
    used = next((x["used_by"] for x in await db.list_secrets() if x["alias"] == alias), [])
    if used:
        raise HTTPException(409, f"Still used by {', '.join(used)} - switch those connections to another secret first.")
    await db.delete_secret(alias)
    await db.audit(request.state.user["username"], "deleted", "secret", alias)


@app.post("/api/secrets/master-key/rotate")
async def rotate_master_key(request: Request):
    try:
        return await vault.rotate_master_key(request.state.user["username"])
    except vault.VaultError as e:
        raise HTTPException(409, str(e)) from e


@app.get("/api/partners/{code}/history")
async def partner_history(code: str, limit: int = 50):
    """Who changed this partner (and its route, adapters, storage, mapping), and what changed."""
    return [e for e in await db.list_audit(500, "partner") if e["object_id"] == code][:limit]


# ---------- alerts ----------

class AlertChannelIn(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    type: Literal["webhook", "email"]
    config: dict = Field(default_factory=dict)
    secret_alias: str | None = None
    enabled: bool = True


class AlertRuleIn(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    enabled: bool = True
    event: str
    filter: dict = Field(default_factory=dict)
    threshold: dict = Field(default_factory=dict)
    channels: list[str] = Field(default_factory=list)
    cooldown_minutes: int = Field(default=15, ge=0, le=1440)


@app.get("/api/alerts")
async def alerts_overview(limit: int = 100):
    rules = await db.list_alert_rules()
    return {"events": alerts.EVENTS, "filters": alerts.FILTERS, "channels": await db.list_alert_channels(),
            "rules": [{**r, "text": alerts.describe_rule(r)} for r in rules],
            "log": await db.list_alert_events(min(limit, 1000)), "watch": alerts.watch_state()}


async def _clean_channel(req: AlertChannelIn) -> dict:
    await _check_alias(req.secret_alias)
    try:
        config = alerts.validate_channel(req.model_dump())
    except alerts.AlertError as e:
        raise HTTPException(422, str(e)) from e
    return {"name": req.name.strip(), "type": req.type, "config": config, "secret_alias": req.secret_alias or None,
            "enabled": req.enabled}


@app.post("/api/alerts/channels", status_code=201)
async def create_alert_channel(req: AlertChannelIn):
    cid = _slug(req.name)
    if not await db.create_alert_channel(cid, **await _clean_channel(req)):
        raise HTTPException(409, f"A channel called '{cid}' already exists.")
    return next(c for c in await db.list_alert_channels() if c["id"] == cid)


@app.put("/api/alerts/channels/{cid}")
async def update_alert_channel(cid: str, req: AlertChannelIn):
    await db.update_alert_channel(cid, **await _clean_channel(req))
    return next((c for c in await db.list_alert_channels() if c["id"] == cid), None)


@app.delete("/api/alerts/channels/{cid}", status_code=204)
async def delete_alert_channel(cid: str):
    await db.delete_alert_channel(cid)


@app.post("/api/alerts/channels/{cid}/test")
async def test_alert_channel(cid: str):
    ch = next((c for c in await db.list_alert_channels() if c["id"] == cid), None)
    if not ch:
        raise HTTPException(404, "Channel not found.")
    try:
        result = await alerts.send(ch, "Test alert", "If you can read this, alerts reach this channel.", "test", {})
    except Exception as e:  # noqa: BLE001 - shown to the user
        await db.update_alert_channel_state(cid, str(e) or type(e).__name__)
        return {"ok": False, "detail": str(e) or type(e).__name__}
    await db.update_alert_channel_state(cid, None)
    return {"ok": True, "detail": result}


async def _clean_rule(req: AlertRuleIn) -> dict:
    if req.event not in alerts.EVENTS:
        raise HTTPException(422, "Choose what the rule alerts on.")
    known = {c["id"] for c in await db.list_alert_channels()}
    if not req.channels or set(req.channels) - known:
        raise HTTPException(422, "Choose at least one channel to send to.")
    field = alerts.FILTERS.get(req.event)
    flt = {field: req.filter[field]} if field and req.filter.get(field) else {}
    threshold = {}
    if req.event == "error_rate":
        try:
            threshold = {"count": max(1, int(req.threshold.get("count") or 5)),
                         "minutes": max(1, int(req.threshold.get("minutes") or 10))}
        except (TypeError, ValueError) as e:
            raise HTTPException(422, "The threshold needs whole numbers.") from e
    return {"name": req.name.strip(), "enabled": req.enabled, "event": req.event, "filter": flt,
            "threshold": threshold, "channels": req.channels, "cooldown_minutes": req.cooldown_minutes}


@app.post("/api/alerts/rules", status_code=201)
async def create_alert_rule(req: AlertRuleIn):
    rid = _slug(req.name)
    if not await db.create_alert_rule(rid, **await _clean_rule(req)):
        raise HTTPException(409, f"A rule called '{rid}' already exists.")
    return next(r for r in await db.list_alert_rules() if r["id"] == rid)


@app.put("/api/alerts/rules/{rid}")
async def update_alert_rule(rid: str, req: AlertRuleIn):
    await db.update_alert_rule(rid, **await _clean_rule(req))
    return next((r for r in await db.list_alert_rules() if r["id"] == rid), None)


@app.delete("/api/alerts/rules/{rid}", status_code=204)
async def delete_alert_rule(rid: str):
    await db.delete_alert_rule(rid)


# ---------- backups and schema version (administrators) ----------

@app.get("/api/backups")
async def list_backups():
    return {"backups": await asyncio.to_thread(backup.list_backups), "folder": str(backup.backup_dir()),
            "keep": backup.keep(), "key_fingerprint": vault.key_info()["fingerprint"]}


@app.post("/api/backups", status_code=201)
async def create_backup(request: Request):
    try:
        manifest = await asyncio.to_thread(backup.create, f"by {request.state.user['username']}")
    except backup.BackupError as e:
        alerts.emit("backup_failed", "Database backup failed", str(e))
        raise HTTPException(500, str(e)) from e
    return manifest


@app.post("/api/backups/{name}/verify")
async def verify_backup(name: str):
    try:
        return await asyncio.to_thread(backup.verify, name)
    except backup.BackupError as e:
        raise HTTPException(404, str(e)) from e


@app.get("/api/backups/{name}/download")
async def download_backup(name: str, request: Request):
    from fastapi.responses import FileResponse

    try:
        path = backup.path_of(name)
    except backup.BackupError as e:
        raise HTTPException(404, str(e)) from e
    await db.audit(request.state.user["username"], "downloaded", "backup", name)
    return FileResponse(path, filename=path.name, media_type="application/octet-stream")


@app.delete("/api/backups/{name}", status_code=204)
async def delete_backup(name: str):
    try:
        await asyncio.to_thread(backup.delete, name)
    except backup.BackupError as e:
        raise HTTPException(404, str(e)) from e


@app.get("/api/system/schema")
async def schema_status():
    return await asyncio.to_thread(migrate.status)


@app.get("/api/audit")
async def audit_log(limit: int = 200, object_type: str | None = None):
    return await db.list_audit(min(limit, 1000), object_type)


# ---------- Payload Hub's own tables, read-only (administrators) ----------

@app.get("/api/system/tables")
async def system_tables():
    """The tool's own tables with sensitive columns (passwords, keys, tokens, secrets) left out."""
    return await db.system_tables()


@app.get("/api/system/tables/{name}/rows")
async def system_rows(name: str, limit: int = 100):
    data = await db.system_rows(name, limit)
    if data is None:
        raise HTTPException(404, "Table not found.")
    return data


# ---------- sample tables ----------

SAMPLE_TABLES = [
    ({"name": "customers", "add_tracking": False, "columns": [
        {"name": "customer_id", "type": "varchar", "length": 20, "primary_key": True},
        {"name": "name", "type": "varchar", "length": 120, "nullable": False},
        {"name": "email", "type": "varchar", "length": 160, "unique": True},
        {"name": "country", "type": "varchar", "length": 2},
        {"name": "created_at", "type": "timestamp", "default": "now"}]},
     [{"customer_id": "C-1001", "name": "Asha Rao", "email": "asha.rao@example.com", "country": "IN"},
      {"customer_id": "C-1002", "name": "Arjun Menon", "email": "arjun.menon@example.com", "country": "IN"},
      {"customer_id": "C-1003", "name": "Meera Iyer", "email": "meera.iyer@example.com", "country": "US"}]),
    ({"name": "orders", "add_tracking": False, "columns": [
        {"name": "order_id", "type": "varchar", "length": 20, "primary_key": True},
        {"name": "customer_id", "type": "varchar", "length": 20, "nullable": False},
        {"name": "order_date", "type": "date"},
        {"name": "status", "type": "varchar", "length": 20, "default": "NEW"},
        {"name": "total", "type": "numeric", "precision": 12, "scale": 2}]},
     [{"order_id": "NR-20931", "customer_id": "C-1002", "order_date": date(2026, 10, 18), "status": "SHIPPED", "total": 4244.00},
      {"order_id": "ZP-45821", "customer_id": "C-1003", "order_date": date(2026, 10, 15), "status": "NEW", "total": 24330.00},
      {"order_id": "AC-1042", "customer_id": "C-1001", "order_date": date(2026, 10, 19), "status": "INVOICED", "total": 61.00}]),
    ({"name": "order_lines", "add_tracking": False, "columns": [
        {"name": "order_id", "type": "varchar", "length": 20, "primary_key": True},
        {"name": "line_no", "type": "integer", "primary_key": True},
        {"name": "sku", "type": "varchar", "length": 30, "nullable": False},
        {"name": "quantity", "type": "integer", "default": "1"},
        {"name": "unit_price", "type": "numeric", "precision": 12, "scale": 2}]},
     [{"order_id": "NR-20931", "line_no": 1, "sku": "YM-01", "quantity": 2, "unit_price": 1299},
      {"order_id": "NR-20931", "line_no": 2, "sku": "BL-07", "quantity": 3, "unit_price": 449},
      {"order_id": "ZP-45821", "line_no": 1, "sku": "MED-PCM-500", "quantity": 200, "unit_price": 18.50},
      {"order_id": "AC-1042", "line_no": 1, "sku": "YM-01", "quantity": 2, "unit_price": 24.50}]),
]


@app.post("/api/db/{target}/samples", status_code=201)
async def create_sample_tables(target: str):
    """Create customers, orders and order_lines with example rows (skips tables that already exist)."""
    database = await _db_target(target)
    try:
        existing = {t["name"] for t in await database.tables()}
        created = []
        for spec, rows in SAMPLE_TABLES:
            if spec["name"] in existing:
                continue
            await database.create_designed(spec)
            await database.insert_rows(spec["name"], rows)
            created.append(spec["name"])
    except adapters.AdapterError as e:
        raise HTTPException(422, str(e)) from e
    return {"created": created}


# ---------- table designer (built-in area or any database connection) ----------

class DesignIn(BaseModel):
    name: str
    columns: list[dict]
    add_tracking: bool = True


class AlterIn(BaseModel):
    action: Literal["add_column", "drop_column", "set_primary_key", "drop_primary_key", "add_unique", "drop_unique"]
    column: dict | str | None = None
    columns: list[str] | None = None
    name: str | None = None


async def _db_target(target: str) -> adapters.Database:
    """'builtin' = the built-in data area; anything else is a database connection id."""
    if target == "builtin":
        return datastore.builtin()
    return await _database(target)


@app.post("/api/db/{target}/ddl")
async def design_preview(target: str, req: DesignIn):
    """The CREATE TABLE statement the designer would run, for this database's dialect."""
    try:
        return {"sql": (await _db_target(target)).ddl(req.model_dump())}
    except adapters.AdapterError as e:
        return {"sql": None, "error": str(e)}


@app.post("/api/db/{target}/build-sql")
async def build_sql_preview(target: str, spec: dict):
    """The SQL a workflow DB-connector step would run (values shown as :placeholders)."""
    try:
        return {"sql": await (await _db_target(target)).preview_structured(spec)}
    except (adapters.AdapterError, KeyError) as e:
        return {"sql": None, "error": str(e)}


@app.post("/api/db/{target}/tables", status_code=201)
async def design_create(target: str, req: DesignIn):
    try:
        await (await _db_target(target)).create_designed(req.model_dump())
    except adapters.AdapterError as e:
        raise HTTPException(422, str(e)) from e
    return {"created": req.name.strip()}


@app.get("/api/db/{target}/tables/{name}/schema")
async def design_schema(target: str, name: str):
    try:
        return await (await _db_target(target)).schema(name)
    except adapters.AdapterError as e:
        raise HTTPException(422, str(e)) from e


@app.post("/api/db/{target}/tables/{name}/alter")
async def design_alter(target: str, name: str, req: AlterIn, request: Request):
    needed = "delete" if req.action.startswith("drop") else "write"
    if not auth.allowed(request.state.user, needed):
        raise HTTPException(403, f"This change needs {needed} permission.")
    body = req.model_dump()
    if req.action == "drop_column":
        body["column"] = req.column if isinstance(req.column, str) else (req.column or {}).get("name")
    try:
        done = await (await _db_target(target)).alter(name, req.action, body)
    except adapters.AdapterError as e:
        raise HTTPException(422, str(e)) from e
    return {"ran": done}


@app.post("/api/partners/{code}/api-key")
async def rotate_partner_api_key(code: str):
    await _partner_or_404(code)
    key = _new_api_key()
    await db.update_partner(code, **_key_fields(key))
    return {"api_key": key}


@app.get("/api/partners/{code}/api-key")
async def partner_api_key(code: str, request: Request):
    """The partner's API key itself - needs write permission, and is recorded in the audit log."""
    p = await _partner_or_404(code)
    await db.audit(request.state.user["username"], "revealed api key", "partner", code)
    return {"api_key": vault.unseal(p.get("api_key_enc"))}


# ---------- connections (adapters) ----------

class ConnectionIn(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    type: Literal["rabbitmq", "activemq", "ibmmq", "http", "kafka", "database", "ftp"]
    config: dict = Field(default_factory=dict)
    secret: str | None = None  # write-only; None on update keeps the stored one
    secret_alias: str | None = None  # use a named secret from the Secrets page instead of `secret`
    owner_partner: str | None = None  # None = common endpoint, shared by all partners


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
    if req.owner_partner:
        await _partner_or_404(req.owner_partner)
    await _check_alias(req.secret_alias)
    if not await db.create_connection(conn_id, req.name, req.type, _clean_config(req.type, req.config),
                                      None if req.secret_alias else vault.seal(req.secret), req.owner_partner or None,
                                      req.secret_alias or None):
        raise HTTPException(409, f"A connection called '{conn_id}' already exists.")
    ok, detail = await connectors.test_connection(conn_id)
    return {**(await db.get_connection(conn_id)), "test_ok": ok, "test_detail": detail}


@app.put("/api/connections/{conn_id}")
async def update_connection(conn_id: str, req: ConnectionIn, request: Request):
    existing = await db.get_connection(conn_id)
    if not existing:
        raise HTTPException(404, "Connection not found.")
    if req.type != existing["type"]:
        raise HTTPException(422, "A connection's type can't change - create a new connection instead.")
    await _check_alias(req.secret_alias)
    await db.update_connection(conn_id, req.name, _clean_config(req.type, req.config),
                               None if req.secret_alias else vault.seal(req.secret) if req.secret else None)
    if (req.secret_alias or None) != existing.get("secret_alias"):
        await db.set_connection_secret_alias(conn_id, req.secret_alias or None)
    if req.secret or (req.secret_alias or None) != existing.get("secret_alias"):
        await db.audit(request.state.user["username"], "changed password", "connection", conn_id,
                       f"uses stored secret '{req.secret_alias}'" if req.secret_alias else "own password")
    ok, detail = await connectors.test_connection(conn_id)
    await connectors.sync()
    return {**(await db.get_connection(conn_id)), "test_ok": ok, "test_detail": detail}


class ConnectionOwner(BaseModel):
    owner_partner: str | None = None


@app.put("/api/connections/{conn_id}/owner")
async def set_connection_owner(conn_id: str, req: ConnectionOwner):
    """Make an endpoint common (None) or dedicated to one partner."""
    c = await db.get_connection(conn_id)
    if not c:
        raise HTTPException(404, "Connection not found.")
    if req.owner_partner:
        await _partner_or_404(req.owner_partner)
        others = [p for p in (next((x["used_by"] for x in await db.list_connections() if x["id"] == conn_id), []))
                  if p != req.owner_partner]
        if others:
            raise HTTPException(422, f"Also used by {', '.join(others)} - it has to stay common.")
    await db.set_connection_owner(conn_id, req.owner_partner or None)
    return await db.get_connection(conn_id)


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
    import hashlib

    p = await db.get_partner(code)
    offered = hashlib.sha256((api_key or "").encode()).hexdigest()
    if not p or not api_key or not secrets.compare_digest(offered, p.get("api_key_hash") or ""):
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
    if result and result["status"] == "COMPLETED" and p["reply_mode"] == "forward":
        reply = await connectors.reply_for(p, txn_id, result)
        if not reply["ok"] and "body" not in reply:
            return JSONResponse({"transaction_id": txn_id, "status": "FORWARD_FAILED", "error": reply["error"]},
                                status_code=502, headers=headers)
        media = {"xml": "application/xml", "json": "application/json"}.get(reply.get("format")) \
            or reply.get("content_type") or "text/plain"  # 'as is' keeps the target system's own content type
        return Response(reply["body"], status_code=reply["status"], media_type=media,
                        headers={**headers, "X-Status": "RESPONDED", "X-Upstream-Status": str(reply["status"])})
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
    return {k: t[k] for k in ("id", "status", "delivery_status", "output_payload", "response_payload",
                              "upstream_status", "error", "created_at")}


# ---------- built-in test receiver (a webhook target for demos) ----------

_received: deque = deque(maxlen=100)


@app.api_route("/test-receiver/{name:path}", methods=["POST", "PUT"], tags=["test receiver"])
async def test_receiver(name: str, request: Request):
    body = (await request.body()).decode("utf-8", errors="replace")
    at = _now_iso()
    _received.appendleft({"at": at, "name": name, "method": request.method,
                          "headers": {k: v for k, v in request.headers.items()
                                      if k.lower().startswith(("x-", "content-type"))},
                          "body": body})
    # A realistic acknowledgement, so forwarding demos have a response to convert and return.
    return {"status": "ACCEPTED", "reference": f"TR-{secrets.token_hex(4).upper()}",
            "receivedAt": at, "bytes": len(body), "transactionId": request.headers.get("x-transaction-id")}


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


# ---------- scheduler ----------

class ScheduleIn(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    description: str | None = None
    enabled: bool = True
    schedule: dict
    action: dict


def _schedule_view(t: dict) -> dict:
    return {**t, "schedule_text": cron.describe(t["schedule"]), "action_text": scheduler.describe_action(t["action"])}


async def _clean_schedule(req: ScheduleIn) -> dict:
    try:
        spec = cron.validate(req.schedule)
        action = await scheduler.validate_action(req.action)
    except (cron.ScheduleError, scheduler.ScheduleError) as e:
        raise HTTPException(422, str(e)) from e
    nxt = cron.next_run(spec) if req.enabled else None
    if req.enabled and nxt is None:
        raise HTTPException(422, "That time has already passed - choose a time in the future.")
    return {"name": req.name.strip(), "description": req.description, "enabled": req.enabled, "schedule": spec,
            "action": action, "next_run_at": nxt}


@app.get("/api/schedules")
async def list_schedules():
    return {"tasks": [_schedule_view(t) for t in await db.list_schedules()], "local_timezone": cron.local_zone(),
            "workflow_schedules": [{"id": w["id"], "name": w["name"], "active": w["active"],
                                    "every_minutes": (w["trigger_config"] or {}).get("interval_minutes"),
                                    "per_row": bool((w["trigger_config"] or {}).get("query")),
                                    "next_run_at": w.get("next_run_at")}
                                   for w in await db.list_workflows() if w["trigger_type"] == "schedule"]}


@app.post("/api/schedules", status_code=201)
async def create_schedule(req: ScheduleIn):
    sid = _slug(req.name)
    if len(sid) < 2:
        raise HTTPException(422, "Give the task a name with letters or digits.")
    if not await db.create_schedule(sid, **await _clean_schedule(req)):
        raise HTTPException(409, f"A task called '{sid}' already exists.")
    return _schedule_view(await db.get_schedule(sid))


@app.put("/api/schedules/{sid}")
async def update_schedule(sid: str, req: ScheduleIn):
    if not await db.get_schedule(sid):
        raise HTTPException(404, "Task not found.")
    await db.update_schedule(sid, **await _clean_schedule(req))
    return _schedule_view(await db.get_schedule(sid))


class ScheduleState(BaseModel):
    enabled: bool


@app.put("/api/schedules/{sid}/enabled")
async def pause_resume_schedule(sid: str, req: ScheduleState):
    t = await db.get_schedule(sid)
    if not t:
        raise HTTPException(404, "Task not found.")
    nxt = cron.next_run(t["schedule"], last_run=t["last_run_at"] if t["schedule"]["kind"] == "once" else None) \
        if req.enabled else None
    if req.enabled and nxt is None:
        raise HTTPException(422, "This one-off task's time has passed - edit it to choose a new time.")
    await db.update_schedule(sid, enabled=req.enabled, next_run_at=nxt)
    return _schedule_view(await db.get_schedule(sid))


@app.post("/api/schedules/{sid}/run")
async def run_schedule_now(sid: str):
    t = await db.get_schedule(sid)
    if not t:
        raise HTTPException(404, "Task not found.")
    return await scheduler.execute(t, "manual")


@app.get("/api/schedules/{sid}/runs")
async def schedule_runs(sid: str, limit: int = 50):
    return await db.list_schedule_runs(sid, min(limit, 500))


@app.delete("/api/schedules/{sid}", status_code=204)
async def delete_schedule(sid: str):
    await db.delete_schedule(sid)


# ---------- ports ----------

class PortIn(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    type: Literal["http", "file", "listener"]
    enabled: bool = True
    config: dict = Field(default_factory=dict)
    target_type: Literal["partner", "workflow", "endpoints"] = "endpoints"
    target_id: str = ""


async def _clean_port(req: PortIn, pid: str | None = None) -> dict:
    try:
        config = await ports.validate({**req.model_dump(), "id": pid})
    except ports.PortError as e:
        raise HTTPException(422, str(e)) from e
    http = req.type == "http"
    return {"name": req.name.strip(), "type": req.type, "enabled": req.enabled, "config": config,
            "target_type": "endpoints" if http else req.target_type, "target_id": "" if http else req.target_id}


def _listener_app(port: dict, stats: dict) -> FastAPI:
    """What an extra HTTP port serves: the partner endpoints (subject to its access mode) and /health,
    behind its IP access rules and request size limit."""
    import ipaddress

    cfg = port["config"]
    nets = [ipaddress.ip_network(n) for n in cfg.get("ip_list") or []]
    selected = set(cfg.get("partners") or []) if cfg.get("access_mode") == "selected" else None
    sub = FastAPI(title=f"Payload Hub port {port['name']}", docs_url=None, redoc_url=None, openapi_url=None)

    @sub.middleware("http")
    async def guard(request: Request, call_next):
        stats["requests"] += 1
        ip = ipaddress.ip_address(request.client.host) if request.client else None
        listed = ip is not None and any(ip in n for n in nets)
        if (cfg.get("ip_mode") == "allow" and not listed) or (cfg.get("ip_mode") == "deny" and listed):
            stats["rejected"] += 1
            return JSONResponse({"detail": "Access from your address isn't allowed on this port."}, status_code=403)
        parts = request.url.path.split("/")
        if selected is not None and len(parts) > 2 and parts[1] == "inbound" and parts[2] not in selected:
            stats["rejected"] += 1
            return JSONResponse({"detail": "Not found."}, status_code=404)
        if int(request.headers.get("content-length") or 0) > int(cfg.get("max_body_kb") or 10240) * 1024:
            stats["rejected"] += 1
            return JSONResponse({"detail": "The request is too large for this port."}, status_code=413)
        return await call_next(request)

    sub.add_api_route("/inbound/{code}", inbound, methods=["POST"])
    sub.add_api_route("/inbound/{code}/transactions/{txn_id}", inbound_status, methods=["GET"])
    sub.add_api_route("/health", lambda: {"status": "up", "port": port["name"]}, methods=["GET"])
    return sub


ports.http_app_factory = _listener_app


def _port_view(p: dict) -> dict:
    return {**p, "status": ports.status().get(p["id"], {"state": "stopped" if not p["enabled"] else "starting",
                                                         "detail": ""})}


@app.get("/api/ports")
async def list_ports():
    return [_port_view(p) for p in await db.list_ports()]


@app.post("/api/ports", status_code=201)
async def create_port(req: PortIn):
    pid = _slug(req.name)
    if len(pid) < 2:
        raise HTTPException(422, "Give the port a name with letters or digits.")
    if not await db.create_port(pid, **await _clean_port(req, pid)):
        raise HTTPException(409, f"A port called '{pid}' already exists.")
    await ports.sync()
    return _port_view(await db.get_port(pid))


@app.put("/api/ports/{pid}")
async def update_port(pid: str, req: PortIn):
    if not await db.get_port(pid):
        raise HTTPException(404, "Port not found.")
    await db.update_port(pid, **await _clean_port(req, pid))
    await ports.sync()
    return _port_view(await db.get_port(pid))


class PortState(BaseModel):
    enabled: bool


@app.put("/api/ports/{pid}/enabled")
async def enable_port(pid: str, req: PortState):
    if not await db.get_port(pid):
        raise HTTPException(404, "Port not found.")
    await db.update_port(pid, enabled=req.enabled)
    await ports.sync()
    return _port_view(await db.get_port(pid))


@app.post("/api/ports/{pid}/poll")
async def poll_port_now(pid: str):
    if not ports.poll_now(pid):
        raise HTTPException(409, "Only a running file polling port can be polled now.")
    return {"polling": True}


@app.get("/api/ports/{pid}/events")
async def port_events(pid: str, limit: int = 50):
    return await db.list_port_events(pid, min(limit, 500))


@app.delete("/api/ports/{pid}", status_code=204)
async def delete_port(pid: str):
    await db.delete_port(pid)
    await ports.sync()


@app.get("/api/ports-overview")
async def ports_overview(request: Request):
    """Every way data comes in: the HTTP API, partner listeners, workflow triggers and the defined ports."""
    base = str(request.base_url).rstrip("/")
    partners_ = await db.list_partners()
    conns = {c["id"]: c for c in await db.list_connections()}
    rows = [{"kind": "http", "name": "Primary port", "address": base, "target": f"{len(partners_)} partner endpoints "
             f"(POST /inbound/<code>), the admin API and the test receiver", "state": "running", "managed": "built-in"},
            {"kind": "ui", "name": "Web console", "address": APP_URL, "target": "this app", "state": "running",
             "managed": "built-in"}]
    for code, st_ in connectors.listener_status().items():
        c = conns.get(st_["connection"], {})
        rows.append({"kind": c.get("type", "?"), "name": f"Partner {code} inbound", "address": st_["detail"],
                     "target": f"partner {code}", "state": st_["state"], "managed": "partner"})
    triggers = workflows.trigger_status()
    for w in await db.list_workflows():
        if w["trigger_type"] == "database":
            t = triggers.get(w["id"], {"state": "stopped", "detail": ""})
            rows.append({"kind": "database", "name": f"Workflow {w['name']} trigger",
                         "address": t["detail"] or (w["trigger_config"] or {}).get("source", ""),
                         "target": f"workflow {w['id']}", "state": t["state"] if w["active"] else "stopped",
                         "managed": "workflow"})
    for p in await db.list_ports():
        v = _port_view(p)
        cfg = p["config"]
        if p["type"] == "http":
            address = f"{cfg['protocol']}://{cfg['bind']}:{cfg['port']}"
            target = "all partner endpoints" if cfg["access_mode"] == "all" else \
                f"endpoints of {', '.join(cfg['partners'])}"
            kind = cfg["protocol"]
        else:
            address = cfg.get("monitor_dir") if p["type"] == "file" else \
                f"{conns.get(cfg.get('connection'), {}).get('name', cfg.get('connection'))} → {cfg.get('source')}"
            target = f"{p['target_type']} {p['target_id']}"
            kind = "file" if p["type"] == "file" else conns.get(cfg.get("connection"), {}).get("type", "?")
        rows.append({"kind": kind, "name": p["name"], "address": address, "target": target,
                     "state": v["status"]["state"] if p["enabled"] else "disabled", "managed": "port", "id": p["id"]})
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
