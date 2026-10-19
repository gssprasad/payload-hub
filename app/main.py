"""FastAPI app: REST API, monitoring endpoints and the web UI."""
import logging
import os
import re
import uuid
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from . import agent, db, mapping_doc, pipeline
from .transform import SAMPLES, TransformError, parse, transform

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_app):
    await db.init()
    await pipeline.start()
    yield
    await pipeline.stop()
    await db.close()


app = FastAPI(title="Payload Hub", lifespan=lifespan)


class TransformRequest(BaseModel):
    """Either partner_code (uses the partner's mapping and queue) or an ad-hoc expression + formats."""
    payload: str = Field(min_length=1)
    partner_code: str | None = None
    input_format: Literal["json", "xml"] | None = None
    output_format: Literal["json", "xml"] | None = None
    expression: str | None = None
    mapping_name: str = "custom"


@app.get("/")
async def index():
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
        if not p["active"]:
            raise HTTPException(409, f"Partner '{p['name']}' is inactive.")
        if not p["expression"]:
            raise HTTPException(409, f"Partner '{p['name']}' has no mapping yet - set one on the Partners page.")
        msg = {"mapping_name": p["code"], "input_format": p["input_format"], "output_format": p["output_format"],
               "expression": p["expression"], "topic_in": p["topic_in"], "topic_out": p["topic_out"],
               "topic_dlq": p["topic_dlq"], "partner_code": p["code"]}
    else:
        if not (req.input_format and req.output_format and req.expression):
            raise HTTPException(422, "Give a partner_code, or input_format, output_format and expression.")
        msg = {"mapping_name": req.mapping_name, "input_format": req.input_format, "output_format": req.output_format,
               "expression": req.expression, "topic_in": pipeline.TOPIC_IN, "topic_out": pipeline.TOPIC_OUT,
               "topic_dlq": pipeline.TOPIC_DLQ, "partner_code": None}
    txn_id = str(uuid.uuid4())
    await db.create_transaction(txn_id, msg["mapping_name"], msg["input_format"], msg["output_format"],
                                msg["expression"], req.payload, msg["partner_code"], msg["topic_in"])
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
    fields = req.model_dump(exclude={"code"})
    fields["mapping_source"] = "manual" if req.expression else None
    if not await db.create_partner(code, **fields, **topics):
        raise HTTPException(409, f"A partner with code '{code}' already exists.")
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
