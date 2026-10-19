"""FastAPI app: REST API, monitoring endpoints and the web UI."""
import logging
import uuid
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from . import agent, db, mapping_doc, pipeline
from .transform import SAMPLES

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
    payload: str = Field(min_length=1)
    input_format: Literal["json", "xml"]
    output_format: Literal["json", "xml"]
    expression: str = Field(min_length=1)
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


@app.post("/api/transform")
async def submit_transform(req: TransformRequest):
    txn_id = str(uuid.uuid4())
    await db.create_transaction(txn_id, req.mapping_name, req.input_format, req.output_format, req.expression, req.payload)
    result = await pipeline.submit(txn_id, {"id": txn_id, **req.model_dump()})
    return result or await db.get_transaction(txn_id)


@app.get("/api/transactions")
async def transactions(limit: int = 50, status: str | None = None, q: str | None = None,
                       since: datetime | None = None, until: datetime | None = None):
    """Transaction log. q searches transaction id, mapping name, input, output and error text."""
    return await db.list_transactions(min(limit, 500), status, (q or "").strip() or None, since, until)


@app.get("/api/transactions/{txn_id}")
async def transaction(txn_id: uuid.UUID):
    txn = await db.get_transaction(txn_id)
    if not txn:
        raise HTTPException(404, "Transaction not found")
    return txn


@app.get("/api/stats")
async def stats():
    return await db.stats()


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
