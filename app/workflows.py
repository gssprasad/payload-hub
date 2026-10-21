"""Workflows: multi-step runs where each step can use the results of the earlier ones.

A run has a context: {"trigger": <the record or input that started it>, "<save_as>": <step output>, ...}.
Steps:
    db_query   SELECT on a database connection (or the built-in data area); values bound as parameters
    db_update  INSERT / UPDATE / DELETE (skipped in a dry run)
    api_call   HTTP request through an HTTP connection, with its retry policy
    transform  JMESPath over the context
    condition  JMESPath test; when false the run stops (status STOPPED)
    pdf        Jinja2 HTML template -> PDF, stored with the run
    deliver    send a generated file through any adapter (skipped in a dry run)
Triggers: manual (button / API), schedule (every N minutes, optionally one run per row of a query),
and database (one run per new row of table:key_column).
"""
import asyncio
import io
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone

import jmespath
from jinja2 import StrictUndefined
from jinja2.sandbox import SandboxedEnvironment

from . import adapters, connectors, datastore, db
from .transform import _OPTIONS

log_ = log = logging.getLogger("workflows")

STEP_TYPES = ("db", "db_query", "db_update", "api_call", "transform", "branch", "condition", "pdf", "deliver")
DB_OPS = ("select_one", "select_all", "insert", "update", "delete")
NO_RESULT = ("condition", "deliver", "db_update", "branch")
NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,40}$")
MAX_PARALLEL = 4

_env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=False)
_html_env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=True)
_env.filters["money"] = _html_env.filters["money"] = lambda v, cur="": f"{cur}{float(v or 0):,.2f}".strip()
_slots = asyncio.Semaphore(MAX_PARALLEL)
_listeners: dict[str, dict] = {}
_scheduler: asyncio.Task | None = None


class StepError(Exception):
    pass


class StopRun(Exception):
    pass


# ---------- helpers ----------

def _jsonable(value):
    return json.loads(json.dumps(value, default=adapters._json_value))


def render(template: str, ctx: dict) -> str:
    return _env.from_string(template or "").render(**ctx)


def value(template, ctx: dict):
    """'{{ expr }}' alone keeps the value's type (number, list...); anything else renders to text."""
    if not isinstance(template, str):
        return template
    m = re.fullmatch(r"\s*\{\{(.+?)\}\}\s*", template, flags=re.S)
    if m:
        return _env.compile_expression(m.group(1).strip(), undefined_to_none=False)(**ctx)
    return render(template, ctx)


async def _database(conn_id: str) -> adapters.Database:
    if not conn_id or conn_id == "builtin":
        return datastore.builtin()
    conn = await db.get_connection(conn_id)
    if not conn or conn["type"] != "database":
        raise StepError(f"'{conn_id}' isn't a database connection.")
    return await connectors.adapter_for(conn_id)


def validate(steps: list[dict], names: set | None = None, where: str = "") -> list[dict]:
    """Check a workflow's steps (including the steps inside branches) before saving. Returns cleaned steps."""
    names = set() if names is None else names
    out = []
    for i, raw in enumerate(steps or [], start=1):
        s = {k: v for k, v in raw.items() if not k.startswith("_")}  # drop UI-only keys
        t, here = s.get("type"), f"{where}step {i}"
        if t not in STEP_TYPES:
            raise StepError(f"{here}: unknown type '{t}'.")
        name = (s.get("save_as") or "").strip()
        needs_name = t not in NO_RESULT and not (t == "db" and s.get("operation") in ("insert", "update", "delete"))
        if needs_name and not NAME.match(name):
            raise StepError(f"{here} ({t}): give it a name to save its result under (letters, digits, _).")
        if name and not NAME.match(name):
            raise StepError(f"{here}: '{name}' isn't a valid name (letters, digits, _).")
        if name in names:
            raise StepError(f"{here}: the name '{name}' is already used by another step.")
        if name == "trigger":
            raise StepError(f"{here}: 'trigger' is reserved for the record that starts the run.")
        if name:
            names.add(name)
        if t == "db":
            if s.get("operation") not in DB_OPS:
                raise StepError(f"{here}: choose what the database step does.")
            if not s.get("table"):
                raise StepError(f"{here}: choose a table.")
            if s["operation"] in ("update", "delete") and not s.get("where") and not s.get("allow_all_rows"):
                raise StepError(f"{here}: {s['operation']} without conditions would change every row - add a "
                                "condition or tick 'all rows'.")
        if t == "branch":
            if not (s.get("expression") or "").strip():
                raise StepError(f"{here}: the branch needs a condition.")
            s["then"] = validate(s.get("then") or [], names, f"{here} → then → ")
            s["else"] = validate(s.get("else") or [], names, f"{here} → else → ")
        for k in ("retry_attempts", "retry_delay"):
            if k in s and s[k] is not None:
                try:
                    s[k] = max(0, int(s[k]))
                except (TypeError, ValueError) as e:
                    raise StepError(f"{here}: {k.replace('_', ' ')} must be a number.") from e
        out.append({**s, "save_as": name or None})
    return out


# ---------- step implementations ----------

async def _db_query(cfg, ctx, dry):
    database = await _database(cfg.get("connection"))
    params = {k: value(v, ctx) for k, v in (cfg.get("params") or {}).items()}
    result = await database.execute(cfg.get("sql") or "", params, write=False)
    rows = _jsonable(result["rows"])
    if (cfg.get("mode") or "one") == "all":
        return rows
    if not rows:
        if cfg.get("required", True):
            raise StepError("The query returned no row.")
        return None
    return rows[0]


async def _db(cfg, ctx, dry):
    """The no-SQL database connector: select / insert / update / delete built from the step's settings."""
    database = await _database(cfg.get("connection"))
    spec = dict(cfg)
    spec["where"] = [{**w, "value": value(w.get("value", ""), ctx)} for w in cfg.get("where") or []]
    spec["values"] = {k: value(v, ctx) for k, v in (cfg.get("values") or {}).items() if str(v).strip()}
    op = cfg.get("operation")
    if dry and op in ("insert", "update", "delete"):
        return {"skipped": "dry run", "would_run": await database.preview_structured(cfg),
                "values": _jsonable(spec["values"]), "where": _jsonable(spec["where"])}
    result = await database.run_structured(spec)
    if op == "select_all":
        return _jsonable(result["rows"])
    if op == "select_one":
        if not result["rows"]:
            if cfg.get("required", True):
                raise StepError("No matching row.")
            return None
        return _jsonable(result["rows"][0])
    return {k: v for k, v in result.items() if k != "rows"}


async def _db_update(cfg, ctx, dry):
    params = {k: value(v, ctx) for k, v in (cfg.get("params") or {}).items()}
    if dry:
        return {"skipped": "dry run", "would_run": cfg.get("sql"), "params": _jsonable(params)}
    database = await _database(cfg.get("connection"))
    result = await database.execute(cfg.get("sql") or "", params, write=True)
    return {k: v for k, v in result.items() if k != "rows"}


async def _api_call(cfg, ctx, dry):
    conn = await db.get_connection(cfg.get("connection") or "")
    if not conn or conn["type"] != "http":
        raise StepError("Choose an HTTP connection for the API call.")
    adapter = await connectors.adapter_for(conn["id"])
    method = (cfg.get("method") or "GET").upper()
    path = render(cfg.get("path") or "", ctx)
    body = render(cfg["body"], ctx).encode() if cfg.get("body") and method not in ("GET", "HEAD") else b""
    content_type = cfg.get("content_type") or "application/json"
    policy, retry_on = adapters.retry_policy(adapter.config), adapter._retry_statuses()
    status = text = ctype = url = None
    for attempt in range(1, policy["attempts"] + 1):
        try:
            status, ctype, text, url = await adapter.request(path, body, {"X-Workflow-Run": ctx["_run_id"]},
                                                             content_type, method=method)
        except adapters.AdapterError as e:
            if attempt == policy["attempts"]:
                raise StepError(str(e)) from e
            status = None
        if status is not None and status not in retry_on:
            break
        if attempt < policy["attempts"]:
            await asyncio.sleep(min(policy["backoff"] * 2 ** (attempt - 1), policy["backoff_max"]))
    data = text
    for parse in (json.loads, lambda t: __import__("xmltodict").parse(t)):
        try:
            data = parse(text)
            break
        except Exception:  # noqa: BLE001
            continue
    if status >= 400 and cfg.get("fail_on_error", True):
        raise StepError(f"{method} {url} answered HTTP {status}: {str(text)[:200]}")
    return {"status": status, "data": data}


async def _transform(cfg, ctx, dry):
    try:
        return _jsonable(jmespath.search(cfg.get("expression") or "@", _public(ctx), options=_OPTIONS))
    except jmespath.exceptions.JMESPathError as e:
        raise StepError(f"Expression error: {e}") from e


async def _condition(cfg, ctx, dry):
    try:
        ok = jmespath.search(cfg.get("expression") or "`true`", _public(ctx), options=_OPTIONS)
    except jmespath.exceptions.JMESPathError as e:
        raise StepError(f"Expression error: {e}") from e
    if not ok:
        raise StopRun(cfg.get("message") or f"Condition not met: {cfg.get('expression')}")
    return True


def make_pdf(html: str) -> bytes:
    from xhtml2pdf import pisa

    buf = io.BytesIO()
    result = pisa.CreatePDF(html, dest=buf, encoding="utf-8")
    if result.err:
        raise StepError("The PDF template has HTML the PDF engine couldn't lay out.")
    return buf.getvalue()


async def _pdf(cfg, ctx, dry):
    try:
        html = _html_env.from_string(cfg.get("template") or "").render(**_public(ctx))
        filename = render(cfg.get("filename") or "document.pdf", ctx)
    except Exception as e:  # noqa: BLE001 - template errors
        raise StepError(f"Template error: {e}") from e
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    data = await asyncio.to_thread(make_pdf, html)
    file_id = await db.add_file(ctx["_run_id"], filename, "application/pdf", data)
    return {"file_id": file_id, "filename": filename, "size": len(data)}


async def _deliver(cfg, ctx, dry):
    ref = ctx.get(cfg.get("file") or "")
    if not isinstance(ref, dict) or "file_id" not in ref:
        raise StepError(f"'{cfg.get('file')}' isn't the name of a PDF step's result.")
    target = render(cfg.get("target") or "", ctx)
    if dry:
        return {"skipped": "dry run", "would_send": ref["filename"], "to": f"{cfg.get('connection')}:{target}"}
    f = await db.get_file(ref["file_id"])
    try:
        adapter = await connectors.adapter_for(cfg.get("connection") or "")
        detail = await adapter.send(target, bytes(f["content"]),
                                    {"X-Filename": f["filename"], "X-Workflow-Run": ctx["_run_id"]},
                                    f["content_type"])
    except adapters.AdapterError as e:
        raise StepError(str(e)) from e
    return {"delivered": detail}


STEPS = {"db": _db, "db_query": _db_query, "db_update": _db_update, "api_call": _api_call, "transform": _transform,
         "condition": _condition, "pdf": _pdf, "deliver": _deliver}


def _public(ctx: dict) -> dict:
    return {k: v for k, v in ctx.items() if not k.startswith("_")}


# ---------- running ----------

class _Outcome:
    def __init__(self):
        self.status, self.error, self.failed = "COMPLETED", None, None


async def _run_steps(steps, ctx, dry, log, outcome, run_id, wf_id, path="", depth=0, start_at=0):
    """Run a list of steps (recursing into branches). Stops the list when the run fails or stops."""
    for i, step in enumerate(steps):
        here = f"{path}{i + 1}"
        label = step.get("label") or step["type"]
        top = int(here.split(".")[0]) - 1
        if depth == 0 and i < start_at:
            log.append({"index": top, "path": here, "depth": depth, "label": label, "type": step["type"], "status": "REUSED"})
            continue
        entry = {"index": top, "path": here, "depth": depth, "label": label, "type": step["type"],
                 "save_as": step.get("save_as")}
        t0 = time.perf_counter()
        if step.get("disabled"):
            entry.update(status="SKIPPED", output="Switched off", ms=0)
            log.append(entry)
            continue
        if step["type"] == "branch":
            try:
                ok = bool(jmespath.search(step["expression"], _public(ctx), options=_OPTIONS))
            except jmespath.exceptions.JMESPathError as e:
                entry.update(status="FAILED", error=f"Condition error: {e}", ms=0)
                log.append(entry)
                outcome.status, outcome.error, outcome.failed = "FAILED", entry["error"], top
                return
            side = "then" if ok else "else"
            entry.update(status="OK", output=f"condition is {str(ok).lower()} → {side}", ms=0)
            log.append(entry)
            await _run_steps(step.get(side) or [], ctx, dry, log, outcome, run_id, wf_id,
                             f"{here}.{side}.", depth + 1)
            if outcome.status != "COMPLETED":
                return
            continue
        attempts = max(1, int(step.get("retry_attempts") or 1))
        delay = max(0, int(step.get("retry_delay") or 0))
        for attempt in range(1, attempts + 1):
            try:
                out = await STEPS[step["type"]](step, ctx, dry)
                if step.get("save_as"):
                    ctx[step["save_as"]] = out
                entry.update(status="SKIPPED" if isinstance(out, dict) and "skipped" in out else "OK",
                             output=_preview(out), attempts=attempt)
                break
            except StopRun as e:
                entry.update(status="STOPPED", output=str(e), attempts=attempt)
                outcome.status, outcome.error = "STOPPED", str(e)
                break
            except Exception as e:  # noqa: BLE001
                msg = str(e) if isinstance(e, (StepError, adapters.AdapterError)) else f"{type(e).__name__}: {e}"
                if not isinstance(e, (StepError, adapters.AdapterError)):
                    log_.exception("workflow %s step %s crashed", wf_id, here)
                if attempt < attempts:
                    wait = delay * (2 ** (attempt - 1)) if step.get("retry_backoff", True) else delay
                    entry.setdefault("retries", []).append(f"attempt {attempt}: {msg[:200]} - retrying in {wait} s")
                    await asyncio.sleep(wait)
                    continue
                entry.update(error=msg, attempts=attempt)
                if step.get("on_error") == "continue":
                    entry["status"] = "FAILED_CONTINUED"
                    if step.get("save_as"):
                        ctx[step["save_as"]] = None
                else:
                    entry["status"] = "FAILED"
                    outcome.status, outcome.error, outcome.failed = "FAILED", msg, top
        entry["ms"] = round((time.perf_counter() - t0) * 1000, 1)
        log.append(entry)
        await db.update_run(run_id, steps_log=log, context=_jsonable(_public(ctx)))
        if outcome.status != "COMPLETED":
            return


async def run(wf: dict, input_: dict | None, source: str, dry_run: bool = False, start_at: int = 0,
              context: dict | None = None) -> dict:
    run_id = str(uuid.uuid4())
    input_ = _jsonable(input_ or {})
    await db.create_run(run_id, wf["id"], source, dry_run, input_)
    ctx = {**(context or {}), "trigger": input_, "_run_id": run_id}
    steps_log, outcome = [], _Outcome()
    started = time.perf_counter()
    async with _slots:
        await _run_steps(wf["steps"], ctx, dry_run, steps_log, outcome, run_id, wf["id"], start_at=start_at)
    await db.update_run(run_id, status=outcome.status, error=outcome.error, failed_step=outcome.failed,
                        steps_log=steps_log, context=_jsonable(_public(ctx)), finished_at=datetime.now(timezone.utc),
                        duration_ms=round((time.perf_counter() - started) * 1000, 1))
    from . import metrics

    metrics.WORKFLOW_RUNS.labels(wf["id"], outcome.status).inc()
    metrics.WORKFLOW_SECONDS.labels(wf["id"]).observe(time.perf_counter() - started)
    if outcome.status == "FAILED":
        from . import alerts

        alerts.emit("workflow_failed", f"Workflow {wf.get('name', wf['id'])} failed", outcome.error or "",
                    workflow=wf["id"], run=run_id)
    return await db.get_run(run_id)


def _preview(out, limit: int = 4000):
    text = json.dumps(out, default=adapters._json_value, indent=1)
    return text if len(text) <= limit else text[:limit] + "\n… (truncated)"


async def rerun(run_id: str) -> dict:
    """Run again from the failed step, reusing the results of the steps before it."""
    old = await db.get_run(run_id)
    if not old:
        raise StepError("Run not found.")
    wf = await db.get_workflow(old["workflow_id"])
    start = old["failed_step"] or 0
    context = {k: v for k, v in (old["context"] or {}).items() if k != "trigger"}
    return await run(wf, old["input"], f"rerun of {str(old['id'])[:8]}", old["dry_run"], start, context)


# ---------- triggers ----------

async def sync():
    """Start or stop database-trigger listeners to match the workflows (each trigger runs on one instance)."""
    from . import cluster

    wanted = {w["id"]: w for w in await db.list_workflows()
              if w["active"] and w["trigger_type"] == "database"}
    for wf_id in list(_listeners):
        entry = _listeners[wf_id]
        key = (wanted.get(wf_id) or {}).get("trigger_config")
        standby = entry["state"] == "standby"
        if key != entry["key"] or standby or (entry.get("listener") and not cluster.holds(f"trigger:{wf_id}")):
            _listeners.pop(wf_id)
            if entry.get("listener"):
                await entry["listener"].stop()
                await cluster.release(f"trigger:{wf_id}")
    for wf_id, w in wanted.items():
        if wf_id in _listeners:
            continue
        cfg = w["trigger_config"] or {}
        entry = {"key": cfg, "listener": None, "state": "starting", "detail": ""}
        _listeners[wf_id] = entry
        if not await cluster.own(f"trigger:{wf_id}"):
            entry.update(state="standby", detail="watched by another instance")
            continue
        try:
            database = await _database(cfg.get("connection"))

            async def handler(body, meta, wf_id=wf_id):
                wf = await db.get_workflow(wf_id)
                if wf and wf["active"]:
                    await run(wf, json.loads(body), meta.get("source", "database"))

            entry["listener"] = await database.listen(cfg.get("source") or "", handler)
            entry.update(state="running", detail=entry["listener"].describe)
        except Exception as e:  # noqa: BLE001
            entry.update(state="error", detail=str(e) or type(e).__name__)
            await cluster.release(f"trigger:{wf_id}")


async def _schedule_loop():
    rounds = 0
    while True:
        await asyncio.sleep(20)
        rounds += 1
        try:
            for wf in await db.claim_due_workflows():  # claimed atomically: one instance runs each due workflow
                cfg = wf["trigger_config"] or {}
                if cfg.get("query"):
                    database = await _database(cfg.get("connection"))
                    rows = (await database.execute(cfg["query"], {}, write=False))["rows"]
                    for row in rows[: int(cfg.get("max_rows") or 100)]:
                        asyncio.create_task(run(wf, row, "schedule"))
                else:
                    asyncio.create_task(run(wf, {}, "schedule"))
            if rounds % 3 == 0:
                await sync()  # take over triggers another instance gave up
        except Exception:  # noqa: BLE001
            log.exception("workflow scheduler")


def trigger_status() -> dict:
    return {k: {"state": v["state"], "detail": v["detail"]} for k, v in _listeners.items()}


async def start():
    global _scheduler
    await sync()
    _scheduler = asyncio.create_task(_schedule_loop(), name="workflow-scheduler")


async def stop():
    if _scheduler:
        _scheduler.cancel()
    for entry in _listeners.values():
        if entry.get("listener"):
            await entry["listener"].stop()
