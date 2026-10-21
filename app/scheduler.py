"""Scheduler: tasks that run a workflow, send a message to a partner or call an API - every N seconds/minutes/
hours, daily at a time, on a cron expression, or once. Each run is kept in the task's history."""
import asyncio
import logging
import os
import time
from datetime import datetime, timezone

from . import adapters, connectors, cron, db

log = logging.getLogger("scheduler")

TICK_SECONDS = float(os.getenv("SCHEDULER_TICK_SECONDS", "2"))
ACTIONS = ("workflow", "partner", "http", "backup")
_loop: asyncio.Task | None = None
_running: set[asyncio.Task] = set()


class ScheduleError(ValueError):
    pass


async def validate_action(action: dict) -> dict:
    kind = action.get("type")
    if kind not in ACTIONS:
        raise ScheduleError("Choose what the task does.")
    if kind == "workflow":
        if not await db.get_workflow(action.get("workflow_id") or ""):
            raise ScheduleError("Choose a workflow.")
        if not isinstance(action.get("input") or {}, dict):
            raise ScheduleError("The workflow input must be a JSON object.")
        return {"type": kind, "workflow_id": action["workflow_id"], "input": action.get("input") or {}}
    if kind == "backup":
        return {"type": kind}
    if kind == "partner":
        p = await db.get_partner(action.get("partner_code") or "")
        if not p:
            raise ScheduleError("Choose a partner.")
        if not (action.get("payload") or p.get("sample_input")):
            raise ScheduleError("Give the message to send (the partner has no sample input).")
        return {"type": kind, "partner_code": p["code"], "payload": action.get("payload") or None}
    conn = await db.get_connection(action.get("connection") or "")
    if not conn or conn["type"] != "http":
        raise ScheduleError("Choose an HTTP connection to call.")
    method = (action.get("method") or "GET").upper()
    if method not in ("GET", "POST", "PUT", "PATCH", "DELETE"):
        raise ScheduleError("Unknown HTTP method.")
    return {"type": kind, "connection": conn["id"], "method": method, "path": action.get("path") or "",
            "body": action.get("body") or "", "content_type": action.get("content_type") or "application/json"}


def describe_action(action: dict) -> str:
    kind = action.get("type")
    if kind == "workflow":
        return f"⚙️ run workflow {action.get('workflow_id')}"
    if kind == "partner":
        return f"🤝 send a message to {action.get('partner_code')}"
    if kind == "backup":
        return "💾 back up the database"
    if kind == "http":
        return f"🌐 {action.get('method')} {action.get('connection')}/{(action.get('path') or '').lstrip('/')}"
    return "?"


async def _perform(task: dict) -> tuple[str, str, str | None]:
    """Run the task's action once. Returns (status, detail, reference)."""
    from . import workflows

    action, source = task["action"], f"scheduler:{task['id']}"
    if action["type"] == "workflow":
        wf = await db.get_workflow(action["workflow_id"])
        if not wf:
            return "FAILED", f"workflow '{action['workflow_id']}' no longer exists", None
        if not wf["active"]:
            return "SKIPPED", f"workflow '{wf['name']}' is paused", None
        run = await workflows.run(wf, action.get("input") or {}, source)
        steps = len(run["steps_log"])
        status = {"COMPLETED": "OK", "STOPPED": "OK"}.get(run["status"], "FAILED")
        return status, run.get("error") or f"{run['status'].lower()} · {steps} step(s) · {run['duration_ms']} ms", \
            str(run["id"])
    if action["type"] == "backup":
        from . import alerts, backup

        try:
            m = await asyncio.to_thread(backup.create, f"scheduled task {task['id']}")
        except backup.BackupError as e:
            alerts.emit("backup_failed", "Scheduled database backup failed", str(e))
            return "FAILED", str(e), None
        return "OK", f"{m['file']} · {m['size'] / 1e6:.1f} MB", m["file"]
    if action["type"] == "partner":
        p = await db.get_partner(action["partner_code"])
        if not p:
            return "FAILED", f"partner '{action['partner_code']}' no longer exists", None
        try:
            txn_id, result = await connectors.ingest(p, action.get("payload") or p["sample_input"], "SCHEDULER",
                                                     wait=30)
        except connectors.IngestError as e:
            return "FAILED", str(e), None
        if result is None:
            return "ACCEPTED", "queued - still processing after 30 s", txn_id
        if result["status"] == "COMPLETED":
            return "OK", f"transformed for {p['name']}", txn_id
        return "FAILED", result.get("error") or result["status"], txn_id
    adapter = await connectors.adapter_for(action["connection"])
    try:
        status, _ctype, body, url = await adapter.request(action.get("path") or "", (action.get("body") or "").encode(),
                                                          {"X-Scheduled-Task": task["id"]},
                                                          action.get("content_type") or "application/json",
                                                          action.get("method"))
    except adapters.AdapterError as e:
        return "FAILED", str(e), None
    return ("OK" if status < 400 else "FAILED"), f"{action.get('method')} {url} → HTTP {status} · {len(body)} bytes", None


async def execute(task: dict, trigger: str) -> dict:
    """Run a task now and record it. trigger: 'schedule' or 'manual'."""
    run_id = await db.start_schedule_run(task["id"], trigger)
    started = time.perf_counter()
    try:
        status, detail, ref = await _perform(task)
    except Exception as e:  # noqa: BLE001 - recorded on the task
        log.exception("scheduled task %s failed", task["id"])
        status, detail, ref = "FAILED", f"{type(e).__name__}: {e}", None
    ms = round((time.perf_counter() - started) * 1000, 1)
    await db.finish_schedule_run(run_id, status, (detail or "")[:2000], ref, ms)
    from . import metrics

    metrics.SCHEDULE_RUNS.labels(task["id"], status).inc()
    if status == "FAILED":
        from . import alerts

        alerts.emit("schedule_failed", f"Scheduled task {task['name']} failed", detail or "", task=task["id"])
    now = datetime.now(timezone.utc)
    fields = {"last_run_at": now, "last_status": status, "last_detail": (detail or "")[:500],
              "run_count": task["run_count"] + 1, "fail_count": task["fail_count"] + (status == "FAILED")}
    if trigger == "schedule":
        nxt = cron.next_run(task["schedule"], now, last_run=task["next_run_at"])
        fields.update(next_run_at=nxt, running=False)
        if nxt is None:
            fields["enabled"] = False  # a one-off task has done its job
    await db.update_schedule(task["id"], **fields)
    return {"status": status, "detail": detail, "ref": ref, "duration_ms": ms}


async def _tick():
    while True:
        try:
            for task in await db.claim_due_schedules():
                t = asyncio.create_task(execute(task, "schedule"), name=f"schedule-{task['id']}")
                _running.add(t)
                t.add_done_callback(_running.discard)
        except Exception:  # noqa: BLE001
            log.exception("scheduler tick")
        await asyncio.sleep(TICK_SECONDS)


def next_run_for(task: dict) -> datetime | None:
    return cron.next_run(task["schedule"]) if task.get("enabled", True) else None


async def start():
    global _loop
    await db.reset_running_schedules()
    _loop = asyncio.create_task(_tick(), name="scheduler")


async def stop():
    if _loop:
        _loop.cancel()
    for t in list(_running):
        t.cancel()
