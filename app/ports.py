"""Ports: the ways data comes in, beyond a partner's own HTTP endpoint.

- file: a file polling port. Watches a folder on this server; each new file (matching the filter and older than
  the minimum age, so half-written files are left alone) is moved to the work folder, handed to the target, then
  moved to the completion folder (or deleted) - or to the error folder, with a note saying why.
- listener: a listener on any inbound connection (MQ queue, Kafka topic, FTP/SFTP folder, database table).

A port feeds a partner (the content becomes the partner's input payload) or a workflow (the content becomes
the run's `trigger`).
"""
import asyncio
import csv
import fnmatch
import io
import json
import logging
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import xmltodict

from . import connectors, db

log = logging.getLogger("ports")

TYPES = ("file", "listener")
TARGETS = ("partner", "workflow")
FILE_DEFAULTS = {"filter": "*", "interval_seconds": 10, "min_age_seconds": 5, "max_files": 20, "max_size_mb": 10,
                 "after_success": "move", "encoding": "utf-8"}
SUPERVISE_SECONDS = 30
_running: dict[str, dict] = {}
_lock = asyncio.Lock()
_supervisor: asyncio.Task | None = None


class PortError(ValueError):
    pass


def _allowed_roots() -> list[Path]:
    """FILE_POLLING_ROOTS (separated by ':') limits which server folders ports may watch. Empty = any."""
    return [Path(p).expanduser().resolve() for p in os.getenv("FILE_POLLING_ROOTS", "").split(os.pathsep) if p]


def _folder(path: str, what: str) -> str:
    if not path or not os.path.isabs(os.path.expanduser(path)):
        raise PortError(f"The {what} must be a full path, e.g. /data/inbound.")
    resolved = Path(path).expanduser().resolve()
    roots = _allowed_roots()
    if roots and not any(resolved == r or r in resolved.parents for r in roots):
        raise PortError(f"The {what} must be inside {', '.join(map(str, roots))} (FILE_POLLING_ROOTS).")
    return str(resolved)


async def validate(port: dict) -> dict:
    """Check a port definition; returns the cleaned config."""
    if port.get("type") not in TYPES:
        raise PortError("Choose the kind of port.")
    if port.get("target_type") not in TARGETS:
        raise PortError("Choose whether the port feeds a partner or a workflow.")
    if port["target_type"] == "partner" and not await db.get_partner(port.get("target_id") or ""):
        raise PortError("Choose the partner the port feeds.")
    if port["target_type"] == "workflow" and not await db.get_workflow(port.get("target_id") or ""):
        raise PortError("Choose the workflow the port starts.")
    cfg = dict(port.get("config") or {})
    if port["type"] == "listener":
        conn = await db.get_connection(cfg.get("connection") or "")
        if not conn:
            raise PortError("Choose the connection to listen on.")
        from .adapters import TYPES as CONNECTION_TYPES

        if not CONNECTION_TYPES[conn["type"]]["inbound"]:
            raise PortError(f"A {conn['type']} connection can't receive messages.")
        if not (cfg.get("source") or "").strip():
            raise PortError("Give the queue, topic, folder or table to listen on.")
        return {"connection": conn["id"], "source": cfg["source"].strip()}
    out = {k: cfg.get(k, v) for k, v in FILE_DEFAULTS.items()}
    out["monitor_dir"] = _folder(cfg.get("monitor_dir", ""), "monitoring folder")
    for key, sub in (("work_dir", ".work"), ("completion_dir", "done"), ("error_dir", "error")):
        out[key] = _folder(cfg.get(key) or os.path.join(out["monitor_dir"], sub), key.replace("_dir", " folder"))
    if len({out["monitor_dir"], out["work_dir"], out["completion_dir"], out["error_dir"]}) < 4:
        raise PortError("The monitoring, work, completion and error folders must all be different.")
    try:
        for k in ("interval_seconds", "min_age_seconds", "max_files", "max_size_mb"):
            out[k] = int(out[k])
    except (TypeError, ValueError) as e:
        raise PortError("Interval, minimum age, files per poll and maximum size must be numbers.") from e
    if out["interval_seconds"] < 2:
        raise PortError("Poll at most every 2 seconds.")
    if out["after_success"] not in ("move", "delete"):
        raise PortError("After a file is processed it is moved to the completion folder or deleted.")
    out["filter"] = (out["filter"] or "*").strip()
    return out


# ---------- handing content to the target ----------

def _parse_file(name: str, text: str):
    """Structured data from a file, when its type is known: JSON, XML, or CSV (a list of rows)."""
    ext = name.lower().rsplit(".", 1)[-1] if "." in name else ""
    try:
        if ext == "json":
            return json.loads(text)
        if ext == "xml":
            return xmltodict.parse(text)
        if ext == "csv":
            sample = text[:4096]
            dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t") if sample.strip() else csv.excel
            return list(csv.DictReader(io.StringIO(text), dialect=dialect))
    except Exception:  # noqa: BLE001 - the raw text is still passed on
        return None
    return None


async def dispatch(port: dict, text: str, item: str, extra: dict | None = None) -> tuple[str, str, str | None]:
    """Give one file or message to the port's target. Returns (status, detail, reference)."""
    from . import workflows

    data = _parse_file(item, text) if port["type"] == "file" else None
    if port["target_type"] == "partner":
        p = await db.get_partner(port["target_id"])
        if not p:
            return "FAILED", f"partner '{port['target_id']}' no longer exists", None
        payload = text
        if isinstance(data, list):  # CSV rows -> the partner's input format
            payload = json.dumps({"rows": data}) if p["input_format"] == "json" \
                else xmltodict.unparse({"rows": {"row": data}}, pretty=True)
        try:
            txn_id, result = await connectors.ingest(p, payload, f"PORT:{port['id']}", wait=30)
        except connectors.IngestError as e:
            return "FAILED", str(e), None
        if result is None:
            return "ACCEPTED", "queued - still processing after 30 s", txn_id
        if result["status"] == "COMPLETED":
            return "OK", f"transformed for {p['name']}", txn_id
        return "FAILED", result.get("error") or result["status"], txn_id
    wf = await db.get_workflow(port["target_id"])
    if not wf:
        return "FAILED", f"workflow '{port['target_id']}' no longer exists", None
    if port["type"] == "file":
        trigger = {"file": {"name": item, "size": len(text.encode()), "content": text, "data": data, **(extra or {})}}
    else:
        try:
            parsed = json.loads(text)
        except ValueError:
            parsed = None
        trigger = parsed if isinstance(parsed, dict) else {"message": text, "data": parsed, **(extra or {})}
    run = await workflows.run(wf, trigger, f"port:{port['id']}")
    status = "OK" if run["status"] in ("COMPLETED", "STOPPED") else "FAILED"
    return status, run.get("error") or f"workflow {run['status'].lower()}", str(run["id"])


# ---------- file polling ----------

def _due_files(cfg: dict) -> list[os.DirEntry]:
    now = time.time()
    patterns = [p.strip() for p in cfg["filter"].split(";") if p.strip()] or ["*"]
    out = []
    with os.scandir(cfg["monitor_dir"]) as it:
        for e in it:
            if not e.is_file() or e.name.startswith("."):
                continue
            if not any(fnmatch.fnmatch(e.name.lower(), p.lower()) for p in patterns):
                continue
            if now - e.stat().st_mtime < cfg["min_age_seconds"]:
                continue  # may still be being written
            out.append(e)
    out.sort(key=lambda e: e.stat().st_mtime)
    return out[: cfg["max_files"]]


def _stamp(name: str) -> str:
    return f"{datetime.now().strftime('%Y%m%dT%H%M%S%f')[:-3]}_{name}"


async def _process_file(port: dict, entry: os.DirEntry):
    cfg = port["config"]
    name = entry.name
    modified = datetime.fromtimestamp(entry.stat().st_mtime, timezone.utc).isoformat()
    work = os.path.join(cfg["work_dir"], _stamp(name))
    try:
        await asyncio.to_thread(os.replace, entry.path, work)  # claim it - another poller won't see it
    except FileNotFoundError:
        return
    size = os.path.getsize(work)
    status, detail, ref = "FAILED", "", None
    if size > cfg["max_size_mb"] * 1024 * 1024:
        detail = f"file is {size / 1024 / 1024:.1f} MB - the limit is {cfg['max_size_mb']} MB"
    else:
        try:
            raw = await asyncio.to_thread(Path(work).read_bytes)
            text = raw.decode(cfg.get("encoding") or "utf-8")
            status, detail, ref = await dispatch(port, text, name, {"modified": modified})
        except UnicodeDecodeError:
            detail = f"not {cfg.get('encoding') or 'utf-8'} text"
        except Exception as e:  # noqa: BLE001
            log.exception("port %s: processing %s", port["id"], name)
            detail = f"{type(e).__name__}: {e}"
    final = os.path.basename(work)
    if status in ("OK", "ACCEPTED"):
        if cfg["after_success"] == "delete":
            await asyncio.to_thread(os.remove, work)
        else:
            await asyncio.to_thread(shutil.move, work, os.path.join(cfg["completion_dir"], final))
    else:
        await asyncio.to_thread(shutil.move, work, os.path.join(cfg["error_dir"], final))
        await asyncio.to_thread(Path(cfg["error_dir"], final + ".error.txt").write_text,
                                f"{datetime.now().isoformat(timespec='seconds')} {detail}\n")
    await db.record_port_event(port["id"], name, status, detail, ref)


async def _file_loop(port: dict, wake: asyncio.Event, entry: dict):
    cfg = port["config"]
    for key in ("work_dir", "completion_dir", "error_dir"):
        await asyncio.to_thread(os.makedirs, cfg[key], exist_ok=True)
    while True:
        try:
            if not os.path.isdir(cfg["monitor_dir"]):
                raise PortError(f"folder {cfg['monitor_dir']} doesn't exist")
            files = await asyncio.to_thread(_due_files, cfg)
            for f in files:
                await _process_file(port, f)
            entry.update(state="running", detail=f"watching {cfg['monitor_dir']} ({cfg['filter']}) every "
                                                 f"{cfg['interval_seconds']} s")
            await db.update_port(port["id"], last_poll_at=datetime.now(timezone.utc))
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - shown on the Ports page, polling continues
            entry.update(state="error", detail=str(e) or type(e).__name__)
            await db.update_port(port["id"], last_poll_at=datetime.now(timezone.utc), last_error=entry["detail"])
        wake.clear()
        try:
            await asyncio.wait_for(wake.wait(), cfg["interval_seconds"])
        except asyncio.TimeoutError:
            pass


# ---------- lifecycle ----------

async def _start(port: dict):
    entry = {"key": port["updated_at"], "state": "starting", "detail": "", "task": None, "listener": None,
             "wake": asyncio.Event()}
    _running[port["id"]] = entry
    if port["type"] == "file":
        entry["task"] = asyncio.create_task(_file_loop(port, entry["wake"], entry), name=f"port-{port['id']}")
        return

    async def handler(body: bytes, meta: dict):
        current = await db.get_port(port["id"])
        if not current or not current["enabled"]:
            return
        status, detail, ref = await dispatch(current, body.decode("utf-8", errors="replace"), meta.get("source", ""),
                                             {"source": meta.get("source")})
        await db.record_port_event(port["id"], meta.get("source"), status, detail, ref)

    try:
        adapter = await connectors.adapter_for(port["config"]["connection"])
        entry["listener"] = await adapter.listen(port["config"]["source"], handler)
        entry.update(state="running", detail=f"listening on {entry['listener'].describe}")
    except Exception as e:  # noqa: BLE001
        entry.update(state="error", detail=str(e) or type(e).__name__)


async def _stop(pid: str):
    entry = _running.pop(pid, None)
    if not entry:
        return
    if entry.get("task"):
        entry["task"].cancel()
        await asyncio.gather(entry["task"], return_exceptions=True)
    if entry.get("listener"):
        try:
            await entry["listener"].stop()
        except Exception:  # noqa: BLE001
            log.exception("stopping port %s", pid)


async def sync():
    """Start, restart or stop ports so they match their definitions."""
    async with _lock:
        wanted = {p["id"]: p for p in await db.list_ports() if p["enabled"]}
        for pid in list(_running):
            entry, p = _running[pid], wanted.get(pid)
            dead = entry.get("task") is not None and entry["task"].done()
            failed_listener = entry["state"] == "error" and entry.get("listener") is None and entry.get("task") is None
            if not p or p["updated_at"] != entry["key"] or dead or failed_listener:  # failed listeners are retried
                await _stop(pid)
        for pid, p in wanted.items():
            if pid not in _running:
                await _start(p)


def poll_now(pid: str) -> bool:
    entry = _running.get(pid)
    if entry and entry.get("task"):
        entry["wake"].set()
        return True
    return False


def status() -> dict:
    return {pid: {"state": e["state"], "detail": e["detail"]} for pid, e in _running.items()}


async def _supervise():
    while True:
        await asyncio.sleep(SUPERVISE_SECONDS)
        try:
            await sync()
        except Exception:  # noqa: BLE001
            log.exception("port supervisor")


async def start():
    global _supervisor
    await sync()
    _supervisor = asyncio.create_task(_supervise(), name="port-supervisor")


async def stop():
    if _supervisor:
        _supervisor.cancel()
    for pid in list(_running):
        await _stop(pid)
