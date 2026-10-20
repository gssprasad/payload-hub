"""Alerts: when something fails, tell people - by webhook (Slack, Teams, anything that takes JSON) or email.

Events come from two places:
- the moment something fails: a transaction, a delivery, a workflow run, a scheduled task, a file on a port,
  a backup (emit());
- a watcher that checks every WATCH_SECONDS for things that are *down* - Kafka, the database, pipeline workers,
  partner listeners, workflow triggers, ports - and for error-rate thresholds. Down-states alert once when they
  go down and once when they recover.

A rule picks an event (optionally for one partner / workflow / port), the channels to send to, and a cool-down:
repeats of the same thing within the cool-down are logged as suppressed instead of sent.
"""
import asyncio
import logging
import os
import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

import httpx

from . import db

log = logging.getLogger("alerts")

EVENTS = {
    "transaction_failed": "A transaction failed (mapping or parsing error)",
    "delivery_failed": "An outbound delivery failed after its retries",
    "workflow_failed": "A workflow run failed",
    "schedule_failed": "A scheduled task failed",
    "port_error": "A port failed to process a file or message, or is in error",
    "listener_down": "A partner listener or workflow trigger is in error",
    "service_down": "Kafka, the database or the pipeline workers are down",
    "error_rate": "Too many failed transactions in a time window",
    "backup_failed": "A database backup failed",
}
FILTERS = {"transaction_failed": "partner", "delivery_failed": "partner", "error_rate": "partner",
           "workflow_failed": "workflow", "schedule_failed": "task", "port_error": "port"}
WATCH_SECONDS = float(os.getenv("ALERT_WATCH_SECONDS", "30"))
APP_NAME = os.getenv("PAYLOAD_HUB_NAME", "Payload Hub")
_states: dict[str, bool] = {}  # watcher: key -> is down
_tasks: set[asyncio.Task] = set()
_watcher: asyncio.Task | None = None


class AlertError(ValueError):
    pass


# ---------- sending ----------

async def _channel_secret(ch: dict) -> str | None:
    from . import vault

    return await vault.secret_value(ch["secret_alias"]) if ch.get("secret_alias") else None


async def send(ch: dict, subject: str, detail: str, event: str, attrs: dict) -> str:
    """Send one alert through one channel. Returns a short result; raises on failure."""
    cfg = ch["config"]
    text = f"[{APP_NAME}] {subject}" + (f"\n{detail}" if detail else "")
    if ch["type"] == "webhook":
        url = await _channel_secret(ch) or cfg.get("url")
        if not url:
            raise AlertError("No webhook URL.")
        fmt = cfg.get("format", "generic")
        body = {"text": text} if fmt in ("slack", "teams") else {
            "app": APP_NAME, "event": event, "subject": subject, "detail": detail, "attributes": attrs,
            "at": datetime.now(timezone.utc).isoformat()}
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json=body)
        if r.status_code >= 400:
            raise AlertError(f"webhook answered HTTP {r.status_code}")
        return f"HTTP {r.status_code}"
    if ch["type"] == "email":
        to = [a.strip() for a in (cfg.get("to") or "").replace(";", ",").split(",") if a.strip()]
        if not to:
            raise AlertError("No recipients.")
        msg = EmailMessage()
        msg["Subject"] = f"[{APP_NAME}] {subject}"
        msg["From"] = cfg.get("from") or cfg.get("username") or "payload-hub@localhost"
        msg["To"] = ", ".join(to)
        msg.set_content(f"{subject}\n\n{detail or ''}\n\nEvent: {event}\n"
                        + "".join(f"{k}: {v}\n" for k, v in attrs.items()))
        password = await _channel_secret(ch)

        def deliver():
            host, port, security = cfg.get("host") or "localhost", int(cfg.get("port") or 25), cfg.get("security", "starttls")
            smtp = smtplib.SMTP_SSL(host, port, timeout=20, context=ssl.create_default_context()) \
                if security == "ssl" else smtplib.SMTP(host, port, timeout=20)
            with smtp:
                if security == "starttls":
                    smtp.starttls(context=ssl.create_default_context())
                if cfg.get("username"):
                    smtp.login(cfg["username"], password or "")
                smtp.send_message(msg)

        await asyncio.to_thread(deliver)
        return f"emailed {len(to)} recipient(s)"
    raise AlertError(f"Unknown channel type '{ch['type']}'.")


async def _fire(rule: dict, event: str, key: str, subject: str, detail: str, attrs: dict, recovery: bool):
    if not recovery and rule["cooldown_minutes"] > 0 and await db.alert_sent_since(
            rule["id"], key, datetime.now(timezone.utc) - timedelta(minutes=rule["cooldown_minutes"])):
        await db.record_alert(rule["id"], event, key, subject, detail, "suppressed", [])
        return
    results, sent = [], 0
    for ch in await db.alert_channels(rule["channels"]):
        if not ch["enabled"]:
            continue
        try:
            results.append({"channel": ch["id"], "ok": True, "result": await send(ch, subject, detail, event, attrs)})
            sent += 1
            await db.update_alert_channel_state(ch["id"], None)
        except Exception as e:  # noqa: BLE001 - recorded on the alert and the channel
            results.append({"channel": ch["id"], "ok": False, "result": str(e) or type(e).__name__})
            await db.update_alert_channel_state(ch["id"], str(e) or type(e).__name__)
    status = "no_channel" if not results else "sent" if sent == len(results) else "partial" if sent else "failed"
    await db.record_alert(rule["id"], event, key, subject, detail, status, results)
    await db.mark_rule_fired(rule["id"])


async def _dispatch(event: str, subject: str, detail: str, attrs: dict, key: str, recovery: bool):
    try:
        for rule in await db.alert_rules_for(event):
            field = FILTERS.get(event)
            wanted = (rule["filter"] or {}).get(field) if field else None
            if wanted and attrs.get(field) != wanted:
                continue
            await _fire(rule, event, key, subject, detail, attrs, recovery)
    except Exception:  # noqa: BLE001 - alerting must never break the caller
        log.exception("alert dispatch failed for %s", event)


def emit(event: str, subject: str, detail: str = "", key: str | None = None, recovery: bool = False, **attrs):
    """Raise an event (returns at once; sending happens in the background). key groups repeats for cool-downs."""
    attrs = {k: v for k, v in attrs.items() if v is not None}
    task = asyncio.create_task(_dispatch(event, subject, (detail or "")[:2000], attrs,
                                         key or attrs.get(FILTERS.get(event, ""), event), recovery))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


# ---------- the watcher ----------

def _transition(key: str, down: bool, event: str, what: str, detail: str, **attrs):
    was = _states.get(key)
    _states[key] = down
    if down and not was:
        emit(event, f"{what} is down", detail, key=key, **attrs)
    elif was and not down:
        emit(event, f"{what} has recovered", "", key=key, recovery=True, **attrs)


async def _watch_once():
    from . import connectors, pipeline, ports, workflows

    try:
        await db.ping()
        _transition("service:database", False, "service_down", "The database", "")
    except Exception as e:  # noqa: BLE001
        _transition("service:database", True, "service_down", "The database", str(e))
    try:
        await pipeline.producer.client.fetch_all_metadata()
        _transition("service:kafka", False, "service_down", "Kafka", "")
    except Exception as e:  # noqa: BLE001
        _transition("service:kafka", True, "service_down", "Kafka", str(e) or type(e).__name__)
    stopped = [t.get_name() for t in pipeline._tasks if t.done()]
    _transition("service:workers", bool(stopped), "service_down", "The pipeline workers",
                f"stopped: {', '.join(stopped)}")
    for code, st in connectors.listener_status().items():
        _transition(f"listener:{code}", st["state"] == "error", "listener_down", f"Partner {code}'s listener",
                    st["detail"], partner=code)
    for wf_id, st in workflows.trigger_status().items():
        _transition(f"trigger:{wf_id}", st["state"] == "error", "listener_down", f"Workflow {wf_id}'s trigger",
                    st["detail"], workflow=wf_id)
    for pid, st in ports.status().items():
        _transition(f"port:{pid}", st["state"] == "error", "port_error", f"Port {pid}", st["detail"], port=pid)
    for rule in await db.alert_rules_for("error_rate"):
        t = rule["threshold"] or {}
        count, minutes = int(t.get("count") or 5), int(t.get("minutes") or 10)
        partner = (rule["filter"] or {}).get("partner")
        failed = await db.count_failed_transactions(minutes, partner)
        key = f"error_rate:{rule['id']}"
        high = failed >= count
        was = _states.get(key)
        _states[key] = high
        if high and not was:
            await _fire(rule, "error_rate", key, f"{failed} failed transactions in {minutes} min"
                        + (f" for {partner}" if partner else ""), f"Threshold: {count} in {minutes} minutes.",
                        {"partner": partner} if partner else {}, False)
        elif was and not high:
            await _fire(rule, "error_rate", key, "Failed transactions back below the threshold"
                        + (f" for {partner}" if partner else ""), "", {}, True)


async def _watch():
    from . import cluster

    await asyncio.sleep(5)  # let listeners and ports start
    while True:
        try:
            if await cluster.own("alert-watcher"):  # one instance watches, so alerts aren't doubled
                await _watch_once()
        except Exception:  # noqa: BLE001
            log.exception("alert watcher")
        await asyncio.sleep(WATCH_SECONDS)


def watch_state() -> dict:
    return {k: ("down" if v else "ok") for k, v in sorted(_states.items())}


async def start():
    global _watcher
    _watcher = asyncio.create_task(_watch(), name="alert-watcher")


async def stop():
    if _watcher:
        _watcher.cancel()
    if _tasks:
        await asyncio.wait(list(_tasks), timeout=5)


def validate_channel(ch: dict) -> dict:
    cfg = dict(ch.get("config") or {})
    if ch.get("type") == "webhook":
        if not (cfg.get("url") or ch.get("secret_alias")):
            raise AlertError("Give the webhook URL, or a stored secret that holds it.")
        if cfg.get("url") and not str(cfg["url"]).startswith(("http://", "https://")):
            raise AlertError("The webhook URL must start with http:// or https://.")
        if cfg.get("format", "generic") not in ("generic", "slack", "teams"):
            raise AlertError("The format is generic JSON, Slack or Teams.")
        return {"url": cfg.get("url") or "", "format": cfg.get("format", "generic")}
    if ch.get("type") == "email":
        if not cfg.get("host") or not cfg.get("to"):
            raise AlertError("Email needs the SMTP server and at least one recipient.")
        if cfg.get("security", "starttls") not in ("starttls", "ssl", "none"):
            raise AlertError("Security is STARTTLS, SSL or none.")
        return {k: cfg.get(k, d) for k, d in (("host", ""), ("port", 587), ("security", "starttls"),
                                               ("username", ""), ("from", ""), ("to", ""))}
    raise AlertError("The channel type is webhook or email.")


def describe_rule(rule: dict) -> str:
    field = FILTERS.get(rule["event"])
    scope = (rule["filter"] or {}).get(field) if field else None
    text = EVENTS.get(rule["event"], rule["event"])
    if rule["event"] == "error_rate":
        t = rule["threshold"] or {}
        text = f"{t.get('count', 5)}+ failed transactions in {t.get('minutes', 10)} min"
    return text + (f" ({field} {scope})" if scope else "")

