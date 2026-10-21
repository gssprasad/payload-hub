"""Deployment packages: move configuration built in one environment (DEV) to the next (TEST, PROD) - like the
webMethods Deployer.

A package is a JSON file with the chosen partners, connections, workflows, scheduled tasks, ports and alert rules
(and the secret *names* they use - never secret values). Exporting can add dependencies automatically. Importing
takes an optional *overrides* file for the target environment (e.g. the production database host), shows a plan -
create / update / unchanged, and any problems - and only changes something when it is applied. Applying takes a
database backup first, validates everything the same way the screens do, and is recorded in the deployment history.
"""
import copy
import hashlib
import json
import os
from datetime import datetime, timezone

from . import adapters, cron, db, scheduler, version, workflows

FORMAT = "payload-hub-package/1"
# import order: what others refer to comes first
TYPES = ("secrets", "connections", "partners", "workflows", "alert_channels", "alert_rules", "schedules", "ports")
LABELS = {"secrets": "Secret (name only)", "connections": "Connection", "partners": "Partner", "workflows": "Workflow",
          "alert_channels": "Alert channel", "alert_rules": "Alert rule", "schedules": "Scheduled task", "ports": "Port"}
PARTNER_EXPORT = [f for f in db.PARTNER_FIELDS
                  if f not in ("api_key_hash", "api_key_enc", "api_key_hint", "topic_in", "topic_out", "topic_dlq")]


class DeployError(ValueError):
    pass


def environment() -> str:
    return os.getenv("PAYLOAD_HUB_ENVIRONMENT", "DEV").upper()


# ---------- reading assets in a comparable, exportable shape ----------

def _plain(value):
    return json.loads(json.dumps(value, default=str))


async def _read(kind: str, key: str) -> dict | None:
    if kind == "secrets":
        s = await db.get_secret(key)
        return s and {"alias": s["alias"], "kind": s["kind"], "description": s["description"]}
    if kind == "connections":
        c = await db.get_connection(key)
        return c and {"id": c["id"], "name": c["name"], "type": c["type"], "config": c["config"],
                      "secret_alias": c.get("secret_alias"), "owner_partner": c.get("owner_partner"),
                      "has_own_password": bool(c.get("has_secret"))}
    if kind == "partners":
        p = await db.get_partner(key)
        return p and {"code": p["code"], **{f: p.get(f) for f in PARTNER_EXPORT}}
    if kind == "workflows":
        w = await db.get_workflow(key)
        return w and {"id": w["id"], **{f: w.get(f) for f in ("name", "description", "trigger_type", "trigger_config",
                                                                "steps", "active")}}
    if kind == "schedules":
        t = await db.get_schedule(key)
        return t and {"id": t["id"], **{f: t[f] for f in ("name", "description", "enabled", "schedule", "action")}}
    if kind == "ports":
        p = await db.get_port(key)
        return p and {"id": p["id"], **{f: p[f] for f in ("name", "type", "enabled", "config", "target_type",
                                                          "target_id")}}
    if kind == "alert_channels":
        c = next((x for x in await db.list_alert_channels() if x["id"] == key), None)
        return c and {"id": c["id"], **{f: c[f] for f in ("name", "type", "config", "secret_alias", "enabled")}}
    if kind == "alert_rules":
        r = next((x for x in await db.list_alert_rules() if x["id"] == key), None)
        return r and {"id": r["id"], **{f: r[f] for f in ("name", "enabled", "event", "filter", "threshold",
                                                          "channels", "cooldown_minutes")}}
    raise DeployError(f"Unknown asset type '{kind}'.")


def _key(kind: str, asset: dict) -> str:
    return asset["alias"] if kind == "secrets" else asset["code"] if kind == "partners" else asset["id"]


async def inventory() -> dict:
    """Everything that can go into a package, by type: [{id, name}]."""
    return {
        "secrets": [{"id": s["alias"], "name": s["alias"]} for s in await db.list_secrets()],
        "connections": [{"id": c["id"], "name": c["name"]} for c in await db.list_connections()],
        "partners": [{"id": p["code"], "name": p["name"]} for p in await db.list_partners()],
        "workflows": [{"id": w["id"], "name": w["name"]} for w in await db.list_workflows()],
        "schedules": [{"id": t["id"], "name": t["name"]} for t in await db.list_schedules()],
        "ports": [{"id": p["id"], "name": p["name"]} for p in await db.list_ports()],
        "alert_channels": [{"id": c["id"], "name": c["name"]} for c in await db.list_alert_channels()],
        "alert_rules": [{"id": r["id"], "name": r["name"]} for r in await db.list_alert_rules()],
    }


# ---------- references between assets ----------

def _step_connections(steps) -> set[str]:
    found = set()
    for s in workflows._walk_steps(steps or []):
        if s.get("connection") and s["connection"] != "builtin":
            found.add(s["connection"])
    return found


def references(kind: str, a: dict) -> list[tuple[str, str]]:
    """(type, id) of the assets this one needs."""
    out = []
    if kind == "connections" and a.get("secret_alias"):
        out.append(("secrets", a["secret_alias"]))
    if kind == "partners":
        for f in ("inbound_connection", "outbound_connection", "reply_connection", "store_connection"):
            if a.get(f):
                out.append(("connections", a[f]))
    if kind == "workflows":
        out += [("connections", c) for c in _step_connections(a.get("steps"))]
        conn = (a.get("trigger_config") or {}).get("connection")
        if conn and conn != "builtin":
            out.append(("connections", conn))
    if kind == "schedules":
        act = a.get("action") or {}
        if act.get("type") == "workflow":
            out.append(("workflows", act.get("workflow_id")))
        elif act.get("type") == "partner":
            out.append(("partners", act.get("partner_code")))
        elif act.get("type") == "http":
            out.append(("connections", act.get("connection")))
    if kind == "ports":
        cfg = a.get("config") or {}
        if a.get("target_type") in ("partner", "workflow"):
            out.append(("partners" if a["target_type"] == "partner" else "workflows", a["target_id"]))
        if cfg.get("connection"):
            out.append(("connections", cfg["connection"]))
        if cfg.get("key_password_alias"):
            out.append(("secrets", cfg["key_password_alias"]))
        for code in cfg.get("partners") or []:
            out.append(("partners", code))
    if kind == "alert_channels" and a.get("secret_alias"):
        out.append(("secrets", a["secret_alias"]))
    if kind == "alert_rules":
        out += [("alert_channels", c) for c in a.get("channels") or []]
    return [(k, i) for k, i in out if i]


# ---------- export ----------

async def export(selection: dict, name: str, with_dependencies: bool = True, username: str | None = None) -> dict:
    """selection: {type: [ids]}. Returns the package."""
    chosen: dict[str, dict[str, dict]] = {k: {} for k in TYPES}
    todo = [(k, i) for k in TYPES for i in (selection.get(k) or [])]
    while todo:
        kind, key = todo.pop()
        if key in chosen[kind]:
            continue
        asset = await _read(kind, key)
        if asset is None:
            raise DeployError(f"{LABELS[kind]} '{key}' doesn't exist.")
        chosen[kind][key] = _plain(asset)
        if with_dependencies:
            todo += references(kind, asset)
    from . import migrate

    return {"format": FORMAT, "name": name, "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "created_by": username, "source_environment": environment(), "app_version": version.VERSION,
            "schema_version": migrate.current(),
            "assets": {k: sorted(v.values(), key=lambda a, k=k: _key(k, a)) for k, v in chosen.items() if v}}


# ---------- import: plan and apply ----------

def _merge(base, patch):
    if isinstance(base, dict) and isinstance(patch, dict):
        out = dict(base)
        for k, v in patch.items():
            out[k] = _merge(base.get(k), v) if k in base else v
        return out
    return patch


def with_overrides(package: dict, overrides: dict | None) -> dict:
    """Apply an environment's overrides: {type: {id: {field: value, ...}}} - nested values are merged."""
    pkg = copy.deepcopy(package)
    for kind, patches in (overrides or {}).items():
        if kind in ("environment", "description"):
            continue
        if kind not in TYPES:
            raise DeployError(f"Overrides: unknown asset type '{kind}'.")
        assets = {_key(kind, a): a for a in pkg["assets"].get(kind, [])}
        for key, patch in (patches or {}).items():
            if key not in assets:
                raise DeployError(f"Overrides: the package has no {LABELS[kind].lower()} '{key}'.")
            assets[key].update(_merge(assets[key], patch))
    return pkg


def check_format(package: dict):
    from . import migrate

    if not isinstance(package, dict) or package.get("format") != FORMAT:
        raise DeployError("This isn't a Payload Hub deployment package.")
    target_head = migrate.head()
    src = package.get("schema_version")
    if src and src > target_head:
        raise DeployError(f"The package comes from a newer version (schema {src}, this environment has "
                          f"{target_head}) - upgrade this environment first.")


async def plan(package: dict, overrides: dict | None = None) -> dict:
    """What importing would do, without changing anything."""
    check_format(package)
    pkg = with_overrides(package, overrides)
    in_package = {(k, _key(k, a)) for k in TYPES for a in pkg["assets"].get(k, [])}
    items, problems = [], []
    for kind in TYPES:
        for a in pkg["assets"].get(kind, []):
            key = _key(kind, a)
            current = await _read(kind, key)
            item = {"type": kind, "label": LABELS[kind], "id": key, "name": a.get("name") or key, "changes": [],
                    "problems": []}
            if kind == "secrets":
                item["action"] = "present" if current else "missing"
                if not current:
                    item["problems"].append(f"Create the secret '{key}' in this environment (Secrets page) - "
                                            "values are never copied between environments.")
            elif current is None:
                item["action"] = "create"
            else:
                compare = {k: v for k, v in a.items() if k != "has_own_password"}
                changed = sorted(k for k in compare if _plain(current.get(k)) != _plain(compare[k]))
                item["action"], item["changes"] = ("update", changed) if changed else ("unchanged", [])
            for ref_kind, ref in references(kind, a):
                if (ref_kind, ref) not in in_package and await _read(ref_kind, ref) is None:
                    item["problems"].append(f"Needs {LABELS[ref_kind].lower()} '{ref}', which is neither in the "
                                            "package nor in this environment.")
            if kind == "connections" and a.get("has_own_password") and not a.get("secret_alias") \
                    and (current is None or not current.get("has_own_password")):
                item["problems"].append("Uses its own password, which isn't copied: give it a secret alias in the "
                                        "overrides, or enter the password here after deploying.")
                item["warning_only"] = True
            problems += [{"id": key, "type": kind, "problem": p, "blocking": not item.get("warning_only")}
                         for p in item["problems"]]
            items.append(item)
    counts = {a: sum(1 for i in items if i["action"] == a) for a in ("create", "update", "unchanged", "missing")}
    return {"package": {k: pkg.get(k) for k in ("name", "source_environment", "app_version", "schema_version",
                                                  "created_at", "created_by")},
            "target_environment": environment(), "items": items, "problems": problems, "counts": counts,
            "can_apply": not any(p["blocking"] for p in problems)}


def _clean_config(type_: str, config: dict) -> dict:
    fields = adapters.TYPES[type_]["fields"]
    return {f["key"]: config.get(f["key"], f["default"]) for f in fields if f["kind"] != "secret"}


async def _apply_one(kind: str, a: dict, exists: bool):
    from . import alerts, pipeline, ports, vault

    key = _key(kind, a)
    if kind == "connections":
        if a["type"] not in adapters.TYPES:
            raise DeployError(f"Connection '{key}': unknown type '{a['type']}'.")
        config = _clean_config(a["type"], a.get("config") or {})
        if exists:
            await db.update_connection(key, a["name"], config, None)
        else:
            await db.create_connection(key, a["name"], a["type"], config, None, a.get("owner_partner"))
        if a.get("secret_alias"):
            await db.set_connection_secret_alias(key, a["secret_alias"])
        if exists and a.get("owner_partner") is not None:
            await db.set_connection_owner(key, a.get("owner_partner"))
    elif kind == "partners":
        fields = {f: a.get(f) for f in PARTNER_EXPORT if f in a}
        if not exists or (await db.get_partner(key))["queue_mode"] != a.get("queue_mode"):
            topics = pipeline.partner_topics(key) if a.get("queue_mode") == "dedicated" else pipeline.SHARED_TOPICS
            if a.get("queue_mode") == "dedicated":
                await pipeline.ensure_topics(topics)
            fields.update(dict(zip(("topic_in", "topic_out", "topic_dlq"), topics)))
        if exists:
            await db.update_partner(key, **fields)
        else:
            import secrets as _secrets

            new_key = "ph_" + _secrets.token_urlsafe(24)  # each environment has its own partner keys
            fields.update(api_key_hash=hashlib.sha256(new_key.encode()).hexdigest(), api_key_enc=vault.seal(new_key),
                          api_key_hint=f"{new_key[:5]}…{new_key[-4:]}")
            await db.create_partner(key, **fields)
    elif kind == "workflows":
        steps = workflows.validate(a.get("steps") or [])
        fields = {"name": a["name"], "description": a.get("description"), "trigger_type": a.get("trigger_type", "manual"),
                  "trigger_config": a.get("trigger_config") or {}, "steps": steps, "active": a.get("active", True)}
        await (db.update_workflow(key, **fields) if exists else db.create_workflow(key, **fields))
    elif kind == "alert_channels":
        config = alerts.validate_channel(a)
        fields = {"name": a["name"], "type": a["type"], "config": config, "secret_alias": a.get("secret_alias"),
                  "enabled": a.get("enabled", True)}
        await (db.update_alert_channel(key, **fields) if exists else db.create_alert_channel(key, **fields))
    elif kind == "alert_rules":
        if a["event"] not in alerts.EVENTS:
            raise DeployError(f"Alert rule '{key}': unknown event '{a['event']}'.")
        fields = {f: a.get(f) for f in ("name", "enabled", "event", "filter", "threshold", "channels",
                                        "cooldown_minutes")}
        await (db.update_alert_rule(key, **fields) if exists else db.create_alert_rule(key, **fields))
    elif kind == "schedules":
        spec = cron.validate(a["schedule"])
        action = await scheduler.validate_action(a["action"])
        enabled = a.get("enabled", True)
        fields = {"name": a["name"], "description": a.get("description"), "enabled": enabled, "schedule": spec,
                  "action": action, "next_run_at": cron.next_run(spec) if enabled else None}
        if enabled and fields["next_run_at"] is None:
            fields["enabled"] = False  # a one-off task whose time has passed
        await (db.update_schedule(key, **fields) if exists else db.create_schedule(key, **fields))
    elif kind == "ports":
        config = await ports.validate({**a, "id": key})
        fields = {"name": a["name"], "type": a["type"], "enabled": a.get("enabled", True), "config": config,
                  "target_type": a.get("target_type", "endpoints"), "target_id": a.get("target_id", "")}
        await (db.update_port(key, **fields) if exists else db.create_port(key, **fields))


async def apply(package: dict, overrides: dict | None = None, username: str | None = None,
                take_backup: bool = True) -> dict:
    """Import a package: plan, back up, create/update everything, restart listeners, record the deployment."""
    import asyncio

    from . import backup, connectors, ports

    result = await plan(package, overrides)
    if not result["can_apply"]:
        raise DeployError("The package can't be deployed yet: "
                          + "; ".join(p["problem"] for p in result["problems"] if p["blocking"]))
    pkg = with_overrides(package, overrides)
    backup_file = None
    if take_backup:
        try:
            backup_file = (await asyncio.to_thread(backup.create, f"before deploying {pkg.get('name')}"))["file"]
        except backup.BackupError as e:
            raise DeployError(f"Couldn't take the backup before deploying ({e}) - nothing was changed.") from e
    done, status, error = [], "DEPLOYED", None
    try:
        for item in result["items"]:
            if item["type"] == "secrets" or item["action"] == "unchanged":
                continue
            asset = next(a for a in pkg["assets"][item["type"]] if _key(item["type"], a) == item["id"])
            await _apply_one(item["type"], asset, item["action"] == "update")
            await db.audit(username, f"deployed ({item['action']})", item["type"].rstrip("s"), item["id"],
                           f"package {pkg.get('name')} from {pkg.get('source_environment')}"
                           + (f": {', '.join(item['changes'])}" if item["changes"] else ""))
            done.append(item)
    except Exception as e:  # noqa: BLE001 - recorded; the backup restores the previous state
        status, error = "FAILED", f"{type(e).__name__}: {e}" if not isinstance(e, ValueError) else str(e)
    await connectors.sync()
    await workflows.sync()
    await ports.sync()
    summary = {"applied": [{"type": i["type"], "id": i["id"], "action": i["action"]} for i in done],
               "counts": result["counts"]}
    deployment_id = await db.record_deployment(username, pkg.get("name"), pkg.get("source_environment"),
                                               pkg.get("app_version"), environment(), status, error, backup_file,
                                               hashlib.sha256(json.dumps(package, sort_keys=True).encode()).hexdigest(),
                                               summary)
    return {"deployment": deployment_id, "status": status, "error": error, "backup": backup_file, **summary,
            "problems": result["problems"]}
