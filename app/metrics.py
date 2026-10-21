"""Prometheus metrics (GET /metrics), for the Grafana dashboard in monitoring/.

Counters and histograms are updated where things happen; the gauges below (Kafka backlog, which listeners / ports /
services are up) are refreshed every REFRESH_SECONDS. Every instance exposes its own /metrics; the dashboard adds
them up (and takes the maximum for shared things such as the Kafka backlog).
"""
import asyncio
import logging
import os

from prometheus_client import Counter, Gauge, Histogram, Info

log = logging.getLogger("metrics")
REFRESH_SECONDS = float(os.getenv("PAYLOAD_HUB_METRICS_REFRESH_SECONDS", "15"))
_LATENCY = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 30)

INBOUND_SECONDS = Histogram("payload_inbound_seconds", "Partner HTTP calls, from request to answer",
                            ["partner", "code"], buckets=_LATENCY)
DELIVERIES = Counter("payload_deliveries_total", "Outbound deliveries", ["partner", "result"])
FORWARDS = Counter("payload_forwards_total", "Calls to a partner's target system", ["partner", "result"])
FORWARD_SECONDS = Histogram("payload_forward_seconds", "Target system response time", ["partner"], buckets=_LATENCY)
WORKFLOW_RUNS = Counter("payload_workflow_runs_total", "Workflow runs", ["workflow", "status"])
WORKFLOW_SECONDS = Histogram("payload_workflow_seconds", "Workflow run time", ["workflow"],
                             buckets=(0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300))
SCHEDULE_RUNS = Counter("payload_schedule_runs_total", "Scheduled task runs", ["task", "status"])
PORT_ITEMS = Counter("payload_port_items_total", "Files / messages handled by ports", ["port", "result"])
ALERTS = Counter("payload_alerts_total", "Alerts raised", ["event", "status"])
KAFKA_BACKLOG = Gauge("payload_kafka_backlog", "Messages waiting in a Payload Hub queue", ["queue", "kind"])
KAFKA_MESSAGES = Gauge("payload_kafka_messages", "Messages held in a Payload Hub topic", ["queue", "kind"])
COMPONENT_UP = Gauge("payload_component_up", "1 when running, 0 when in error (standby components aren't listed)",
                     ["kind", "name"])
DB_POOL = Gauge("payload_db_pool_connections", "Database connections of this instance", ["state"])
INSTANCE = Info("payload_instance", "This Payload Hub instance")

_task: asyncio.Task | None = None


async def _refresh():
    from . import cluster, connectors, db, pipeline, ports, workflows

    INSTANCE.info({"instance": cluster.INSTANCE})
    while True:
        try:
            try:
                await db.ping()
                COMPONENT_UP.labels("service", "database").set(1)
            except Exception:  # noqa: BLE001
                COMPONENT_UP.labels("service", "database").set(0)
            try:
                rows = await pipeline.queue_stats(max_age=REFRESH_SECONDS)
                COMPONENT_UP.labels("service", "kafka").set(1)
                KAFKA_BACKLOG.clear()
                KAFKA_MESSAGES.clear()
                for r in rows:
                    KAFKA_MESSAGES.labels(r["queue"], r["kind"]).set(r["messages"])
                    if r["backlog"] is not None:
                        KAFKA_BACKLOG.labels(r["queue"], r["kind"]).set(r["backlog"])
            except Exception:  # noqa: BLE001
                COMPONENT_UP.labels("service", "kafka").set(0)
            for t in pipeline._tasks:
                COMPONENT_UP.labels("worker", t.get_name()).set(0 if t.done() else 1)
            for kind, states in (("listener", connectors.listener_status()), ("trigger", workflows.trigger_status()),
                                 ("port", ports.status())):
                for name, st in states.items():
                    if st["state"] in ("running", "error"):  # standby = another instance runs it
                        COMPONENT_UP.labels(kind, name).set(1 if st["state"] == "running" else 0)
            stats = db.pool.get_stats()
            DB_POOL.labels("size").set(stats.get("pool_size", 0))
            DB_POOL.labels("available").set(stats.get("pool_available", 0))
            DB_POOL.labels("waiting").set(stats.get("requests_waiting", 0))
        except Exception:  # noqa: BLE001
            log.exception("refreshing metrics")
        await asyncio.sleep(REFRESH_SECONDS)


async def start():
    global _task
    _task = asyncio.create_task(_refresh(), name="metrics")


async def stop():
    if _task:
        _task.cancel()
