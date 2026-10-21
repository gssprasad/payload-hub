"""Builds monitoring/grafana/dashboards/payload-hub.json - the Payload Hub Grafana dashboard.

    python monitoring/build_dashboard.py

Edit the panels here rather than in the JSON. Grafana picks the file up within a minute.
"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
P = 'partner=~"$partner"'
panels, _y, _id = [], [0], [0]


def _next_id():
    _id[0] += 1
    return _id[0]


def row(title):
    panels.append({"type": "row", "title": title, "id": _next_id(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": _y[0]}, "panels": []})
    _y[0] += 1


def place(items, height):
    """Lay out panels side by side on one line; widths add up to 24."""
    x = 0
    for panel, width in items:
        panel["gridPos"] = {"h": height, "w": width, "x": x, "y": _y[0]}
        panel["id"] = _next_id()
        panels.append(panel)
        x += width
    _y[0] += height


def stat(title, expr, unit="short", decimals=None, thresholds=None, description=""):
    steps = thresholds or [{"color": "green", "value": None}]
    return {"type": "stat", "title": title, "description": description, "datasource": DS,
            "targets": [{"refId": "A", "expr": expr, "datasource": DS}],
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "area",
                        "textMode": "value"},
            "fieldConfig": {"defaults": {"unit": unit, "decimals": decimals, "noValue": "–",
                                         "thresholds": {"mode": "absolute", "steps": steps}}, "overrides": []}}


def series(title, targets, unit="short", stack=False, description=""):
    return {"type": "timeseries", "title": title, "description": description, "datasource": DS,
            "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": legend, "datasource": DS}
                        for i, (e, legend) in enumerate(targets)],
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
            "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 12,
                                                                  "stacking": {"mode": "normal" if stack else "none"}}},
                            "overrides": []}}


def table(title, expr, description=""):
    return {"type": "table", "title": title, "description": description, "datasource": DS,
            "targets": [{"refId": "A", "expr": expr, "format": "table", "instant": True, "datasource": DS}],
            "transformations": [{"id": "organize", "options": {
                "excludeByName": {"Time": True, "__name__": True, "job": True, "environment": True},
                "indexByName": {"kind": 0, "name": 1, "Value": 2, "instance": 3}}}],
            "fieldConfig": {"defaults": {"mappings": [{"type": "value", "options": {
                "1": {"text": "🟢 up", "color": "green"}, "0": {"text": "🔴 down", "color": "red"}}}]},
                "overrides": [{"matcher": {"id": "byName", "options": "Value"},
                               "properties": [{"id": "displayName", "value": "State"},
                                              {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}]}}


def quantile(q, metric, by="", extra=""):
    grouping = f"le{', ' + by if by else ''}"
    return f"histogram_quantile({q}, sum by ({grouping}) (rate({metric}_bucket{{{extra}}}[$__rate_interval])))"


RED, ORANGE, GREEN = "red", "orange", "green"

row("Health now")
place([
    (stat("Instances up", 'count(up{job="payload-hub"} == 1) or vector(0)',
          thresholds=[{"color": RED, "value": None}, {"color": GREEN, "value": 1}],
          description="Payload Hub backends Prometheus can reach."), 4),
    (stat("Components down", "count(payload_component_up == 0) or vector(0)",
          thresholds=[{"color": GREEN, "value": None}, {"color": RED, "value": 1}],
          description="Services, pipeline workers, listeners, triggers or ports in error."), 4),
    (stat("Messages / s", f"sum(rate(payload_transactions_total{{{P}}}[$__rate_interval]))", "reqps", 1), 4),
    (stat("Success rate", f'sum(rate(payload_transactions_total{{status="COMPLETED",{P}}}[$__rate_interval])) / '
                          f"clamp_min(sum(rate(payload_transactions_total{{{P}}}[$__rate_interval])), 0.0001)",
          "percentunit", 1, [{"color": RED, "value": None}, {"color": ORANGE, "value": 0.95},
                             {"color": GREEN, "value": 0.99}]), 4),
    (stat("Partner calls p95", quantile(0.95, "payload_inbound_seconds", extra=P), "s", 2,
          [{"color": GREEN, "value": None}, {"color": ORANGE, "value": 1}, {"color": RED, "value": 2}]), 4),
    (stat("Waiting in Kafka", "sum(max by (queue, kind) (payload_kafka_backlog)) or vector(0)", "short", 0,
          [{"color": GREEN, "value": None}, {"color": ORANGE, "value": 100}, {"color": RED, "value": 1000}]), 4),
], 4)

row("Traffic")
place([
    (series("Transactions by result", [
        (f"sum by (status) (rate(payload_transactions_total{{{P}}}[$__rate_interval]))", "{{status}}")],
        "reqps", stack=True), 12),
    (series("Transactions by partner", [
        (f"sum by (partner) (rate(payload_transactions_total{{{P}}}[$__rate_interval]))", "{{partner}}")],
        "reqps"), 12),
], 8)
place([
    (series("Partner call time (caller's view)", [
        (quantile(0.5, "payload_inbound_seconds", extra=P), "p50"),
        (quantile(0.95, "payload_inbound_seconds", extra=P), "p95"),
        (quantile(0.99, "payload_inbound_seconds", extra=P), "p99")], "s"), 12),
    (series("Partner call answers by HTTP status", [
        (f"sum by (code) (rate(payload_inbound_seconds_count{{{P}}}[$__rate_interval]))", "HTTP {{code}}")],
        "reqps", stack=True, description="200 ok · 202 still processing after 15 s · 4xx bad requests · "
                                         "502 target system unreachable"), 12),
], 8)

row("Pipeline")
place([
    (series("Inside the pipeline", [
        (quantile(0.95, "payload_end_to_end_seconds"), "end to end p95"),
        (quantile(0.95, "payload_transform_seconds"), "transform p95")], "s"), 8),
    (series("Waiting in Kafka, by queue", [
        ("max by (queue, kind) (payload_kafka_backlog)", "{{queue}} {{kind}}")], "short"), 8),
    (series("In flight and duplicates", [
        ("sum(payload_in_flight)", "in flight"),
        ("sum(rate(payload_duplicates_total[$__rate_interval]))", "duplicates ignored / s")], "short"), 8),
], 8)

row("Routes and deliveries")
place([
    (series("Deliveries", [
        (f"sum by (partner, result) (rate(payload_deliveries_total{{{P}}}[$__rate_interval]))",
         "{{partner}} {{result}}")], "reqps"), 8),
    (series("Calls to target systems", [
        (f"sum by (partner, result) (rate(payload_forwards_total{{{P}}}[$__rate_interval]))",
         "{{partner}} {{result}}")], "reqps"), 8),
    (series("Target system response time p95", [
        (quantile(0.95, "payload_forward_seconds", by="partner", extra=P), "{{partner}}")], "s"), 8),
], 8)

row("Workflows, schedules, ports, alerts")
place([
    (series("Workflow runs", [
        ("sum by (workflow, status) (increase(payload_workflow_runs_total[$__rate_interval]))",
         "{{workflow}} {{status}}")], "short", stack=True), 6),
    (series("Scheduled tasks", [
        ("sum by (task, status) (increase(payload_schedule_runs_total[$__rate_interval]))", "{{task}} {{status}}")],
        "short", stack=True), 6),
    (series("Files / messages on ports", [
        ("sum by (port, result) (increase(payload_port_items_total[$__rate_interval]))", "{{port}} {{result}}")],
        "short", stack=True), 6),
    (series("Alerts raised", [
        ("sum by (event, status) (increase(payload_alerts_total[$__rate_interval]))", "{{event}} {{status}}")],
        "short", stack=True), 6),
], 8)

row("Instances and components")
place([
    (table("Components", "payload_component_up",
           description="Standby pollers (running on another instance) aren't listed."), 12),
    (series("Database connections per instance", [
        ('payload_db_pool_connections{state="size"}', "{{instance}} open"),
        ('payload_db_pool_connections{state="waiting"}', "{{instance}} requests waiting")], "short"), 12),
], 9)

dashboard = {
    "uid": "payload-hub", "title": "Payload Hub", "tags": ["payload-hub"], "timezone": "browser",
    "schemaVersion": 41, "version": 1, "refresh": "30s", "time": {"from": "now-6h", "to": "now"},
    "templating": {"list": [{
        "name": "partner", "label": "Partner", "type": "query", "datasource": DS,
        "query": {"query": "label_values(payload_transactions_total, partner)", "refId": "partner"},
        "definition": "label_values(payload_transactions_total, partner)",
        "includeAll": True, "multi": True, "allValue": ".*", "current": {"text": "All", "value": "$__all"},
        "refresh": 2}]},
    "annotations": {"list": []},
    "panels": panels,
}
out = Path(__file__).parent / "grafana" / "dashboards" / "payload-hub.json"
out.write_text(json.dumps(dashboard, indent=1, ensure_ascii=False) + "\n")
print(f"wrote {out} with {sum(1 for p in panels if p['type'] != 'row')} panels")
