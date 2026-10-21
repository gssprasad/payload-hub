# Payload Hub monitoring

Prometheus collects Payload Hub's metrics, Grafana shows them on a ready-made dashboard, and Alertmanager sends
Prometheus' alerts. It runs separately from Payload Hub on purpose: it keeps the history and keeps alerting when
Payload Hub itself is down or overloaded. Payload Hub's own Monitoring page stays for quick day-to-day checks.

## Start

```bash
cd monitoring
echo "GRAFANA_ADMIN_PASSWORD=<choose one>" > .env      # .env is git-ignored
docker compose up -d
```

- **Grafana** http://localhost:3000 - sign in as `admin`; the *Payload Hub* dashboard opens first.
- **Prometheus** http://localhost:9090 - *Status → Targets* shows whether it reaches Payload Hub; *Alerts* shows the rules.
- **Alertmanager** http://localhost:9094 (9093 inside Docker; 9093 on the host is often Kafka's controller).

Ports can be changed with `GRAFANA_PORT`, `PROMETHEUS_PORT`, `ALERTMANAGER_PORT` in `.env`.

## Which instances it watches

`prometheus/targets.json` - by default Payload Hub on this machine (`host.docker.internal:8000`). List every instance
(and add labels such as the environment); Prometheus re-reads the file within a minute:

```json
[{"targets": ["hub-1.company.local:8000", "hub-2.company.local:8000"], "labels": {"environment": "production"}}]
```

For the Docker Compose stack in the project root, the backend is published on port 8000 of the host, so the default
works there too.

## The dashboard

*Health now* (instances, components down, messages/s, success rate, partner call p95, Kafka backlog), *Traffic*
(by result, by partner, caller's latency, HTTP statuses), *Pipeline* (end-to-end and transform time, backlog per
queue, in-flight, duplicates), *Routes and deliveries*, *Workflows, schedules, ports, alerts*, and *Instances and
components*. The *Partner* selector at the top filters the partner panels.

The dashboard is generated: edit `build_dashboard.py`, run `python monitoring/build_dashboard.py`, and Grafana
picks the new `grafana/dashboards/payload-hub.json` up within a minute.

## Alerts

`prometheus/alerts.yml`: an instance not answering, a component down (database, Kafka, worker, listener, trigger,
port), more than 5% of transactions failing, partner calls slow (p95 > 2 s), more than 1,000 messages waiting in
Kafka, deliveries failing, requests queueing for database connections. They show in Prometheus, Alertmanager and
Grafana; to get messages, uncomment a receiver (Slack, Teams, e-mail) in `alertmanager/alertmanager.yml` and run
`docker compose restart alertmanager`.

If your company already runs Prometheus/Grafana (or Datadog, Dynatrace, New Relic - they read the same format),
point it at `http://<instance>:8000/metrics` and import `grafana/dashboards/payload-hub.json` and
`prometheus/alerts.yml` instead of running this stack.

## The metrics

`payload_transactions_total{status,partner}`, `payload_inbound_seconds{partner,code}` (histogram),
`payload_end_to_end_seconds`, `payload_transform_seconds`, `payload_in_flight`, `payload_duplicates_total`,
`payload_kafka_backlog{queue,kind}`, `payload_kafka_messages{queue,kind}`, `payload_deliveries_total{partner,result}`,
`payload_forwards_total{partner,result}`, `payload_forward_seconds{partner}`, `payload_workflow_runs_total{workflow,status}`,
`payload_workflow_seconds{workflow}`, `payload_schedule_runs_total{task,status}`, `payload_port_items_total{port,result}`,
`payload_alerts_total{event,status}`, `payload_component_up{kind,name}`, `payload_db_pool_connections{state}`,
`payload_instance_info{instance}`.
