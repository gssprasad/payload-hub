# Running Payload Hub - operations runbook

## What to protect

| What | Where | How it's protected |
|------|-------|--------------------|
| Database (partners, mappings, routes, workflows, transactions, users, encrypted credentials, audit log) | PostgreSQL `payloadhub` | nightly backup + on demand (Backups page) |
| Master key (decrypts every stored credential) | `data/secret.key` or `PAYLOAD_HUB_SECRET_KEY` | **copy it to a safe place yourself** - it is deliberately *not* inside backups |
| Backups | `data/backups` (`PAYLOAD_HUB_BACKUP_DIR`) | copy off the machine (e.g. a nightly sync to a file share or object storage) |
| Data in other databases you connected, files in polled folders | their own systems | their own backups |

## Backups

- **Nightly**: a scheduled task *Nightly database backup* (daily 02:00, server time) runs `pg_dump`. Create it with
  one click on the Backups page if it's missing. The newest `PAYLOAD_HUB_BACKUP_KEEP` (default 14) are kept.
- **On demand**: *Back up now* on the Backups page, or `python -m app.backup create` (do this before upgrades).
- Each backup has a manifest (`.json`) with its SHA-256, the schema version and the master-key fingerprint.
- **Verify** (page or `python -m app.backup verify <file>`) checks the checksum and that `pg_restore` can read it.
- A failed backup raises the `backup_failed` alert - add a rule for it.

## Restore

1. Stop Payload Hub (backend and web console).
2. Put the master key that matches the backup in place - the Backups page shows each backup's key fingerprint.
3. `python -m app.backup verify <file>`
4. `python -m app.backup restore <file> --yes` - one transaction: if it fails, nothing changes.
5. Start Payload Hub; it applies any newer migrations.

**Restore drill (do it monthly)** - restore into a scratch database without touching production:

```bash
python -m app.backup restore <file> --into postgresql://localhost/restore_drill --yes
psql -d restore_drill -c "select count(*) from partners"
dropdb restore_drill
```

Restoring onto a **new server**: install PostgreSQL, create an empty `payloadhub` database, start Payload Hub once
(it creates the restricted data role), stop it, then restore as above.

## Upgrades and schema migrations

The schema is versioned with Alembic (`migrations/versions`). The app migrates the database at start-up, under a
database lock, one transaction per migration.

```bash
python -m app.backup create          # always back up first
python -m app.migrate status         # current / latest / pending
python -m app.migrate upgrade        # or just start the new version
python -m app.migrate downgrade 0001_baseline   # step back (the baseline itself can't be undone - restore instead)
```

To let a DBA apply migrations, set `PAYLOAD_HUB_AUTO_MIGRATE=false`: the app then refuses to start while
migrations are pending, with a message saying what to run.

**Developers** - every schema change is a new file: `python -m app.migrate new "add invoice table"`, write
`upgrade()` and `downgrade()` SQL, and never edit a migration that has been released.

## Alerts

Alerts page → *Channels*: a webhook (Slack / Teams incoming webhook, or any URL that takes JSON) or email (SMTP;
password as a stored secret). *Send a test alert* to check it. Then *Rules*:

| Event | Suggested rule |
|-------|----------------|
| `service_down` | always - Kafka, database or pipeline workers down (and recovered) |
| `listener_down`, `port_error` | always - a partner listener, workflow trigger or port in error |
| `delivery_failed` | per important partner, cool-down 15 min |
| `error_rate` | e.g. 10 failed transactions in 10 minutes |
| `workflow_failed`, `schedule_failed`, `backup_failed` | always |

The watcher checks every `ALERT_WATCH_SECONDS` (default 30). Repeats within a rule's cool-down are logged as
*suppressed*; down-states alert once when they go down and once when they recover.

## Audit trail

Every change made through the app is recorded with who, when and **what changed** (before → after values, never
secrets): Secrets page → *Audit log*, and each partner's *Activity* tab shows its own change history.

## Master key

- Back up `data/secret.key` separately from the database backups.
- Rotate it from Secrets → *Master key* (file-based keys): everything is re-encrypted in one transaction and the old
  key is kept as `secret.key.<time>.bak`. Take a backup right after rotating - older backups need the old key.

## Running more than one instance

Start several backends against the same PostgreSQL database and Kafka cluster (behind a load balancer for HTTP):

- **Partner calls** can land on any instance: if another instance finishes the transaction, the instance holding
  the caller finds out within 50 ms (it checks all its waiting callers in one database query), so the caller
  always gets its answer.
- **Queue consumers** (Kafka, RabbitMQ, ActiveMQ, IBM MQ) and **HTTP ports** run on every instance and share the work.
- **Pollers** (folders, FTP/SFTP, database tables), **workflow database triggers** and the **alert watcher** run on
  exactly one instance - the one holding its database lock. The Ports page shows the others as *standby*.
- **Schedules** (scheduled tasks and workflow schedules) are claimed by one instance per run.
- **Failover**: when an instance dies, its database connection drops and its locks are freed; another instance
  takes the pollers over within `PAYLOAD_HUB_SUPERVISE_SECONDS` (default 15-30 s). Messages already in Kafka are
  handed to the remaining instances once Kafka notices (its consumer session timeout, about 10 s); nothing is lost.
  Files a poller was working on when it died are put back and processed again by the new owner.
- Migrations run once (under a lock) whichever instance starts first.

## Health checks

- `GET /health` - 200 when the database, Kafka and the pipeline workers are up, 503 otherwise (for load balancers
  and uptime monitors).
- `GET /metrics` - Prometheus metrics.
