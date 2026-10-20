# Payload Hub

A Python payload-transformation service built on Kafka. It is the Python equivalent of an
Apache Camel + Spring Boot integration, plus an AI agent that writes the mappings for you.

```
UI / REST ──► payload.in ──► transformer ──► payload.out ──► result sink ──► PostgreSQL
                                   │
                                   └── failures ──► payload.dlq
```

| Java stack            | Here                                                    |
|-----------------------|---------------------------------------------------------|
| Spring Boot           | FastAPI + Uvicorn (`app/main.py`)                       |
| Apache Camel route    | Kafka consumer + JMESPath mapping (`app/pipeline.py`, `app/transform.py`) |
| Kafka                 | Apache Kafka 4 (Homebrew, KRaft mode) via `aiokafka`    |
| JPA / transaction log | PostgreSQL 17 via `psycopg` (`app/db.py`)               |
| Actuator / Micrometer | `/health` and Prometheus `/metrics`                     |
| (new) mapping agent   | Claude Opus 5.5 tool-using agent (`app/agent.py`)       |

## Run it

The services start automatically with macOS once started with `brew services`:

```bash
brew services start postgresql@17
```

```bash
brew services start kafka
```

Then the backend, from this folder:

```bash
.venv/bin/uvicorn app.main:app --port 8000
```

Port 8000 is the backend: http://localhost:8000 is a start page with a link to the full app,
the API documentation (`/docs`), `/health`, `/metrics`, and the original single-page tester at
`/classic`. The full application is the Streamlit app on port 8501 (below).

### Streamlit UI (optional)

With the backend running, start the Streamlit front end in a second terminal:

```bash
.venv/bin/streamlit run streamlit_app.py
```

It opens at http://localhost:8501 with the same features in four tabs: Transform, Mapping agent
(with Excel upload), Transactions (search) and Monitoring. It calls the backend's REST API, so
the backend must be running. Point it elsewhere with `PAYLOAD_HUB_API=http://host:8000`.

### VS Code

Open this folder in VS Code. The Run and Debug panel has **Backend (FastAPI + Kafka pipeline)**,
**Streamlit UI**, and **Backend + Streamlit** to start both at once. Stop any copies already
running in a terminal first, because the ports are shared.

### First-time setup on another machine

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

## Mapping agent

Type what you want in plain English, e.g. *"Turn this order into a shipment as XML with the
order id prefixed SHP-, the recipient's full name, and one package per item with sku and
quantity."* The agent:

1. inspects the payload in the Input box,
2. writes a JMESPath mapping and tests it against the real transformer, fixing it until the
   output matches your request,
3. saves it (it then appears in the Mapping dropdown with 🤖), and
4. explains what it did and any assumptions.

Click **Transform via Kafka** to run the new mapping through the pipeline. Every agent run is
logged in the `agent_runs` table and shown in the UI, with the tool calls and token usage.

The agent needs an Anthropic API key. Set it before starting the app:

```bash
export ANTHROPIC_API_KEY=your-key-here
```

Each agent run is a paid Claude API call (typically a few cents).

## Overview (landing page)

After sign-in the app opens on **📈 Overview**:

- **Status lights:** PostgreSQL, Kafka, pipeline workers, inbound listeners, adapters.
- **Server:** CPU, memory, disk, the app's memory and uptime.
- **PostgreSQL:** version, database size, connections.
- **Kafka:** topics, messages, backlog.
- **Platform counts:** partners, adapters, workflows, users, data tables.
- **Today:** transactions, success rate, latency, deliveries, workflow and agent runs.
- A 24-hour chart and the latest errors.

It refreshes every 10 s (`GET /api/overview`).

## Partner profile

Click a partner (or **📇 Partner profile**) for everything about it, on five tabs:

- **Overview:** identity, input/output formats and Content-Types, Kafka queues, all endpoints.
- **Inbound:** HTTP URL, Content-Type, auth header, API key, async option, curl, and any listener's
  adapter, address, folder or queue, username and password.
- **Outbound & response:** response mode, the forward API with its method, address, content types
  and response mapping, the delivery adapter, and each adapter's retry policy and last test.
- **Mapping & data:** mapping, samples, storage table and column mapping, mapping documents.
- **Activity & files:** statistics and a JSON export of the configuration (without keys or
  passwords).

Passwords and keys are masked. Administrators can switch on *Show keys and passwords*; writers can
show the partner's API key.

## AI providers for the agent

The mapping agent can use **Claude** (Opus 5.5 by default, Sonnet 5.5, Haiku 4.5, Fable 5.1),
**OpenAI**, **Gemini**, **GitHub Models** (GitHub's official API for the model families behind
Copilot, which has no public app API) or **Azure OpenAI**.

Administrators paste API keys under **AI Agent → 🔑 AI providers**, then:

- **Save and test:** checks the key by listing the provider's models.
- **Load models:** fetches the provider's current models.
- **Set default:** sets the default provider and model.

Anyone with write permission can pick another provider or model for a single run. Keys are
encrypted (Fernet) with `PAYLOAD_HUB_SECRET_KEY`, or with a key generated into `data/secret.key`
(git-ignored). Only the last 4 characters are ever shown. Environment variables (`ANTHROPIC_API_KEY`,
`OPENAI_API_KEY`, `GEMINI_API_KEY`, `GITHUB_TOKEN`, `AZURE_OPENAI_API_KEY`) still work as a
fallback. Every agent run records its provider and model.

## Users and sign-in

Both UIs require signing in. On first start the app creates **Administrator** with the password
from `ADMIN_PASSWORD`, or `manage` if that isn't set. **Change it after the first sign-in**
(sidebar → *My account*).

Administrators add users on the **Users** page and give each one any of:

| Permission | Allows |
|------------|--------|
| Read   | Viewing partners, transactions, monitoring and connections |
| Write  | Onboarding and editing partners, mappings and connections; sending messages; running the AI agent |
| Delete | Removing partners, documents and connections |
| Admin  | Managing users |

The menu (left sidebar) appears after signing in. You stay signed in across browser refreshes:
the session token is kept in the page URL (`?session=...`), so don't share that URL. Signing out
ends the session.

Permissions are enforced by the API itself (`/api/...` needs a session token), not just hidden in
the UI. Passwords are stored as PBKDF2-SHA256 hashes. Sessions last 12 hours and end at once
when a user signs out, changes password or is disabled. Partner endpoints (`/inbound/...`) keep
using partner API keys; `/health` and `/metrics` stay open for monitoring tools.

## Partners

The Streamlit app's pages: **Overview** (landing page), **Partners**, **Adapters & Connections**, **Workflows**, **AI Agent**, **Send & Test**,
**Transactions**, **Data**, **Monitoring** and (administrators) **Users**.

**Onboarding.** On the Partners page, *Onboard a new partner* opens a step-by-step wizard: basics,
payloads and mapping (and an optional mapping document), Kafka queues, inbound, outbound, and a
review with the flow diagram. Kafka queue choices:

| Queue mode | Kafka topics | Use it when |
|------------|--------------|-------------|
| Shared     | `payload.in`, `payload.out`, `payload.dlq` | low volume, quick onboarding |
| Dedicated  | `partner.<code>.in`, `.out`, `.dlq` (created automatically) | isolating a partner's traffic, failures and backlog |

A partner can be moved between shared and dedicated queues later (Queue tab). The pipeline
subscribes by topic pattern, so new dedicated queues are picked up within about 5 seconds with
no restart.

**Per-partner mapping.** Each partner has a Mapping tab (formats, sample input, expected output,
JMESPath expression, *Test mapping*) and a Mapping documents tab (upload, preview, download and
delete Excel/CSV specifications). *Generate mapping* asks the AI agent to build the partner's
mapping from its documents and sample input, and saves it on the partner.

**Sending.** On Send & Test, choose *Partner* to send with that partner's mapping and queue, or
*Ad-hoc mapping* to try any expression on the shared queues.

**AI Agent.** A page for the mapping agent: shows whether an Anthropic key is configured, builds a
mapping for a partner (from its documents and sample input, saved on the partner) or a standalone
mapping (upload an Excel spec and paste a sample payload), and keeps a run history showing each
step the agent took.

**Monitoring.** A separate page with a partner filter: totals, success rate, latency, a
per-minute chart, a by-partner table, every Kafka queue with messages and backlog, and recent
agent runs.

## Connections, HTTP endpoints and MQ adapters

```
 HTTP POST /inbound/<partner> ─┐                                           ┌─► RabbitMQ / ActiveMQ / IBM MQ queue
 RabbitMQ / ActiveMQ / IBM MQ ─┼─► Kafka <queue>.in ─► transformer ─► .out ─┼─► HTTP webhook
 Kafka topic ──────────────────┘                    └─► .dlq (failures)     └─► Kafka topic   (or store only)
```

**Connections page.** Add a connection once (RabbitMQ, ActiveMQ / Artemis over STOMP, IBM MQ, an
HTTP webhook or Kafka), test it, send a test message through it, and see which partners use it.
Passwords are write-only: the API never returns them. A built-in **test receiver**
(`/test-receiver/<name>`) acts as a webhook target for demos; its inbox is on the Connections page.

**Partner HTTP endpoint.** Every partner gets `POST /inbound/<code>` with its own API key
(header `X-API-Key`), shown on the partner's *Flow & endpoints* tab with a curl example and a
*Try it* button. It returns the transformed output (200) or the error (422). With `?wait=false`
it returns 202 and a transaction id to poll at `/inbound/<code>/transactions/<id>`.

**API access.** Click a partner in the Partners table (or *🔑 API access*) for its endpoint URL,
API key, request format, what the caller gets back, and a ready-to-run curl command.

**Route: caller → System B → caller.** At the top of a partner's *Flow & endpoints* tab:
partner A calls this partner's endpoint, the payload is transformed and sent to **System B**,
and System B's answer goes back to A.

| Setting | Options |
|---------|---------|
| When a caller sends a message | *Reply at once with the transformed payload* (no System B, default), or *Send to System B and reply with its answer* |
| System B endpoint | 👤 **This partner's endpoints** (dedicated - other partners can't see or use them), 🌍 **Common endpoints** (shared by all partners), or ➕ **New endpoint for this partner**, created right there: API (HTTP), file on FTP/SFTP, database table, RabbitMQ / ActiveMQ / IBM MQ queue, Kafka topic. *Make it common* / *Dedicate to this partner* switches an endpoint's scope |
| Where | API: method and path · file: folder · database: table · queue/topic: its name |
| System B receives | the transformed payload as is (JSON/XML), or **CSV** (one row per item of the transformed list, nested fields as `customer.name` columns; comma, semicolon, pipe or tab) - e.g. a CSV file dropped in an SFTP folder |
| The caller gets System B's answer | **translated** into the caller's format (or JSON / XML), optionally reshaped with a JMESPath mapping, or **as is** (System B's own body and content type) |

An API answers with its own response and HTTP status. A file, table or queue doesn't answer, so
the caller gets a **delivery receipt** - `{status: DELIVERED, system, type, target, detail, format,
transactionId}` - which can be translated like any answer. If System B can't be reached (after the
endpoint's retries) the caller gets 502.

Messages from MQ or Kafka on this route are sent to System B in the background, and the converted
response is stored on the transaction. Response headers: `X-Transaction-Id`, `X-Status`
(`COMPLETED` or `RESPONDED`) and `X-Upstream-Status`.

**Inbound and outbound per partner.** On *Flow & endpoints* (or in the onboarding wizard), pick
a connection and queue/topic to listen on, and where to deliver results. Deliveries are retried 3
times, then parked on the partner's failure queue. The tab shows the whole flow as a diagram with
live status.

Local brokers for testing (Homebrew):

```bash
brew install rabbitmq activemq && brew services start rabbitmq && brew services start activemq
```

Homebrew's RabbitMQ enables its STOMP plugin on port 61613, which clashes with ActiveMQ.
Disable it (`rabbitmq-plugins disable rabbitmq_stomp`) and enable ActiveMQ's STOMP connector in
its `activemq.xml`.

**IBM MQ** needs IBM's MQ client libraries (the IBM MQ redistributable client or MQ for
Developers), then `pip install ibmmq`. Until then the Connections page shows IBM MQ as "client
not installed". IBM MQ can't run on Streamlit Cloud.

On Streamlit Cloud the partner HTTP endpoints run inside the app and aren't reachable from
outside; use *Try it* for demos. They're fully callable when the backend runs on your laptop or a server.

## Workflows (multi-step: database → API → database → PDF → deliver)

The **⚙️ Workflows** page chains steps where each step can use the results of the earlier ones.
Every run has a context: `trigger` (the record that started it) plus each step's result under its
*Save result as* name.

**Building:** the *Steps* tab is a visual builder. Add steps from the palette, edit each one in its
card, move it up or down, duplicate it, switch it off (skipped when running) or remove it. The
*Flow diagram* view draws the workflow with its branches; *JSON* shows the saved definition.

| Step | Does |
|------|------|
| 🧱 DB connector | No-SQL database step: pick a connection, a table and an action (get one row, get rows, insert, update, delete); choose columns, fill **conditions** (`=`, `!=`, `>`, `>=`, `<`, `<=`, `like`, `in`, `is null`, `is not null`) and **values** in small tables with templates like `{{ trigger.id }}`; order and row limit. *Show the SQL* previews the statement; values are always sent as parameters. Update/delete without a condition must be ticked explicitly. Insert returns `{rowcount, key}` |
| 🗄️ SQL query | SELECT on any database connection or the built-in area; `:name` placeholders filled from *Parameters* (e.g. `order_id = {{ trigger.order_id }}`), never pasted into the SQL; one row or all rows |
| ✏️ SQL update | INSERT / UPDATE / DELETE written as SQL (skipped in a dry run) |
| 🌐 API call | GET/POST/PUT/PATCH/DELETE through an HTTP connection (path and body are templates), with the connection's retry policy; result `{status, data}` |
| 🔀 Transform | JMESPath over all results |
| 🔱 Branch (if / else) | JMESPath condition, e.g. ``order.total > `1000` ``; true runs the **Yes** steps, otherwise the **No** steps (each can hold any steps, including further branches), then the run carries on |
| ❓ Stop unless | JMESPath test; when false the run stops (status STOPPED) |
| 📄 Create PDF | Jinja2 HTML template (`{{ … }}`, `{% for %}`, `| money`) → PDF (xhtml2pdf), stored with the run |
| 📤 Deliver file | Send a PDF through any adapter, e.g. SFTP folder, HTTP API (skipped in a dry run) |

**Retry per step:** database, API, PDF and deliver steps have *If this step fails*: number of
attempts, the wait between them (optionally doubling each time), and then either **stop the run**
or **carry on** (the step's result becomes `null`, status ⚠️ FAILED_CONTINUED, so a later Branch can
check it). Every retry is listed in the run's timeline.

**Triggers:** manual (*Run now*, or `POST /api/workflows/<id>/run`), schedule (every N minutes,
optionally one run per row of a query), or database (one run per new row of `table:key`).

**Running:** *Dry run* runs everything except updates and deliveries. Each run shows a
step-by-step timeline with every step's result, and the PDF inline and as a download. Failed runs
can be **rerun from the failed step**, reusing earlier results. Up to 4 runs execute in parallel.

**Example (`Order invoice`, on the sample tables in Local PostgreSQL):**

1. read the order;
2. call the customer system (built-in mock at `/mock/customers/<id>`) with `customer_id`;
3. read the customer and the order lines;
4. total them, but only if there are lines;
5. mark the order INVOICED;
6. create the invoice PDF (template in `samples/workflows/invoice-template.html`);
7. deliver it to the SFTP test server's `invoices` folder.

*customer-followup* shows the new pieces: a DB connector finds the customer, a branch checks
whether it exists, the Yes path calls the customer API with 3 retries (carrying on if it still
fails) and lists the recent orders, the No path marks the customer unknown.

*Invoice on request* runs the same steps for every new row in `invoice_requests`, and *Scheduled
invoices* (paused) does it hourly for every order with status NEW.

## Retry policy for outbound endpoints

Every outbound-capable connection (HTTP, RabbitMQ, ActiveMQ, IBM MQ, Kafka) has a retry policy on
the *Adapters & Connections* page: delivery attempts, the first wait (doubling each time) and the
longest wait. HTTP connections also list which status codes are temporary (default
`408,425,429,500,502,503,504`); other errors, such as 400 or 401, are treated as permanent and not
retried. Failed deliveries are parked on the partner's failure queue. Forwarding to an API uses
the same policy, with waits capped at 5 s because the caller is waiting.

The partner's *API access* view and *Flow & endpoints* tab list every inbound and outbound
address (HTTP endpoint, MQ listener, forward API, delivery target, storage table) with its retry
policy.

## Database adapter

*Adapters & Connections → Add a connection → Database* connects PostgreSQL, MySQL/MariaDB,
SQL Server, Oracle or SQLite (drivers: psycopg, PyMySQL, pymssql, python-oracledb thin mode,
built-in sqlite3; all through SQLAlchemy). Pick the database type first: the port fills in
automatically. Then enter host, database (or service name, or file path), user, password and
optional extra options (e.g. `sslmode=require`). Like other connections it has *Test*, a retry
policy, and shows which partners use it.

- **Outbound:** choose the database as a partner's outbound connection and a table as the
  target. A JSON object (or list of objects) is inserted, with each field going to the column of
  the same name. `transaction_id` and `partner_code` are filled automatically when the table has them.
- **Inbound:** use `table:key_column` (e.g. `erp_outbox:id`) as the inbound source. New rows (key
  greater than the last one seen) are polled every *poll interval* seconds and become messages,
  with no duplicates across restarts. Rows are JSON, or XML (`<row>`) for XML partners.
- **Store in the database** (per partner) can target any database connection, not just the
  built-in data area.
- **Data page:** the *Database* picker switches tables, table creation and the SQL console to any
  database connection. There, statements run with that connection's database user (whose own
  rights apply too), and read queries are always rolled back.

## FTP / FTPS / SFTP adapter (files)

*Adapters & Connections → Add a connection → FTP / FTPS / SFTP*. Pick the protocol first (the port
fills in: SFTP 22, FTP/FTPS 21). Then set host, user, password, base folder, inbound file pattern
(e.g. `*.xml`), what happens after reading (move to an archive subfolder, or delete), the outbound
file name pattern (`{partner}_{timestamp}_{txn}.{ext}`, also `{date}`), the poll interval, passive
mode, and the retry policy.

- **Inbound:** use a folder as the partner's inbound source. Matching files are picked up once
  their size is stable for one poll (so half-uploaded files are skipped), become messages, and are
  then archived (`processed/<time>_<name>`) or deleted.
- **Outbound:** use a folder as the outbound target. Each transformed payload is written as a new
  file, via a temporary `.part` name and a rename, so readers never see half a file.
- **📂 Files** on the connection card: browse folders, view files, and upload a file (e.g. to drop
  a test message into a partner's inbound folder).

SFTP host keys aren't checked yet (fine for trusted networks; pin host keys before production use).

Local test servers (development only): `.venv/bin/python scripts/dev_file_servers.py` runs FTP on
127.0.0.1:2121 and SFTP on 127.0.0.1:2222, both with user `demo` / `demo123`, serving `data/ftp`
and `data/sftp`. Needs `pip install pyftpdlib`.

## Seeing tables

- An empty database on the Data page offers **Create sample tables**: `customers`, `orders` and
  `order_lines` with primary keys (composite on order_lines), a unique email and example rows.
- Administrators also get **🛡️ Payload Hub system tables** in the Database picker: the tool's own
  tables (partners, transactions, events, connections, users...), read-only, with password hashes,
  session tokens, connection secrets, partner API keys and document contents left out.

## Table designer

Data → **🧱 Table designer** works on the built-in area and on every database connection, e.g. the
**Local PostgreSQL (default)** connection to the server's `postgres` database. Design columns in a
grid: 🔑 PK (tick several for a composite key), name, type (varchar/text/integer/bigint/numeric/
boolean/date/timestamp/json/uuid), length, precision/scale, can-be-empty, unique, auto-increment
and default (a value, `now`, `true`/`false`). The SQL preview updates live in the selected
database's dialect. **🛠️ Modify a table** adds or drops columns, sets or removes the primary key,
and adds or removes unique constraints (SQLite can't change keys on an existing table).

## Data tables and storing partner data

The **Data** page creates tables (column editor with types), browses their rows, and has a SQL
console. Everything lives in the `userdata` schema and runs as the restricted role
`payloadhub_data`, which can't touch Payload Hub's own tables. Permissions: SELECT needs read;
INSERT, UPDATE, CREATE and ALTER need write; DELETE, DROP and TRUNCATE need delete. One statement
at a time, read queries run read-only, and every statement has a 15 s limit.

Per partner, *Flow & endpoints → Store in the database* is an on/off switch. When on, every
completed message writes rows into a chosen table. You pick:

- the source (transformed or original payload);
- optionally, a list to turn into one row per item (e.g. `salesOrder.lines`);
- a JMESPath expression per column. Use `$root.<path>` for whole-payload fields, or
  `$transaction_id`, `$partner` and `$received_at`.

*Preview* shows the rows for the partner's sample without writing anything. Each message's
timeline shows `STORED` or `STORE_FAILED`.

## Mapping documents (Excel)

In the Mapping agent panel, drop or choose an Excel workbook (`.xlsx`, `.xlsm`) or a CSV file.
The app shows a preview of every visible sheet. When you click **Ask agent**, the whole
document goes to the agent as the source of truth: it implements every target field and rule,
checks its output row by row, and lists any rows it couldn't implement (with sheet and row
number). Instructions in the text box are optional and add to, or override, the document.

Column layouts don't need to follow a fixed template. The usual columns (source field,
target field, data type, mandatory, transformation rule, notes) are understood. An example is
in `samples/order-to-shipment-mapping.xlsx`, also downloadable from the UI.

Limits: 10 MB per file, 2,000 rows per sheet. Old `.xls` files must be re-saved as `.xlsx`.
The document's filename and content are stored with each agent run.

## Tracking transactions

The Transaction log has a search bar. Search by:

- **Transaction ID**, full or the first few characters (e.g. `c8fc6ec5`)
- **Any value in the payload**: input, output or error text (e.g. an order number `1042`,
  `INV-1042`, a customer name, an error message)
- **Mapping name**
- **Status**, and a **From / To** date-time range

Results show the match count and keep updating live. Click a row for the full input, output,
mapping and the step-by-step event timeline.

## Monitoring

- **UI**: health of PostgreSQL, Kafka and both consumers; totals, success rate, average and
  p95 latency; per-minute chart; full transaction log with a per-transaction event timeline.
- **`GET /health`**: 200 when the database, Kafka and consumers are up, 503 otherwise.
- **`GET /metrics`**: Prometheus metrics: `payload_transactions_total{status,mapping}`,
  `payload_transform_seconds`, `payload_end_to_end_seconds`, `payload_in_flight`.
- **SQL**: `transactions`, `transaction_events`, `mappings` and `agent_runs` tables in the
  `payloadhub` database.

## API

| Method | Path                        | Purpose                                         |
|--------|-----------------------------|-------------------------------------------------|
| POST   | `/api/transform`            | Submit a payload, wait for the pipeline result  |
| POST   | `/api/agent`                | Ask the agent to build and save a mapping       |
| GET    | `/api/samples`              | Built-in and agent-saved mappings               |
| GET    | `/api/transactions`         | Search the log (`?q=1042&status=FAILED&since=...&until=...`) |
| POST   | `/api/mapping-doc`          | Upload an Excel/CSV mapping document (multipart `file`) |
| GET    | `/api/transactions/{id}`    | One transaction with its event timeline         |
| GET    | `/api/stats`                | Totals, latency, per-minute counts              |
| GET    | `/api/agent/runs`           | Agent run history                               |
| GET/POST | `/api/partners`           | List / onboard partners (`queue_mode`: shared or dedicated) |
| GET/PUT/DELETE | `/api/partners/{code}` | One partner                                  |
| POST   | `/api/partners/{code}/queue` | Move between shared and dedicated queues       |
| POST   | `/api/partners/{code}/test`  | Run the partner's mapping on its sample input  |
| POST/GET | `/api/partners/{code}/documents` | Upload / list mapping documents          |
| POST   | `/api/partners/{code}/agent` | Generate the partner's mapping with the agent  |
| GET    | `/api/queues`               | Kafka topics with messages and backlog         |
| GET    | `/api/stats/partners`       | Per-partner totals and latency                 |

## Mapping functions

Standard JMESPath plus `concat(a, b, ...)`, `upper(s)`, `lower(s)`, `multiply(a, b)` and
`to_float(v)`. XML input is parsed with xmltodict: attributes are `"@name"` keys and all
values are strings.

## Deploy to Streamlit Community Cloud

Streamlit Cloud runs only `streamlit_app.py`, so the pipeline runs inside it
(`EMBED_BACKEND = "true"`) against hosted Kafka and PostgreSQL.

1. **Kafka (Aiven, free plan)**: create an Apache Kafka service. Free plans limit the number of
   topics, and each dedicated partner adds three, so use shared queues for most demo partners. On its Overview page choose
   the **SASL** authentication method, then copy the host:port, user, password and the CA
   certificate. Topics are created automatically on first start.
2. **PostgreSQL (Neon, free plan)**: create a project and copy its connection string.
3. **GitHub**: push this repository (a private repository is fine).
4. **Secrets**: run the helper in your terminal. It asks for the Neon and Aiven details (hiding
   passwords), reads `ca.pem`, tests both connections, writes `.streamlit/secrets.toml`
   (git-ignored) and copies it to the clipboard:

   ```bash
   .venv/bin/python scripts/setup_cloud_secrets.py
   ```

5. **Streamlit Cloud** (share.streamlit.io): **Create app** > select the repository, branch
   `main`, main file `streamlit_app.py`. Under **Advanced settings**, pick Python 3.12 or newer
   and paste the secrets. For an app that already exists: **Manage app > ⋮ > Settings > Secrets**.
6. Open the app URL and sign in as `Administrator` with `ADMIN_PASSWORD`.

Streamlit Cloud puts idle apps to sleep; open the link shortly before a demo so it is awake.
