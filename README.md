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

Open http://localhost:8000 for the built-in web UI.

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

## Partners

The Streamlit app has four pages: **Partners**, **Send & Test**, **Transactions** and
**Monitoring**.

**Onboarding.** On the Partners page, open *Onboard a new partner* and enter the name, input and
output formats, a sample input, an optional expected output and an optional mapping. Choose the
queues:

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

**Monitoring.** A separate page with a partner filter: totals, success rate, latency, a
per-minute chart, a by-partner table, every Kafka queue with messages and backlog, and recent
agent runs.

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
6. Open the app URL and sign in with `APP_PASSWORD`.

Streamlit Cloud puts idle apps to sleep; open the link shortly before a demo so it is awake.
