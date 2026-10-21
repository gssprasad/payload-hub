# Installing Payload Hub on another machine (Linux or Windows)

**Recommended: Docker.** One command starts PostgreSQL, Kafka, the backend and the web console - the same on Linux and
Windows. (Running natively without Docker is described at the end; on Windows use Docker or WSL2.)

Needs: 4 GB RAM free (8 GB comfortable), 10 GB disk, ports 8000 and 8501 free.

## 1. Install Docker

| System | How |
|---|---|
| **Linux** (Ubuntu, Debian, RHEL, …) | Docker Engine + Compose plugin: https://docs.docker.com/engine/install/ - then `sudo usermod -aG docker $USER` and log out/in |
| **Windows 10/11** | Docker Desktop: https://docs.docker.com/desktop/setup/install/windows-install/ - keep "Use WSL 2" ticked, restart when asked |

Check: `docker compose version` prints a version.

## 2. Get Payload Hub

The repository is private, so sign in to GitHub:

- **Easiest:** on github.com open the repository → green **Code** button → **Download ZIP**, and unzip it, or
- **git:** `git clone https://github.com/gssprasad/payload-hub.git` - when asked for a password, use a GitHub
  *personal access token* (GitHub → Settings → Developer settings → Fine-grained tokens, read access to this repository).

## 3. Settings

In the `payload-hub` folder copy `.env.example` to `.env` and fill it in:

```
POSTGRES_PASSWORD=<a database password you choose>
ADMIN_PASSWORD=<the password for the Administrator user>
PAYLOAD_HUB_SECRET_KEY=<a new master key - see below>
PAYLOAD_HUB_ENVIRONMENT=DEV
TZ=Asia/Kolkata
```

Master key - generate one (keep a copy somewhere safe; stored passwords can't be decrypted without it):

- Linux: `python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"`
- Windows (PowerShell): `[Convert]::ToBase64String((1..32 | % {[byte](Get-Random -Max 256)})).Replace('+','-').Replace('/','_')`

## 4. Start

```
docker compose up -d --build
```

The first start downloads and builds for a few minutes. Then:

- Web console: **http://localhost:8501** - sign in as `Administrator` with your `ADMIN_PASSWORD`
- Partner endpoints and API: **http://localhost:8000** (`/health` should say `"status":"UP"`)

Check it end to end: `python scripts/smoke_test.py http://localhost:8000 <admin password> http://localhost:8501`
(needs Python with `pip install httpx`).

Day to day: `docker compose stop` / `docker compose start`, logs with `docker compose logs -f backend`, upgrade with
`git pull` (or a new ZIP) and `docker compose up -d --build` - the database is migrated automatically.

## 5. Bring your data over (optional)

- **Only the configuration** (partners, connections, workflows, schedules, ports, alert rules): export a package on the
  old machine (*Administer → Deployments*), deploy it on the new one. Secrets are not copied - create them on the new
  machine's Secrets page.
- **Everything, including transactions:** take a backup on the old machine (*Backups → Back up now* → *Download*),
  put the **old machine's master key** in the new `.env` (`PAYLOAD_HUB_SECRET_KEY`), copy the backup file into the
  `payload-hub` folder, and restore it with the app stopped:

  ```
  docker compose stop backend console
  docker compose run --rm -v "$PWD:/import" backend python -m app.backup restore /import/payloadhub_<time>.dump --yes
  docker compose start backend console
  ```

  (Windows PowerShell: use `${PWD}` instead of `$PWD`.)

## 6. Reaching it from other computers

Open ports 8501 (console) and 8000 (partner endpoints) in the firewall (Linux: `sudo ufw allow 8501,8000/tcp`;
Windows Defender Firewall: allow Docker Desktop). For anything beyond a test network, put HTTPS in front
(see DEPLOYMENT.md). Monitoring: `cd monitoring && docker compose up -d` (Grafana on port 3000).

## Without Docker (advanced)

**Linux** - tested in CI (Ubuntu):

1. Python 3.13, PostgreSQL 16+ (with client tools), Java 17+ and Apache Kafka 3.9+/4.x (KRaft mode).
2. `python3.13 -m venv .venv && .venv/bin/pip install -r requirements.txt`
3. Create the database: `sudo -u postgres createuser -P payloadhub && sudo -u postgres createdb -O payloadhub payloadhub`
   (the user needs `CREATEROLE`, or create the role `payloadhub_data` once as postgres).
4. Environment: `DATABASE_URL=postgresql://payloadhub:<pw>@localhost/payloadhub`, `KAFKA_BOOTSTRAP=localhost:9092`,
   `ADMIN_PASSWORD`, `PAYLOAD_HUB_SECRET_KEY`.
5. Run `.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000` and
   `.venv/bin/streamlit run streamlit_app.py --server.port 8501` - as systemd services for production.

**Windows** - Kafka isn't supported natively on Windows by its maintainers, and native Windows hasn't been tested
for Payload Hub. Use Docker Desktop (above), or install Ubuntu in **WSL2** (`wsl --install -d Ubuntu`) and follow the
Linux steps inside it.
