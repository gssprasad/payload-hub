# Deployment path: DEV → TEST → STAGING → PRODUCTION

Two things move through the environments, separately:

| What | How it moves | Built where |
|------|--------------|-------------|
| **The application** (code) | a versioned Docker image, built and tested once, then deployed unchanged to each environment | GitHub Actions (`.github/workflows/release.yml`) |
| **The configuration** built in the tool - partners, mappings, connections, workflows, scheduled tasks, ports, alert rules | **deployment packages** (JSON), with per-environment **overrides** | the *Deployments* page (Administer) or `scripts/deploy_package.py` |

Secret values and partner API keys never move: each environment has its own master key, secrets and partner keys.

```
 developer ──PR──▶ CI (lint, 200+ tests, Docker smoke test) ──merge──▶ main
                                                                         │ git tag v1.4.0
                                                                         ▼
                        ┌──────────── Release workflow ─────────────────────────────────────┐
  DEV (Deployments  ──▶ │ build image once ─▶ test it ─▶ publish ghcr.io/<owner>/payload-hub:v1.4.0 │
  page: export a        │      ─▶ TEST ─▶ STAGING ─(approval)─▶ PRODUCTION                    │
  package, commit it    │   each: backup → switch image → health → smoke test → rollback on failure │
  to deploy/packages)   │         → deploy configuration packages with that environment's overrides │
                        └───────────────────────────────────────────────────────────────────┘
```

## Every environment shows what it is

Set `PAYLOAD_HUB_ENVIRONMENT` (DEV, TEST, STAGING, PROD). The sidebar shows it as a coloured badge (production in
red) with the version, and `/health` reports both - so nobody changes production thinking it's a test system, and you
can always see which version runs where.

## Moving configuration: deployment packages

1. **Export** in the source environment: *Administer → Deployments → Export a package*. Pick partners, workflows,
   scheduled tasks, ports, alert rules…; with *Add dependencies* on, the connections, workflows, alert channels and
   secret names they use come along. Download the JSON.
2. **Overrides** - one file per environment with what differs there (`deploy/environments/test.example.json`):
   database hosts, API URLs, folders, the secret aliases to use, which schedules are on. Nested settings are merged,
   so you only write what changes.
3. **Check** in the target environment: *Deploy a package* shows the plan - each item *create / update (which fields)
   / unchanged* - and problems, e.g. a secret that doesn't exist there yet (create it on the Secrets page; values are
   never copied) or a reference to something missing. Nothing changes until you deploy.
4. **Deploy**: a database backup is taken first; everything is validated the same way as on the screens; listeners,
   ports and triggers restart; the deployment is in *History* and every change in the audit log. If something fails
   part-way, the backup has the state from before.

Packages from a newer version than the target are refused (upgrade the target first). Deploying the same package again
changes nothing.

**In a pipeline** (or from a laptop) the same works from the command line - passwords come from the environment:

```bash
export PAYLOAD_HUB_PASSWORD=...                       # the target's administrator password
python scripts/deploy_package.py export https://hub-dev.company.local --all -o deploy/packages/release.json
python scripts/deploy_package.py plan  https://hub-test.company.local deploy/packages/release.json --overrides deploy/environments/test.json
python scripts/deploy_package.py apply https://hub-test.company.local deploy/packages/release.json --overrides deploy/environments/test.json
```

Recommended: keep packages in git under `deploy/packages/` and overrides under `deploy/environments/<environment>.json`
(no secrets in them - only secret *aliases*). Then a release deploys code and configuration together, reviewed in a PR.

## Releasing the application

```bash
git tag v1.4.0 && git push origin v1.4.0
```

The *Release* workflow builds the image (with the version baked in), runs the full stack with it and the smoke test,
publishes `ghcr.io/<owner>/payload-hub:v1.4.0`, then deploys to **test**, **staging** and **production** in turn. Each
deployment, on the server, by `deploy/server/deploy.sh`:

1. pulls the image - if that fails nothing changes;
2. backs up the database with the version still running;
3. starts the new version, which applies its database migrations;
4. waits for `/health` to be UP and runs the smoke test (sign in, partner call, backup + verify);
5. **if 4 fails, switches back to the previous version automatically** and the workflow fails;
6. then deploys the configuration packages with that environment's overrides.

**Rolling back** later: run the *Release* workflow by hand (*Actions → Release → Run workflow*) with the earlier tag,
or on the server `./deploy.sh ghcr.io/<owner>/payload-hub:v1.3.2`. An older release runs fine on the newer database
(migrations are written to be backward compatible for one release: add tables and columns, don't drop or rename in the
same release). Restoring the pre-deployment backup is only needed if data itself must go back.

## One-time setup per environment

**The server** (a VM with Docker; or adapt the deploy step for Kubernetes - see below):

```bash
sudo mkdir -p /opt/payload-hub && sudo chown deploy /opt/payload-hub
# copy deploy/server/docker-compose.yml, deploy/server/deploy.sh and scripts/smoke_test.py there, then:
cp .env.example .env    # deploy/server/.env.example: PAYLOAD_HUB_ENVIRONMENT, DATABASE_URL, KAFKA_BOOTSTRAP,
                        # PAYLOAD_HUB_SECRET_KEY (this environment's own), ADMIN_PASSWORD, ...
pip install httpx       # for the smoke test
echo <token with read:packages> | docker login ghcr.io -u <user> --password-stdin   # if the image is private
```

PostgreSQL and Kafka are the environment's own services (managed / highly available - see the README's production
notes). Put HTTPS in front (load balancer or reverse proxy) for the console (8501) and the partner endpoints (8000).

**GitHub** - *Settings → Environments*: create `test`, `staging`, `production`, each with

| Kind | Name | Value |
|------|------|-------|
| variable | `DEPLOY_HOST` | the server (leave it out to skip this environment) |
| variable | `DEPLOY_USER` / `DEPLOY_PATH` | default `deploy` / `/opt/payload-hub` |
| variable | `HUB_URL` | the backend's URL, for deploying packages |
| secret | `DEPLOY_SSH_KEY` | a private key whose public key is in the server's `authorized_keys` |
| secret | `HUB_ADMIN_PASSWORD` | the environment's administrator password |

and on **production** (and staging) tick **Required reviewers** - the release waits for an approval there.

## Kubernetes instead of VMs

The image and everything above stays; only the "switch the server" step changes: run the backend as a Deployment
(2+ replicas - see *Running more than one instance* in OPERATIONS.md), the console as a second Deployment, a Secret
for the `.env` values, readiness probe `GET /health`. A rolling update gives the same safety as `deploy.sh`
(`kubectl rollout undo` to go back), and the configuration packages are deployed the same way.
