#!/usr/bin/env bash
# Switch this server to another Payload Hub version - safely.
#   ./deploy.sh ghcr.io/<owner>/payload-hub:v1.4.0
# 1. pulls the image (nothing changes if that fails)        2. remembers the version running now, backs up the database
# 3. starts the new version (it migrates the database)      4. waits until /health is UP
# 5. runs the smoke test                                     6. if 4 or 5 fail: goes back to the previous version
# Run it in the folder with docker-compose.yml and .env. Needs: docker compose, curl, python3 (with httpx for the
# smoke test: pip install httpx). ADMIN_PASSWORD from .env is used for the smoke test.
set -euo pipefail
cd "$(dirname "$0")"
NEW_IMAGE="${1:?usage: ./deploy.sh <image:tag>}"
API_PORT="$(grep -E '^PAYLOAD_HUB_API_PORT=' .env 2>/dev/null | cut -d= -f2- || true)"; API_PORT="${API_PORT:-8000}"
CONSOLE_PORT="$(grep -E '^PAYLOAD_HUB_CONSOLE_PORT=' .env 2>/dev/null | cut -d= -f2- || true)"; CONSOLE_PORT="${CONSOLE_PORT:-8501}"
ADMIN_PASSWORD="$(grep -E '^ADMIN_PASSWORD=' .env | cut -d= -f2-)"
HEALTH_URL="http://127.0.0.1:${API_PORT}/health"
STATE=.deployed-image

say() { echo "[$(date '+%H:%M:%S')] $*"; }

HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-300}"   # seconds to wait for /health to say UP

healthy() {
  for _ in $(seq 1 $((HEALTH_TIMEOUT / 5))); do
    if curl -sf "$HEALTH_URL" | grep -q '"status":"UP"'; then return 0; fi
    sleep 5
  done
  return 1
}

start() { PAYLOAD_HUB_IMAGE="$1" docker compose up -d --remove-orphans; }

say "pulling $NEW_IMAGE"
if ! PAYLOAD_HUB_IMAGE="$NEW_IMAGE" docker compose pull; then
  docker image inspect "$NEW_IMAGE" > /dev/null 2>&1 || { say "can't get $NEW_IMAGE - nothing changed"; exit 1; }
  say "using the local copy of $NEW_IMAGE"
fi
PREVIOUS="$(cat "$STATE" 2>/dev/null || true)"
if [ -n "$PREVIOUS" ] && [ "${SKIP_BACKUP:-0}" != "1" ]; then
  # a restore point from just before the new version (which may migrate the database) starts
  say "backing up the database (running version: $PREVIOUS)"
  PAYLOAD_HUB_IMAGE="$PREVIOUS" docker compose exec -T backend python -m app.backup create > /dev/null \
    || { say "backup failed - not deploying (SKIP_BACKUP=1 to deploy without one)"; exit 1; }
fi
say "running now: ${PREVIOUS:-nothing}; switching to $NEW_IMAGE"
start "$NEW_IMAGE"
if healthy && python3 smoke_test.py "http://127.0.0.1:${API_PORT}" "$ADMIN_PASSWORD" "http://127.0.0.1:${CONSOLE_PORT}"; then
  echo "$NEW_IMAGE" > "$STATE"
  say "deployed $NEW_IMAGE"
  exit 0
fi
say "the new version isn't healthy"
PAYLOAD_HUB_IMAGE="$NEW_IMAGE" docker compose logs --tail 80 backend || true
if [ -n "$PREVIOUS" ]; then
  say "rolling back to $PREVIOUS"
  start "$PREVIOUS"
  if healthy; then say "rolled back to $PREVIOUS - investigate $NEW_IMAGE before trying again"; else say "ROLLBACK FAILED - check the server now"; fi
fi
exit 1
