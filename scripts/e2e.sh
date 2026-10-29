#!/usr/bin/env bash
# End-to-end: counter actors on a two-node celld fleet, through S3 and SQS,
# to SCD1/SCD2 Delta tables, with failures injected along the way.
#
#   scripts/e2e.sh          build images if missing, run, check, leave it up
#   scripts/e2e.sh --down   also tear the stack down at the end
#
# Phases:
#   1. traffic on both nodes (KV increments, SQL inserts, a reset)
#   2. purge the doorbell queue mid-traffic (the sweep must recover)
#   3. kill a decoder mid-traffic (its leases expire, the other continues)
#   4. kill a celld node (cells move to the survivor in a new epoch)
# After each phase the tables must match what the app reports.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/deploy/docker-compose.yml")
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
NAMES=(alice bob carol dave)
A=http://127.0.0.1:8080
B=http://127.0.0.1:8082

log() { printf '\n== %s\n' "$*"; }

for img in celld-cdc/celld:v0.6.3 celld-cdc/celld-cdc:dev; do
  docker image inspect "$img" >/dev/null 2>&1 || "$ROOT/scripts/build-images.sh"
done

log "start the stack"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d
for _ in $(seq 1 60); do curl -sf "$A/state?name=probe" >/dev/null && break; sleep 2; done
for _ in $(seq 1 60); do curl -sf "$B/state?name=probe" >/dev/null && break; sleep 2; done

# req ARGS...: curl that retries for a minute, through takeovers.
req() {
  for _ in $(seq 1 60); do curl -sf "$@" && return 0; sleep 1; done
  echo "request failed: $*" >&2; return 1
}

# traffic NODE ROUNDS: increments, clicks, and one reset on carol
traffic() {
  local node=$1 rounds=$2
  for _ in $(seq 1 "$rounds"); do
    for n in "${NAMES[@]}"; do
      req "$node/?name=$n" >/dev/null
    done
    req -XPOST "$node/click?name=bob&delta=2&note=from-e2e" >/dev/null
    req -XPOST "$node/click?name=dave&delta=1" >/dev/null
  done
}

expected() {
  local node=${1:-$A}
  printf '{' >"$WORK/expected.json"
  local sep=""
  for n in "${NAMES[@]}"; do
    printf '%s"%s":%s' "$sep" "$n" "$(req "$node/state?name=$n")" >>"$WORK/expected.json"
    sep=","
  done
  printf '}' >>"$WORK/expected.json"
}

dump() {
  "${COMPOSE[@]}" run --rm --no-deps -T materializer dump "$1" 2>/dev/null
}

# Poll until the tables match the app, or fail after $1 seconds.
converge() {
  local deadline=$((SECONDS + ${1:-180}))
  while true; do
    dump scd1 >"$WORK/scd1.jsonl" || true
    dump scd2 >"$WORK/scd2.jsonl" || true
    if python3 "$ROOT/scripts/e2e_check.py" "$WORK/expected.json" "$WORK/scd1.jsonl" "$WORK/scd2.jsonl" >"$WORK/check.txt"; then
      cat "$WORK/check.txt"
      return 0
    fi
    if ((SECONDS > deadline)); then
      echo "did not converge:"; cat "$WORK/check.txt"
      "${COMPOSE[@]}" logs --tail 40 decoder-1 decoder-2 materializer sweep
      return 1
    fi
    sleep 5
  done
}

log "phase 1: traffic on both nodes"
traffic "$A" 5
traffic "$B" 5
curl -sf -XPOST "$A/click?name=carol&delta=7&note=before-reset" >/dev/null
curl -sf -XPOST "$A/reset?name=carol" >/dev/null
curl -sf "$B/?name=carol" >/dev/null
expected
converge 240

log "phase 2: purge the doorbell queue mid-traffic"
traffic "$A" 3 &
sleep 1
docker exec celld-cdc-localstack-1 awslocal sqs purge-queue \
  --queue-url http://localhost:4566/000000000000/cdc-doorbell
wait
expected
converge 240

log "phase 3: kill decoder-1 mid-traffic"
traffic "$B" 3 &
sleep 1
"${COMPOSE[@]}" kill decoder-1
wait
expected
converge 240
"${COMPOSE[@]}" up -d decoder-1

log "phase 4: kill celld-a; its cells move to celld-b"
"${COMPOSE[@]}" kill celld-a
traffic "$B" 3
expected "$B"
converge 300
"${COMPOSE[@]}" up -d celld-a

log "change log summary"
dump changes | python3 -c '
import collections, json, sys
ops = collections.Counter()
for line in sys.stdin:
    e = json.loads(line)
    ops[(e["actor_name"], e["table"], e["op"])] += 1
for k, v in sorted(ops.items(), key=str):
    print(f"  {k[0]:>6} {k[1]:>8} {k[2]}  {v}")
'
log "all phases passed"
if [[ "${1:-}" == "--down" ]]; then "${COMPOSE[@]}" down -v; fi
