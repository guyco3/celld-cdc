#!/usr/bin/env bash
# Build the images docker-compose runs:
#   celld-cdc/celld-build:v0.6.3  celld compiled from a checkout (CELLD_SRC)
#   celld-cdc/celld:v0.6.3        celld + esbuild, for nodes and `celld deploy`
#   celld-cdc/celld-cdc:dev       the celld-cdc workers
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CELLD_SRC="${CELLD_SRC:-$ROOT/../celld}"
CELLD_TAG="${CELLD_TAG:-v0.6.3}"

if ! docker image inspect "celld-cdc/celld-build:$CELLD_TAG" >/dev/null 2>&1; then
  [ -d "$CELLD_SRC" ] || { echo "no celld checkout at $CELLD_SRC (set CELLD_SRC)"; exit 1; }
  echo "building celld from $CELLD_SRC (first build takes a while)"
  docker build --target build --build-arg CELLD_PROFILE=lab \
    -t "celld-cdc/celld-build:$CELLD_TAG" "$CELLD_SRC"
fi
docker build -t "celld-cdc/celld:$CELLD_TAG" \
  --build-arg "CELLD_BUILD_IMAGE=celld-cdc/celld-build:$CELLD_TAG" "$ROOT/deploy/celld"
# celld-cdc: by default reuse the cargo volume scripts/cargo.sh builds in
# (CDC_PROFILE=dev or release); CDC_FROM_SOURCE=1 builds the root
# Dockerfile instead, which needs no volume but compiles everything again.
if [ "${CDC_FROM_SOURCE:-0}" = 1 ]; then
  docker build -t celld-cdc/celld-cdc:dev "$ROOT"
else
  PROFILE="${CDC_PROFILE:-dev}"
  DIR=$([ "$PROFILE" = dev ] && echo debug || echo "$PROFILE")
  CARGO_DOCKER_FLAGS="-e CARGO_BUILD_JOBS=4" "$ROOT/scripts/cargo.sh" build --profile "$PROFILE" -p celld-cdc
  docker run --rm -v celld-cdc-target:/t -v "$ROOT/deploy/cdc":/out debian:bookworm-slim \
    cp "/t/$DIR/celld-cdc" /out/celld-cdc
  docker build -t celld-cdc/celld-cdc:dev "$ROOT/deploy/cdc"
  rm -f "$ROOT/deploy/cdc/celld-cdc"
fi
