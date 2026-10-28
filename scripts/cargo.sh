#!/usr/bin/env bash
# Run cargo inside the pinned Rust toolchain image (matches celld's Dockerfile).
# Usage: scripts/cargo.sh build | test | clippy ...
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE=celld-cdc/rust-dev:1.98.1
docker image inspect "$IMAGE" >/dev/null 2>&1 ||
  docker build -q -t "$IMAGE" "$ROOT/deploy/dev" >/dev/null
exec docker run --rm ${CARGO_DOCKER_FLAGS:-} \
  -v "$ROOT":/src -w /src \
  -v celld-cdc-cargo-registry:/usr/local/cargo/registry \
  -v celld-cdc-cargo-git:/usr/local/cargo/git \
  -v celld-cdc-target:/src/target \
  -e CARGO_TERM_COLOR=never \
  -e CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-3}" \
  -e CARGO_PROFILE_DEV_DEBUG=line-tables-only \
  -e CARGO_PROFILE_TEST_DEBUG=line-tables-only \
  "$IMAGE" cargo "$@"
