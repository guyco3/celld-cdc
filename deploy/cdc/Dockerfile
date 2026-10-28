# The celld-cdc worker image around a binary built by scripts/cargo.sh
# (scripts/build-images.sh stages it here). For a self-contained build from
# source, use the Dockerfile at the repository root.
FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY celld-cdc /usr/local/bin/celld-cdc
ENTRYPOINT ["/usr/local/bin/celld-cdc"]
