# syntax=docker/dockerfile:1.7
# The celld-cdc worker image: one binary, every subcommand.
FROM rust:1.98.1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,id=celld-cdc-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=celld-cdc-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=celld-cdc-release-target,target=/src/target,sharing=locked \
    cargo build --release --locked -p celld-cdc && \
    install -m 755 target/release/celld-cdc /usr/local/bin/celld-cdc

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY LICENSE /usr/share/doc/celld-cdc/
COPY --from=build /usr/local/bin/celld-cdc /usr/local/bin/celld-cdc
ENTRYPOINT ["/usr/local/bin/celld-cdc"]
