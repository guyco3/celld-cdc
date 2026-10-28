# celld plus esbuild (needed by `celld deploy`), built from a celld checkout.
#   docker build --target build --build-arg CELLD_PROFILE=lab \
#     -t celld-cdc/celld-build:v0.6.3 ../celld
ARG CELLD_BUILD_IMAGE=celld-cdc/celld-build:v0.6.3
FROM ${CELLD_BUILD_IMAGE} AS celld

FROM node:22-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    npm install -g esbuild@0.25.10 && npm cache clean --force
COPY --from=celld /out/celld /usr/local/bin/celld
ENTRYPOINT ["/usr/local/bin/celld"]
