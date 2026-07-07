# syntax=docker/dockerfile:1

# --- Build stage -------------------------------------------------------------
# Pin to the exact toolchain in rust-toolchain.toml so rustup doesn't re-download
# a different patch release. The Debian-based image ships a C toolchain, which the
# `bundled` SQLite (rusqlite) and tantivy/blake3 build scripts need.
FROM rust:1.96.0-slim-bookworm AS build

WORKDIR /src

# Copy the full workspace and build the server binary in release mode. A
# BuildKit cache mount keeps the cargo registry and target dir warm across
# builds so unchanged dependencies aren't recompiled every time.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p webdav-server \
    && cp target/release/webdav-server /usr/local/bin/webdav-server

# --- Runtime stage -----------------------------------------------------------
# Slim glibc base — the binary is dynamically linked against glibc. No TLS or
# outbound HTTP, so no ca-certificates needed.
FROM debian:bookworm-slim AS runtime

# Run as a non-root user; give it ownership of the data volume mount point.
RUN useradd --system --create-home --uid 10001 chishiki \
    && mkdir -p /data \
    && chown chishiki:chishiki /data

COPY --from=build /usr/local/bin/webdav-server /usr/local/bin/webdav-server

USER chishiki

# Persist the content-addressable store + SQLite metadata + search index here.
ENV CHISHIKI_DATA=/data
# Bind to all interfaces so the server is reachable from outside the container
# (the binary's own default is 127.0.0.1, which would only serve localhost).
ENV CHISHIKI_ADDR=0.0.0.0:4918
VOLUME ["/data"]

# 4918 is the WebDAV port (RFC 4918).
EXPOSE 4918

ENTRYPOINT ["/usr/local/bin/webdav-server"]
