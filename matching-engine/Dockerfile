# syntax=docker/dockerfile:1.6
# ---- Stage 1: build ----
FROM rust:1.82-bookworm AS build
WORKDIR /workspace

# librdkafka build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake build-essential pkg-config libssl-dev libsasl2-dev zlib1g-dev protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Cache deps by copying manifests first.
COPY Cargo.toml ./Cargo.toml
COPY crates/matching-engine/Cargo.toml crates/matching-engine/Cargo.toml
COPY crates/orderbook/Cargo.toml       crates/orderbook/Cargo.toml
COPY crates/kafka-bridge/Cargo.toml    crates/kafka-bridge/Cargo.toml
COPY crates/redis-bridge/Cargo.toml    crates/redis-bridge/Cargo.toml
COPY crates/proto-gen/Cargo.toml       crates/proto-gen/Cargo.toml
RUN mkdir -p crates/matching-engine/src crates/orderbook/src crates/kafka-bridge/src \
             crates/redis-bridge/src   crates/proto-gen/src && \
    echo 'fn main(){}'              > crates/matching-engine/src/main.rs && \
    echo '//stub'                   > crates/orderbook/src/lib.rs && \
    echo '//stub'                   > crates/kafka-bridge/src/lib.rs && \
    echo '//stub'                   > crates/redis-bridge/src/lib.rs && \
    echo '//stub'                   > crates/proto-gen/src/lib.rs && \
    echo 'fn main(){}'              > crates/proto-gen/build.rs

# Cache dep compile.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo build --release --bin matching-engine || true

# Real sources.
COPY proto                              proto
COPY crates                             crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo build --release --bin matching-engine && \
    cp target/release/matching-engine /workspace/matching-engine

# ---- Stage 2: runtime ----
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 ca-certificates libsasl2-2 zlib1g && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r app && useradd -r -g app app
WORKDIR /app
COPY --from=build /workspace/matching-engine /app/matching-engine
USER app
ENV RUST_LOG=info
ENTRYPOINT ["/app/matching-engine"]
