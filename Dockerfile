# Build stage — pinned to the same toolchain as rust-toolchain.toml.
FROM rust:1.89.0-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release --locked

# Runtime stage — just the binary on a slim base.
FROM debian:bookworm-slim
COPY --from=builder /app/target/release/rustkv /usr/local/bin/rustkv
ENV RUSTKV_LISTEN=0.0.0.0:8080 \
    RUSTKV_RAFT_LISTEN=0.0.0.0:9080 \
    RUSTKV_DATA_DIR=/data
VOLUME /data
EXPOSE 8080 9080
ENTRYPOINT ["rustkv"]
