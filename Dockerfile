FROM rust:1.85-bookworm AS builder

WORKDIR /app

# Prepare dependency cache first for faster rebuilds.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY web ./web

RUN cargo build --release

FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/flowsync /usr/local/bin/flowsync

EXPOSE 3030

ENTRYPOINT ["flowsync"]
CMD ["server", "--host", "0.0.0.0", "--port", "3030", "--db", "/data/flowsync.db"]
