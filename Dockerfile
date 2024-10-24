FROM --platform=$BUILDPLATFORM rust:1.80.1 AS archive-router-builder
RUN apt-get update && apt-get install protobuf-compiler -y
WORKDIR /archive-router
COPY ./ .
RUN rm -r crates/network-scheduler
RUN rm -rf crates/logs-collector
RUN --mount=type=ssh cargo build --release

FROM --platform=$BUILDPLATFORM debian:bullseye-slim AS archive-router
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /archive-router
COPY --from=archive-router-builder /archive-router/target/release/router ./router
ENTRYPOINT ["/archive-router/router"]
EXPOSE 3000

FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:0.1.67-rust-1.80.1-slim-bookworm AS chef
WORKDIR /app

FROM --platform=$BUILDPLATFORM chef AS network-planner

COPY Cargo.toml .
COPY Cargo.lock .
COPY crates ./crates

RUN cargo chef prepare --recipe-path recipe.json

FROM --platform=$BUILDPLATFORM chef AS network-builder

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get -y install protobuf-compiler pkg-config libssl-dev build-essential

COPY --from=network-planner /app/recipe.json recipe.json
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml .
COPY Cargo.lock .
COPY crates ./crates

RUN --mount=type=ssh cargo build --release --workspace

FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS network-base

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get -y install ca-certificates net-tools

FROM --platform=$BUILDPLATFORM network-base AS network-scheduler

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get -y install curl

WORKDIR /run

COPY --from=network-builder /app/target/release/network-scheduler /usr/local/bin/network-scheduler
COPY --from=network-builder /app/crates/network-scheduler/config.yml .

ENV P2P_LISTEN_ADDRS="/ip4/0.0.0.0/udp/12345/quic-v1"
ENV HTTP_LISTEN_ADDR="0.0.0.0:8000"

CMD ["network-scheduler"]

COPY crates/network-scheduler/healthcheck.sh .
RUN chmod +x ./healthcheck.sh
HEALTHCHECK --interval=5s CMD ./healthcheck.sh

FROM --platform=$BUILDPLATFORM network-base AS pings-collector

COPY --from=network-builder /app/target/release/pings-collector /usr/local/bin/pings-collector

ENV P2P_LISTEN_ADDRS="/ip4/0.0.0.0/udp/12345/quic-v1"
ENV BUFFER_DIR="/run"

CMD ["pings-collector"]

COPY healthcheck.sh .
RUN chmod +x ./healthcheck.sh
HEALTHCHECK --interval=5s CMD ./healthcheck.sh

FROM --platform=$BUILDPLATFORM network-base AS peer-checker

COPY --from=network-builder /app/target/release/peer-checker /usr/local/bin/peer-checker

ENV P2P_LISTEN_ADDRS="/ip4/0.0.0.0/udp/12345/quic-v1"
ENV BUFFER_DIR="/run"

CMD ["peer-checker"]

COPY healthcheck.sh .
RUN chmod +x ./healthcheck.sh
HEALTHCHECK --interval=5s CMD ./healthcheck.sh
