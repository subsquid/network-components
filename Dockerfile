FROM --platform=$BUILDPLATFORM rust:1.65.0 AS archive-router-builder
RUN apt-get update && apt-get install protobuf-compiler -y
WORKDIR /archive-router
COPY ./ .
RUN rm -r crates/network-scheduler
RUN rm -r crates/query-gateway
RUN cargo build --release

FROM --platform=$BUILDPLATFORM debian:bullseye-slim AS archive-router
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /archive-router
COPY --from=archive-router-builder /archive-router/target/release/router ./router
ENTRYPOINT ["/archive-router/router"]
EXPOSE 3000

FROM --platform=$BUILDPLATFORM rust:1.70-bookworm AS network-builder

RUN apt update
RUN apt install -y -V protobuf-compiler

WORKDIR /usr/src

COPY Cargo.toml .
COPY Cargo.lock .
COPY crates ./crates

COPY subsquid-network/Cargo.toml ./subsquid-network/
COPY subsquid-network/Cargo.lock ./subsquid-network/
COPY subsquid-network/transport ./subsquid-network/transport

RUN cargo build --release --workspace

FROM --platform=$BUILDPLATFORM debian:bookworm-slim as network-scheduler

RUN apt-get update && apt-get install ca-certificates net-tools -y

WORKDIR /run

COPY --from=network-builder /usr/src/target/release/network-scheduler /usr/local/bin/network-scheduler
COPY --from=network-builder /usr/src/crates/network-scheduler/config.yml .

ENV P2P_LISTEN_ADDR="/ip4/0.0.0.0/tcp/12345"
ENV HTTP_LISTEN_ADDR="0.0.0.0:8000"
ENV BOOTSTRAP="true"

CMD ["network-scheduler"]

RUN echo "PORT=\${HTTP_LISTEN_ADDR##*:}; netstat -an | grep \$PORT > /dev/null; if [ 0 != \$? ]; then exit 1; fi;" > ./healthcheck.sh
RUN chmod +x ./healthcheck.sh
HEALTHCHECK --interval=5s CMD ./healthcheck.sh

FROM --platform=$BUILDPLATFORM debian:bookworm-slim as query-gateway

RUN apt-get update && apt-get install ca-certificates net-tools -y

WORKDIR /run

COPY --from=network-builder /usr/src/target/release/query-gateway /usr/local/bin/query-gateway
COPY --from=network-builder /usr/src/crates/query-gateway/config.yml .

ENV P2P_LISTEN_ADDR="/ip4/0.0.0.0/tcp/12345"
ENV HTTP_LISTEN_ADDR="0.0.0.0:8000"
ENV BOOTSTRAP="true"
ENV PRIVATE_NODE="true"

CMD ["query-gateway"]

RUN echo "PORT=\${HTTP_LISTEN_ADDR##*:}; netstat -an | grep \$PORT > /dev/null; if [ 0 != \$? ]; then exit 1; fi;" > ./healthcheck.sh
RUN chmod +x ./healthcheck.sh
HEALTHCHECK --interval=5s CMD ./healthcheck.sh
