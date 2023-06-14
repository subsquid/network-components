FROM rust:1.65.0 AS builder
RUN apt-get update && apt-get install protobuf-compiler -y
WORKDIR /archive-router
COPY ./ .
RUN rm -r crates/network-scheduler
RUN rm -r crates/query-gateway
RUN cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /archive-router
COPY --from=builder /archive-router/target/release/router ./router
ENTRYPOINT ["/archive-router/router"]
EXPOSE 3000
