FROM rust:1.65.0 AS builder
WORKDIR /archive-router
COPY ./ .
RUN cargo build --release

FROM debian:bullseye-slim
WORKDIR /archive-router
COPY --from=builder /archive-router/target/release/archive-router-bin ./archive-router
ENTRYPOINT ["/archive-router/archive-router"]
EXPOSE 3000
