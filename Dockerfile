# Builder pinned to Rust 1.89 on bullseye:
# - Cargo.lock is now v4 (introduced in Cargo 1.78, April 2024) due to the
#   crates added for the v2 auth middleware (moka, dashmap, reqwest, ipnet,
#   etc.). The previous rust:1.65.0 builder cannot parse v4 lockfiles.
# - Bullseye matches the runtime image below — keeps the binary's
#   glibc-symbol set compatible with `debian:bullseye-slim` at runtime.
FROM rust:1.89-bullseye AS builder
WORKDIR /archive-router
COPY ./ .
RUN cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /archive-router
COPY --from=builder /archive-router/target/release/router ./router
ENTRYPOINT ["/archive-router/router"]
EXPOSE 3000
