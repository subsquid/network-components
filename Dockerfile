# Builder pinned to Rust 1.89 on bookworm:
# - Cargo.lock is now v4 (introduced in Cargo 1.78, April 2024) due to the
#   crates added for the v2 auth middleware (moka, dashmap, reqwest, ipnet,
#   etc.). The previous rust:1.65.0 builder cannot parse v4 lockfiles.
# - Bookworm matches the runtime image below — keeps the binary's
#   glibc-symbol set compatible with `debian:bookworm-slim` at runtime.
#   Bullseye's security archive no longer serves packages, so the runtime
#   stage could not install ca-certificates there.
FROM rust:1.89-bookworm AS builder
WORKDIR /archive-router
COPY ./ .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /archive-router
COPY --from=builder /archive-router/target/release/router ./router
ENTRYPOINT ["/archive-router/router"]
EXPOSE 3000
