# SQD Network components

Rust crates for SQD Network nodes: the archive router and the services that collect pings and logs from workers, gateways, and portals.

SQD is an open data platform for Web3. SQD Network is a decentralized data lake and query engine run by worker, gateway, and scheduler nodes. This repository holds the shared and standalone Rust components used by those nodes. For the network protocol and node implementations, see [subsquid/sqd-network](https://github.com/subsquid/sqd-network).

## Crates

This is a Cargo workspace. The crates under `crates/` are:

| Crate | Type | Description |
|---|---|---|
| `router` | binary | Archive router. Reads dataset definitions from S3, assigns data ranges to workers on a scheduling interval, and exposes an HTTP API (port 3000) so clients can find which worker holds a given range. |
| `router-controller` | library | Scheduling and assignment logic used by `router`. |
| `logs-collector` | binary | Collects query-execution logs from workers and gateways over the network transport and stores them in ClickHouse. |
| `portal-logs-collector` | binary | Collects logs from SQD Portal instances and stores them in ClickHouse. |
| `pings-collector` | binary | Collects pings from workers and stores them in ClickHouse. |
| `collector-utils` | library | Shared ClickHouse storage types and helpers used by the collector binaries. |

The collector binaries connect to the network over [`sqd-network-transport`](https://github.com/subsquid/sqd-network) (libp2p / QUIC) and persist to ClickHouse.

## Build

Requires the Rust toolchain pinned in `rust-toolchain` (channel 1.83) and `protobuf-compiler` for building the protocol message dependencies.

```bash
# Build the whole workspace
cargo build --release --workspace

# Build a single crate, for example the router
cargo build --release -p router
```

Binaries are written to `target/release/`.

### Docker

The `Dockerfile` defines separate build targets for the router and each collector. For example:

```bash
docker build --target logs-collector -t logs-collector .
docker build --target pings-collector -t pings-collector .
docker build --target portal-logs-collector -t portal-logs-collector .
```

## Usage

Each binary is configured with command-line flags and environment variables (run with `--help` for the full list). For example, the router takes one or more S3 datasets and a set of managed workers:

```bash
router \
  --dataset eth-mainnet=s3://my-bucket/eth-mainnet \
  --worker <worker-id> \
  --replication 2 \
  --scheduling-unit 50 \
  --scheduling-interval 300
```

The collector binaries read their P2P listen address from `P2P_LISTEN_ADDRS` and their ClickHouse connection details from flags and environment variables.

## Scripts

The `scripts/` directory contains Python helpers used during development and operations (worker reachability and status checks, chunk summaries, scheduling simulation) and a traffic generator under `scripts/traffic_generator`.

## Documentation

- SQD documentation: https://docs.sqd.dev
- SQD Network: https://docs.sqd.dev/en/network
- SQD: https://sqd.dev

## License

This project is licensed under the AGPL v3.0 license. See [LICENSE.txt](LICENSE.txt) for details.
