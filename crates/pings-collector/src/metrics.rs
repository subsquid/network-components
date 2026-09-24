//! Prometheus metrics, served at `/metrics`. Every series carries the instance's `shard`.

use std::sync::LazyLock;

use collector_utils::CollectorMetrics;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;

/// Request errors by `RequestError` variant.
pub static COMMON: LazyLock<CollectorMetrics> = LazyLock::new(|| {
    CollectorMetrics::new(&[
        ("Timeout(Connect)", "connect_timeout"),
        ("Timeout(Request)", "request_timeout"),
        ("UnsupportedProtocol", "unsupported_protocol"),
        ("ResponseTooLarge", "response_too_large"),
        ("Io", "io"),
    ])
});
pub static HEARTBEATS_STORED: LazyLock<Counter> = LazyLock::new(Default::default);
pub static HEARTBEATS_DISCARDED: LazyLock<Family<[(&'static str, &'static str); 1], Counter>> =
    LazyLock::new(Default::default);
pub static STORAGE_ERRORS: LazyLock<Counter> = LazyLock::new(Default::default);

pub fn registry(shard: u8, total_shards: u8) -> Registry {
    for reason in ["unsupported_version", "invalid"] {
        drop(HEARTBEATS_DISCARDED.get_or_create(&[("reason", reason)]));
    }

    let mut registry = COMMON.registry("pings_collector", shard, total_shards);
    registry.register(
        "heartbeats_stored",
        "Heartbeats inserted into ClickHouse",
        HEARTBEATS_STORED.clone(),
    );
    registry.register(
        "heartbeats_discarded",
        "Heartbeats not stored, by reason (unsupported_version, invalid)",
        HEARTBEATS_DISCARDED.clone(),
    );
    registry.register(
        "storage_errors",
        "Failed ClickHouse inserts",
        STORAGE_ERRORS.clone(),
    );
    registry
}
