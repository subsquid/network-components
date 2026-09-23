//! Prometheus metrics, served at `/metrics`. Every series carries the instance's `shard`.

use std::sync::LazyLock;

use collector_utils::CollectorMetrics;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

type Label = [(&'static str, &'static str); 1];

/// Request errors by `FetchLogsError` variant.
pub static COMMON: LazyLock<CollectorMetrics> = LazyLock::new(|| {
    CollectorMetrics::new(&[
        ("Timeout(Connect)", "connect_timeout"),
        ("Timeout(Request)", "request_timeout"),
        ("InvalidRequest", "invalid_request"),
        ("Failure", "failure"),
        ("InvalidResponse", "invalid_response"),
    ])
});
pub static BACKLOGGED_WORKERS: LazyLock<Gauge> = LazyLock::new(Default::default);
pub static BUFFER_BYTES: LazyLock<Gauge> = LazyLock::new(Default::default);
pub static BUFFER_MAX_BYTES: LazyLock<Gauge> = LazyLock::new(Default::default);
/// Rows collected more than 1200 s after the worker logged them don't count for rewards.
pub static LOG_LAG: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new([30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0]));
pub static LOGS_STORED: LazyLock<Counter> = LazyLock::new(Default::default);
pub static LOGS_DISCARDED: LazyLock<Family<Label, Counter>> = LazyLock::new(Default::default);
pub static STORAGE_ERRORS: LazyLock<Family<Label, Counter>> = LazyLock::new(Default::default);

pub fn registry(shard: u8, total_shards: u8) -> Registry {
    drop(LOGS_DISCARDED.get_or_create(&[("reason", "invalid")]));
    for operation in ["read", "insert"] {
        drop(STORAGE_ERRORS.get_or_create(&[("operation", operation)]));
    }

    let mut registry = COMMON.registry("logs_collector", shard, total_shards);
    registry.register(
        "backlogged_workers",
        "Workers that had more logs than the last round could collect",
        BACKLOGGED_WORKERS.clone(),
    );
    registry.register(
        "buffer_bytes",
        "Estimated size of the logs buffered for the next insert",
        BUFFER_BYTES.clone(),
    );
    registry.register(
        "buffer_max_bytes",
        "Buffer size at which collection waits for the buffered logs to be stored",
        BUFFER_MAX_BYTES.clone(),
    );
    registry.register(
        "log_lag_seconds",
        "Time from a worker logging a query to its row being collected, per stored row",
        LOG_LAG.clone(),
    );
    registry.register(
        "logs_stored",
        "Logs inserted into ClickHouse",
        LOGS_STORED.clone(),
    );
    registry.register(
        "logs_discarded",
        "Logs discarded for good, by reason",
        LOGS_DISCARDED.clone(),
    );
    registry.register(
        "storage_errors",
        "Failed ClickHouse operations, by operation (read, insert)",
        STORAGE_ERRORS.clone(),
    );
    registry
}
