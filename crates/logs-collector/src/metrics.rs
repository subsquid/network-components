//! Prometheus metrics, served at `/metrics`. Every series carries the instance's `shard`.

use std::sync::LazyLock;
use std::time::Duration;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::Registry;

type Label = [(&'static str, &'static str); 1];
type Labels = Vec<(&'static str, &'static str)>;
type HistogramFamily = Family<Label, Histogram, fn() -> Histogram>;

/// The transport doesn't export `FetchLogsError`, so the `reason` of a failed request is read
/// from the start of its `Debug` form, which names the variant.
const ERROR_REASONS: [(&str, &str); 5] = [
    ("Timeout(Connect)", "connect_timeout"),
    ("Timeout(Request)", "request_timeout"),
    ("InvalidRequest", "invalid_request"),
    ("Failure", "failure"),
    ("InvalidResponse", "invalid_response"),
];

pub static WORKERS: LazyLock<Gauge> = LazyLock::new(Default::default);
pub static BACKLOGGED_WORKERS: LazyLock<Gauge> = LazyLock::new(Default::default);
pub static BUFFER_BYTES: LazyLock<Gauge> = LazyLock::new(Default::default);
pub static BUFFER_MAX_BYTES: LazyLock<Gauge> = LazyLock::new(Default::default);
pub static REQUESTS: LazyLock<Family<Labels, Counter>> = LazyLock::new(Default::default);
pub static REQUEST_DURATION: LazyLock<HistogramFamily> = LazyLock::new(|| {
    Family::new_with_constructor(|| {
        Histogram::new([0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 20.0, 30.0])
    })
});
pub static ROUND_DURATION: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new([1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]));
/// Rows collected more than 1200 s after the worker logged them don't count for rewards.
pub static LOG_LAG: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new([30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0]));
pub static LOGS_STORED: LazyLock<Counter> = LazyLock::new(Default::default);
pub static LOGS_DISCARDED: LazyLock<Family<Label, Counter>> = LazyLock::new(Default::default);
pub static LOGS_DEFERRED: LazyLock<Counter> = LazyLock::new(Default::default);
pub static STORAGE_ERRORS: LazyLock<Family<Label, Counter>> = LazyLock::new(Default::default);

/// Records a request to a worker. `error` is the error's `Debug` form, `None` on success.
pub fn observe_request(duration: Duration, error: Option<&str>) {
    let result = if error.is_some() { "error" } else { "ok" };
    let mut labels = vec![("result", result)];
    if let Some(error) = error {
        labels.push(("reason", error_reason(error)));
    }
    REQUESTS.get_or_create(&labels).inc();
    REQUEST_DURATION
        .get_or_create(&[("result", result)])
        .observe(duration.as_secs_f64());
}

fn error_reason(error: &str) -> &'static str {
    ERROR_REASONS
        .iter()
        .find(|(prefix, _)| error.starts_with(prefix))
        .map_or("other", |&(_, reason)| reason)
}

pub fn registry(shard: u8, total_shards: u8) -> Registry {
    // Create every known series up front, so rates and ratios read 0 instead of no data.
    drop(REQUESTS.get_or_create(&vec![("result", "ok")]));
    for (_, reason) in ERROR_REASONS.iter().chain(&[("", "other")]) {
        drop(REQUESTS.get_or_create(&vec![("result", "error"), ("reason", reason)]));
    }
    for result in ["ok", "error"] {
        drop(REQUEST_DURATION.get_or_create(&[("result", result)]));
    }
    drop(LOGS_DISCARDED.get_or_create(&[("reason", "invalid")]));
    for operation in ["read", "insert"] {
        drop(STORAGE_ERRORS.get_or_create(&[("operation", operation)]));
    }

    let mut registry = Registry::with_prefix_and_labels(
        "logs_collector",
        [("shard".into(), shard.to_string().into())].into_iter(),
    );
    registry.register(
        "shard",
        "The shard this instance collects, out of total_shards",
        Info::new([("total_shards", total_shards.to_string())]),
    );
    registry.register(
        "workers",
        "Workers this instance collects logs from",
        WORKERS.clone(),
    );
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
        "Buffer size at which logs are deferred to a later round",
        BUFFER_MAX_BYTES.clone(),
    );
    registry.register(
        "requests",
        "Log requests to workers, by result, and by reason for errors",
        REQUESTS.clone(),
    );
    registry.register(
        "request_duration_seconds",
        "Duration of log requests to workers, by result",
        REQUEST_DURATION.clone(),
    );
    registry.register(
        "round_duration_seconds",
        "Duration of collection rounds, including the insert",
        ROUND_DURATION.clone(),
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
        "logs_deferred",
        "Logs rejected because the buffer was full; they are collected again in a later round",
        LOGS_DEFERRED.clone(),
    );
    registry.register(
        "storage_errors",
        "Failed ClickHouse operations, by operation (read, insert)",
        STORAGE_ERRORS.clone(),
    );
    registry
}

#[cfg(test)]
mod tests {
    use prometheus_client::encoding::text::encode;

    use super::*;

    #[test]
    fn every_series_carries_the_shard() {
        let mut body = String::new();
        encode(&mut body, &registry(3, 4)).unwrap();

        let samples: Vec<&str> = body.lines().filter(|l| !l.starts_with('#')).collect();
        assert!(!samples.is_empty());
        for sample in samples {
            assert!(sample.starts_with("logs_collector_"), "{sample}");
            assert!(sample.contains(r#"shard="3""#), "{sample}");
        }
        assert!(body.contains(r#"logs_collector_shard_info{shard="3",total_shards="4"} 1"#));
        // Known series exist before anything is counted.
        assert!(body.contains(
            r#"logs_collector_requests_total{shard="3",result="error",reason="request_timeout"}"#
        ));
    }

    #[test]
    fn error_reasons_come_from_the_variant() {
        assert_eq!(error_reason("Timeout(Connect)"), "connect_timeout");
        assert_eq!(error_reason("Timeout(Request)"), "request_timeout");
        assert_eq!(error_reason(r#"Failure("Dial error")"#), "failure");
        assert_eq!(error_reason("Unexpected"), "other");
    }
}
