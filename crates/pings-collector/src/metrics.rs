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

/// The transport doesn't export `RequestError`, so the `reason` of a failed request is read
/// from the start of its `Debug` form, which names the variant.
const ERROR_REASONS: [(&str, &str); 5] = [
    ("Timeout(Connect)", "connect_timeout"),
    ("Timeout(Request)", "request_timeout"),
    ("UnsupportedProtocol", "unsupported_protocol"),
    ("ResponseTooLarge", "response_too_large"),
    ("Io", "io"),
];

pub static WORKERS: LazyLock<Gauge> = LazyLock::new(Default::default);
pub static REQUESTS: LazyLock<Family<Labels, Counter>> = LazyLock::new(Default::default);
pub static REQUEST_DURATION: LazyLock<HistogramFamily> = LazyLock::new(|| {
    Family::new_with_constructor(|| {
        Histogram::new([0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 20.0, 30.0])
    })
});
/// A round longer than the request interval means fewer heartbeats per worker.
pub static ROUND_DURATION: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new([1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]));
pub static HEARTBEATS_STORED: LazyLock<Counter> = LazyLock::new(Default::default);
pub static HEARTBEATS_DISCARDED: LazyLock<Family<Label, Counter>> = LazyLock::new(Default::default);
pub static STORAGE_ERRORS: LazyLock<Counter> = LazyLock::new(Default::default);

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
    for reason in ["unsupported_version", "invalid"] {
        drop(HEARTBEATS_DISCARDED.get_or_create(&[("reason", reason)]));
    }

    let mut registry = Registry::with_prefix_and_labels(
        "pings_collector",
        [("shard".into(), shard.to_string().into())].into_iter(),
    );
    registry.register(
        "shard",
        "The shard this instance collects, out of total_shards",
        Info::new([("total_shards", total_shards.to_string())]),
    );
    registry.register(
        "workers",
        "Workers this instance collects heartbeats from",
        WORKERS.clone(),
    );
    registry.register(
        "requests",
        "Heartbeat requests to workers, by result, and by reason for errors",
        REQUESTS.clone(),
    );
    registry.register(
        "request_duration_seconds",
        "Duration of heartbeat requests to workers, by result",
        REQUEST_DURATION.clone(),
    );
    registry.register(
        "round_duration_seconds",
        "Duration of collection rounds, including the insert",
        ROUND_DURATION.clone(),
    );
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
            assert!(sample.starts_with("pings_collector_"), "{sample}");
            assert!(sample.contains(r#"shard="3""#), "{sample}");
        }
        assert!(body.contains(r#"pings_collector_shard_info{shard="3",total_shards="4"} 1"#));
        // Known series exist before anything is counted.
        assert!(body.contains(
            r#"pings_collector_requests_total{shard="3",result="error",reason="request_timeout"}"#
        ));
    }

    #[test]
    fn error_reasons_come_from_the_variant() {
        assert_eq!(error_reason("Timeout(Connect)"), "connect_timeout");
        assert_eq!(error_reason("ResponseTooLarge"), "response_too_large");
        assert_eq!(error_reason(r#"Io(Custom { kind: Other })"#), "io");
        assert_eq!(error_reason("Unexpected"), "other");
    }
}
