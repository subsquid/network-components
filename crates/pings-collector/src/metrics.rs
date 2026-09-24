//! Prometheus metrics, served at `/metrics`. Every series carries the instance's `shard`.

use std::sync::LazyLock;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

type Label = [(&'static str, &'static str); 1];

pub static WORKERS: LazyLock<Gauge> = LazyLock::new(Default::default);
pub static REQUESTS: LazyLock<Family<Label, Counter>> = LazyLock::new(Default::default);
pub static HEARTBEATS_STORED: LazyLock<Counter> = LazyLock::new(Default::default);
pub static HEARTBEATS_DROPPED: LazyLock<Family<Label, Counter>> = LazyLock::new(Default::default);
pub static STORAGE_ERRORS: LazyLock<Counter> = LazyLock::new(Default::default);

pub fn registry(shard: u8) -> Registry {
    let mut registry = Registry::with_prefix_and_labels(
        "pings_collector",
        [("shard".into(), shard.to_string().into())].into_iter(),
    );
    registry.register(
        "workers",
        "Workers this instance collects heartbeats from",
        WORKERS.clone(),
    );
    registry.register(
        "requests",
        "Heartbeat requests to workers, by result (ok, error)",
        REQUESTS.clone(),
    );
    registry.register(
        "heartbeats_stored",
        "Heartbeats inserted into ClickHouse",
        HEARTBEATS_STORED.clone(),
    );
    registry.register(
        "heartbeats_dropped",
        "Heartbeats not stored, by reason (unsupported_version, invalid)",
        HEARTBEATS_DROPPED.clone(),
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
        REQUESTS.get_or_create(&[("result", "ok")]).inc();
        let mut body = String::new();
        encode(&mut body, &registry(3)).unwrap();

        let samples: Vec<&str> = body.lines().filter(|l| !l.starts_with('#')).collect();
        assert!(!samples.is_empty());
        for sample in samples {
            assert!(sample.starts_with("pings_collector_"), "{sample}");
            assert!(sample.contains(r#"shard="3""#), "{sample}");
        }
        assert!(body.contains("pings_collector_requests_total{"), "{body}");
    }
}
