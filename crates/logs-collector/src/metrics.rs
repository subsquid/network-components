//! Prometheus metrics, served at `/metrics`. Every series carries the instance's `shard`.

use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};

use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use sqd_network_transport::util::CancellationToken;

type Label = [(&'static str, &'static str); 1];

pub static WORKERS: LazyLock<Gauge> = LazyLock::new(Default::default);
pub static BACKLOGGED_WORKERS: LazyLock<Gauge> = LazyLock::new(Default::default);
pub static REQUESTS: LazyLock<Family<Label, Counter>> = LazyLock::new(Default::default);
pub static LOGS_STORED: LazyLock<Counter> = LazyLock::new(Default::default);
pub static LOGS_DROPPED: LazyLock<Family<Label, Counter>> = LazyLock::new(Default::default);
pub static STORAGE_ERRORS: LazyLock<Counter> = LazyLock::new(Default::default);

pub fn registry(shard: u8) -> Registry {
    let mut registry = Registry::with_prefix_and_labels(
        "logs_collector",
        [("shard".into(), shard.to_string().into())].into_iter(),
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
        "requests",
        "Log requests to workers, by result (ok, error)",
        REQUESTS.clone(),
    );
    registry.register(
        "logs_stored",
        "Logs inserted into ClickHouse",
        LOGS_STORED.clone(),
    );
    registry.register(
        "logs_dropped",
        "Logs dropped, by reason: invalid logs are discarded, buffer_full ones are re-collected later",
        LOGS_DROPPED.clone(),
    );
    registry.register(
        "storage_errors",
        "Failed ClickHouse queries and inserts",
        STORAGE_ERRORS.clone(),
    );
    registry
}

/// Serves `registry` at `/metrics` until `cancel` fires.
pub async fn serve(port: u16, registry: Registry, cancel: CancellationToken) -> anyhow::Result<()> {
    let registry = Arc::new(registry);
    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || async move {
            let mut body = String::new();
            match encode(&mut body, &registry) {
                Ok(()) => (
                    [(
                        header::CONTENT_TYPE,
                        "application/openmetrics-text; version=1.0.0; charset=utf-8",
                    )],
                    body,
                )
                    .into_response(),
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }),
    );
    axum::Server::try_bind(&SocketAddr::from(([0, 0, 0, 0], port)))?
        .serve(app.into_make_service())
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_series_carries_the_shard() {
        REQUESTS.get_or_create(&[("result", "ok")]).inc();
        let mut body = String::new();
        encode(&mut body, &registry(3)).unwrap();

        let samples: Vec<&str> = body.lines().filter(|l| !l.starts_with('#')).collect();
        assert!(!samples.is_empty());
        for sample in samples {
            assert!(sample.starts_with("logs_collector_"), "{sample}");
            assert!(sample.contains(r#"shard="3""#), "{sample}");
        }
        assert!(body.contains("logs_collector_requests_total{"), "{body}");
    }
}
