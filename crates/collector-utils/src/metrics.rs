use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::header;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::Registry;

type Labels = Vec<(&'static str, &'static str)>;
type Histograms = Family<[(&'static str, &'static str); 1], Histogram, fn() -> Histogram>;

/// Metrics every collector keeps. Its `registry` is where a collector adds its own.
pub struct CollectorMetrics {
    pub workers: Gauge,
    round_duration: Histogram,
    requests: Family<Labels, Counter>,
    request_duration: Histograms,
    /// The transport doesn't export its error types, so the `reason` of a failed request is
    /// looked up by the start of the error's `Debug` form, which names the variant.
    error_reasons: &'static [(&'static str, &'static str)],
}

impl CollectorMetrics {
    pub fn new(error_reasons: &'static [(&'static str, &'static str)]) -> Self {
        Self {
            workers: Gauge::default(),
            round_duration: Histogram::new([1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]),
            requests: Family::default(),
            request_duration: Family::new_with_constructor(|| {
                Histogram::new([0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 20.0, 30.0])
            }),
            error_reasons,
        }
    }

    /// Awaits a request to a worker, recording its result and duration.
    pub async fn observe_request<T, E: Debug>(
        &self,
        request: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let start = Instant::now();
        let response = request.await;
        let result = if response.is_ok() { "ok" } else { "error" };
        let mut labels = vec![("result", result)];
        if let Err(e) = &response {
            labels.push(("reason", self.error_reason(&format!("{e:?}"))));
        }
        self.requests.get_or_create(&labels).inc();
        self.request_duration
            .get_or_create(&[("result", result)])
            .observe(start.elapsed().as_secs_f64());
        response
    }

    pub fn observe_round(&self, duration: Duration) {
        self.round_duration.observe(duration.as_secs_f64());
    }

    fn error_reason(&self, error: &str) -> &'static str {
        self.error_reasons
            .iter()
            .find(|(prefix, _)| error.starts_with(prefix))
            .map_or("other", |&(_, reason)| reason)
    }

    /// A registry with these metrics, in which every series is named `{prefix}_*` and carries
    /// `shard`. Every known series is created up front, so rates read 0 instead of no data.
    pub fn registry(&self, prefix: &str, shard: u8, total_shards: u8) -> Registry {
        drop(self.requests.get_or_create(&vec![("result", "ok")]));
        for &(_, reason) in self.error_reasons.iter().chain(&[("", "other")]) {
            drop(
                self.requests
                    .get_or_create(&vec![("result", "error"), ("reason", reason)]),
            );
        }
        for result in ["ok", "error"] {
            drop(self.request_duration.get_or_create(&[("result", result)]));
        }

        let mut registry = Registry::with_prefix_and_labels(
            prefix,
            [("shard".into(), shard.to_string().into())].into_iter(),
        );
        registry.register(
            "shard",
            "The shard this instance collects, out of total_shards",
            Info::new([("total_shards", total_shards.to_string())]),
        );
        registry.register(
            "workers",
            "Workers this instance collects from, after the shard filter",
            self.workers.clone(),
        );
        registry.register(
            "requests",
            "Requests to workers, by result, and by reason for errors",
            self.requests.clone(),
        );
        registry.register(
            "request_duration_seconds",
            "Duration of requests to workers, by result",
            self.request_duration.clone(),
        );
        registry.register(
            "round_duration_seconds",
            "Duration of collection rounds, including the insert",
            self.round_duration.clone(),
        );
        registry
    }
}

/// Serves `registry` at `/metrics` on `port`, in the OpenMetrics text format.
pub async fn serve_metrics(port: u16, registry: Registry) -> anyhow::Result<()> {
    let registry = Arc::new(registry);
    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || async move {
            let mut body = String::new();
            encode(&mut body, &registry).expect("writing to a String never fails");
            let content_type = "application/openmetrics-text; version=1.0.0; charset=utf-8";
            ([(header::CONTENT_TYPE, content_type)], body)
        }),
    );
    axum::Server::try_bind(&SocketAddr::from(([0, 0, 0, 0], port)))?
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Its `Debug` form starts like the transport's `Failure(..)` variant.
    #[derive(Debug)]
    struct Failure;

    #[tokio::test]
    async fn requests_are_counted_by_the_error_variant() {
        let metrics = CollectorMetrics::new(&[("Failure", "failure")]);
        let registry = metrics.registry("test", 3, 4);
        let failed = async { Err::<(), _>(Failure) };
        metrics.observe_request(failed).await.unwrap_err();

        let mut body = String::new();
        encode(&mut body, &registry).unwrap();
        for sample in body.lines().filter(|l| !l.starts_with('#')) {
            assert!(sample.starts_with("test_"), "{sample}");
            assert!(sample.contains(r#"shard="3""#), "{sample}");
        }
        assert!(body.contains(r#"test_shard_info{shard="3",total_shards="4"} 1"#));
        assert!(
            body.contains(r#"test_requests_total{shard="3",result="error",reason="failure"} 1"#)
        );
        // Created up front, before anything is counted.
        assert!(body.contains(r#"test_requests_total{shard="3",result="error",reason="other"} 0"#));
    }
}
