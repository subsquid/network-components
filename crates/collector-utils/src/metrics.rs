use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::header;
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;

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
