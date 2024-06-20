use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use axum::routing::get;
use axum::{Extension, Json, Router};
use prometheus_client::registry::Registry;
use tokio::sync::RwLock;

use subsquid_network_transport::util::CancellationToken;

use crate::cli::Config;
use crate::scheduler::{ChunkStatus, Scheduler};
use crate::worker_state::WorkerState;

async fn active_workers(
    Extension(scheduler): Extension<Arc<RwLock<Scheduler>>>,
) -> Json<Vec<WorkerState>> {
    Json(scheduler.read().await.active_workers())
}

async fn chunks(
    Extension(scheduler): Extension<Arc<RwLock<Scheduler>>>,
) -> Json<HashMap<String, Vec<ChunkStatus>>> {
    let chunks_summary = scheduler.read().await.get_chunks_summary();
    Json(chunks_summary)
}

async fn get_config() -> Json<Config> {
    Json(Config::get().clone())
}

async fn get_metrics(Extension(metrics_registry): Extension<Arc<RwLock<Registry>>>) -> String {
    let mut result = String::new();
    prometheus_client::encoding::text::encode(&mut result, metrics_registry.read().await.deref())
        .unwrap();
    result
}

pub async fn run_server(
    scheduler: Arc<RwLock<Scheduler>>,
    addr: SocketAddr,
    metrics_registry: Registry,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    log::info!("Starting HTTP server listening on {addr}");
    let metrics_registry = Arc::new(RwLock::new(metrics_registry));
    let app = Router::new()
        .route("/workers/pings", get(active_workers))
        .route("/chunks", get(chunks))
        .route("/config", get(get_config))
        .route("/metrics", get(get_metrics))
        .layer(Extension(scheduler))
        .layer(Extension(metrics_registry));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(cancel_token.cancelled_owned())
        .await?;
    Ok(())
}
