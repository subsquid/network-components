use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use prometheus_client::registry::Registry;
use serde_partial::{Field, SerializePartial};
use sqd_network_transport::util::CancellationToken;
use tokio::sync::RwLock;

use crate::cli::Config;
use crate::scheduler::{ChunksSummary, Scheduler};
use crate::worker_state::WorkerState;

const JAIL_INFO_FIELDS: [Field<'static, WorkerState>; 3] = [
    Field::new("peer_id"),
    Field::new("jailed"),
    Field::new("jail_reason"),
];

async fn active_workers(Extension(scheduler): Extension<Scheduler>) -> Json<Vec<WorkerState>> {
    Json(scheduler.active_workers())
}

async fn workers_jail_info(Extension(scheduler): Extension<Scheduler>) -> Response {
    let active_workers = scheduler.active_workers();
    let jail_info = active_workers
        .iter()
        .map(|w| w.with_fields(|_| JAIL_INFO_FIELDS))
        .collect::<Vec<_>>();
    Json(jail_info).into_response()
}

async fn chunks(Extension(scheduler): Extension<Scheduler>) -> Json<ChunksSummary> {
    let chunks_summary = scheduler.get_chunks_summary();
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
    scheduler: Scheduler,
    addr: SocketAddr,
    metrics_registry: Registry,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    log::info!("Starting HTTP server listening on {addr}");
    let metrics_registry = Arc::new(RwLock::new(metrics_registry));
    let app = Router::new()
        .route("/workers/pings", get(active_workers))
        .route("/workers/jail_info", get(workers_jail_info))
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
