use crate::cli::Config;
use crate::data_chunk::DataChunk;
use crate::scheduler::Scheduler;
use crate::worker_registry::{Worker, WorkerRegistry};
use axum::routing::get;
use axum::{Extension, Json, Router, Server};
use itertools::Itertools;
use router_controller::messages::RangeSet;
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use subsquid_network_transport::PeerId;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize)]
struct ChunkStatus {
    begin: u32,
    end: u32,
    size_bytes: u64,
    assigned_to: Vec<String>,
    downloaded_by: Vec<String>,
}

async fn active_workers(
    Extension(worker_registry): Extension<Arc<RwLock<WorkerRegistry>>>,
) -> Json<Vec<Worker>> {
    Json(worker_registry.read().await.active_workers().await)
}

async fn chunks(
    Extension(worker_registry): Extension<Arc<RwLock<WorkerRegistry>>>,
    Extension(scheduler): Extension<Arc<RwLock<Scheduler>>>,
) -> Json<HashMap<String, Vec<ChunkStatus>>> {
    let workers = worker_registry.read().await.known_workers().await;
    let chunks = scheduler.read().await.known_chunks();
    let assigned_ranges = {
        let scheduler = scheduler.read().await;
        map_ranges(&workers, |id| {
            scheduler.get_worker_state(id).unwrap_or_default().datasets
        })
    };
    let stored_ranges = {
        let registry = worker_registry.read().await;
        map_ranges(&workers, |id| registry.stored_ranges(id))
    };
    let chunk_statuses = chunks
        .into_iter()
        .map(|chunk| {
            let assigned_to = find_workers_with_chunk(&chunk, &assigned_ranges);
            let downloaded_by = find_workers_with_chunk(&chunk, &stored_ranges);
            let chunk_status = ChunkStatus {
                begin: chunk.block_range.begin,
                end: chunk.block_range.end,
                size_bytes: chunk.size_bytes,
                assigned_to,
                downloaded_by,
            };
            (chunk.dataset_url, chunk_status)
        })
        .into_group_map();
    Json(chunk_statuses)
}

fn find_workers_with_chunk(
    chunk: &DataChunk,
    ranges: &HashMap<String, Vec<(PeerId, RangeSet)>>,
) -> Vec<String> {
    let ranges = match ranges.get(&chunk.dataset_url) {
        Some(ranges) => ranges,
        None => return vec![],
    };
    ranges
        .iter()
        .filter_map(|(worker_id, ranget_set)| {
            ranget_set
                .includes(chunk.block_range)
                .then_some(worker_id.to_string())
        })
        .collect()
}

/// Returns a mapping: dataset -> [(worker_id, range_set)]
fn map_ranges<F>(workers: &[Worker], state_getter: F) -> HashMap<String, Vec<(PeerId, RangeSet)>>
where
    F: Fn(&PeerId) -> HashMap<String, RangeSet>,
{
    workers
        .iter()
        .flat_map(|w| {
            state_getter(&w.peer_id)
                .into_iter()
                .map(|(dataset, range_set)| (dataset, (w.peer_id, range_set)))
        })
        .into_group_map()
}

async fn get_config(Extension(config): Extension<Arc<Config>>) -> Json<Config> {
    Json(config.deref().clone())
}

pub async fn run_server(
    worker_registry: Arc<RwLock<WorkerRegistry>>,
    scheduler: Arc<RwLock<Scheduler>>,
    config: Config,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    log::info!("Starting HTTP server listening on {addr}");
    let app = Router::new()
        .route("/workers/pings", get(active_workers))
        .route("/chunks", get(chunks))
        .route("/config", get(get_config))
        .layer(Extension(worker_registry))
        .layer(Extension(scheduler))
        .layer(Extension(Arc::new(config)));
    Server::bind(&addr).serve(app.into_make_service()).await?;
    Ok(())
}
