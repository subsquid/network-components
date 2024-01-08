use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::routing::get;
use axum::{Extension, Json, Router, Server};
use itertools::Itertools;
use serde::Serialize;
use tokio::sync::RwLock;

use subsquid_messages::RangeSet;
use subsquid_network_transport::PeerId;

use crate::cli::Config;
use crate::data_chunk::{chunks_to_worker_state, DataChunk};
use crate::scheduler::Scheduler;
use crate::worker_state::WorkerState;

#[derive(Debug, Clone, Serialize)]
struct ChunkStatus {
    begin: u32,
    end: u32,
    size_bytes: u64,
    assigned_to: Vec<String>,
    downloaded_by: Vec<String>,
}

async fn active_workers(
    Extension(scheduler): Extension<Arc<RwLock<Scheduler>>>,
) -> Json<Vec<WorkerState>> {
    Json(scheduler.read().await.active_workers())
}

async fn chunks(
    Extension(scheduler): Extension<Arc<RwLock<Scheduler>>>,
) -> Json<HashMap<String, Vec<ChunkStatus>>> {
    let workers = scheduler.read().await.all_workers();
    let units = scheduler.read().await.known_units();
    let assigned_ranges = workers
        .iter()
        .flat_map(|w| {
            let chunks = w
                .assigned_units
                .iter()
                .flat_map(|unit_id| units.get(unit_id).unwrap().clone());
            chunks_to_worker_state(chunks)
                .datasets
                .into_iter()
                .map(|(dataset, ranges)| (dataset, (w.peer_id, ranges)))
        })
        .into_group_map();
    let stored_ranges = workers
        .iter()
        .flat_map(|w| {
            w.stored_ranges
                .iter()
                .map(|(dataset, ranges)| (dataset.clone(), (w.peer_id, ranges.clone())))
        })
        .into_group_map();
    let chunk_statuses = units
        .into_values()
        .flatten()
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

async fn get_config() -> Json<Config> {
    Json(Config::get().clone())
}

pub async fn run_server(scheduler: Arc<RwLock<Scheduler>>, addr: SocketAddr) -> anyhow::Result<()> {
    log::info!("Starting HTTP server listening on {addr}");
    let app = Router::new()
        .route("/workers/pings", get(active_workers))
        .route("/chunks", get(chunks))
        .route("/config", get(get_config))
        .layer(Extension(scheduler));
    Server::bind(&addr).serve(app.into_make_service()).await?;
    Ok(())
}
