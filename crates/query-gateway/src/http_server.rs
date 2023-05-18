use axum::extract::{Extension, Host, Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Router, Server};
use duration_string::DurationString;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::client::{QueryClient, QueryResult};
use crate::config::{DatasetId, PeerId};

async fn get_worker(
    Host(host): Host,
    Path((dataset, start_block)): Path<(String, u32)>,
    Extension(client): Extension<Arc<QueryClient>>,
) -> impl IntoResponse {
    log::info!("Get worker dataset={dataset} start_block={start_block}");
    let dataset_id = match client.get_dataset_id(&dataset).await {
        Some(dataset_id) => dataset_id,
        None => return (StatusCode::NOT_FOUND, format!("Unknown dataset: {dataset}")),
    };

    let worker_id = match client.find_worker(&dataset_id, start_block).await {
        Some(worker_id) => worker_id,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("No available worker for dataset {dataset} block {start_block}"),
            )
        }
    };

    (
        StatusCode::OK,
        format!("{host}/query/{dataset_id}/{worker_id}"),
    )
}

#[derive(Debug, Clone, Deserialize)]
struct QueryTimeout {
    timeout: Option<DurationString>,
}

async fn execute_query(
    Path((dataset_id, PeerId(worker_id))): Path<(DatasetId, PeerId)>,
    Query(QueryTimeout { timeout }): Query<QueryTimeout>,
    Extension(client): Extension<Arc<QueryClient>>,
    query: String, // request body
) -> impl IntoResponse {
    log::info!("Execute query dataset_id={dataset_id} worker_id={worker_id}");
    match client
        .execute_query(dataset_id, query, worker_id, timeout)
        .await
    {
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string().into_bytes(),
        ),
        Ok(QueryResult::BadRequest(err)) => (StatusCode::BAD_REQUEST, err.into_bytes()),
        Ok(QueryResult::ServerError(err)) => (StatusCode::INTERNAL_SERVER_ERROR, err.into_bytes()),
        Ok(QueryResult::Timeout) => (
            StatusCode::GATEWAY_TIMEOUT,
            "Query execution timed out".to_string().into_bytes(),
        ),
        Ok(QueryResult::Ok(data)) => (StatusCode::OK, data),
    }
}

pub async fn run_server(query_client: QueryClient, addr: &SocketAddr) -> anyhow::Result<()> {
    log::info!("Starting HTTP server listening on {addr}");
    let app = Router::new()
        .route("/network/:dataset/:start_block/worker", get(get_worker))
        .route("/query/:dataset_id/:worker_id", post(execute_query))
        .layer(Extension(Arc::new(query_client)));
    Server::bind(addr).serve(app.into_make_service()).await?;
    Ok(())
}
