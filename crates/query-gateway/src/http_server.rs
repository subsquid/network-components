use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Extension, Host, Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Router, Server};
use duration_string::DurationString;
use flate2::write::GzDecoder;
use serde::Deserialize;

use subsquid_messages::OkResult;

use crate::client::{QueryClient, QueryResult};
use crate::config::{DatasetId, PeerId};

async fn get_height(
    Path(dataset): Path<String>,
    Extension(client): Extension<Arc<QueryClient>>,
) -> impl IntoResponse {
    log::debug!("Get height dataset={dataset}");
    let dataset_id = match client.get_dataset_id(&dataset).await {
        Some(dataset_id) => dataset_id,
        None => return (StatusCode::NOT_FOUND, format!("Unknown dataset: {dataset}")),
    };

    match client.get_height(&dataset_id).await {
        Some(height) => (StatusCode::OK, height.to_string()),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("No data for dataset {dataset}"),
        ),
    }
}

async fn get_worker(
    Host(host): Host,
    Path((dataset, start_block)): Path<(String, u32)>,
    Extension(client): Extension<Arc<QueryClient>>,
) -> impl IntoResponse {
    log::debug!("Get worker dataset={dataset} start_block={start_block}");
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
        format!("http://{host}/query/{dataset_id}/{worker_id}"),
    )
}

#[derive(Debug, Clone, Deserialize)]
struct ExecuteParams {
    timeout: Option<DurationString>,
    #[serde(default)]
    profiling: bool,
}

async fn execute_query(
    Path((dataset_id, PeerId(worker_id))): Path<(DatasetId, PeerId)>,
    Query(ExecuteParams { timeout, profiling }): Query<ExecuteParams>,
    Extension(client): Extension<Arc<QueryClient>>,
    headers: HeaderMap,
    query: String, // request body
) -> Response {
    log::debug!("Execute query dataset_id={dataset_id} worker_id={worker_id}");
    match client
        .execute_query(dataset_id, query, worker_id, timeout, profiling)
        .await
    {
        Err(err) => server_error(err.to_string()),
        Ok(QueryResult::ServerError(err)) => server_error(err),
        Ok(QueryResult::BadRequest(err)) => bad_request(err),
        Ok(QueryResult::Timeout) => query_timeout(),
        Ok(QueryResult::Ok(result)) => ok_response(result, headers),
    }
}

fn server_error(err: String) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, err.into_bytes()).into_response()
}

fn bad_request(err: String) -> Response {
    (StatusCode::BAD_REQUEST, err.into_bytes()).into_response()
}

fn query_timeout() -> Response {
    let msg = "Query execution timed out".to_string().into_bytes();
    (StatusCode::GATEWAY_TIMEOUT, msg).into_response()
}

fn ok_response(result: OkResult, request_headers: HeaderMap) -> Response {
    let OkResult {
        mut data,
        exec_plan,
    } = result;
    if let Some(exec_plan) = exec_plan {
        save_exec_plan(exec_plan);
    }

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json".parse().unwrap());

    // If client accepts gzip compressed data, return it as-is. Otherwise, decompress.
    let gzip_accepted = request_headers
        .get_all("accept-encoding")
        .iter()
        .filter_map(|x| x.to_str().ok())
        .any(|x| x.contains("gzip"));
    if gzip_accepted {
        headers.insert("content-encoding", "gzip".parse().unwrap());
    } else {
        data = match decode_gzip(data) {
            Ok(data) => data,
            Err(err) => return server_error(err.to_string()),
        }
    }

    (StatusCode::OK, headers, data).into_response()
}

fn save_exec_plan(exec_plan: Vec<u8>) {
    tokio::spawn(async move {
        let mut output_path = std::env::temp_dir();
        let now = chrono::Utc::now();
        output_path.extend([format!("exec_plan_{}.json.gz", now.to_rfc3339())]);
        if let Err(e) = tokio::fs::write(&output_path, exec_plan).await {
            log::error!("Error saving exec_plan: {e:?}");
        } else {
            log::info!("Exec plan saved to {}", output_path.display());
        }
    });
}

fn decode_gzip(data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let buffer = Vec::new();
    let mut decoder = GzDecoder::new(buffer);
    decoder.write_all(data.as_slice())?;
    Ok(decoder.finish()?)
}

pub async fn run_server(query_client: QueryClient, addr: &SocketAddr) -> anyhow::Result<()> {
    log::info!("Starting HTTP server listening on {addr}");
    let app = Router::new()
        .route("/network/:dataset/height", get(get_height))
        .route("/network/:dataset/:start_block/worker", get(get_worker))
        .route("/query/:dataset_id/:worker_id", post(execute_query))
        .layer(Extension(Arc::new(query_client)));
    Server::bind(addr).serve(app.into_make_service()).await?;
    Ok(())
}
