use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use axum::body::{boxed, Body};
use axum::extract::{Extension, Path};
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::middleware::from_fn;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use prometheus::{gather, Encoder, TextEncoder};
use tracing::info;

use router_controller::controller::{Controller, PingMessage, WorkerState};

use crate::middleware::logging;
use crate::metrics::NETWORK_ERRORS;

#[axum_macros::debug_handler]
async fn ping(
    Json(msg): Json<PingMessage>,
    Extension(controller): Extension<Arc<Controller>>,
) -> Json<WorkerState> {
    let worker_id = msg.worker_id.clone();
    let worker_url = msg.worker_url.clone();
    let current_state = format!("{:?}", msg.state);
    let desired_state = controller.ping(msg);
    info!(
        ping_from = worker_id,
        worker_url,
        current_state,
        desired_state = format!("{:?}", desired_state)
    );
    Json(desired_state.deref().clone())
}

#[axum_macros::debug_handler]
async fn get_worker(
    Path((dataset, start_block)): Path<(String, u32)>,
    Extension(controller): Extension<Arc<Controller>>,
) -> Response {
    match controller.get_worker(&dataset, start_block) {
        Ok(Some(url)) => url.into_response(),
        Ok(None) => {
            let status = StatusCode::SERVICE_UNAVAILABLE;
            let msg = format!("not ready to serve block {} of dataset {}", start_block, dataset);
            NETWORK_ERRORS.with_label_values(&[&dataset, "get_worker", status.as_str()]).inc();
            (status, msg).into_response()
        },
        Err(err) => (StatusCode::NOT_FOUND, err).into_response()
    }
}

#[axum_macros::debug_handler]
async fn get_height(
    Path(dataset): Path<String>,
    Extension(controller): Extension<Arc<Controller>>,
) -> Response {
    match controller.get_confirmed_height(&dataset) {
        Ok(Some(height)) => height.to_string().into_response(),
        Ok(None) => {
            let status = StatusCode::SERVICE_UNAVAILABLE;
            let msg = format!("height for dataset {} is unknown", dataset);
            NETWORK_ERRORS.with_label_values(&[&dataset, "get_height", status.as_str()]).inc();
            (status, msg).into_response()
        },
        Err(err) => (StatusCode::NOT_FOUND, err).into_response()
    }
}

#[axum_macros::debug_handler]
async fn get_metrics() -> Response {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    encoder
        .encode(&gather(), &mut buffer)
        .expect("Failed to encode metrics");
    Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(boxed(Body::from(buffer)))
        .unwrap()
}

pub struct Server {
    controller: Arc<Controller>,
}

impl Server {
    pub fn new(controller: Arc<Controller>) -> Self {
        Server { controller }
    }

    pub async fn run(&self) {
        let app = Router::new()
            .route("/ping", post(ping))
            .route("/network/:dataset/:start_block/worker", get(get_worker))
            .route("/network/:dataset/height", get(get_height))
            .route("/metrics", get(get_metrics))
            .layer(from_fn(logging))
            .layer(Extension(self.controller.clone()));
        let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap()
    }
}
