use crate::middleware::logging;
use archive_router::config::Config;
use archive_router::prometheus::{gather, Encoder, TextEncoder};
use archive_router_controller::controller::{Controller, PingMessage, WorkerState};
use axum::body::{boxed, Body};
use axum::extract::{Extension, Path};
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::middleware::from_fn;
use axum::response::{IntoResponse, Response, Result};
use axum::routing::{get, post};
use axum::{Json, Router};
use std::net::SocketAddr;
use std::sync::Arc;

#[axum_macros::debug_handler]
async fn ping(
    Json(msg): Json<PingMessage<Config>>,
    Extension(controller): Extension<Arc<Controller<Config>>>,
) -> Json<WorkerState<Config>> {
    Json((*controller.ping(msg)).clone())
}

#[axum_macros::debug_handler]
async fn get_worker(
    Path((dataset, start_block)): Path<(String, u32)>,
    Extension(controller): Extension<Arc<Controller<Config>>>,
) -> Response {
    match controller.get_worker(&dataset, start_block) {
        Some(url) => Json((*url).clone()).into_response(),
        None => (StatusCode::SERVICE_UNAVAILABLE, "no suitable worker").into_response(),
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
    controller: Arc<Controller<Config>>,
}

impl Server {
    pub fn new(controller: Arc<Controller<Config>>) -> Self {
        Server { controller }
    }

    pub async fn run(&self) -> Result<(), hyper::Error> {
        let app = Router::new()
            .route("/ping", post(ping))
            .route("/worker/:dataset/:start_block", get(get_worker))
            .route("/metrics", get(get_metrics))
            .layer(from_fn(logging))
            .layer(Extension(self.controller.clone()));
        let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
    }
}
