use crate::middleware::logging;
use archive_router::dataset::DataRange;
use archive_router::prometheus::{gather, Encoder, TextEncoder};
use archive_router::url::Url;
use archive_router::{ArchiveRouter, WorkerState};
use axum::body::{boxed, Body};
use axum::extract::{Extension, Path};
use axum::http::header::CONTENT_TYPE;
use axum::middleware::from_fn;
use axum::response::{Response, Result};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

#[derive(Deserialize)]
struct Ping {
    worker_id: String,
    worker_url: Url,
    state: Option<WorkerState>,
    pause: Option<bool>,
}

#[axum_macros::debug_handler]
async fn ping(
    Json(ping): Json<Ping>,
    Extension(router): Extension<Arc<Mutex<ArchiveRouter>>>,
) -> Json<WorkerState> {
    let mut router = router.lock().unwrap();
    let desired_state = router
        .ping(ping.worker_id, ping.worker_url, ping.state, ping.pause)
        .clone();
    Json(desired_state)
}

#[axum_macros::debug_handler]
async fn get_worker(
    Path(start_block): Path<i32>,
    Extension(router): Extension<Arc<Mutex<ArchiveRouter>>>,
) -> Result<Json<Url>> {
    let router = router.lock().unwrap();
    let url = router.get_worker(start_block)?.clone();
    Ok(Json(url))
}

#[axum_macros::debug_handler]
async fn get_dataset_range(
    Extension(router): Extension<Arc<Mutex<ArchiveRouter>>>,
) -> Result<Json<DataRange>> {
    let router = router.lock().unwrap();
    let range = router.get_dataset_range();
    Ok(Json(range))
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
    router: Arc<Mutex<ArchiveRouter>>,
}

impl Server {
    pub fn new(router: Arc<Mutex<ArchiveRouter>>) -> Self {
        Server { router }
    }

    pub async fn run(&self) -> Result<(), hyper::Error> {
        let app = Router::new()
            .route("/ping", post(ping))
            .route("/worker/:start_block", get(get_worker))
            .route("/dataset-range", get(get_dataset_range))
            .route("/metrics", get(get_metrics))
            .layer(from_fn(logging))
            .layer(Extension(self.router.clone()));
        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
    }
}
