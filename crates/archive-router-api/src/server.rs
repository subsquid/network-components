use archive_router::error::Error;
use archive_router::url::Url;
use archive_router::uuid::Uuid;
use archive_router::{ArchiveRouter, WorkerState};
use axum::extract::{Extension, Path};
use axum::response::{IntoResponse, Result};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_macros::debug_handler;
use hyper::StatusCode;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

#[derive(Deserialize)]
struct Ping {
    worker_id: Uuid,
    worker_url: Url,
    state: WorkerState,
}

#[debug_handler]
async fn ping(
    Json(payload): Json<Ping>,
    Extension(router): Extension<Arc<Mutex<ArchiveRouter>>>,
) -> impl IntoResponse {
    let mut router = router.lock().unwrap();
    let desired_state = router
        .ping(payload.worker_id, payload.worker_url, payload.state)
        .clone();
    Json(desired_state)
}

#[debug_handler]
async fn get_worker(
    Path(start_block): Path<i32>,
    Extension(router): Extension<Arc<Mutex<ArchiveRouter>>>,
) -> impl IntoResponse {
    let router = router.lock().unwrap();
    match router.get_worker(start_block) {
        Ok(url) => (StatusCode::OK, Json(url.clone()).into_response()),
        Err(e) => match e {
            Error::NoRequestedData => (
                StatusCode::BAD_REQUEST,
                "dataset doesn't have requested data".into_response(),
            ),
            Error::NoSuitableWorker => (
                StatusCode::SERVICE_UNAVAILABLE,
                "no suitable worker".into_response(),
            ),
        },
    }
}

#[debug_handler]
async fn get_dataset_range(
    Extension(router): Extension<Arc<Mutex<ArchiveRouter>>>,
) -> impl IntoResponse {
    let router = router.lock().unwrap();
    let range = router.get_dataset_range();
    Json(range)
}

pub struct Server {
    router: Arc<Mutex<ArchiveRouter>>,
}

impl Server {
    pub fn new(router: ArchiveRouter) -> Self {
        Server {
            router: Arc::new(Mutex::new(router)),
        }
    }

    pub async fn run(&self) -> Result<(), hyper::Error> {
        let app = Router::new()
            .route("/ping", post(ping))
            .route("/worker/:start_block", get(get_worker))
            .route("/dataset-range", get(get_dataset_range))
            .layer(Extension(self.router.clone()));
        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
    }
}
