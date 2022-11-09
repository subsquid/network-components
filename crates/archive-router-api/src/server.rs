use archive_router::dataset::DataRange;
use archive_router::url::Url;
use archive_router::uuid::Uuid;
use archive_router::{ArchiveRouter, WorkerState};
use axum::extract::{Extension, Path};
use axum::response::Result;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

#[derive(Deserialize)]
struct Ping {
    worker_id: Uuid,
    worker_url: Url,
    state: WorkerState,
}

#[axum_macros::debug_handler]
async fn ping(
    Json(payload): Json<Ping>,
    Extension(router): Extension<Arc<Mutex<ArchiveRouter>>>,
) -> Json<WorkerState> {
    let mut router = router.lock().unwrap();
    let desired_state = router
        .ping(payload.worker_id, payload.worker_url, payload.state)
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
            .layer(Extension(self.router.clone()));
        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
    }
}
