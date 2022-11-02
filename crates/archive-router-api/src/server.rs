use archive_router::dataset::DatasetStorage;
use archive_router::url::Url;
use archive_router::uuid::Uuid;
use archive_router::{ArchiveRouter, DataRange, WorkerState};
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

async fn ping<S: DatasetStorage + Send>(
    Json(payload): Json<Ping>,
    Extension(router): Extension<Arc<Mutex<ArchiveRouter<S>>>>,
) -> Json<WorkerState> {
    let mut router = router.lock().unwrap();
    let desired_state = router
        .ping(payload.worker_id, payload.worker_url, payload.state)
        .clone();
    Json(desired_state)
}

async fn get_worker<S: DatasetStorage>(
    Path(start_block): Path<i32>,
    Extension(router): Extension<Arc<Mutex<ArchiveRouter<S>>>>,
) -> Result<Json<Url>> {
    let router = router.lock().unwrap();
    let url = router.get_worker(start_block)?.clone();
    Ok(Json(url))
}

async fn get_dataset_range<S: DatasetStorage>(
    Extension(router): Extension<Arc<Mutex<ArchiveRouter<S>>>>,
) -> Result<Json<DataRange>> {
    let router = router.lock().unwrap();
    let range = router.get_dataset_range()?;
    Ok(Json(range))
}

pub struct Server<S: DatasetStorage + Send> {
    router: Arc<Mutex<ArchiveRouter<S>>>,
}

impl<S: DatasetStorage + Send + 'static> Server<S> {
    pub fn new(router: Arc<Mutex<ArchiveRouter<S>>>) -> Self {
        Server { router }
    }

    pub async fn run(&self) -> Result<(), hyper::Error> {
        let app = Router::new()
            .route("/ping", post(ping::<S>))
            .route("/worker/:start_block", get(get_worker::<S>))
            .route("/dataset-range", get(get_dataset_range::<S>))
            .layer(Extension(self.router.clone()));
        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
    }
}
