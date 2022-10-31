use archive_router::ArchiveRouter;
use archive_router_api::hyper::Error;
use archive_router_api::Server;
use std::sync::{Arc, Mutex};

mod scheduler;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let dataset = "s3://eth/main".to_string();
    let router = Arc::new(Mutex::new(ArchiveRouter::new(dataset)));

    scheduler::start(router.clone());

    Server::new(router).run().await
}
