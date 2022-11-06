use archive_router::dataset::{DatasetStorage, LocalStorage};
use archive_router::ArchiveRouter;
use archive_router_api::hyper::Error;
use archive_router_api::Server;
use clap::Parser;
use cli::Cli;
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod cli;
mod scheduler;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Cli::parse();

    let storage = Box::new(LocalStorage::new(args.dataset.clone()));
    let dataset_storage = Arc::new(DatasetStorage::new(storage));
    let router = Arc::new(Mutex::new(ArchiveRouter::new(
        args.dataset,
        args.replication,
    )));

    let interval = Duration::from_secs(args.scheduling_interval);
    scheduler::start(router.clone(), dataset_storage.clone(), interval);

    Server::new(router, dataset_storage).run().await
}
