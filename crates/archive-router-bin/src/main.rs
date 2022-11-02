use archive_router::dataset::LocalDatasetStorage;
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
    let storage = LocalDatasetStorage {};
    let router = Arc::new(Mutex::new(ArchiveRouter::new(
        args.dataset,
        args.replication,
        storage,
    )));

    let interval = Duration::from_secs(args.scheduling_interval);
    scheduler::start(router.clone(), interval);

    Server::new(router).run().await
}
