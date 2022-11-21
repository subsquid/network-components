use archive_router::aws_config;
use archive_router::aws_sdk_s3;
use archive_router::dataset::{DatasetStorage, LocalStorage, S3Storage, Storage};
use archive_router::ArchiveRouter;
use archive_router_api::hyper::Error;
use archive_router_api::Server;
use clap::Parser;
use cli::Cli;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use url::Url;

mod cli;
mod logger;
mod scheduler;
mod storage_sync;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Cli::parse();
    logger::init();

    let storage = create_storage(&args).await;
    let router = Arc::new(Mutex::new(ArchiveRouter::new(
        args.dataset,
        args.replication,
    )));

    let scheduling_interval = Duration::from_secs(args.scheduling_interval);
    scheduler::start(router.clone(), scheduling_interval);
    let sync_interval = Duration::from_secs(args.sync_interval);
    storage_sync::start(router.clone(), storage, sync_interval);

    Server::new(router).run().await
}

async fn create_storage(args: &Cli) -> Arc<tokio::sync::Mutex<DatasetStorage>> {
    let url = Url::parse(&args.dataset);
    let storage_api: Box<dyn Storage + Send + Sync> = match url {
        Ok(url) => match url.scheme() {
            "s3" => {
                let mut config_loader = aws_config::from_env();
                let s3_endpoint = env::var("AWS_S3_ENDPOINT").ok();
                if let Some(s3_endpoint) = &s3_endpoint {
                    let uri = s3_endpoint.parse().expect("invalid s3-endpoint");
                    let endpoint = aws_sdk_s3::Endpoint::immutable(uri);
                    config_loader = config_loader.endpoint_resolver(endpoint);
                }
                let config = config_loader.load().await;

                let client = aws_sdk_s3::Client::new(&config);
                let host = url.host_str().expect("invalid dataset host").to_string();
                let bucket = host + url.path();
                Box::new(S3Storage::new(client, bucket))
            }
            _ => panic!("unsupported filesystem - {}", url.scheme()),
        },
        Err(..) => Box::new(LocalStorage::new(args.dataset.clone())),
    };

    Arc::new(tokio::sync::Mutex::new(DatasetStorage::new(
        storage_api,
        args.chunk_size,
    )))
}
