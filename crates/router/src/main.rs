use clap::Parser;
use cli::Cli;
use router_controller::controller::ControllerBuilder;
use server::Server;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use storage::{S3Storage, Storage};
use url::Url;

mod cli;
mod logger;
mod metrics;
mod middleware;
mod scheduler;
mod server;
mod storage;
mod dataset;

#[tokio::main]
async fn main() {
    let mut args = Cli::parse();
    logger::init();

    for dataset in &mut args.dataset {
        let storage = create_storage(dataset.url()).await;
        dataset.set_storage(storage);
    }

    let controller = ControllerBuilder::new()
        .set_data_replication(args.replication)
        .set_data_management_unit(args.scheduling_unit)
        .set_workers(args.worker)
        .set_datasets(args.dataset.iter().map(|ds| ds.into()))
        .build();

    let controller = Arc::new(controller);

    let scheduling_interval = Duration::from_secs(args.scheduling_interval);
    scheduler::start(controller.clone(), args.dataset, scheduling_interval);

    Server::new(controller).run().await;
}

async fn create_storage(dataset: &str) -> Arc<dyn Storage + Sync + Send> {
    let url = Url::parse(dataset);
    match url {
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
                Arc::new(S3Storage::new(client, bucket))
            }
            _ => panic!("unsupported filesystem - {}", url.scheme()),
        },
        Err(..) => panic!("unsupported dataset - {}", dataset),
    }
}
