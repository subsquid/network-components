use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use url::Url;

use archive_router::{aws_config, aws_sdk_s3};
use archive_router::dataset::{S3Storage, Storage};
use archive_router_api::hyper::Error;
use archive_router_api::Server;
use archive_router_controller::controller::ControllerBuilder;
use cli::Cli;

mod cli;
mod logger;
mod metrics;
mod scheduler;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Cli::parse();
    logger::init();

    let mut storages: HashMap<String, Box<dyn Storage + Send>> = HashMap::new();
    for (_name, dataset) in &args.dataset {
        let storage = create_storage(dataset).await;
        storages.insert(dataset.clone(), storage);
    }

    let controller = ControllerBuilder::new()
        .set_data_replication(args.replication)
        .set_data_management_unit(args.scheduling_unit)
        .set_workers(args.worker)
        .set_datasets(args.dataset)
        .build();

    let controller = Arc::new(controller);

    let scheduling_interval = Duration::from_secs(args.scheduling_interval);
    scheduler::start(controller.clone(), storages, scheduling_interval);

    Server::new(controller).run().await
}

async fn create_storage(dataset: &String) -> Box<dyn Storage + Send> {
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
                Box::new(S3Storage::new(client, bucket))
            }
            _ => panic!("unsupported filesystem - {}", url.scheme()),
        },
        Err(..) => panic!("unsupported dataset - {}", dataset),
    }
}
