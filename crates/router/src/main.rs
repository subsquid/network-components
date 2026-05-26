use clap::Parser;
use cli::Cli;
use router_controller::controller::ControllerBuilder;
use server::Server;
use std::env;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use storage::{S3Storage, Storage};
use url::Url;

mod auth;
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

    let api_client = match args.network_api_url {
        Some(url) => auth::NetworkApiClient::new(url),
        None => auth::NetworkApiClient::disabled(),
    };
    let worker_jwt_required = !args.disable_v2_auth && !args.enforce_v2_auth_for_ips.0.is_empty();
    let worker_jwt_issuer = create_worker_jwt_issuer(
        env::var("WORKER_JWT_PRIVATE_KEY_PEM").ok(),
        args.worker_jwt_private_key_file,
        Duration::from_secs(args.worker_jwt_ttl_secs),
        worker_jwt_required,
    );
    let auth_state = auth::AuthState::new(
        api_client,
        args.disable_v2_auth,
        args.enforce_v2_auth_for_ips.0,
        args.trusted_ips.0,
        args.internal_allowlist.0,
        worker_jwt_issuer,
        worker_jwt_required,
    );

    Server::new(controller).run(auth_state).await;
}

fn create_worker_jwt_issuer(
    private_key_pem: Option<String>,
    private_key_file: Option<std::path::PathBuf>,
    ttl: Duration,
    required: bool,
) -> Option<auth::WorkerJwtIssuer> {
    let pem = match private_key_pem {
        Some(pem) => Some(pem),
        None => private_key_file.map(|path| {
            fs::read_to_string(&path).unwrap_or_else(|err| {
                panic!("failed to read worker JWT private key file {}: {}", path.display(), err)
            })
        }),
    };

    let Some(pem) = pem else {
        if required {
            panic!(
                "WORKER_JWT_PRIVATE_KEY_PEM or WORKER_JWT_PRIVATE_KEY_FILE is required when V2 auth enforcement is enabled"
            );
        }
        return None;
    };

    Some(
        auth::WorkerJwtIssuer::from_ed25519_pem_with_ttl(pem.as_bytes(), ttl)
            .expect("invalid worker JWT Ed25519 private key PEM or TTL"),
    )
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
