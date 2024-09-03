use std::net::SocketAddr;
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Extension, Json, Router};
use clap::Parser;
use env_logger::Env;

use sqd_network_transport::{
    P2PTransportBuilder, PeerCheckerConfig, PeerCheckerTransportHandle, ProbeRequest, ProbeResult,
    TransportArgs,
};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tokio::signal::unix::{signal, SignalKind};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

    #[arg(
        long,
        env,
        help = "HTTP server listen addr",
        default_value = "0.0.0.0:8000"
    )]
    http_listen_addr: SocketAddr,

    #[arg(
        long,
        env,
        help = "Maximum number of probes that could run in parallel",
        default_value = "1024"
    )]
    max_concurrent_probes: usize,

    #[arg(
        long,
        env,
        help = "Size of the queue for probes waiting to be started",
        default_value = "1024"
    )]
    probe_queue_size: usize,

    #[arg(
        long,
        env,
        help = "Timeout for a single peer probe (in seconds)",
        default_value = "20"
    )]
    probe_timeout_sec: u64,
}

async fn probe_peer(
    Extension(transport_handle): Extension<PeerCheckerTransportHandle>,
    Json(request): Json<ProbeRequest>,
) -> Response {
    match transport_handle.probe_peer(request).await {
        ProbeResult::Ok(res) => (StatusCode::OK, Json(res)).into_response(),
        ProbeResult::Failure { error } => (StatusCode::NOT_FOUND, error).into_response(),
        ProbeResult::Ongoing => (
            StatusCode::TOO_MANY_REQUESTS,
            "Probe for peer already ongoing",
        )
            .into_response(),
        ProbeResult::TooManyProbes => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Too many active/scheduled probes",
        )
            .into_response(),
        ProbeResult::ServerError => {
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logger and parse arguments
    env_logger::Builder::from_env(Env::default().default_filter_or("info, ethers_providers=warn"))
        .init();
    let args: Cli = Cli::parse();

    // Build P2P transport
    let transport_builder = P2PTransportBuilder::from_cli(args.transport)
        .await?
        .with_base_config(|mut config| {
            config.max_concurrent_probes = args.max_concurrent_probes;
            config.probe_timeout = Duration::from_secs(args.probe_timeout_sec);
            config
        });
    let mut config = PeerCheckerConfig::default();
    config.probe_queue_size = args.probe_queue_size;
    let transport_handle = transport_builder.build_peer_checker(config)?;

    // Start HTTP server
    let addr = args.http_listen_addr;
    log::info!("Starting HTTP server listening on {addr}");
    let app = Router::new()
        .route("/probe", post(probe_peer))
        .layer(Extension(transport_handle));

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let shutdown = async move {
        tokio::select! {
            _ = sigint.recv() => (),
            _ = sigterm.recv() =>(),
        }
    };

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;

    log::info!("HTTP server stopped");

    Ok(())
}
