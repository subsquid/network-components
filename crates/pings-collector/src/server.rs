use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream, StreamExt};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use tokio::signal::unix::{signal, SignalKind};

use collector_utils::{PingRow, Storage};
use semver::VersionReq;
use sqd_contract_client::Client as ContractClient;
use sqd_network_transport::util::{CancellationToken, TaskManager};
use sqd_network_transport::{PeerId, PingsCollectorTransportHandle};

lazy_static! {
    pub static ref SUPPORTED_WORKER_VERSIONS: VersionReq =
        std::env::var("SUPPORTED_WORKER_VERSIONS")
            .unwrap_or(">=1.1.0-rc3".to_string())
            .parse()
            .expect("Invalid SUPPORTED_WORKER_VERSIONS");
}

pub struct Server {
    transport_handle: PingsCollectorTransportHandle,
    registered_workers: Arc<RwLock<HashSet<PeerId>>>,
    task_manager: TaskManager,
    request_interval: Duration,
    concurrency_limit: usize,
}

impl Server {
    pub fn new(
        transport_handle: PingsCollectorTransportHandle,
        request_interval: Duration,
        concurrency_limit: usize,
    ) -> Self {
        Self {
            transport_handle,
            registered_workers: Default::default(),
            task_manager: Default::default(),
            request_interval,
            concurrency_limit,
        }
    }

    pub async fn run(
        mut self,
        contract_client: Arc<dyn ContractClient>,
        worker_update_interval: Duration,
        storage: impl Storage + Send + Sync + 'static,
    ) -> anyhow::Result<()> {
        log::info!("Starting pings collector server");

        // Get registered workers from chain
        *self.registered_workers.write() = contract_client
            .active_workers()
            .await?
            .into_iter()
            .map(|w| w.peer_id)
            .collect();

        self.spawn_worker_update_task(contract_client, worker_update_interval);
        self.spawn_heartbeat_collection_task(storage);

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        loop {
            tokio::select! {
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
                else => break
            }
        }
        log::info!("Server shutting down");
        self.task_manager.await_stop().await;
        Ok(())
    }

    fn spawn_worker_update_task(
        &mut self,
        contract_client: Arc<dyn ContractClient>,
        interval: Duration,
    ) {
        // TODO: There's exact same code in logs collector. Move out to collector-utils
        log::info!("Starting worker update task");
        let registered_workers = self.registered_workers.clone();
        let contract_client: Arc<dyn ContractClient> = contract_client;
        let task = move |_| {
            let registered_workers = registered_workers.clone();
            let contract_client = contract_client.clone();
            async move {
                let workers = match contract_client.active_workers().await {
                    Ok(workers) => workers,
                    Err(e) => return log::error!("Error getting registered workers: {e:?}"),
                };
                *registered_workers.write() = workers
                    .into_iter()
                    .map(|w| w.peer_id)
                    .collect::<HashSet<PeerId>>();
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }

    fn spawn_heartbeat_collection_task(&mut self, storage: impl Storage + Send + Sync + 'static) {
        log::info!("Starting heartbeat collection task");
        let transport_handle = self.transport_handle.clone();
        let registered_workers = self.registered_workers.clone();
        let concurrency_limit = self.concurrency_limit;
        let storage = Arc::new(storage);

        let task = move |cancellation_token: CancellationToken| {
            let transport_handle = transport_handle.clone();
            let registered_workers = registered_workers.clone();
            let storage = storage.clone();

            async move {
                let start = std::time::Instant::now();

                let workers: Vec<PeerId> = registered_workers.read().iter().cloned().collect();
                if workers.is_empty() {
                    log::info!("No registered workers to collect heartbeats from");
                    return;
                }

                log::info!("Collecting heartbeats from {} workers", workers.len());

                let ping_rows: Vec<PingRow> = stream::iter(workers.into_iter())
                    .map(|peer_id| {
                        let handle = transport_handle.clone();
                        async move {
                            let heartbeat = match handle.request_heartbeat(peer_id).await {
                                Ok(heartbeat) => heartbeat,
                                Err(e) => {
                                    log::debug!("Failed to get heartbeat from {}: {}", peer_id, e);
                                    return None;
                                }
                            };

                            if !heartbeat.version_matches(&SUPPORTED_WORKER_VERSIONS) {
                                log::debug!(
                                    "Unsupported worker version {peer_id}: {:?}",
                                    heartbeat.version
                                );
                                return None;
                            }

                            match PingRow::new(heartbeat, peer_id.to_string()) {
                                Ok(ping_row) => Some(ping_row),
                                Err(e) => {
                                    log::error!("Error creating ping row: {e}");
                                    None
                                }
                            }
                        }
                    })
                    .buffered(concurrency_limit)
                    .take_until(cancellation_token.cancelled_owned())
                    .filter_map(|x| std::future::ready(x))
                    .collect()
                    .await;

                log::info!(
                    "Collected {} heartbeats in {:?}",
                    ping_rows.len(),
                    start.elapsed()
                );
                if !ping_rows.is_empty() {
                    match storage.store_heartbeats(ping_rows.into_iter()).await {
                        Ok(()) => {
                            log::info!("Stored heartbeats successfully",);
                        }
                        Err(e) => {
                            log::error!("Error storing heartbeats: {e:?}");
                        }
                    }
                }
            }
        };

        self.task_manager
            .spawn_periodic(task, self.request_interval);
    }
}
