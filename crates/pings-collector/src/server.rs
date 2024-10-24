use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use tokio::signal::unix::{signal, SignalKind};

use collector_utils::{PingRow, Storage};
use semver::VersionReq;
use sqd_contract_client::Client as ContractClient;
use sqd_network_transport::util::{CancellationToken, TaskManager};
use sqd_network_transport::{PeerId, Ping, PingsCollectorTransportHandle};

lazy_static! {
    static ref BINCODE_CONFIG: bincode::config::Configuration = Default::default();
    pub static ref SUPPORTED_WORKER_VERSIONS: VersionReq =
        std::env::var("SUPPORTED_WORKER_VERSIONS")
            .unwrap_or(">=1.1.0-rc3".to_string())
            .parse()
            .expect("Invalid SUPPORTED_WORKER_VERSIONS");
}

const PINGS_BATCH_SIZE: usize = 10000;

pub struct Server<S>
where
    S: Stream<Item = Ping> + Send + Unpin + 'static,
{
    incoming_pings: S,
    _transport_handle: PingsCollectorTransportHandle,
    registered_workers: Arc<RwLock<HashSet<PeerId>>>,
    task_manager: TaskManager,
}

impl<S> Server<S>
where
    S: Stream<Item = Ping> + Send + Unpin + 'static,
{
    pub fn new(incoming_pings: S, transport_handle: PingsCollectorTransportHandle) -> Self {
        Self {
            incoming_pings,
            _transport_handle: transport_handle,
            registered_workers: Default::default(),
            task_manager: Default::default(),
        }
    }

    pub async fn run(
        mut self,
        contract_client: Arc<dyn ContractClient>,
        storage_sync_interval: Duration,
        worker_update_interval: Duration,
        buffer_path: impl AsRef<Path>,
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

        let (buffer_writer, buffer_reader) = open_buffer(buffer_path)?;
        let mut collector = Collector::new(buffer_writer, self.registered_workers.clone());
        let writer = StorageWriter::new(buffer_reader, storage, storage_sync_interval);
        self.task_manager.spawn(writer.start());
        self.spawn_worker_update_task(contract_client, worker_update_interval);

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        loop {
            tokio::select! {
                Some(ping) = self.incoming_pings.next() => {
                    _ = collector.collect_ping(ping).map_err(|e| log::error!("Error collecting ping: {e:?}"));
                }
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
}

fn open_buffer(path: impl AsRef<Path>) -> anyhow::Result<(yaque::Sender, yaque::Receiver)> {
    yaque::recovery::recover_with_loss(&path)?;
    Ok(yaque::channel(&path).map_err(|e| {
        log::warn!("Error opening buffer: {e:?}");
        e
    })?)
}

struct Collector {
    buffer_writer: yaque::Sender,
    registered_workers: Arc<RwLock<HashSet<PeerId>>>,
}

impl Collector {
    pub fn new(
        buffer_writer: yaque::Sender,
        registered_workers: Arc<RwLock<HashSet<PeerId>>>,
    ) -> Self {
        Self {
            buffer_writer,
            registered_workers,
        }
    }
    pub fn collect_ping(&mut self, Ping { peer_id, ping }: Ping) -> anyhow::Result<()> {
        if !self.registered_workers.read().contains(&peer_id) {
            log::warn!("Ping from unregistered worker {peer_id}: {ping:?}");
            return Ok(());
        }
        if !ping.version_matches(&SUPPORTED_WORKER_VERSIONS) {
            log::debug!("Unsupported worker version {peer_id}: {:?}", ping.version);
            return Ok(());
        }
        log::debug!("Collecting ping from {peer_id}");
        log::trace!("Ping collected: {ping:?}");
        let ping_row: PingRow = ping.try_into().map_err(|e: &str| anyhow::format_err!(e))?;
        let bytes = bincode::serde::encode_to_vec(ping_row, *BINCODE_CONFIG)?;
        self.buffer_writer
            .try_send(bytes)
            .map_err(|e| anyhow::format_err!(e))?;
        Ok(())
    }
}

struct StorageWriter<S: Storage + Send + Sync + 'static> {
    buffer_reader: yaque::Receiver,
    storage: S,
    interval: Duration,
}

impl<S: Storage + Send + Sync + 'static> StorageWriter<S> {
    pub fn new(buffer_reader: yaque::Receiver, storage: S, interval: Duration) -> Self {
        Self {
            buffer_reader,
            storage,
            interval,
        }
    }

    pub fn start(
        self,
    ) -> impl FnOnce(CancellationToken) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        move |cancel_token| Box::pin(self.run(cancel_token))
    }

    async fn run(mut self, cancel_token: CancellationToken) {
        log::info!("Starting logs saving task");
        loop {
            let timeout = Box::pin(tokio::time::sleep(self.interval));
            let recv_guard = tokio::select! {
                _ = cancel_token.cancelled() => break,
                res = self.buffer_reader.recv_batch_timeout(PINGS_BATCH_SIZE, timeout) => match res {
                    Ok(guard) => guard,
                    Err(e) => {
                        log::error!("Error reading buffer: {e:?}");
                        continue
                    }
                }
            };
            log::info!("Read {} ping rows from buffer", recv_guard.len());
            let ping_rows =
                recv_guard.iter().filter_map(|b| {
                    match bincode::serde::decode_from_slice(b, *BINCODE_CONFIG) {
                        Ok((row, _)) => Some(row),
                        Err(e) => {
                            log::error!("Corrupt ping row in buffer: {e:}");
                            None
                        }
                    }
                });
            match self.storage.store_pings(ping_rows).await {
                Ok(()) => {
                    log::info!("Pings stored successfully");
                    _ = recv_guard.commit().map_err(|e| log::error!("{e:?}"));
                }
                Err(e) => {
                    log::error!("Error storing pings: {e:?}");
                    _ = recv_guard.rollback().map_err(|e| log::error!("{e:?}"));
                }
            }
        }
    }
}
