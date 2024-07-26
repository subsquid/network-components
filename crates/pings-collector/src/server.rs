use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt, TryFutureExt};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::RwLock;

use contract_client::Client as ContractClient;
use subsquid_network_transport::util::TaskManager;
use subsquid_network_transport::{PeerId, Ping, PingsCollectorTransportHandle};

use collector_utils::Storage;

use crate::collector::PingsCollector;

pub struct Server<T, S>
where
    T: Storage + Send + Sync + 'static,
    S: Stream<Item = Ping> + Send + Unpin + 'static,
{
    incoming_pings: S,
    _transport_handle: PingsCollectorTransportHandle,
    pings_collector: Arc<RwLock<PingsCollector<T>>>, // FIXME: make it lock-less if possible
    registered_workers: Arc<RwLock<HashSet<PeerId>>>,
    task_manager: TaskManager,
}

impl<T, S> Server<T, S>
where
    T: Storage + Send + Sync + 'static,
    S: Stream<Item = Ping> + Send + Unpin + 'static,
{
    pub fn new(
        incoming_pings: S,
        transport_handle: PingsCollectorTransportHandle,
        pings_collector: PingsCollector<T>,
    ) -> Self {
        let pings_collector = Arc::new(RwLock::new(pings_collector));
        Self {
            incoming_pings,
            _transport_handle: transport_handle,
            pings_collector,
            registered_workers: Default::default(),
            task_manager: Default::default(),
        }
    }

    pub async fn run(
        mut self,
        contract_client: Arc<dyn ContractClient>,
        storage_sync_interval: Duration,
        worker_update_interval: Duration,
    ) -> anyhow::Result<()> {
        log::info!("Starting pings collector server");

        // Get registered workers from chain
        *self.registered_workers.write().await = contract_client
            .active_workers()
            .await?
            .into_iter()
            .map(|w| w.peer_id)
            .collect();

        self.spawn_saving_task(storage_sync_interval);
        self.spawn_worker_update_task(contract_client, worker_update_interval);

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        loop {
            tokio::select! {
                Some(Ping { peer_id, ping }) = self.incoming_pings.next() => self.collect_ping(peer_id, ping).await, // FIXME: blocking event loop
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
                else => break
            }
        }
        log::info!("Server shutting down");
        self.pings_collector.write().await.storage_sync().await?;
        self.task_manager.await_stop().await;
        Ok(())
    }

    async fn collect_ping(&self, worker_id: PeerId, ping: subsquid_messages::Ping) {
        if !self.registered_workers.read().await.contains(&worker_id) {
            log::warn!("Worker not registered: {worker_id:?}");
            return;
        }
        self.pings_collector
            .write()
            .await
            .collect_ping(worker_id, ping);
    }

    fn spawn_saving_task(&mut self, interval: Duration) {
        log::info!("Starting logs saving task");
        let collector = self.pings_collector.clone();
        let task = move |_| {
            let collector = collector.clone();
            async move {
                let _ = collector
                    .write()
                    .await
                    .storage_sync()
                    .map_err(|e| log::error!("Error saving pings: {e:?}"));
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }

    fn spawn_worker_update_task(
        &mut self,
        contract_client: Arc<dyn ContractClient>,
        interval: Duration,
    ) {
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
                *registered_workers.write().await = workers
                    .into_iter()
                    .map(|w| w.peer_id)
                    .collect::<HashSet<PeerId>>();
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }
}
