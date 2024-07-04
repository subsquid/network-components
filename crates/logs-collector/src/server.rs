use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::RwLock;

use contract_client::Client as ContractClient;
use subsquid_messages::{LogsCollected, Ping, QueryExecuted};
use subsquid_network_transport::util::TaskManager;
use subsquid_network_transport::PeerId;
use subsquid_network_transport::{LogsCollectorEvent, LogsCollectorTransportHandle};

use crate::collector::LogsCollector;
use crate::storage::LogsStorage;

pub struct Server<T, S>
where
    T: LogsStorage + Send + Sync + 'static,
    S: Stream<Item = LogsCollectorEvent> + Send + Unpin + 'static,
{
    incoming_events: S,
    transport_handle: LogsCollectorTransportHandle,
    logs_collector: Arc<RwLock<LogsCollector<T>>>,
    registered_workers: Arc<RwLock<HashSet<PeerId>>>,
    task_manager: TaskManager,
}

impl<T, S> Server<T, S>
where
    T: LogsStorage + Send + Sync + 'static,
    S: Stream<Item = LogsCollectorEvent> + Send + Unpin + 'static,
{
    pub fn new(
        incoming_events: S,
        transport_handle: LogsCollectorTransportHandle,
        logs_collector: LogsCollector<T>,
    ) -> Self {
        let logs_collector = Arc::new(RwLock::new(logs_collector));
        Self {
            incoming_events,
            transport_handle,
            logs_collector,
            registered_workers: Default::default(),
            task_manager: Default::default(),
        }
    }

    pub async fn run(
        mut self,
        contract_client: Arc<dyn ContractClient>,
        store_logs_interval: Duration,
        worker_update_interval: Duration,
    ) -> anyhow::Result<()> {
        log::info!("Starting logs collector server");

        // Perform initial storage sync to get sequence numbers
        self.logs_collector.write().await.storage_sync().await?;

        // Get registered workers from chain
        *self.registered_workers.write().await = contract_client
            .active_workers()
            .await?
            .into_iter()
            .map(|w| w.peer_id)
            .collect();

        self.spawn_saving_task(store_logs_interval);
        self.spawn_worker_update_task(contract_client, worker_update_interval);

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        loop {
            tokio::select! {
                Some(ev) = self.incoming_events.next() => self.on_incoming_event(ev).await, // FIXME: blocking event loop
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
                else => break
            }
        }
        log::info!("Server shutting down");
        self.task_manager.await_stop().await;
        Ok(())
    }

    async fn on_incoming_event(&mut self, ev: LogsCollectorEvent) {
        match ev {
            LogsCollectorEvent::WorkerLogs { peer_id, logs } => {
                self.collect_logs(peer_id, logs).await
            }
            LogsCollectorEvent::Ping { peer_id, ping } => self.collect_ping(peer_id, ping).await,
            LogsCollectorEvent::QuerySubmitted(query_submitted) => {
                match serde_json::to_string(&query_submitted) {
                    Ok(s) => println!("{s}"),
                    Err(e) => log::error!("Error serializing log: {e:?}"),
                }
            }
            LogsCollectorEvent::QueryFinished(query_finished) => {
                match serde_json::to_string(&query_finished) {
                    Ok(s) => println!("{s}"),
                    Err(e) => log::error!("Error serializing log: {e:?}"),
                }
            }
        }
    }

    async fn collect_logs(&self, worker_id: PeerId, logs: Vec<QueryExecuted>) {
        if !self.registered_workers.read().await.contains(&worker_id) {
            log::warn!("Worker not registered: {worker_id:?}");
            return;
        }
        self.logs_collector
            .write()
            .await
            .collect_logs(worker_id, logs);
    }

    async fn collect_ping(&self, worker_id: PeerId, ping: Ping) {
        if !self.registered_workers.read().await.contains(&worker_id) {
            log::warn!("Worker not registered: {worker_id:?}");
            return;
        }
        self.logs_collector
            .write()
            .await
            .collect_ping(worker_id, ping);
    }

    fn spawn_saving_task(&mut self, interval: Duration) {
        log::info!("Starting logs saving task");
        let collector = self.logs_collector.clone();
        let transport_handle = self.transport_handle.clone();
        let task = move |_| {
            let collector = collector.clone();
            let transport_handle = transport_handle.clone();
            async move {
                let sequence_numbers = match collector.write().await.storage_sync().await {
                    Ok(seq_nums) => seq_nums,
                    Err(e) => return log::error!("Error saving logs to storage: {e:?}"),
                };
                let logs_collected = LogsCollected { sequence_numbers };
                if transport_handle.logs_collected(logs_collected).is_err() {
                    log::error!("Error broadcasting logs collected: queue full");
                }
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
