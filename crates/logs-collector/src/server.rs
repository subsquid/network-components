use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};

use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;

use contract_client::Client as ContractClient;
use subsquid_messages::envelope::Msg;
use subsquid_messages::signatures::SignedMessage;
use subsquid_messages::{Envelope, LogsCollected, ProstMsg, QueryLogs};
use subsquid_network_transport::task_manager::TaskManager;
use subsquid_network_transport::transport::P2PTransportHandle;
use subsquid_network_transport::{MsgContent as MsgContentT, PeerId};

use crate::collector::LogsCollector;
use crate::storage::LogsStorage;
use crate::LOGS_TOPIC;

type MsgContent = Box<[u8]>;
type Message = subsquid_network_transport::Message<MsgContent>;

pub struct Server<T: LogsStorage + Send + Sync + 'static> {
    incoming_messages: Receiver<Message>,
    transport_handle: P2PTransportHandle<MsgContent>,
    logs_collector: Arc<RwLock<LogsCollector<T>>>,
    registered_workers: Arc<RwLock<HashSet<PeerId>>>,
    task_manager: TaskManager,
}

impl<T: LogsStorage + Send + Sync + 'static> Server<T> {
    pub fn new(
        incoming_messages: Receiver<Message>,
        transport_handle: P2PTransportHandle<MsgContent>,
        logs_collector: LogsCollector<T>,
    ) -> Self {
        let logs_collector = Arc::new(RwLock::new(logs_collector));
        Self {
            incoming_messages,
            transport_handle,
            logs_collector,
            registered_workers: Default::default(),
            task_manager: Default::default(),
        }
    }

    pub async fn run(
        mut self,
        contract_client: Box<dyn ContractClient>,
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
                Some(msg) = self.incoming_messages.recv() => self.handle_message(msg).await,
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
                else => break
            }
        }
        log::info!("Server shutting down");
        self.task_manager.await_stop().await;
        Ok(())
    }

    async fn handle_message(&mut self, msg: Message) {
        let peer_id = match msg.peer_id {
            Some(peer_id) => peer_id,
            None => return log::warn!("Dropping anonymous message"),
        };
        let envelope = match Envelope::decode(msg.content.as_slice()) {
            Ok(envelope) => envelope,
            Err(e) => return log::warn!("Error decoding message: {e:?}"),
        };
        match envelope.msg {
            Some(Msg::QueryLogs(query_logs)) => self.collect_logs(peer_id, query_logs).await,
            _ => log::debug!("Unexpected msg received: {envelope:?}"),
        }
    }

    async fn collect_logs(
        &self,
        worker_id: PeerId,
        QueryLogs {
            mut queries_executed,
        }: QueryLogs,
    ) {
        if !self.registered_workers.read().await.contains(&worker_id) {
            log::warn!("Worker not registered: {worker_id:?}");
            return;
        }
        queries_executed = queries_executed
            .into_iter()
            .filter_map(|mut log| {
                if log.verify_signature(&worker_id) {
                    Some(log)
                } else {
                    log::error!("Invalid log signature worker_id = {worker_id}");
                    None
                }
            })
            .collect();
        self.logs_collector
            .write()
            .await
            .collect_logs(worker_id, queries_executed);
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

                let msg = Msg::LogsCollected(LogsCollected { sequence_numbers });
                let envelope = Envelope { msg: Some(msg) };
                let msg_content = envelope.encode_to_vec().into();
                let _ = transport_handle
                    .broadcast_msg(msg_content, LOGS_TOPIC)
                    .await
                    .map_err(|e| log::error!("Error sending message: {e:?}"));
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }

    fn spawn_worker_update_task(
        &mut self,
        contract_client: Box<dyn ContractClient>,
        interval: Duration,
    ) {
        log::info!("Starting worker update task");
        let registered_workers = self.registered_workers.clone();
        let contract_client: Arc<dyn ContractClient> = contract_client.into();
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
