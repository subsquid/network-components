use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};

use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use contract_client::Client as ContractClient;
use subsquid_messages::envelope::Msg;
use subsquid_messages::signatures::SignedMessage;
use subsquid_messages::{Envelope, LogsCollected, ProstMsg, QueryLogs};
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
    child_tasks: Vec<JoinHandle<()>>,
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
            child_tasks: vec![],
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

        self.spawn_child_tasks(contract_client, store_logs_interval, worker_update_interval);
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
        self.stop_child_tasks().await?;
        self.transport_handle.stop().await?;
        Ok(())
    }

    fn spawn_child_tasks(
        &mut self,
        contract_client: Box<dyn ContractClient>,
        store_logs_interval: Duration,
        worker_update_interval: Duration,
    ) {
        self.child_tasks = vec![
            self.spawn_saving_task(store_logs_interval),
            self.spawn_worker_update_task(contract_client, worker_update_interval),
        ]
    }
    async fn stop_child_tasks(&mut self) -> anyhow::Result<()> {
        for task in self.child_tasks.iter() {
            task.abort();
        }
        let join_results = tokio::time::timeout(
            Duration::from_secs(1),
            futures::future::join_all(std::mem::take(&mut self.child_tasks)),
        )
        .await?;
        for res in join_results {
            if let Err(e) = res {
                if !e.is_cancelled() {
                    log::error!("Error joining task: {e:?}");
                }
            }
        }
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

    fn spawn_saving_task(&self, interval: Duration) -> JoinHandle<()> {
        let collector = self.logs_collector.clone();
        let transport_handle = self.transport_handle.clone();

        tokio::spawn(async move {
            log::info!("Starting logs saving task");
            loop {
                tokio::time::sleep(interval).await;
                let sequence_numbers = match collector.write().await.storage_sync().await {
                    Err(e) => {
                        log::error!("Error saving logs to storage: {e:?}");
                        continue;
                    }
                    Ok(seq_nums) => seq_nums,
                };

                let msg = Msg::LogsCollected(LogsCollected { sequence_numbers });
                let envelope = Envelope { msg: Some(msg) };
                let msg_content = envelope.encode_to_vec().into();
                if let Err(e) = transport_handle
                    .broadcast_msg(msg_content, LOGS_TOPIC)
                    .await
                {
                    log::error!("Error sending message: {e:?}");
                }
            }
        })
    }

    fn spawn_worker_update_task(
        &self,
        contract_client: Box<dyn ContractClient>,
        interval: Duration,
    ) -> JoinHandle<()> {
        let registered_workers = self.registered_workers.clone();
        tokio::spawn(async move {
            log::info!("Starting worker update task");
            loop {
                tokio::time::sleep(interval).await;
                match contract_client.active_workers().await {
                    Ok(workers) => {
                        *registered_workers.write().await = workers
                            .into_iter()
                            .map(|w| w.peer_id)
                            .collect::<HashSet<PeerId>>();
                    }
                    Err(e) => log::error!("Error getting registered workers: {e:?}"),
                };
            }
        })
    }
}
