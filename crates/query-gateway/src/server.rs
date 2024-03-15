use futures::{Stream, StreamExt};
use std::collections::hash_map::{Entry, OccupiedEntry};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use lazy_static::lazy_static;
use semver::VersionReq;
use tabled::settings::Style;
use tabled::Table;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

use subsquid_messages::signatures::SignedMessage;
use subsquid_messages::{
    envelope::Msg, query_finished, query_result, Envelope, Ping, ProstMsg, Query as QueryMsg,
    QueryFinished, QueryResult as QueryResultMsg, QuerySubmitted, SizeAndHash,
};
use subsquid_network_transport::task_manager::{CancellationToken, TaskManager};
use subsquid_network_transport::transport::P2PTransportHandle;
use subsquid_network_transport::{Keypair, MsgContent as MsgContentT, PeerId};

use crate::allocations::AllocationsManager;
use crate::config::{Config, DatasetId};
use crate::network_state::NetworkState;
use crate::query::{Query, QueryResult};
use crate::task::Task;
use crate::PING_TOPIC;

pub type MsgContent = Box<[u8]>;
pub type Message = subsquid_network_transport::Message<MsgContent>;

const COMP_UNITS_PER_QUERY: u32 = 1;

lazy_static! {
    pub static ref SUPPORTED_WORKER_VERSIONS: VersionReq = ">=0.2.2".parse().unwrap();
}

pub struct Server<S: Stream<Item = Message> + Send + Unpin + 'static> {
    incoming_messages: S,
    transport_handle: P2PTransportHandle<MsgContent>,
    query_receiver: mpsc::Receiver<Query>,
    timeout_sender: mpsc::Sender<String>,
    timeout_receiver: mpsc::Receiver<String>,
    tasks: HashMap<String, Task>,
    network_state: Arc<RwLock<NetworkState>>,
    allocations_manager: Arc<RwLock<AllocationsManager>>,
    keypair: Keypair,
    task_manager: TaskManager,
}

impl<S: Stream<Item = Message> + Send + Unpin + 'static> Server<S> {
    pub fn new(
        incoming_messages: S,
        transport_handle: P2PTransportHandle<MsgContent>,
        query_receiver: mpsc::Receiver<Query>,
        network_state: Arc<RwLock<NetworkState>>,
        allocations_manager: Arc<RwLock<AllocationsManager>>,
        keypair: Keypair,
    ) -> Self {
        let (timeout_sender, timeout_receiver) = mpsc::channel(100);
        Self {
            incoming_messages,
            transport_handle,
            query_receiver,
            timeout_sender,
            timeout_receiver,
            tasks: Default::default(),
            network_state,
            allocations_manager,
            keypair,
            task_manager: Default::default(),
        }
    }

    pub async fn run(mut self, cancel_token: CancellationToken) {
        log::info!("Starting query server");
        let summary_print_interval = Config::get().summary_print_interval;
        if !summary_print_interval.is_zero() {
            self.spawn_summary_task(summary_print_interval);
        }
        self.spawn_worker_monitoring_task(Config::get().worker_monitoring_interval);
        loop {
            tokio::select! {
                Some(query) = self.query_receiver.recv() => self.handle_query(query)
                    .await
                    .unwrap_or_else(|e| log::error!("Error handling query: {e:?}")),
                Some(query_id) = self.timeout_receiver.recv() => self.handle_timeout(query_id)
                    .await
                    .unwrap_or_else(|e| log::error!("Error handling query timeout: {e:?}")),
                Some(msg) = self.incoming_messages.next() => self.handle_message(msg)
                    .await
                    .unwrap_or_else(|e| log::error!("Error handling incoming message: {e:?}")),
                _ = cancel_token.cancelled() => break,
                else => break,
            }
        }
        log::info!("Shutting down query server");
        self.task_manager.await_stop().await;
    }

    fn spawn_summary_task(&mut self, interval: Duration) {
        log::info!("Starting datasets summary task");
        let network_state = self.network_state.clone();
        let task = move |_| {
            let network_state = network_state.clone();
            async move {
                let mut summary = Table::new(network_state.read().await.summary());
                summary.with(Style::sharp());
                log::info!("Datasets summary:\n{summary}");
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }

    fn spawn_worker_monitoring_task(&mut self, interval: Duration) {
        log::info!("Starting worker monitoring task");
        let network_state = self.network_state.clone();
        let transport_handle = self.transport_handle.clone();

        let task = move |cancel_token: CancellationToken| {
            let network_state = network_state.clone();
            let transport_handle = transport_handle.clone();
            async move {
                let workers = network_state.read().await.registered_workers();
                log::info!("Dialing {} workers", workers.len());

                // Dialing all workers concurrently
                let futures = workers
                    .iter()
                    .map(|worker_id| transport_handle.dial_peer(*worker_id));
                // Allow to cancel dialing via token because it can take long to complete
                let results = tokio::select! {
                    results = futures::future::join_all(futures) => results,
                    _ = cancel_token.cancelled() => return
                };
                let mut network_state = network_state.write().await;
                for (worker_id, dial_result) in workers.into_iter().zip(results) {
                    match dial_result {
                        Ok(reachable) => network_state.worker_dialed(worker_id, reachable),
                        Err(e) => log::error!("Error dialing worker: {e:?}"),
                    }
                }
                log::info!("Dialing workers complete");
            }
        };

        // Check the workers right away so the server is ready
        self.task_manager.spawn(task.clone());
        self.task_manager.spawn_periodic(task, interval);
    }

    fn generate_query_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn client_id(&self) -> String {
        PeerId::from(self.keypair.public()).to_string()
    }

    async fn send_msg(&mut self, peer_id: PeerId, msg: Msg) -> anyhow::Result<()> {
        let envelope = Envelope { msg: Some(msg) };
        let msg_content = envelope.encode_to_vec().into();
        self.transport_handle
            .send_direct_msg(msg_content, peer_id)?;
        Ok(())
    }

    async fn send_metrics(&mut self, msg: Msg) {
        self.send_msg(Config::get().scheduler_id, msg)
            .await
            .unwrap_or_else(|e| log::error!("Failed to send metrics: {e:?}"));
    }

    async fn handle_query(&mut self, query: Query) -> anyhow::Result<()> {
        log::debug!("Starting query {query:?}");
        let query_id = Self::generate_query_id();
        let Query {
            dataset_id,
            query,
            worker_id,
            timeout,
            profiling,
            result_sender,
        } = query;
        let dataset = dataset_id.0;

        // Check network_state's cache for allocations first, before DB
        if !self
            .network_state
            .read()
            .await
            .worker_has_allocation(&worker_id)
        {
            log::warn!("Not enough compute units for worker {worker_id}");
            let _ = result_sender.send(QueryResult::NoAllocation);
            return Ok(());
        }

        let enough_cus = self
            .allocations_manager
            .read()
            .await
            .try_spend_cus(worker_id, COMP_UNITS_PER_QUERY)
            .await?;
        if !enough_cus {
            log::warn!("Not enough compute units for worker {worker_id}");
            let _ = result_sender.send(QueryResult::NoAllocation);
            self.network_state
                .write()
                .await
                .no_allocation_for_worker(worker_id); // Save to cache
            return Ok(());
        }

        let timeout_handle = self.spawn_timeout_task(&query_id, timeout);
        let task = Task::new(worker_id, result_sender, timeout_handle);
        self.tasks.insert(query_id.clone(), task);

        let mut worker_msg = QueryMsg {
            query_id: Some(query_id.clone()),
            dataset: Some(dataset.clone()),
            query: Some(query.clone()),
            profiling: Some(profiling),
            client_state_json: Some("{}".to_string()), // This is a placeholder field
            signature: vec![],
        };
        worker_msg.sign(&self.keypair)?;
        self.send_msg(worker_id, Msg::Query(worker_msg)).await?;

        if Config::get().send_metrics {
            let query_hash = SizeAndHash::compute(&query).sha3_256;
            let metrics_msg = Msg::QuerySubmitted(QuerySubmitted {
                client_id: self.client_id(),
                worker_id: worker_id.to_string(),
                query_id,
                dataset,
                query,
                query_hash,
            });
            self.send_metrics(metrics_msg).await;
        }

        Ok(())
    }

    fn spawn_timeout_task(&mut self, query_id: &str, timeout: Duration) -> JoinHandle<()> {
        let query_id = query_id.to_string();
        let timeout_sender = self.timeout_sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            if timeout_sender.send(query_id).await.is_err() {
                log::error!("Error sending query timeout")
            }
        })
    }

    async fn handle_timeout(&mut self, query_id: String) -> anyhow::Result<()> {
        log::debug!("Query {query_id} execution timed out");
        let (query_id, mut task) = self.get_task(query_id)?.remove_entry();

        self.network_state
            .write()
            .await
            .greylist_worker(task.worker_id());

        let task = task.timeout();
        if Config::get().send_metrics {
            let metrics_msg = Msg::QueryFinished(QueryFinished {
                client_id: self.client_id(),
                worker_id: task.worker_id.to_string(),
                query_id,
                exec_time_ms: task.exec_time_ms(),
                result: Some(query_finished::Result::Timeout(())),
            });
            self.send_metrics(metrics_msg).await;
        }
        Ok(())
    }

    async fn handle_message(&mut self, msg: Message) -> anyhow::Result<()> {
        let Message {
            peer_id,
            topic,
            content,
        } = msg;
        let peer_id = peer_id.ok_or_else(|| anyhow::anyhow!("Message sender ID missing"))?;
        let Envelope { msg } = Envelope::decode(content.as_slice())?;
        match msg {
            Some(Msg::QueryResult(result)) => self.query_result(peer_id, result).await?,
            Some(Msg::Ping(ping)) if topic.as_ref().is_some_and(|t| t == PING_TOPIC) => {
                self.ping(peer_id, ping).await
            }
            _ => log::debug!("Unexpected message received: {msg:?}"),
        }
        Ok(())
    }

    async fn ping(&mut self, peer_id: PeerId, ping: Ping) {
        log::trace!("Ping from {peer_id}: {ping:?}");

        let version = ping.sem_version();
        if !SUPPORTED_WORKER_VERSIONS.matches(&version) {
            return log::debug!("Worker {peer_id} version not supported: {}", version);
        }

        let worker_state = ping
            .stored_ranges
            .into_iter()
            .map(|r| (DatasetId::from_url(r.url), r.ranges.into()))
            .collect();
        self.network_state
            .write()
            .await
            .update_dataset_states(peer_id, worker_state);
    }
    async fn query_result(
        &mut self,
        peer_id: PeerId,
        result: QueryResultMsg,
    ) -> anyhow::Result<()> {
        let QueryResultMsg { query_id, result } = result;
        let result = result.ok_or_else(|| anyhow::anyhow!("Result missing"))?;
        log::debug!("Got result for query {query_id}");

        let task_entry = self.get_task(query_id)?;
        let worker_id = task_entry.get().worker_id();
        anyhow::ensure!(peer_id == worker_id, "Invalid message sender");
        let (query_id, mut task) = task_entry.remove_entry();

        let task = task.result_received(result.clone());

        match &result {
            // Greylist worker if server error occurred during query execution
            query_result::Result::ServerError(e) => {
                log::warn!("Server error returned for query {query_id}: {e}");
                self.network_state.write().await.greylist_worker(worker_id);
            }
            // Add worker to the missing allocations cache
            query_result::Result::NoAllocation(()) => {
                self.network_state
                    .write()
                    .await
                    .no_allocation_for_worker(worker_id);
            }
            _ => {}
        }

        if Config::get().send_metrics {
            // This computes hash, which could take some time, hence spawn_blocking is used here
            let result = tokio::task::spawn_blocking(move || Some((&result).into())).await?;
            let metrics_msg = Msg::QueryFinished(QueryFinished {
                client_id: self.client_id(),
                worker_id: peer_id.to_string(),
                query_id,
                exec_time_ms: task.exec_time_ms(),
                result,
            });
            self.send_metrics(metrics_msg).await;
        }

        Ok(())
    }

    #[inline(always)]
    fn get_task(&mut self, query_id: String) -> anyhow::Result<OccupiedEntry<String, Task>> {
        match self.tasks.entry(query_id) {
            Entry::Occupied(entry) => Ok(entry),
            Entry::Vacant(entry) => anyhow::bail!("Unknown query: {}", entry.key()),
        }
    }
}
