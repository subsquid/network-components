use std::collections::hash_map::{Entry, OccupiedEntry};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tabled::settings::Style;
use tabled::Table;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;

use subsquid_messages::signatures::SignedMessage;
use subsquid_messages::{
    envelope::Msg, query_finished, query_result, Envelope, PingV2 as Ping, ProstMsg,
    Query as QueryMsg, QueryFinished, QueryResult as QueryResultMsg, QuerySubmitted, SizeAndHash,
};
use subsquid_network_transport::transport::P2PTransportHandle;
use subsquid_network_transport::{Keypair, MsgContent as MsgContentT, PeerId};

use crate::allocations::AllocationsManager;
use crate::config::{Config, DatasetId};
use crate::network_state::NetworkState;
use crate::query::{Query, QueryResult};
use crate::PING_TOPIC;

pub type MsgContent = Box<[u8]>;
pub type Message = subsquid_network_transport::Message<MsgContent>;

const COMP_UNITS_PER_QUERY: u32 = 1;

pub const SUPPORTED_WORKER_VERSIONS: [&str; 1] = ["0.2.1"];

#[derive(Debug)]
struct Task {
    worker_id: PeerId,
    result_sender: oneshot::Sender<QueryResult>,
    timeout_handle: JoinHandle<()>,
    start_time: Instant,
}

impl Task {
    pub fn new(
        worker_id: PeerId,
        result_sender: oneshot::Sender<QueryResult>,
        timeout_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            worker_id,
            result_sender,
            timeout_handle,
            start_time: Instant::now(),
        }
    }

    pub fn exec_time_ms(&self) -> u32 {
        self.start_time
            .elapsed()
            .as_millis()
            .try_into()
            .expect("Tasks do not take that long")
    }

    pub fn timeout(self) {
        let _ = self.result_sender.send(QueryResult::Timeout);
    }

    pub fn result_received(self, result: query_result::Result) {
        self.timeout_handle.abort();
        let _ = self.result_sender.send(result.into());
    }
}

pub struct Server {
    msg_receiver: mpsc::Receiver<Message>,
    transport_handle: P2PTransportHandle<MsgContent>,
    query_receiver: mpsc::Receiver<Query>,
    timeout_sender: mpsc::Sender<String>,
    timeout_receiver: mpsc::Receiver<String>,
    tasks: HashMap<String, Task>,
    network_state: Arc<RwLock<NetworkState>>,
    allocations_manager: Arc<RwLock<AllocationsManager>>,
    keypair: Keypair,
}

impl Server {
    pub fn new(
        msg_receiver: mpsc::Receiver<Message>,
        transport_handle: P2PTransportHandle<MsgContent>,
        query_receiver: mpsc::Receiver<Query>,
        network_state: Arc<RwLock<NetworkState>>,
        allocations_manager: Arc<RwLock<AllocationsManager>>,
        keypair: Keypair,
    ) -> Self {
        let (timeout_sender, timeout_receiver) = mpsc::channel(100);
        Self {
            msg_receiver,
            transport_handle,
            query_receiver,
            timeout_sender,
            timeout_receiver,
            tasks: Default::default(),
            network_state,
            allocations_manager,
            keypair,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let summary_task = self.spawn_summary_task();
        loop {
            let _ = tokio::select! {
                Some(query) = self.query_receiver.recv() => self.handle_query(query)
                    .await
                    .map_err(|e| log::error!("Error handling query: {e:?}")),
                Some(query_id) = self.timeout_receiver.recv() => self.handle_timeout(query_id)
                    .await
                    .map_err(|e| log::error!("Error handling query timeout: {e:?}")),
                Some(msg) = self.msg_receiver.recv() => self.handle_message(msg)
                    .await
                    .map_err(|e| log::error!("Error handling incoming message: {e:?}")),
                else => break
            };
        }
        summary_task.abort();
        Ok(())
    }

    fn spawn_summary_task(&self) -> JoinHandle<()> {
        let summary_print_interval = Config::get().summary_print_interval;
        let network_state = self.network_state.clone();
        tokio::task::spawn(async move {
            log::info!("Starting datasets summary task");
            loop {
                tokio::time::sleep(summary_print_interval).await;
                let mut summary = Table::new(network_state.read().await.summary());
                summary.with(Style::sharp());
                log::info!("Datasets summary:\n{summary}");
            }
        })
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
            .send_direct_msg(msg_content, peer_id)
            .await?;
        Ok(())
    }

    async fn send_metrics(&mut self, msg: Msg) {
        let _ = self
            .send_msg(Config::get().scheduler_id, msg)
            .await
            .map_err(|e| log::error!("Failed to send metrics: {e:?}"));
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

        let enough_cus = self
            .allocations_manager
            .read()
            .await
            .try_spend_cus(worker_id, COMP_UNITS_PER_QUERY)
            .await?;
        if !enough_cus {
            log::warn!("Not enough compute units for worker {worker_id}");
            let _ = result_sender.send(QueryResult::NotEnoughCUs);
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

    fn spawn_timeout_task(&self, query_id: &str, timeout: Duration) -> JoinHandle<()> {
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
        let (query_id, task) = self.get_task(query_id)?.remove_entry();

        self.network_state
            .write()
            .await
            .greylist_worker(task.worker_id);

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

        task.timeout();
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
            Some(Msg::PingV2(ping)) if topic.as_ref().is_some_and(|t| t == PING_TOPIC) => {
                self.ping(peer_id, ping).await
            }
            _ => log::debug!("Unexpected message received: {msg:?}"),
        }
        Ok(())
    }

    async fn ping(&mut self, peer_id: PeerId, ping: Ping) {
        log::debug!("Got ping from {peer_id}");
        log::trace!("Ping from {peer_id}: {ping:?}");

        let version = ping.version.clone().unwrap_or_default();
        if !SUPPORTED_WORKER_VERSIONS.iter().any(|v| *v == version) {
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
        anyhow::ensure!(
            peer_id == task_entry.get().worker_id,
            "Invalid message sender"
        );
        let (query_id, task) = task_entry.remove_entry();

        // Greylist worker if server error occurred during query execution
        if let query_result::Result::ServerError(ref e) = result {
            log::warn!("Server error returned for query {query_id}: {e}");
            self.network_state
                .write()
                .await
                .greylist_worker(task.worker_id);
        }

        if Config::get().send_metrics {
            let metrics_msg = Msg::QueryFinished(QueryFinished {
                client_id: self.client_id(),
                worker_id: peer_id.to_string(),
                query_id,
                exec_time_ms: task.exec_time_ms(),
                result: Some((&result).into()),
            });
            self.send_metrics(metrics_msg).await;
        }

        task.result_received(result);
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