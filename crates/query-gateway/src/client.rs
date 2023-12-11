use std::cmp::max;
use std::collections::hash_map::{Entry, OccupiedEntry};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use derivative::Derivative;
use rand::prelude::IteratorRandom;
use tabled::settings::Style;
use tabled::{Table, Tabled};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;

use contract_client::Worker;
use subsquid_messages::signatures::SignedMessage;
use subsquid_messages::{
    envelope::Msg, query_finished, query_result, Envelope, OkResult, PingV1, PingV2, ProstMsg,
    Query as QueryMsg, QueryFinished, QueryResult as QueryResultMsg, QuerySubmitted, RangeSet,
    SizeAndHash,
};
use subsquid_network_transport::{Keypair, MsgContent, PeerId};

use crate::config::{Config, DatasetId};
use crate::PING_TOPIC;

type Message = subsquid_network_transport::Message<Box<[u8]>>;

const WORKER_INACTIVE_THRESHOLD: Duration = Duration::from_secs(120);
const WORKER_GREYLIST_TIME: Duration = Duration::from_secs(600);
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(60);
const SUMMARY_PRINT_INTERVAL: Duration = Duration::from_secs(30);
const WORKERS_UPDATE_INTERVAL: Duration = Duration::from_secs(180);

#[derive(Derivative, Debug)]
struct Query {
    pub dataset_id: DatasetId,
    pub query: String,
    pub worker_id: PeerId,
    pub timeout: Duration,
    pub profiling: bool,
    #[derivative(Debug = "ignore")]
    pub result_sender: oneshot::Sender<QueryResult>,
}

#[derive(Debug)]
struct Task {
    worker_id: PeerId,
    result_sender: oneshot::Sender<QueryResult>,
    timeout_handle: JoinHandle<()>,
    start_time: Instant,
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    Ok(OkResult),
    BadRequest(String),
    ServerError(String),
    Timeout,
}

impl From<query_result::Result> for QueryResult {
    fn from(result: query_result::Result) -> Self {
        match result {
            query_result::Result::Ok(ok) => Self::Ok(ok),
            query_result::Result::BadRequest(err) => Self::BadRequest(err),
            query_result::Result::ServerError(err) => Self::ServerError(err),
        }
    }
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

#[derive(Default)]
struct DatasetState {
    worker_ranges: HashMap<PeerId, RangeSet>,
    height: u32,
}

impl DatasetState {
    pub fn get_workers_with_block(&self, block: u32) -> impl Iterator<Item = PeerId> + '_ {
        self.worker_ranges
            .iter()
            .filter_map(move |(peer_id, range_set)| range_set.has(block).then_some(*peer_id))
    }

    pub fn update(&mut self, peer_id: PeerId, state: RangeSet) {
        if let Some(range) = state.ranges.last() {
            self.height = max(self.height, range.end)
        }
        self.worker_ranges.insert(peer_id, state);
    }

    pub fn highest_indexable_block(&self) -> u32 {
        let range_set: RangeSet = self
            .worker_ranges
            .values()
            .cloned()
            .flat_map(|r| r.ranges)
            .into();
        match range_set.ranges.get(0) {
            Some(range) if range.begin == 0 => range.end,
            _ => 0,
        }
    }
}

#[derive(Tabled)]
struct DatasetSummary<'a> {
    #[tabled(rename = "dataset")]
    name: &'a String,
    #[tabled(rename = "highest indexable block")]
    highest_indexable_block: u32,
    #[tabled(rename = "highest seen block")]
    highest_seen_block: u32,
}

impl<'a> DatasetSummary<'a> {
    pub fn new(name: &'a String, state: Option<&DatasetState>) -> Self {
        match state {
            None => Self {
                name,
                highest_indexable_block: 0,
                highest_seen_block: 0,
            },
            Some(state) => Self {
                name,
                highest_indexable_block: state.highest_indexable_block(),
                highest_seen_block: state.height,
            },
        }
    }
}

#[derive(Default)]
struct NetworkState {
    dataset_states: HashMap<DatasetId, DatasetState>,
    last_pings: HashMap<PeerId, Instant>,
    worker_greylist: HashMap<PeerId, Instant>,
    registered_workers: HashSet<PeerId>,
    available_datasets: HashMap<String, DatasetId>,
}

impl NetworkState {
    pub fn new(available_datasets: HashMap<String, DatasetId>) -> Self {
        Self {
            available_datasets,
            ..Default::default()
        }
    }

    pub fn get_dataset_id(&self, dataset: &str) -> Option<DatasetId> {
        self.available_datasets.get(dataset).cloned()
    }

    pub fn find_worker(&self, dataset_id: &DatasetId, start_block: u32) -> Option<PeerId> {
        log::debug!("Looking for worker dataset_id={dataset_id}, start_block={start_block}");
        let dataset_state = match self.dataset_states.get(dataset_id) {
            None => return None,
            Some(state) => state,
        };

        // Choose a random active worker having the requested start_block
        dataset_state
            .get_workers_with_block(start_block)
            .filter(|peer_id| self.worker_is_active(peer_id))
            .choose(&mut rand::thread_rng())
    }

    fn worker_is_active(&self, worker_id: &PeerId) -> bool {
        // Check if worker is registered on chain
        if !self.registered_workers.contains(worker_id) {
            return false;
        }

        let now = Instant::now();

        // Check if the last ping wasn't too long ago
        match self.last_pings.get(worker_id) {
            None => return false,
            Some(ping) if (*ping + WORKER_INACTIVE_THRESHOLD) < now => return false,
            _ => (),
        };

        // Check if the worker is (still) greylisted
        !matches!(
            self.worker_greylist.get(worker_id),
            Some(instant) if (*instant + WORKER_GREYLIST_TIME) > now
        )
    }

    fn update_dataset_states(
        &mut self,
        worker_id: PeerId,
        mut worker_state: HashMap<DatasetId, RangeSet>,
    ) {
        self.last_pings.insert(worker_id, Instant::now());
        for dataset_id in self.available_datasets.values() {
            let dataset_state = worker_state
                .remove(dataset_id)
                .unwrap_or_else(RangeSet::empty);
            self.dataset_states
                .entry(dataset_id.clone())
                .or_default()
                .update(worker_id, dataset_state);
        }
    }

    fn update_registered_workers(&mut self, workers: Vec<Worker>) {
        log::debug!("Updating registered workers: {workers:?}");
        self.registered_workers = workers.into_iter().map(|w| w.peer_id).collect();
    }

    pub fn greylist_worker(&mut self, worker_id: PeerId) {
        log::info!("Grey-listing worker {worker_id}");
        self.worker_greylist.insert(worker_id, Instant::now());
    }

    pub fn get_height(&self, dataset_id: &DatasetId) -> Option<u32> {
        self.dataset_states
            .get(dataset_id)
            .map(|state| state.height)
    }

    pub fn summary(&self) -> impl Iterator<Item = DatasetSummary> {
        self.available_datasets
            .iter()
            .map(|(name, id)| DatasetSummary::new(name, self.dataset_states.get(id)))
    }
}

struct QueryHandler {
    msg_receiver: mpsc::Receiver<Message>,
    msg_sender: mpsc::Sender<Message>,
    query_receiver: mpsc::Receiver<Query>,
    timeout_sender: mpsc::Sender<String>,
    timeout_receiver: mpsc::Receiver<String>,
    tasks: HashMap<String, Task>,
    network_state: Arc<RwLock<NetworkState>>,
    keypair: Keypair,
    scheduler_id: PeerId,
    send_metrics: bool,
}

impl QueryHandler {
    async fn run(mut self, contract_client: Box<dyn contract_client::Client>) {
        let summary_task = self.spawn_summary_task();
        let workers_update_task = self.spawn_workers_update_task(contract_client);
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
        workers_update_task.abort();
    }

    fn spawn_summary_task(&self) -> JoinHandle<()> {
        let network_state = self.network_state.clone();
        tokio::task::spawn(async move {
            log::info!("Starting datasets summary task");
            loop {
                tokio::time::sleep(SUMMARY_PRINT_INTERVAL).await;
                let mut summary = Table::new(network_state.read().await.summary());
                summary.with(Style::sharp());
                log::info!("Datasets summary:\n{summary}");
            }
        })
    }

    fn spawn_workers_update_task(
        &self,
        contract_client: Box<dyn contract_client::Client>,
    ) -> JoinHandle<()> {
        let network_state = self.network_state.clone();
        tokio::task::spawn(async move {
            log::info!("Starting workers update task");
            loop {
                let workers = match contract_client.active_workers().await {
                    Ok(workers) => workers,
                    Err(e) => {
                        log::error!("Error getting registered workers: {e:?}");
                        continue;
                    }
                };
                network_state
                    .write()
                    .await
                    .update_registered_workers(workers);
                tokio::time::sleep(WORKERS_UPDATE_INTERVAL).await;
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
        let msg = Message {
            peer_id: Some(peer_id),
            topic: None,
            content: envelope.encode_to_vec().into(),
        };
        self.msg_sender.send(msg).await?;
        Ok(())
    }

    async fn send_metrics(&mut self, msg: Msg) {
        let _ = self
            .send_msg(self.scheduler_id, msg)
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
        worker_msg.sing(&self.keypair)?;
        self.send_msg(worker_id, Msg::Query(worker_msg)).await?;

        if self.send_metrics {
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

        if self.send_metrics {
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
            Some(Msg::PingV1(ping)) if topic.as_ref().is_some_and(|t| t == PING_TOPIC) => {
                self.ping_v1(peer_id, ping).await
            }
            Some(Msg::PingV2(ping)) if topic.as_ref().is_some_and(|t| t == PING_TOPIC) => {
                self.ping_v2(peer_id, ping).await
            }
            _ => log::warn!("Unexpected message received: {msg:?}"),
        }
        Ok(())
    }

    async fn ping_v1(&mut self, peer_id: PeerId, ping: PingV1) {
        log::debug!("Got ping from {peer_id}");
        log::trace!("Ping from {peer_id}: {ping:?}");
        let worker_state = ping
            .state
            .map(|s| s.datasets)
            .unwrap_or_default()
            .into_iter()
            .map(|(url, ranges)| (DatasetId::from_url(url), ranges))
            .collect();
        self.network_state
            .write()
            .await
            .update_dataset_states(peer_id, worker_state);
    }

    async fn ping_v2(&mut self, peer_id: PeerId, ping: PingV2) {
        log::debug!("Got ping from {peer_id}");
        log::trace!("Ping from {peer_id}: {ping:?}");
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

        if self.send_metrics {
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

pub struct QueryClient {
    network_state: Arc<RwLock<NetworkState>>,
    query_sender: mpsc::Sender<Query>,
}

impl QueryClient {
    pub async fn get_dataset_id(&self, dataset: &str) -> Option<DatasetId> {
        self.network_state.read().await.get_dataset_id(dataset)
    }

    pub async fn get_height(&self, dataset_id: &DatasetId) -> Option<u32> {
        self.network_state.read().await.get_height(dataset_id)
    }

    pub async fn find_worker(&self, dataset_id: &DatasetId, start_block: u32) -> Option<PeerId> {
        self.network_state
            .read()
            .await
            .find_worker(dataset_id, start_block)
    }

    pub async fn execute_query(
        &self,
        dataset_id: DatasetId,
        query: String,
        worker_id: PeerId,
        timeout: Option<impl Into<Duration>>,
        profiling: bool,
    ) -> anyhow::Result<QueryResult> {
        let timeout = timeout.map(Into::into).unwrap_or(DEFAULT_QUERY_TIMEOUT);
        let (result_sender, result_receiver) = oneshot::channel();
        let query = Query {
            dataset_id,
            query,
            worker_id,
            timeout,
            profiling,
            result_sender,
        };
        self.query_sender
            .send(query)
            .await
            .map_err(|_| anyhow::anyhow!("Query handler closed"))?;
        result_receiver
            .await
            .map_err(|_| anyhow::anyhow!("Query dropped"))
    }
}

pub async fn get_client(
    config: Config,
    keypair: Keypair,
    msg_receiver: mpsc::Receiver<Message>,
    msg_sender: mpsc::Sender<Message>,
    contact_client: Box<dyn contract_client::Client>,
) -> anyhow::Result<QueryClient> {
    let (query_sender, query_receiver) = mpsc::channel(100);
    let (timeout_sender, timeout_receiver) = mpsc::channel(100);
    let network_state = Arc::new(RwLock::new(NetworkState::new(config.available_datasets)));

    let handler = QueryHandler {
        msg_receiver,
        msg_sender,
        query_receiver,
        timeout_sender,
        timeout_receiver,
        tasks: Default::default(),
        network_state: network_state.clone(),
        keypair,
        scheduler_id: config.scheduler_id.0,
        send_metrics: config.send_metrics,
    };
    tokio::spawn(handler.run(contact_client));

    let client = QueryClient {
        network_state,
        query_sender,
    };
    Ok(client)
}
