use std::cmp::max;
use std::collections::hash_map::{Entry, OccupiedEntry};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use derivative::Derivative;
use prost::Message as ProstMsg;
use rand::prelude::IteratorRandom;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;

use contract_client::Worker;
use router_controller::messages::{
    envelope::Msg, query_finished, query_result, Envelope, OkResult, Query as QueryMsg,
    QueryFinished, QueryResult as QueryResultMsg, QuerySubmitted, RangeSet,
};
use subsquid_network_transport::{MsgContent, PeerId};

use crate::config::{Config, DatasetId};

type Message = subsquid_network_transport::Message<Box<[u8]>>;

const WORKER_INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);
const WORKER_GREYLIST_TIME: Duration = Duration::from_secs(600);
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(60);

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

    fn update_dataset_state(&mut self, peer_id: PeerId, dataset_id: DatasetId, state: RangeSet) {
        self.last_pings.insert(peer_id, Instant::now());
        self.dataset_states
            .entry(dataset_id)
            .or_default()
            .update(peer_id, state);
    }

    fn update_registered_workers(&mut self, workers: Vec<Worker>) {
        self.registered_workers = workers.into_iter().map(|w| w.peer_id).collect();
    }

    pub fn greylist_worker(&mut self, worker_id: PeerId) {
        self.worker_greylist.insert(worker_id, Instant::now());
    }

    pub fn get_height(&self, dataset_id: &DatasetId) -> Option<u32> {
        self.dataset_states
            .get(dataset_id)
            .map(|state| state.height)
    }
}

struct QueryHandler {
    msg_receiver: mpsc::Receiver<Message>,
    msg_sender: mpsc::Sender<Message>,
    query_receiver: mpsc::Receiver<Query>,
    timeout_sender: mpsc::Sender<String>,
    timeout_receiver: mpsc::Receiver<String>,
    worker_updates: mpsc::Receiver<Vec<Worker>>,
    tasks: HashMap<String, Task>,
    network_state: Arc<RwLock<NetworkState>>,
    router_id: PeerId,
    send_metrics: bool,
}

impl QueryHandler {
    async fn run(mut self) {
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
                Some(workers) = self.worker_updates.recv() => self.handle_worker_update(workers).await,
                else => break
            };
        }
    }

    fn generate_query_id() -> String {
        uuid::Uuid::new_v4().to_string()
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
            .send_msg(self.router_id, msg)
            .await
            .map_err(|e| log::error!("Failed to send metrics: {e:?}"));
    }

    async fn handle_query(&mut self, query: Query) -> anyhow::Result<()> {
        log::info!("Starting query {query:?}");
        let query_id = Self::generate_query_id();
        let Query {
            dataset_id,
            query,
            worker_id,
            timeout,
            profiling,
            result_sender,
        } = query;

        let timeout_handle = self.spawn_timeout_task(&query_id, timeout);
        let task = Task::new(worker_id, result_sender, timeout_handle);
        self.tasks.insert(query_id.clone(), task);

        let query = QueryMsg {
            query_id,
            dataset: dataset_id.0,
            query,
            profiling,
        };
        let worker_msg = Msg::Query(query.clone());
        self.send_msg(worker_id, worker_msg).await?;

        if self.send_metrics {
            let metrics_msg = Msg::QuerySubmitted(QuerySubmitted {
                query: Some(query),
                worker_id: worker_id.to_string(),
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
        log::info!("Query {query_id} execution timed out");
        let (query_id, task) = self.get_task(query_id)?.remove_entry();

        self.network_state
            .write()
            .await
            .greylist_worker(task.worker_id);

        if self.send_metrics {
            let metrics_msg = Msg::QueryFinished(QueryFinished {
                query_id,
                worker_id: task.worker_id.to_string(),
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
            Some(Msg::DatasetState(state)) => {
                let dataset_id = topic.ok_or_else(|| anyhow::anyhow!("Message topic missing"))?;
                self.update_dataset_state(peer_id, dataset_id.into(), state)
                    .await;
            }
            _ => log::warn!("Unexpected message received: {msg:?}"),
        }
        Ok(())
    }

    async fn update_dataset_state(
        &mut self,
        peer_id: PeerId,
        dataset_id: DatasetId,
        state: RangeSet,
    ) {
        log::debug!("Updating dataset state. worker_id={peer_id} dataset_id={dataset_id}");
        self.network_state
            .write()
            .await
            .update_dataset_state(peer_id, dataset_id, state)
    }

    async fn query_result(
        &mut self,
        peer_id: PeerId,
        result: QueryResultMsg,
    ) -> anyhow::Result<()> {
        let QueryResultMsg { query_id, result } = result;
        let result = result.ok_or_else(|| anyhow::anyhow!("Result missing"))?;
        log::info!("Got result for query {query_id}");

        let task_entry = self.get_task(query_id)?;
        anyhow::ensure!(
            peer_id == task_entry.get().worker_id,
            "Invalid message sender"
        );
        let (query_id, task) = task_entry.remove_entry();

        if self.send_metrics {
            let metrics_msg = Msg::QueryFinished(QueryFinished {
                query_id,
                worker_id: peer_id.to_string(),
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

    async fn handle_worker_update(&self, workers: Vec<Worker>) -> Result<(), ()> {
        self.network_state
            .write()
            .await
            .update_registered_workers(workers);
        Ok(())
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
    msg_receiver: mpsc::Receiver<Message>,
    msg_sender: mpsc::Sender<Message>,
    worker_updates: mpsc::Receiver<Vec<Worker>>,
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
        worker_updates,
        tasks: Default::default(),
        network_state: network_state.clone(),
        router_id: config.router_id.0,
        send_metrics: config.send_metrics,
    };
    tokio::spawn(handler.run());

    let client = QueryClient {
        network_state,
        query_sender,
    };
    Ok(client)
}
