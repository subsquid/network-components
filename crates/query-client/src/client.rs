use contract_client::Worker;
use prost::Message as ProstMsg;
use rand::prelude::IteratorRandom;
use serde::Deserialize;
use std::collections::hash_map::{Entry, OccupiedEntry};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;

use grpc_libp2p::{MsgContent, PeerId};

use router_controller::messages::{
    envelope::Msg, Envelope, Query as QueryMsg, QueryError, QueryResult as QueryResultMsg, RangeSet,
};

type Message = grpc_libp2p::Message<Box<[u8]>>;

const WORKER_INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);
const WORKER_GREYLIST_TIME: Duration = Duration::from_secs(600);
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(60);

/// This struct exists not to confuse dataset name with it's encoded ID
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
pub struct DatasetId(String);

impl Display for DatasetId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for DatasetId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Debug)]
struct Query {
    pub dataset_id: DatasetId,
    pub query: String,
    pub worker_id: PeerId,
    pub timeout: Duration,
    pub result_sender: oneshot::Sender<QueryResult>,
}

#[derive(Debug)]
struct Task {
    worker_id: PeerId,
    result_sender: oneshot::Sender<QueryResult>,
    timeout_handle: JoinHandle<()>,
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    Ok(Vec<u8>),
    Error(String),
    Timeout,
}

impl Task {
    pub fn timeout(self) {
        let _ = self.result_sender.send(QueryResult::Timeout);
    }

    pub fn query_error(self, error: String) {
        self.timeout_handle.abort();
        let _ = self.result_sender.send(QueryResult::Error(error));
    }

    pub fn result_received(self, data: Vec<u8>) {
        self.timeout_handle.abort();
        let _ = self.result_sender.send(QueryResult::Ok(data));
    }
}

#[derive(Default)]
struct NetworkState {
    dataset_states: HashMap<DatasetId, HashMap<PeerId, RangeSet>>,
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
            .iter()
            .filter(|(peer_id, _)| self.worker_is_active(peer_id))
            .filter_map(|(peer_id, range_set)| range_set.has(start_block).then_some(*peer_id))
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
            .insert(peer_id, state);
    }

    fn update_registered_workers(&mut self, workers: Vec<Worker>) {
        self.registered_workers = workers.into_iter().map(|w| w.peer_id).collect();
    }

    pub fn greylist_worker(&mut self, worker_id: PeerId) {
        self.worker_greylist.insert(worker_id, Instant::now());
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

    fn generate_query_id(&self, _query: &Query) -> String {
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

    async fn handle_query(&mut self, query: Query) -> anyhow::Result<()> {
        log::info!("Starting query {query:?}");
        let query_id = self.generate_query_id(&query);

        let query_id2 = query_id.clone();
        let timeout_sender = self.timeout_sender.clone();
        let timeout_handle = tokio::spawn(async move {
            tokio::time::sleep(query.timeout).await;
            if timeout_sender.send(query_id2).await.is_err() {
                log::error!("Error sending query timeout")
            }
        });

        let task = Task {
            worker_id: query.worker_id,
            result_sender: query.result_sender,
            timeout_handle,
        };
        self.tasks.insert(query_id.clone(), task);

        let msg = Msg::Query(QueryMsg {
            query_id,
            dataset: query.dataset_id.0,
            query: query.query,
        });
        self.send_msg(query.worker_id, msg).await
    }

    async fn handle_timeout(&mut self, query_id: String) -> anyhow::Result<()> {
        log::info!("Query {query_id} execution timed out");
        let task = self.get_task(query_id)?.remove();
        self.network_state
            .write()
            .await
            .greylist_worker(task.worker_id);
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
            Some(Msg::QueryError(error)) => self.query_error(peer_id, error).await?,
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

    async fn query_error(&mut self, peer_id: PeerId, error: QueryError) -> anyhow::Result<()> {
        log::error!("Query error: {error:?}");
        let QueryError { query_id, error } = error;
        let task_entry = self.get_task(query_id)?;
        anyhow::ensure!(
            peer_id == task_entry.get().worker_id,
            "Invalid message sender"
        );
        task_entry.remove().query_error(error);
        Ok(())
    }

    async fn query_result(
        &mut self,
        peer_id: PeerId,
        result: QueryResultMsg,
    ) -> anyhow::Result<()> {
        let QueryResultMsg { query_id, data, .. } = result;
        log::info!("Got result for query {query_id}");
        let task_entry = self.get_task(query_id)?;
        anyhow::ensure!(
            peer_id == task_entry.get().worker_id,
            "Invalid message sender"
        );
        task_entry.remove().result_received(data);
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
    ) -> anyhow::Result<QueryResult> {
        let timeout = timeout.map(Into::into).unwrap_or(DEFAULT_QUERY_TIMEOUT);
        let (result_sender, result_receiver) = oneshot::channel();
        let query = Query {
            dataset_id,
            query,
            worker_id,
            timeout,
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
    available_datasets: HashMap<String, DatasetId>,
    msg_receiver: mpsc::Receiver<Message>,
    msg_sender: mpsc::Sender<Message>,
    worker_updates: mpsc::Receiver<Vec<Worker>>,
) -> anyhow::Result<QueryClient> {
    let (query_sender, query_receiver) = mpsc::channel(100);
    let (timeout_sender, timeout_receiver) = mpsc::channel(100);
    let network_state = Arc::new(RwLock::new(NetworkState::new(available_datasets)));

    let handler = QueryHandler {
        msg_receiver,
        msg_sender,
        query_receiver,
        timeout_sender,
        timeout_receiver,
        worker_updates,
        tasks: Default::default(),
        network_state: network_state.clone(),
    };
    tokio::spawn(handler.run());

    let client = QueryClient {
        network_state,
        query_sender,
    };
    Ok(client)
}
