use prost::Message as ProstMsg;
use rand::prelude::IteratorRandom;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use grpc_libp2p::{MsgContent, PeerId};

use router_controller::messages::{
    envelope::Msg, Envelope, GetWorker, GetWorkerResult, Query as QueryMsg, QueryError,
    QueryResult, RangeSet,
};

type Message = grpc_libp2p::Message<Box<[u8]>>;

const WORKER_INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);
const WORKER_GREYLIST_TIME: Duration = Duration::from_secs(600);

/// This struct exists because `PeerId` doesn't implement `Deserialize`
#[derive(Debug, Clone, Copy)]
pub struct RouterId(PeerId);

impl<'de> Deserialize<'de> for RouterId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let peer_id = String::deserialize(deserializer)?
            .parse()
            .map_err(|_| serde::de::Error::custom("Invalid peer ID"))?;
        Ok(Self(peer_id))
    }
}

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

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub router_id: RouterId,
    pub available_datasets: HashMap<String, DatasetId>,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub dataset: String,
    pub start_block: u32,
    pub query: String,
}

#[derive(Debug)]
struct Task {
    query: Query,
    state: TaskState,
}

impl Task {
    pub fn new(query: Query) -> Self {
        Self {
            query,
            state: TaskState::LookingForWorker,
        }
    }

    pub fn start_processing(
        &mut self,
        worker_id: PeerId,
        timeout_handle: JoinHandle<()>,
    ) -> anyhow::Result<()> {
        match self.state {
            TaskState::LookingForWorker => {}
            _ => anyhow::bail!("start_processing: unexpected task state"),
        }
        self.state = TaskState::Processing {
            worker_id,
            timeout_handle,
        };
        Ok(())
    }

    pub fn timeout(&mut self) -> anyhow::Result<(PeerId, Query)> {
        let worker_id = match self.state {
            TaskState::Processing { worker_id, .. } => worker_id,
            _ => anyhow::bail!("timeout: unexpected task state"),
        };
        self.state = TaskState::Timeout;
        Ok((worker_id, self.query.clone()))
    }

    pub fn get_worker_error(&mut self, error: String) -> anyhow::Result<()> {
        match self.state {
            TaskState::LookingForWorker => {}
            _ => anyhow::bail!("get_worker_error: unexpected task state"),
        };
        self.state = TaskState::Error { error };
        Ok(())
    }

    pub fn query_error(&mut self, error: String, peer_id: &PeerId) -> anyhow::Result<()> {
        match &self.state {
            TaskState::Processing {
                worker_id,
                timeout_handle,
            } => {
                anyhow::ensure!(worker_id == peer_id, "Invalid message sender");
                timeout_handle.abort();
            }
            TaskState::Timeout => anyhow::bail!("Task already timed out"),
            _ => anyhow::bail!("query_error: unexpected task state"),
        };
        self.state = TaskState::Error { error };
        Ok(())
    }

    pub fn result_received(&mut self, peer_id: &PeerId) -> anyhow::Result<()> {
        match &self.state {
            TaskState::Processing {
                worker_id,
                timeout_handle,
            } => {
                anyhow::ensure!(worker_id == peer_id, "Invalid message sender");
                timeout_handle.abort();
            }
            TaskState::Timeout => anyhow::bail!("Task already timed out"),
            _ => anyhow::bail!("result_received: unexpected task state"),
        };
        self.state = TaskState::ResultReceived;
        Ok(())
    }

    pub fn result_saved(&mut self, result_path: PathBuf) -> anyhow::Result<()> {
        match self.state {
            TaskState::ResultReceived => (),
            _ => anyhow::bail!("result_saved: unexpected task state"),
        };
        self.state = TaskState::Success { result_path };
        Ok(())
    }

    pub fn result_save_error(&mut self, error: String) -> anyhow::Result<()> {
        match self.state {
            TaskState::ResultReceived => (),
            _ => anyhow::bail!("result_save_error: unexpected task state"),
        };
        self.state = TaskState::Error { error };
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum TaskState {
    LookingForWorker,
    Processing {
        worker_id: PeerId,
        timeout_handle: JoinHandle<()>,
    },
    ResultReceived,
    Success {
        result_path: PathBuf,
    },
    Error {
        error: String,
    },
    Timeout,
}

pub struct QueryClient {
    msg_receiver: Receiver<Message>,
    msg_sender: Sender<Message>,
    query_receiver: Receiver<Query>,
    timeout_sender: Sender<String>,
    timeout_receiver: Receiver<String>,
    tasks: HashMap<String, Task>,
    dataset_states: HashMap<DatasetId, HashMap<PeerId, RangeSet>>,
    last_pings: HashMap<PeerId, Instant>,
    worker_greylist: HashMap<PeerId, Instant>,
    output_dir: PathBuf,
    query_timeout: Duration,
    config: Config,
}

impl QueryClient {
    pub fn start(
        output_dir: Option<PathBuf>,
        query_timeout: Duration,
        config: Config,
        msg_receiver: Receiver<Message>,
        msg_sender: Sender<Message>,
    ) -> anyhow::Result<Sender<Query>> {
        let output_dir = output_dir.unwrap_or_else(std::env::temp_dir);
        let (query_sender, query_receiver) = mpsc::channel(100);
        let (timeout_sender, timeout_receiver) = mpsc::channel(100);
        let client = Self {
            msg_receiver,
            msg_sender,
            query_receiver,
            timeout_sender,
            timeout_receiver,
            tasks: Default::default(),
            dataset_states: Default::default(),
            last_pings: Default::default(),
            worker_greylist: Default::default(),
            output_dir,
            query_timeout,
            config,
        };
        tokio::spawn(client.run());
        Ok(query_sender)
    }

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
        self.tasks
            .insert(query_id.clone(), Task::new(query.clone()));

        if let Some(worker_id) = self.find_worker(&query.dataset, query.start_block) {
            log::info!("Found worker {worker_id} for query {query_id}");
            let dataset_id = self
                .config
                .available_datasets
                .get(query.dataset.as_str())
                .expect("Worker was found so the dataset is available")
                .to_owned();
            self.start_query(query_id, dataset_id, worker_id).await
        } else {
            log::info!("Worker not found locally. Falling back to router.");
            let msg = Msg::GetWorker(GetWorker {
                query_id,
                dataset: query.dataset,
                start_block: query.start_block,
            });
            self.send_msg(self.config.router_id.0, msg).await
        }
    }

    fn find_worker(&self, dataset: &str, start_block: u32) -> Option<PeerId> {
        log::debug!("Looking for worker dataset={dataset}, start_block={start_block}");
        // dataset_states uses encoded dataset IDs
        let dataset = match self.config.available_datasets.get(dataset) {
            None => return None,
            Some(dataset) => dataset,
        };
        let dataset_state = match self.dataset_states.get(dataset) {
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

    async fn start_query(
        &mut self,
        query_id: String,
        dataset_id: DatasetId,
        worker_id: PeerId,
    ) -> anyhow::Result<()> {
        let task = self
            .tasks
            .get_mut(&query_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown query: {query_id}"))?;
        log::debug!("Starting query {query_id}");

        let timeout = self.query_timeout;
        let query_id2 = query_id.clone();
        let timeout_sender = self.timeout_sender.clone();
        let timeout_handle = tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            if timeout_sender.send(query_id2).await.is_err() {
                log::error!("Error sending query timeout")
            }
        });

        let msg = Msg::Query(QueryMsg {
            query_id,
            dataset: dataset_id.0,
            query: task.query.query.clone(),
        });
        task.start_processing(worker_id, timeout_handle)?;
        self.send_msg(worker_id, msg).await
    }

    async fn handle_timeout(&mut self, query_id: String) -> anyhow::Result<()> {
        let task = self.get_task(&query_id)?;
        let (worker_id, query) = task.timeout()?;
        log::info!("Query {query_id} execution timed out");
        self.worker_greylist.insert(worker_id, Instant::now());
        self.handle_query(query).await
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
            Some(Msg::GetWorkerResult(result)) => self.got_worker(peer_id, result).await?,
            Some(Msg::GetWorkerError(error)) => self.get_worker_error(peer_id, error)?,
            Some(Msg::QueryResult(result)) => self.query_result(peer_id, result).await?,
            Some(Msg::QueryError(error)) => self.query_error(peer_id, error).await?,
            Some(Msg::DatasetState(state)) => {
                let dataset_id = topic.ok_or_else(|| anyhow::anyhow!("Message topic missing"))?;
                self.update_dataset_state(peer_id, dataset_id.into(), state);
            }
            _ => log::warn!("Unexpected message received: {msg:?}"),
        }
        Ok(())
    }

    fn update_dataset_state(&mut self, peer_id: PeerId, dataset_id: DatasetId, state: RangeSet) {
        log::debug!("Updating dataset state. worker_id={peer_id} dataset_id={dataset_id}");
        self.last_pings.insert(peer_id, Instant::now());
        self.dataset_states
            .entry(dataset_id)
            .or_default()
            .insert(peer_id, state);
    }

    async fn got_worker(&mut self, peer_id: PeerId, result: GetWorkerResult) -> anyhow::Result<()> {
        log::info!("Got worker: {result:?}");
        anyhow::ensure!(peer_id == self.config.router_id.0, "Invalid message sender");

        let GetWorkerResult {
            query_id,
            worker_id,
            encoded_dataset,
        } = result;
        let worker_id = worker_id.parse()?;
        let dataset_id = DatasetId(encoded_dataset);

        if self.worker_is_active(&worker_id) {
            self.start_query(query_id, dataset_id, worker_id).await
        } else {
            let error = "Got inactive/greylisted worker from router".to_owned();
            log::error!("{error}");
            self.get_task(&query_id)?.get_worker_error(error)
        }
    }

    fn get_worker_error(&mut self, peer_id: PeerId, error: QueryError) -> anyhow::Result<()> {
        log::error!("GetWorker error: {error:?}");
        anyhow::ensure!(peer_id == self.config.router_id.0, "Invalid message sender");
        let QueryError { query_id, error } = error;
        self.get_task(&query_id)?.get_worker_error(error)
    }

    async fn query_error(&mut self, peer_id: PeerId, error: QueryError) -> anyhow::Result<()> {
        log::error!("Query error: {error:?}");
        let QueryError { query_id, error } = error;
        self.get_task(&query_id)?.query_error(error, &peer_id)?;
        Ok(())
    }

    async fn query_result(&mut self, peer_id: PeerId, result: QueryResult) -> anyhow::Result<()> {
        let QueryResult { query_id, data, .. } = result;
        log::info!("Got result for query {query_id}");

        let result_path = self.output_dir.join(format!("{query_id}.zip"));
        let task = self.get_task(&query_id)?;
        task.result_received(&peer_id)?;

        log::info!("Saving query results to {}", result_path.display());
        if let Ok(true) = tokio::fs::try_exists(&result_path).await {
            log::warn!("Result file {} will be overwritten", result_path.display());
        }
        match tokio::fs::write(&result_path, data).await {
            Ok(_) => task.result_saved(result_path),
            Err(e) => task.result_save_error(e.to_string()),
        }
    }

    #[inline(always)]
    fn get_task(&mut self, query_id: &str) -> anyhow::Result<&mut Task> {
        self.tasks
            .get_mut(query_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown query: {query_id}"))
    }
}
