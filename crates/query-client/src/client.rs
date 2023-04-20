use grpc_libp2p::{MsgContent, PeerId};
use prost::Message as ProstMsg;
use rand::prelude::IteratorRandom;
use router_controller::messages::envelope::Msg;
use router_controller::messages::{
    Envelope, GetWorker, GetWorkerResult, Query as QueryMsg, QueryError, QueryResult, RangeSet,
};
use serde::{Deserialize, Deserializer};

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;

use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{Duration, Instant};

type Message = grpc_libp2p::Message<Box<[u8]>>;

const WORKER_INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);

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

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub router_id: RouterId,
    pub available_datasets: HashMap<String, String>,
}

pub struct QueryClient {
    msg_receiver: Receiver<Message>,
    msg_sender: Sender<Message>,
    query_receiver: Receiver<Query>,
    queries: HashMap<String, QueryState>,
    dataset_states: HashMap<String, HashMap<PeerId, RangeSet>>,
    last_pings: HashMap<PeerId, Instant>,
    output_dir: PathBuf,
    config: Config,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub dataset: String,
    pub start_block: u32,
    pub query: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum QueryState {
    LookingForWorker { dataset: String, query: String },
    Processing { worker_id: PeerId },
    Success { result_path: PathBuf },
    Error { error: String },
}

impl QueryClient {
    pub fn start(
        output_dir: Option<PathBuf>,
        config: Config,
        msg_receiver: Receiver<Message>,
        msg_sender: Sender<Message>,
    ) -> anyhow::Result<Sender<Query>> {
        let output_dir = output_dir.unwrap_or_else(std::env::temp_dir);
        let (query_sender, query_receiver) = mpsc::channel(100);
        let client = Self {
            msg_receiver,
            msg_sender,
            query_receiver,
            queries: Default::default(),
            dataset_states: Default::default(),
            last_pings: Default::default(),
            output_dir,
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
        let Query {
            dataset,
            start_block,
            query,
        } = query;

        if let Some(worker_id) = self.find_worker(&dataset, start_block) {
            log::info!("Found worker {worker_id} for query {query_id}");
            let encoded_dataset = self
                .config
                .available_datasets
                .get(dataset.as_str())
                .expect("Worker was found so the dataset is available")
                .to_string();
            let msg = Msg::Query(QueryMsg {
                query_id: query_id.clone(),
                dataset: encoded_dataset,
                query,
            });
            self.queries
                .insert(query_id, QueryState::Processing { worker_id });
            self.send_msg(worker_id, msg).await
        } else {
            log::info!("Worker not found locally. Falling back to router.");
            let msg = Msg::GetWorker(GetWorker {
                query_id: query_id.clone(),
                dataset: dataset.clone(),
                start_block,
            });
            self.queries
                .insert(query_id, QueryState::LookingForWorker { dataset, query });
            self.send_msg(self.config.router_id.0, msg).await
        }
    }

    fn find_worker(&self, dataset: &str, start_block: u32) -> Option<PeerId> {
        // dataset_states uses encoded dataset IDs
        let dataset = match self.config.available_datasets.get(dataset) {
            None => return None,
            Some(dataset) => dataset.as_str(),
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
        if let Some(last_ping) = self.last_pings.get(worker_id) {
            *last_ping > Instant::now() - WORKER_INACTIVE_THRESHOLD
        } else {
            false
        }
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
            Some(Msg::GetWorkerError(error)) => self.query_error(peer_id, error),
            Some(Msg::QueryResult(result)) => self.query_result(peer_id, result).await,
            Some(Msg::QueryError(error)) => self.query_error(peer_id, error),
            Some(Msg::DatasetState(state)) => {
                let dataset = topic.ok_or_else(|| anyhow::anyhow!("Message topic missing"))?;
                self.update_dataset_state(peer_id, dataset, state);
            }
            _ => log::warn!("Unexpected message received: {msg:?}"),
        }
        Ok(())
    }

    fn update_dataset_state(&mut self, peer_id: PeerId, dataset: String, state: RangeSet) {
        log::info!("Updating dataset state. worker_id={peer_id} dataset={dataset}");
        self.last_pings.insert(peer_id, Instant::now());
        self.dataset_states
            .entry(dataset)
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

        let query_state = self
            .queries
            .remove(&query_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown query"))?;

        let query = match query_state {
            QueryState::LookingForWorker { query, .. } => {
                self.queries
                    .insert(query_id.clone(), QueryState::Processing { worker_id });
                query
            }
            _ => {
                self.queries.insert(query_id, query_state);
                anyhow::bail!("Invalid state for assigning worker");
            }
        };

        let msg = Msg::Query(QueryMsg {
            query_id,
            dataset: encoded_dataset,
            query,
        });
        self.send_msg(worker_id, msg).await?;
        Ok(())
    }

    fn query_error(&mut self, peer_id: PeerId, error: QueryError) {
        log::error!("Query error: {error:?}");
        let QueryError { query_id, error } = error;
        let query_state = match self.queries.entry(query_id) {
            Entry::Vacant(_) => return log::warn!("Unknown query"),
            Entry::Occupied(entry) => entry.into_mut(),
        };

        let expected_sender = match query_state {
            QueryState::LookingForWorker { .. } => &self.config.router_id.0,
            QueryState::Processing { worker_id } => worker_id,
            _ => return log::warn!("Received error for finished query"),
        };

        if &peer_id != expected_sender {
            log::warn!("Invalid message sender: {peer_id} != {expected_sender}")
        } else {
            *query_state = QueryState::Error { error }
        }
    }

    async fn query_result(&mut self, peer_id: PeerId, result: QueryResult) {
        let QueryResult { query_id, data, .. } = result;
        log::info!("Got result for query {query_id}");

        let query_state = match self.queries.entry(query_id.clone()) {
            Entry::Vacant(_) => return log::warn!("Unknown query"),
            Entry::Occupied(entry) => entry.into_mut(),
        };
        let worker_id = match query_state {
            QueryState::Processing { worker_id } => worker_id.to_owned(),
            _ => return log::warn!("Invalid query state for accepting result"),
        };
        if peer_id != worker_id {
            return log::warn!("Invalid message sender: {peer_id} != {worker_id}");
        }

        let result_path = self.output_dir.join(query_id + ".zip");
        log::info!("Saving query results to {}", result_path.display());
        if let Ok(true) = tokio::fs::try_exists(&result_path).await {
            log::warn!("Result file {} will be overwritten", result_path.display());
        }
        match tokio::fs::write(&result_path, data).await {
            Ok(_) => *query_state = QueryState::Success { result_path },
            Err(e) => {
                log::error!("Error saving result: {e:?}");
                *query_state = QueryState::Error {
                    error: e.to_string(),
                };
            }
        }
    }
}
