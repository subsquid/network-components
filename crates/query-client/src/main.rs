use clap::Parser;
use grpc_libp2p::transport::P2PTransportBuilder;
use grpc_libp2p::util::{get_keypair, BootNode};
use grpc_libp2p::{MsgContent, PeerId};
use prost::Message as ProstMsg;
use router_controller::messages::envelope::Msg;
use router_controller::messages::{
    Envelope, GetWorker, GetWorkerResult, Query as QueryMsg, QueryError, QueryResult,
};
use simple_logger::SimpleLogger;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

type Message = grpc_libp2p::Message<Box<[u8]>>;

#[derive(Parser)]
pub struct Cli {
    #[arg(short, long, help = "Path to libp2p key file")]
    pub key: Option<PathBuf>,

    #[arg(
        short,
        long,
        help = "Listen addr",
        default_value = "/ip4/0.0.0.0/tcp/0"
    )]
    pub listen: String,

    #[arg(short, long, help = "Connect to boot node '<peer_id> <address>'.")]
    boot_nodes: Vec<BootNode>,

    #[arg(short, long, help = "Path to output directory (default: temp dir)")]
    pub output_dir: Option<PathBuf>,

    #[arg(short, long, help = "Peer ID of the router")]
    pub router_id: String,
}

struct QueryClient {
    msg_receiver: Receiver<Message>,
    msg_sender: Sender<Message>,
    query_receiver: Receiver<Query>,
    queries: HashMap<String, QueryState>,
    output_dir: PathBuf,
    router_id: PeerId,
}

#[derive(Debug, Clone)]
struct Query {
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
        router_id: String,
        msg_receiver: Receiver<Message>,
        msg_sender: Sender<Message>,
    ) -> anyhow::Result<Sender<Query>> {
        let output_dir = output_dir.unwrap_or_else(std::env::temp_dir);
        let router_id = router_id.parse()?;
        let (query_sender, query_receiver) = mpsc::channel(100);
        let client = Self {
            msg_receiver,
            msg_sender,
            query_receiver,
            queries: Default::default(),
            output_dir,
            router_id,
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
            peer_id,
            content: envelope.encode_to_vec().into(),
        };
        self.msg_sender.send(msg).await?;
        Ok(())
    }

    async fn handle_query(&mut self, query: Query) -> anyhow::Result<()> {
        log::info!("starting query {query:?}");
        let query_id = self.generate_query_id(&query);
        let Query {
            dataset,
            start_block,
            query,
        } = query;
        let msg = Msg::GetWorker(GetWorker {
            query_id: query_id.clone(),
            dataset: dataset.clone(),
            start_block,
        });
        self.queries
            .insert(query_id, QueryState::LookingForWorker { dataset, query });
        self.send_msg(self.router_id, msg).await
    }

    async fn handle_message(&mut self, msg: Message) -> anyhow::Result<()> {
        let Message { peer_id, content } = msg;
        let Envelope { msg } = Envelope::decode(content.as_slice())?;
        match msg {
            Some(Msg::GetWorkerResult(result)) => self.got_worker(peer_id, result).await?,
            Some(Msg::GetWorkerError(error)) => self.query_error(peer_id, error),
            Some(Msg::QueryResult(result)) => self.query_result(peer_id, result).await,
            Some(Msg::QueryError(error)) => self.query_error(peer_id, error),
            _ => log::warn!("Unexpected message received: {msg:?}"),
        }
        Ok(())
    }

    async fn got_worker(&mut self, peer_id: PeerId, result: GetWorkerResult) -> anyhow::Result<()> {
        log::info!("Got worker: {result:?}");
        anyhow::ensure!(peer_id == self.router_id, "Invalid message sender");

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
            QueryState::LookingForWorker { .. } => &self.router_id,
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
        if tokio::fs::try_exists(&result_path).await.is_ok() {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()?;
    let args = Cli::parse();

    let keypair = get_keypair(args.key).await?;
    let mut transport_builder = P2PTransportBuilder::from_keypair(keypair);
    let listen_addr = args.listen.parse()?;
    transport_builder.listen_on(std::iter::once(listen_addr));
    transport_builder.boot_nodes(args.boot_nodes);
    let (msg_receiver, msg_sender) = transport_builder.run().await?;

    let query_sender =
        QueryClient::start(args.output_dir, args.router_id, msg_receiver, msg_sender)?;
    let mut reader = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = reader.next_line().await? {
        let mut parts: Vec<String> = line.splitn(3, ' ').map(|s| s.to_string()).collect();
        if parts.len() != 3 {
            log::error!("Invalid input");
            continue;
        }
        let query = parts.pop().expect("parts length is 3");
        let start_block = match u32::from_str(&parts.pop().expect("parts length is 3")) {
            Ok(x) => x,
            Err(_) => {
                log::error!("Invalid input");
                continue;
            }
        };
        let dataset = parts.pop().expect("parts length is 3");
        let query = Query {
            dataset,
            start_block,
            query,
        };
        query_sender.send(query).await?;
    }

    Ok(())
}
