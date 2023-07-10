use async_trait::async_trait;
use ethers::prelude::{abigen, ContractError, Http, JsonRpcClient, Middleware};
use ethers::providers::{Provider, Ws};
use ethers::types::{Address, U256};
use futures::{Stream, StreamExt};
use lazy_static::lazy_static;
use libp2p::PeerId;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub use tokio::sync::mpsc::Receiver;

abigen!(TSQD, "abi/tSQD.json");
abigen!(WorkerRegistration, "abi/WorkerRegistration.json");

lazy_static! {
    pub static ref TSQD_CONTRACT_ADDR: Address = std::env::var("TSQD_CONTRACT_ADDR")
        .unwrap_or("0x5FbDB2315678afecb367f032d93F642f64180aa3".to_string())
        .parse()
        .expect("Invalid tSQD contract address");
    pub static ref WORKER_REGISTRATION_CONTRACT_ADDR: Address =
        std::env::var("WORKER_REGISTRATION_CONTRACT_ADDR")
            .unwrap_or("0xA7E47a7aE0FB29BeF4485f6CAb2ee1b85c1D38aB".to_string())
            .parse()
            .expect("Invalid WorkerRegistration contract address");
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Invalid RPC URL: {0:?}")]
    InvalidRpcUrl(#[from] url::ParseError),
    #[error("Invalid Peer ID: {0:?}")]
    InvalidPeerId(#[from] libp2p::multihash::Error),
    #[error("Contract error: {0}")]
    Contract(String),
    #[error("RPC provider error: {0}")]
    Provider(#[from] ethers::providers::ProviderError),
    #[error("Unsupported RPC protocol")]
    InvalidProtocol,
}

impl<T: JsonRpcClient> From<ContractError<Provider<T>>> for ClientError {
    fn from(err: ContractError<Provider<T>>) -> Self {
        Self::Contract(err.to_string())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Worker {
    pub peer_id: PeerId,
    pub address: Address,
    pub bond: U256,
    pub registered_at: u128,
    pub deregistered_at: Option<u128>,
}

impl TryFrom<worker_registration::Worker> for Worker {
    type Error = ClientError;

    fn try_from(worker: worker_registration::Worker) -> Result<Self, Self::Error> {
        let peer_id = PeerId::from_bytes(&worker.peer_id)?;
        let deregistered_at = (worker.deregistered_at > 0).then_some(worker.deregistered_at);
        Ok(Self {
            peer_id,
            address: worker.creator,
            bond: worker.bond,
            registered_at: worker.registered_at,
            deregistered_at,
        })
    }
}

#[async_trait]
pub trait Client: Send + Sync {
    /// Get current active worker set
    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError>;

    /// Get a stream which yields an updated set of active workers after every change
    async fn active_workers_stream(&self) -> Result<Receiver<Vec<Worker>>, ClientError>;
}

pub async fn get_client(rpc_url: &str) -> Result<Box<dyn Client>, ClientError> {
    if rpc_url.starts_with("http") {
        let provider = Provider::<Http>::try_from(rpc_url)?;
        Ok(Box::new(EthersClient::new(provider)))
    } else if rpc_url.starts_with("ws") {
        let provider = Provider::<Ws>::connect(rpc_url).await?;
        Ok(Box::new(EthersClient::new(provider)))
    } else {
        Err(ClientError::InvalidProtocol)
    }
}

trait RawClient: JsonRpcClient + Clone + 'static {}
impl<T: JsonRpcClient + Clone + 'static> RawClient for T {}

#[allow(dead_code)]
#[derive(Clone)]
struct EthersClient<T: RawClient> {
    tsqd: TSQD<Provider<T>>,
    worker_registration: WorkerRegistration<Provider<T>>,
}

impl<T: RawClient> EthersClient<T> {
    pub fn new(provider: Provider<T>) -> Self {
        let client = Arc::new(provider);
        let tsqd = TSQD::new(*TSQD_CONTRACT_ADDR, client.clone());
        let worker_registration =
            WorkerRegistration::new(*WORKER_REGISTRATION_CONTRACT_ADDR, client);
        Self {
            tsqd,
            worker_registration,
        }
    }
}

#[async_trait]
impl<T: RawClient> Client for EthersClient<T> {
    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError> {
        let workers = self
            .worker_registration
            .get_active_workers()
            .call()
            .await?
            .into_iter()
            .filter_map(|worker| worker.try_into().ok())
            .collect();
        Ok(workers)
    }

    async fn active_workers_stream(&self) -> Result<Receiver<Vec<Worker>>, ClientError> {
        let workers = self.active_workers().await?;
        let client = (*self).clone();
        let (tx, rx) = mpsc::channel(100);
        let updater = WorkerSetUpdater::new(client, workers, tx);
        tokio::spawn(updater.run());
        Ok(rx)
    }
}

struct WorkerSetUpdater<T: RawClient> {
    client: EthersClient<T>,
    result_sender: mpsc::Sender<Vec<Worker>>,
    last_worker_set: Vec<Worker>,
}

impl<T: RawClient> WorkerSetUpdater<T> {
    pub fn new(
        client: EthersClient<T>,
        workers: Vec<Worker>,
        result_sender: mpsc::Sender<Vec<Worker>>,
    ) -> Self {
        Self {
            client,
            result_sender,
            last_worker_set: workers,
        }
    }

    pub async fn run(mut self) {
        // Send active worker set immediately and then check for updates every new block
        if self
            .result_sender
            .send(self.last_worker_set.clone())
            .await
            .is_err()
        {
            return; // Stream receiver was dropped
        }

        let raw_client = self.client.worker_registration.client();
        let mut block_stream = get_block_stream(&raw_client).await;

        loop {
            if block_stream.next().await.is_none() {
                // Block stream ended -> reconnect
                block_stream = get_block_stream(&raw_client).await;
            }
            if self.next_update().await.is_err() {
                break; // Stream receiver was dropped
            }
        }
    }

    async fn next_update(&mut self) -> Result<(), mpsc::error::SendError<Vec<Worker>>> {
        match self.client.active_workers().await {
            Err(e) => log::error!("Error getting workers: {e:?}"),
            Ok(new_workers) if new_workers == self.last_worker_set => {
                log::debug!("Active worker set unchanged")
            }
            Ok(new_workers) => {
                log::debug!("New set of active workers");
                self.last_worker_set = new_workers.clone();
                self.result_sender.send(new_workers).await?;
            }
        }
        Ok(())
    }
}

async fn get_block_stream(raw_client: &impl Middleware) -> impl Stream + '_ {
    loop {
        match raw_client.watch_blocks().await {
            Ok(stream) => return stream,
            Err(e) => {
                // Wait a bit and try to connect again
                log::error!("Cannot get blocks: {e:?}");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}
