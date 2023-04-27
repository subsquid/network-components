use ethers::prelude::{abigen, ContractError, Middleware};
use ethers::providers::{Http, Provider};
use ethers::types::{Address, U256};
use futures::StreamExt;
use lazy_static::lazy_static;
use libp2p::PeerId;
use std::sync::Arc;
use tokio::sync::mpsc;

pub use tokio::sync::mpsc::Receiver;

abigen!(
    TSQD,
    "../../../subsquid-network-contracts/deployments/localhost/tSQD.json"
);

abigen!(
    WorkerRegistration,
    "../../../subsquid-network-contracts/deployments/localhost/WorkerRegistration.json"
);

lazy_static! {
    pub static ref TSQD_CONTRACT_ADDR: Address = std::env::var("TSQD_CONTRACT_ADDR")
        .unwrap_or("0x5fbdb2315678afecb367f032d93f642f64180aa3".to_string())
        .parse()
        .expect("Invalid tSQD contract address");
    pub static ref WORKER_REGISTRATION_CONTRACT_ADDR: Address =
        std::env::var("WORKER_REGISTRATION_CONTRACT_ADDR")
            .unwrap_or("0x6a220ec0269e695f3788e03a9c34dacdd4227784".to_string())
            .parse()
            .expect("Invalid WorkerRegistration contract address");
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct Client {
    tsqd: TSQD<Provider<Http>>,
    worker_registration: WorkerRegistration<Provider<Http>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Invalid RPC URL: {0:?}")]
    InvalidRpcUrl(#[from] url::ParseError),
    #[error("Invalid Peer ID: {0:?}")]
    InvalidPeerId(#[from] libp2p::multihash::Error),
    #[error("Contract error: {0:?}")]
    Contract(#[from] ContractError<Provider<Http>>),
    #[error("RPC provider error: {0:?}")]
    Provider(#[from] ethers::providers::ProviderError),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Worker {
    pub peer_id: PeerId,
    pub address: Address,
    pub bond: U256,
    pub registered_at: U256,
    pub deregistered_at: Option<U256>,
}

impl TryFrom<worker_registration::Worker> for Worker {
    type Error = ClientError;

    fn try_from(worker: worker_registration::Worker) -> Result<Self, Self::Error> {
        // Strip the padding – all but last trailing zeros
        let peer_id_bytes: Vec<u8> = worker.peer_id.concat();
        let last_trailing_zero = peer_id_bytes
            .iter()
            .position(|x| *x != 0)
            .ok_or(libp2p::multihash::Error::InvalidSize(0))?
            .saturating_sub(1);
        let peer_id = PeerId::from_bytes(&peer_id_bytes[last_trailing_zero..])?;

        let deregistered_at = (worker.deregistered_at > 0.into()).then_some(worker.deregistered_at);

        Ok(Self {
            peer_id,
            address: worker.account,
            bond: worker.bond,
            registered_at: worker.registered_at,
            deregistered_at,
        })
    }
}

impl Client {
    pub fn new(rpc_url: &str) -> Result<Self, ClientError> {
        let provider = Provider::<Http>::try_from(rpc_url)?;
        let client = Arc::new(provider);
        let tsqd = TSQD::new(*TSQD_CONTRACT_ADDR, client.clone());
        let worker_registration =
            WorkerRegistration::new(*WORKER_REGISTRATION_CONTRACT_ADDR, client);
        Ok(Self {
            tsqd,
            worker_registration,
        })
    }

    /// Get current active worker set
    pub async fn active_workers(&self) -> Result<Vec<Worker>, ClientError> {
        self.worker_registration
            .get_active_workers()
            .call()
            .await?
            .into_iter()
            .map(|worker| worker.try_into())
            .collect()
    }

    /// Get a stream which yields an updated set of active workers after every change
    pub async fn active_workers_stream(&self) -> mpsc::Receiver<Vec<Worker>> {
        let client = self.clone();
        let (tx, rx) = mpsc::channel(100);
        let updater = WorkerSetUpdater::new(client, tx);
        tokio::spawn(updater.run());
        rx
    }
}

struct WorkerSetUpdater {
    client: Client,
    result_sender: mpsc::Sender<Vec<Worker>>,
    last_worker_set: Option<Vec<Worker>>,
}

impl WorkerSetUpdater {
    pub fn new(client: Client, result_sender: mpsc::Sender<Vec<Worker>>) -> Self {
        Self {
            client,
            result_sender,
            last_worker_set: None,
        }
    }

    pub async fn run(mut self) {
        let raw_client = self.client.worker_registration.client();
        let mut block_stream = match raw_client.watch_blocks().await {
            Ok(stream) => stream,
            Err(e) => return log::error!("Cannot get blocks: {e:?}"),
        };

        // Send active worker set immediately and then check for updates every new block
        loop {
            if self.next_update().await.is_err() {
                break; // Stream receiver was dropped
            }
            if block_stream.next().await.is_none() {
                break; // Block stream ended -> connection error(?)
            }
        }
    }

    async fn next_update(&mut self) -> Result<(), mpsc::error::SendError<Vec<Worker>>> {
        match (
            self.client.active_workers().await,
            &mut self.last_worker_set,
        ) {
            (Err(e), _) => log::error!("Error getting workers: {e:?}"),
            (Ok(new_workers), Some(old_workers)) if &new_workers == old_workers => {
                log::debug!("Active worker set unchanged")
            }
            (Ok(new_workers), last_worker_set) => {
                log::debug!("New set of active workers");
                *last_worker_set = Some(new_workers.clone());
                self.result_sender.send(new_workers).await?;
            }
        }
        Ok(())
    }
}
