use ethers::prelude::{abigen, ContractError};
use ethers::providers::{Http, Provider};
use ethers::types::{Address, U256};
use lazy_static::lazy_static;
use libp2p::PeerId;
use std::sync::Arc;

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
}

#[derive(Debug, Clone)]
pub struct Worker {
    pub peer_id: PeerId,
    pub address: Address,
    pub bond: U256,
    pub registered_at: U256,
    pub deregistered_at: U256,
}

impl TryFrom<worker_registration::Worker> for Worker {
    type Error = ClientError;

    fn try_from(worker: worker_registration::Worker) -> Result<Self, Self::Error> {
        Ok(Self {
            peer_id: PeerId::from_bytes(worker.peer_id.concat().as_slice())?,
            address: worker.account,
            bond: worker.bond,
            registered_at: worker.registered_at,
            deregistered_at: worker.deregistered_at,
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

    pub async fn get_active_workers(&self) -> Result<Vec<Worker>, ClientError> {
        self.worker_registration
            .get_active_workers()
            .call()
            .await?
            .into_iter()
            .map(|worker| worker.try_into())
            .collect()
    }
}
