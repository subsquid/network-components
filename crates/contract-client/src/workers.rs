use std::sync::Arc;

use async_trait::async_trait;
use ethers::contract::abigen;
use ethers::prelude::{JsonRpcClient, Multicall, Provider};
use lazy_static::lazy_static;
use libp2p::PeerId;

use crate::cli::RpcArgs;
use crate::error::ClientError;
use crate::transport::Transport;
use crate::{Address, U256};

abigen!(WorkerRegistration, "abi/WorkerRegistration.json");

lazy_static! {
    pub static ref WORKER_REGISTRATION_CONTRACT_ADDR: Address =
        std::env::var("WORKER_REGISTRATION_CONTRACT_ADDR")
            .as_deref()
            .unwrap_or("0x7Bf0B1ee9767eAc70A857cEbb24b83115093477F")
            .parse()
            .expect("Invalid WorkerRegistration contract address");
    pub static ref MULTICALL_CONTRACT_ADDR: Option<Address> =
        std::env::var("MULTICALL_CONTRACT_ADDR")
            .ok()
            .map(|addr| addr.parse().expect("Invalid Multicall contract address"));
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Worker {
    pub peer_id: PeerId,
    pub onchain_id: U256,
    pub address: Address,
    pub bond: U256,
    pub registered_at: u128,
    pub deregistered_at: Option<u128>,
}

impl Worker {
    fn new(worker: worker_registration::Worker, onchain_id: U256) -> Result<Self, ClientError> {
        let peer_id = PeerId::from_bytes(&worker.peer_id)?;
        let deregistered_at = (worker.deregistered_at > 0).then_some(worker.deregistered_at);
        Ok(Self {
            peer_id,
            onchain_id,
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
}

pub async fn get_client(RpcArgs { rpc_url, .. }: &RpcArgs) -> Result<Box<dyn Client>, ClientError> {
    match Transport::connect(rpc_url).await? {
        Transport::Http(provider) => Ok(RpcProvider::new(provider)),
        Transport::Ws(provider) => Ok(RpcProvider::new(provider)),
    }
}

#[derive(Clone)]
struct RpcProvider<T: JsonRpcClient + Clone + 'static> {
    client: Arc<Provider<T>>,
    contract: WorkerRegistration<Provider<T>>,
}

impl<T: JsonRpcClient + Clone + 'static> RpcProvider<T> {
    pub fn new(provider: Provider<T>) -> Box<Self> {
        let client = Arc::new(provider);
        let contract = WorkerRegistration::new(*WORKER_REGISTRATION_CONTRACT_ADDR, client.clone());
        Box::new(Self { client, contract })
    }
}

#[async_trait]
impl<M: JsonRpcClient + Clone + 'static> Client for RpcProvider<M> {
    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError> {
        let workers_call = self.contract.method("getActiveWorkers", ())?;
        let onchain_ids_call = self.contract.method("getActiveWorkerIds", ())?;
        let mut multicall = Multicall::new(self.client.clone(), *MULTICALL_CONTRACT_ADDR).await?;
        multicall
            .add_call::<Vec<worker_registration::Worker>>(workers_call, false)
            .add_call::<Vec<U256>>(onchain_ids_call, false);
        let (workers, onchain_ids): (Vec<worker_registration::Worker>, Vec<U256>) =
            multicall.call().await?;

        let workers = workers
            .into_iter()
            .zip(onchain_ids)
            .filter_map(
                |(worker, onchain_id)| match Worker::new(worker, onchain_id) {
                    Ok(worker) => Some(worker),
                    Err(e) => {
                        log::debug!("Error reading worker from chain: {e:?}");
                        None
                    }
                },
            )
            .collect();
        Ok(workers)
    }
}
