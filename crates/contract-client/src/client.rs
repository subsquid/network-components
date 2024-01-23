use async_trait::async_trait;
use ethers::contract::{abigen, Multicall};
use ethers::prelude::{JsonRpcClient, Provider};
use ethers::types::Bytes;
use lazy_static::lazy_static;
use libp2p::PeerId;
use std::iter::zip;
use std::sync::Arc;

use crate::transport::Transport;
use crate::{Address, ClientError, RpcArgs, U256};

abigen!(GatewayRegistry, "abi/GatewayRegistry.json");
abigen!(NetworkController, "abi/NetworkController.json");
abigen!(Strategy, "abi/Strategy.json");
abigen!(WorkerRegistration, "abi/WorkerRegistration.json");

lazy_static! {
    pub static ref GATEWAY_REGISTRY_CONTRACT_ADDR: Address =
        std::env::var("GATEWAY_REGISTRY_CONTRACT_ADDR")
            .as_deref()
            .unwrap_or("0xFb1754Fa0FC1892F9bF0B072F5C7b0a4e6f5d247")
            .parse()
            .expect("Invalid GatewayRegistry contract address");
    pub static ref GATEWAY_CONTRACT_CREATION_BLOCK: u64 =
        std::env::var("GATEWAY_CONTRACT_CREATION_BLOCK")
            .as_deref()
            .unwrap_or("6010034")
            .parse()
            .expect("Invalid GatewayRegistry creation block");
    pub static ref WORKER_REGISTRATION_CONTRACT_ADDR: Address =
        std::env::var("WORKER_REGISTRATION_CONTRACT_ADDR")
            .as_deref()
            .unwrap_or("0x7Bf0B1ee9767eAc70A857cEbb24b83115093477F")
            .parse()
            .expect("Invalid WorkerRegistration contract address");
    pub static ref NETWORK_CONTROLLER_CONTRACT_ADDR: Address =
        std::env::var("NETWORK_CONTROLLER_CONTRACT_ADDR")
            .as_deref()
            .unwrap_or("0xa4285F5503D903BB10978AD652D072e79cc92F0a")
            .parse()
            .expect("Invalid NetworkController contract address");
    pub static ref MULTICALL_CONTRACT_ADDR: Option<Address> =
        std::env::var("MULTICALL_CONTRACT_ADDR")
            .ok()
            .map(|addr| addr.parse().expect("Invalid Multicall contract address"));
}

#[derive(Debug, Clone)]
pub struct Allocation {
    pub worker_onchain_id: U256,
    pub computation_units: U256,
}

impl From<(U256, U256)> for Allocation {
    fn from((worker_onchain_id, computation_units): (U256, U256)) -> Self {
        Self {
            worker_onchain_id,
            computation_units,
        }
    }
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
    /// Get the current epoch number
    async fn current_epoch(&self) -> Result<u32, ClientError>;

    /// Get current active worker set
    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError>;

    /// Get client's allocations for the current epoch.
    async fn current_allocations(
        &self,
        client_id: PeerId,
        worker_ids: Option<Vec<U256>>,
    ) -> Result<Vec<Allocation>, ClientError>;
}

pub async fn get_client(RpcArgs { rpc_url, .. }: &RpcArgs) -> Result<Box<dyn Client>, ClientError> {
    match Transport::connect(rpc_url).await? {
        Transport::Http(provider) => Ok(RpcProvider::new(provider).await?),
        Transport::Ws(provider) => Ok(RpcProvider::new(provider).await?),
    }
}

#[derive(Clone)]
struct RpcProvider<T: JsonRpcClient + Clone + 'static> {
    client: Arc<Provider<T>>,
    gateway_registry: GatewayRegistry<Provider<T>>,
    network_controller: NetworkController<Provider<T>>,
    worker_registration: WorkerRegistration<Provider<T>>,
    default_strategy_addr: Address,
}

impl<T: JsonRpcClient + Clone + 'static> RpcProvider<T> {
    pub async fn new(provider: Provider<T>) -> Result<Box<Self>, ClientError> {
        let client = Arc::new(provider);
        let gateway_registry =
            GatewayRegistry::new(*GATEWAY_REGISTRY_CONTRACT_ADDR, client.clone());
        let default_strategy_addr = gateway_registry.default_strategy().call().await?;
        let network_controller =
            NetworkController::new(*NETWORK_CONTROLLER_CONTRACT_ADDR, client.clone());
        let worker_registration =
            WorkerRegistration::new(*WORKER_REGISTRATION_CONTRACT_ADDR, client.clone());
        Ok(Box::new(Self {
            client,
            gateway_registry,
            worker_registration,
            network_controller,
            default_strategy_addr,
        }))
    }
}

#[async_trait]
impl<M: JsonRpcClient + Clone + 'static> Client for RpcProvider<M> {
    async fn current_epoch(&self) -> Result<u32, ClientError> {
        let epoch = self
            .network_controller
            .epoch_number()
            .call()
            .await?
            .try_into()
            .expect("Epoch number should not exceed u32 range");
        Ok(epoch)
    }

    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError> {
        let workers_call = self.worker_registration.method("getActiveWorkers", ())?;
        let onchain_ids_call = self.worker_registration.method("getActiveWorkerIds", ())?;
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

    async fn current_allocations(
        &self,
        client_id: PeerId,
        worker_ids: Option<Vec<U256>>,
    ) -> Result<Vec<Allocation>, ClientError> {
        let worker_ids = match worker_ids {
            Some(worker_ids) => worker_ids,
            None => self
                .active_workers()
                .await?
                .into_iter()
                .map(|w| w.onchain_id)
                .collect(),
        };
        if worker_ids.is_empty() {
            return Ok(vec![]);
        }

        let gateway_id: Bytes = client_id.to_bytes().into();
        let strategy_addr = self
            .gateway_registry
            .get_used_strategy(gateway_id.clone())
            .call()
            .await?;
        let strategy = Strategy::new(strategy_addr, self.client.clone());

        // A little hack to make less requests
        if strategy_addr == self.default_strategy_addr {
            let cus_per_epoch = strategy
                .computation_units_per_epoch(gateway_id, U256::zero())
                .call()
                .await?;
            return Ok(worker_ids
                .into_iter()
                .map(|w| Allocation {
                    worker_onchain_id: w,
                    computation_units: cus_per_epoch,
                })
                .collect());
        }

        let mut multicall = Multicall::new(self.client.clone(), *MULTICALL_CONTRACT_ADDR).await?;
        for worker_id in worker_ids.iter() {
            multicall.add_call::<Vec<U256>>(
                strategy.method("computationUnitsPerEpoch", (gateway_id.clone(), *worker_id))?,
                false,
            );
        }
        let compute_units: Vec<U256> = multicall.call().await?;
        Ok(zip(worker_ids, compute_units).map(Into::into).collect())
    }
}
