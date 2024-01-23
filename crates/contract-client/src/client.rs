use async_trait::async_trait;
use ethers::prelude::{JsonRpcClient, Provider};
use ethers::types::Bytes;
use libp2p::PeerId;
use std::iter::zip;
use std::sync::Arc;

use crate::contracts::{GatewayRegistry, NetworkController, Strategy, WorkerRegistration};
use crate::transport::Transport;
use crate::{contracts, Address, ClientError, RpcArgs, U256};

#[derive(Debug, Clone)]
pub struct Allocation {
    pub worker_peer_id: PeerId,
    pub worker_onchain_id: U256,
    pub computation_units: U256,
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
    fn new(worker: contracts::Worker, onchain_id: U256) -> Result<Self, ClientError> {
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
    /// Using regular clone is not possible for trait objects
    fn clone_client(&self) -> Box<dyn Client>;

    /// Get the current epoch number
    async fn current_epoch(&self) -> Result<u32, ClientError>;

    /// Get current active worker set
    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError>;

    /// Get client's allocations for the current epoch.
    async fn current_allocations(
        &self,
        client_id: PeerId,
        worker_ids: Option<Vec<Worker>>,
    ) -> Result<Vec<Allocation>, ClientError>;
}

pub async fn get_client(RpcArgs { rpc_url }: &RpcArgs) -> Result<Box<dyn Client>, ClientError> {
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
        let gateway_registry = GatewayRegistry::get(client.clone());
        let default_strategy_addr = gateway_registry.default_strategy().call().await?;
        let network_controller = NetworkController::get(client.clone());
        let worker_registration = WorkerRegistration::get(client.clone());
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
    fn clone_client(&self) -> Box<dyn Client> {
        Box::new(self.clone())
    }

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
        let mut multicall = contracts::multicall(self.client.clone()).await?;
        multicall
            .add_call::<Vec<contracts::Worker>>(workers_call, false)
            .add_call::<Vec<U256>>(onchain_ids_call, false);
        let (workers, onchain_ids): (Vec<contracts::Worker>, Vec<U256>) = multicall.call().await?;

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
        workers: Option<Vec<Worker>>,
    ) -> Result<Vec<Allocation>, ClientError> {
        let workers = match workers {
            Some(workers) => workers,
            None => self.active_workers().await?,
        };
        if workers.is_empty() {
            return Ok(vec![]);
        }

        let gateway_id: Bytes = client_id.to_bytes().into();
        let strategy_addr = self
            .gateway_registry
            .get_used_strategy(gateway_id.clone())
            .call()
            .await?;
        let strategy = Strategy::get(strategy_addr, self.client.clone());
        log::info!("{strategy_addr}");

        // A little hack to make less requests: default strategy distributes CUs evenly,
        // so we can just query for one worker and return the same number for all.
        if strategy_addr == self.default_strategy_addr {
            let first_worker_id = workers.first().expect("non empty").onchain_id;
            let cus_per_epoch = strategy
                .computation_units_per_epoch(gateway_id, first_worker_id)
                .call()
                .await?;
            return Ok(workers
                .into_iter()
                .map(|w| Allocation {
                    worker_peer_id: w.peer_id,
                    worker_onchain_id: w.onchain_id,
                    computation_units: cus_per_epoch,
                })
                .collect());
        }

        let mut multicall = contracts::multicall(self.client.clone()).await?;
        for worker in workers.iter() {
            multicall.add_call::<U256>(
                strategy.method(
                    "computationUnitsPerEpoch",
                    (gateway_id.clone(), worker.onchain_id),
                )?,
                false,
            );
        }
        let compute_units: Vec<U256> = multicall.call_array().await?;
        Ok(zip(workers, compute_units)
            .map(|(w, cus)| Allocation {
                worker_peer_id: w.peer_id,
                worker_onchain_id: w.onchain_id,
                computation_units: cus,
            })
            .collect())
    }
}
