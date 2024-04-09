use std::collections::HashMap;
use std::iter::zip;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use ethers::prelude::{BlockId, Bytes, JsonRpcClient, Middleware, Provider};
use subsquid_network_transport::PeerId;

use crate::contracts::{
    AllocationsViewer, GatewayRegistry, NetworkController, Strategy, WorkerRegistration,
};
use crate::transport::Transport;
use crate::{contracts, Address, ClientError, RpcArgs, U256};

#[derive(Debug, Clone)]
pub struct Allocation {
    pub worker_peer_id: PeerId,
    pub worker_onchain_id: U256,
    pub computation_units: U256,
}

#[derive(Debug, Clone)]
pub struct GatewayCluster {
    pub operator_addr: Address,
    pub gateway_ids: Vec<PeerId>,
    pub allocated_computation_units: U256,
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

    /// Get the time when the current epoch started
    async fn current_epoch_start(&self) -> Result<SystemTime, ClientError>;

    /// Get the on-chain ID for the worker
    async fn worker_id(&self, peer_id: PeerId) -> Result<U256, ClientError>;

    /// Get current active worker set
    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError>;

    /// Check if client is registered on chain
    async fn is_client_registered(&self, client_id: PeerId) -> Result<bool, ClientError>;

    /// Get client's allocations for the current epoch.
    async fn current_allocations(
        &self,
        client_id: PeerId,
        worker_ids: Option<Vec<Worker>>,
    ) -> Result<Vec<Allocation>, ClientError>;

    /// Get the current list of all gateway clusters with their allocated CUs
    async fn all_gateways(&self, worker_id: U256) -> Result<Vec<GatewayCluster>, ClientError>;
}

pub async fn get_client(
    RpcArgs {
        rpc_url,
        l1_rpc_url,
    }: &RpcArgs,
) -> Result<Box<dyn Client>, ClientError> {
    let l1_transport = match l1_rpc_url {
        None => None,
        Some(rpc_url) => Some(Transport::connect(rpc_url).await?),
    };
    let l2_transport = Transport::connect(rpc_url).await?;

    match (l1_transport, l2_transport) {
        (None, Transport::Http(provider)) => {
            log::warn!("Layer 1 RPC URL not provided. Assuming the main RPC URL is L1");
            let client = Arc::new(provider);
            Ok(RpcProvider::new(client.clone(), client).await?)
        }
        (None, Transport::Ws(provider)) => {
            log::warn!("Layer 1 RPC URL not provided. Assuming the main RPC URL is L1");
            let client = Arc::new(provider);
            Ok(RpcProvider::new(client.clone(), client).await?)
        }
        (Some(Transport::Http(l1_provider)), Transport::Http(l2_provider)) => {
            Ok(RpcProvider::new(Arc::new(l1_provider), Arc::new(l2_provider)).await?)
        }
        (Some(Transport::Http(l1_provider)), Transport::Ws(l2_provider)) => {
            Ok(RpcProvider::new(Arc::new(l1_provider), Arc::new(l2_provider)).await?)
        }
        (Some(Transport::Ws(l1_provider)), Transport::Http(l2_provider)) => {
            Ok(RpcProvider::new(Arc::new(l1_provider), Arc::new(l2_provider)).await?)
        }
        (Some(Transport::Ws(l1_provider)), Transport::Ws(l2_provider)) => {
            Ok(RpcProvider::new(Arc::new(l1_provider), Arc::new(l2_provider)).await?)
        }
    }
}

#[derive(Clone)]
struct RpcProvider<L1, L2> {
    l1_client: Arc<Provider<L1>>,
    l2_client: Arc<Provider<L2>>,
    gateway_registry: GatewayRegistry<Provider<L2>>,
    network_controller: NetworkController<Provider<L2>>,
    worker_registration: WorkerRegistration<Provider<L2>>,
    allocations_viewer: AllocationsViewer<Provider<L2>>,
    default_strategy_addr: Address,
}

impl<L1, L2> RpcProvider<L1, L2>
where
    L1: JsonRpcClient + Clone + 'static,
    L2: JsonRpcClient + Clone + 'static,
{
    pub async fn new(
        l1_client: Arc<Provider<L1>>,
        l2_client: Arc<Provider<L2>>,
    ) -> Result<Box<Self>, ClientError> {
        let gateway_registry = GatewayRegistry::get(l2_client.clone());
        let default_strategy_addr = gateway_registry.default_strategy().call().await?;
        let network_controller = NetworkController::get(l2_client.clone());
        let worker_registration = WorkerRegistration::get(l2_client.clone());
        let allocations_viewer = AllocationsViewer::get(l2_client.clone());
        Ok(Box::new(Self {
            l1_client,
            l2_client,
            gateway_registry,
            worker_registration,
            network_controller,
            allocations_viewer,
            default_strategy_addr,
        }))
    }
}

#[async_trait]
impl<L1, L2> Client for RpcProvider<L1, L2>
where
    L1: JsonRpcClient + Clone + 'static,
    L2: JsonRpcClient + Clone + 'static,
{
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

    async fn current_epoch_start(&self) -> Result<SystemTime, ClientError> {
        let next_epoch_start_block = self.network_controller.next_epoch().call().await?;
        let epoch_length_blocks = self.network_controller.epoch_length().call().await?;
        let block_num: u64 = (next_epoch_start_block - epoch_length_blocks)
            .try_into()
            .expect("Epoch number should not exceed u64 range");
        log::debug!("Current epoch: {block_num} Epoch length: {epoch_length_blocks} Next epoch: {next_epoch_start_block}");
        // Blocks returned by `next_epoch()` and `epoch_length()` are **L1 blocks**
        let block = self
            .l1_client
            .get_block(BlockId::Number(block_num.into()))
            .await?
            .ok_or(ClientError::BlockNotFound)?;
        Ok(UNIX_EPOCH + Duration::from_secs(block.timestamp.as_u64()))
    }

    async fn worker_id(&self, peer_id: PeerId) -> Result<U256, ClientError> {
        let peer_id = peer_id.to_bytes().into();
        let id: U256 = self.worker_registration.worker_ids(peer_id).call().await?;
        Ok(id)
    }

    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError> {
        let workers_call = self.worker_registration.method("getActiveWorkers", ())?;
        let onchain_ids_call = self.worker_registration.method("getActiveWorkerIds", ())?;
        let mut multicall = contracts::multicall(self.l2_client.clone()).await?;
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

    async fn is_client_registered(&self, client_id: PeerId) -> Result<bool, ClientError> {
        let client_id = client_id.to_bytes().into();
        let gateway_info: contracts::Gateway =
            self.gateway_registry.get_gateway(client_id).call().await?;
        Ok(gateway_info.operator != Address::zero())
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
        let strategy = Strategy::get(strategy_addr, self.l2_client.clone());

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

        let mut multicall = contracts::multicall(self.l2_client.clone()).await?;
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

    async fn all_gateways(&self, worker_id: U256) -> Result<Vec<GatewayCluster>, ClientError> {
        const GATEWAYS_PAGE_SIZE: U256 = U256([10000, 0, 0, 0]);

        let latest_block = self.l2_client.get_block_number().await?;

        let mut clusters = HashMap::new();
        for page in 0.. {
            let allocations = self
                .allocations_viewer
                .get_allocations(worker_id, page.into(), GATEWAYS_PAGE_SIZE)
                .block(latest_block)
                .call()
                .await?;
            let page_size = U256::from(allocations.len());

            for allocation in allocations {
                let gateway_peer_id = match PeerId::from_bytes(&allocation.gateway_id) {
                    Ok(peer_id) => peer_id,
                    _ => continue,
                };
                clusters
                    .entry(allocation.operator)
                    .or_insert_with(|| GatewayCluster {
                        operator_addr: allocation.operator,
                        gateway_ids: Vec::new(),
                        allocated_computation_units: allocation.allocated,
                    })
                    .gateway_ids
                    .push(gateway_peer_id);
            }

            if page_size < GATEWAYS_PAGE_SIZE {
                break;
            }
        }
        Ok(clusters.into_values().collect())
    }
}
