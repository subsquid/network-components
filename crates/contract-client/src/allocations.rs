use async_trait::async_trait;
use ethers::contract::abigen;
use ethers::prelude::gas_escalator::{Frequency, GeometricGasPrice};
use ethers::prelude::gas_oracle::{GasOracleMiddleware, ProviderOracle};
use ethers::prelude::{
    GasEscalatorMiddleware, JsonRpcClient, LocalWallet, Middleware, MiddlewareBuilder,
    NonceManagerMiddleware, Signer, SignerMiddleware, U64,
};
use ethers::providers::Provider;
use lazy_static::lazy_static;
use std::str::FromStr;
use std::sync::Arc;

use crate::cli::RpcArgs;
use crate::error::ClientError;
use crate::transport::Transport;
use crate::{Address, U256};

abigen!(GatewayRegistry, "abi/GatewayRegistry.json");

lazy_static! {
    pub static ref GATEWAY_REGISTRY_CONTRACT_ADDR: Address =
        std::env::var("GATEWAY_REGISTRY_CONTRACT_ADDR")
            .as_deref()
            .unwrap_or("0x9657d3dB87963d5e17dDa0746972E6958401Ab2a")
            .parse()
            .expect("Invalid GatewayRegistry contract address");
    pub static ref GATEWAY_CONTRACT_CREATION_BLOCK: u64 =
        std::env::var("GATEWAY_CONTRACT_CREATION_BLOCK")
            .as_deref()
            .unwrap_or("53976941")
            .parse()
            .expect("Invalid GatewayRegistry creation block");
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

impl From<Allocation> for (U256, U256) {
    fn from(allocation: Allocation) -> Self {
        (allocation.worker_onchain_id, allocation.computation_units)
    }
}

fn allocations_from_events(
    events: impl IntoIterator<Item = AllocatedCUsFilter>,
) -> impl IntoIterator<Item = Allocation> {
    events
        .into_iter()
        .flat_map(|e| e.worker_ids.into_iter().zip(e.cus).map(Into::into))
}

#[async_trait]
pub trait Client: Send + Sync {
    /// Get the connected wallet's address
    fn address(&self) -> Address;

    /// Get available (i.e. non-allocated) computation units
    async fn available_cus(&self) -> Result<U256, ClientError>;

    /// Get allocated computation units and last scanned block number
    async fn allocated_cus(
        &self,
        from_block: Option<u64>,
    ) -> Result<(Vec<Allocation>, u64), ClientError>;

    /// Allocate computation units to workers
    async fn allocate_cus(&self, allocations: Vec<Allocation>) -> Result<(), ClientError>;
}

pub async fn get_client(rpc_args: &RpcArgs) -> Result<Box<dyn Client>, ClientError> {
    match Transport::connect(&rpc_args.rpc_url).await? {
        Transport::Http(provider) => Ok(RpcProviderWithSigner::new(provider, rpc_args).await?),
        Transport::Ws(provider) => Ok(RpcProviderWithSigner::new(provider, rpc_args).await?),
    }
}

fn get_wallet(rpc_args: &RpcArgs, chain_id: u64) -> Result<LocalWallet, ClientError> {
    let password = &rpc_args.keystore_password;
    match &rpc_args.wallet_key {
        None => (),
        Some(key) => return Ok(LocalWallet::from_str(key)?.with_chain_id(chain_id)),
    }
    match &rpc_args.keystore_path {
        None => Err(ClientError::WalletMissing),
        Some(path) => Ok(LocalWallet::decrypt_keystore(path, password)?.with_chain_id(chain_id)),
    }
}

struct RpcProviderWithSigner<T: JsonRpcClient + Clone + 'static> {
    provider: Provider<T>,
    address: Address,
    max_log_blocks: U64,
    contract: GatewayRegistry<
        NonceManagerMiddleware<
            SignerMiddleware<
                GasOracleMiddleware<
                    GasEscalatorMiddleware<Provider<T>>,
                    ProviderOracle<Provider<T>>,
                >,
                LocalWallet,
            >,
        >,
    >,
}

impl<T: JsonRpcClient + Clone + 'static> RpcProviderWithSigner<T> {
    pub async fn new(provider: Provider<T>, rpc_args: &RpcArgs) -> Result<Box<Self>, ClientError> {
        let chain_id = provider.get_chainid().await?.as_u64();
        let wallet = get_wallet(rpc_args, chain_id)?;
        let address = wallet.address();
        let escalator = GeometricGasPrice::new(1.125, 60_u64, None::<u64>);
        let gas_oracle = ProviderOracle::new(provider.clone());
        let middleware = provider
            .clone()
            .wrap_into(|p| GasEscalatorMiddleware::new(p, escalator, Frequency::PerBlock))
            .wrap_into(|p| GasOracleMiddleware::new(p, gas_oracle))
            .with_signer(wallet)
            .nonce_manager(address);
        let client = Arc::new(middleware);
        let contract = GatewayRegistry::new(*GATEWAY_REGISTRY_CONTRACT_ADDR, client);
        Ok(Box::new(Self {
            provider,
            address,
            contract,
            max_log_blocks: rpc_args.max_log_blocks.into(),
        }))
    }
}

#[async_trait]
impl<M: JsonRpcClient + Clone + 'static> Client for RpcProviderWithSigner<M> {
    fn address(&self) -> Address {
        self.address
    }

    async fn available_cus(&self) -> Result<U256, ClientError> {
        Ok(self.contract.computation_units(self.address).call().await?)
    }

    async fn allocated_cus(
        &self,
        from_block: Option<u64>,
    ) -> Result<(Vec<Allocation>, u64), ClientError> {
        let mut from_block = from_block
            .unwrap_or(*GATEWAY_CONTRACT_CREATION_BLOCK)
            .into();
        let current_block = self.provider.get_block_number().await?;
        log::info!("Getting allocated compute units. start_block={from_block} current_block={current_block}");

        let mut to_block = std::cmp::min(from_block + self.max_log_blocks, current_block);
        let mut allocations = Vec::new();
        while from_block < current_block {
            let events = self
                .contract
                .event::<AllocatedCUsFilter>()
                .from_block(from_block)
                .to_block(to_block)
                .topic1(self.address)
                .query()
                .await?;
            log::trace!("Events from {from_block} to {to_block}: {events:?}");
            allocations.extend(allocations_from_events(events));
            from_block = to_block + 1;
            to_block = std::cmp::min(from_block + self.max_log_blocks, current_block);
        }
        Ok((allocations, to_block.as_u64()))
    }

    async fn allocate_cus(&self, allocations: Vec<Allocation>) -> Result<(), ClientError> {
        log::info!("Allocating compute units: {allocations:?}");
        let (worker_ids, cus) = allocations
            .into_iter()
            .map(|a| (a.worker_onchain_id, a.computation_units))
            .unzip();
        let receipt = self
            .contract
            .allocate_computation_units(worker_ids, cus)
            .send()
            .await?
            .await?
            .ok_or_else(|| ClientError::TxReceiptMissing);
        log::debug!("Tx receipt: {receipt:?}");
        Ok(())
    }
}
