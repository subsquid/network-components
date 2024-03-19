use ethers::contract::abigen;
use ethers::prelude::{Multicall, MulticallError};
use ethers::providers::Middleware;
use lazy_static::lazy_static;
use std::sync::Arc;

use crate::Address;

pub use gateway_registry::Gateway;
pub use worker_registration::Worker;

abigen!(GatewayRegistry, "abi/GatewayRegistry.json");
abigen!(NetworkController, "abi/NetworkController.json");
abigen!(Strategy, "abi/Strategy.json");
abigen!(WorkerRegistration, "abi/WorkerRegistration.json");

lazy_static! {
    pub static ref GATEWAY_REGISTRY_CONTRACT_ADDR: Address =
        std::env::var("GATEWAY_REGISTRY_CONTRACT_ADDR")
            .as_deref()
            .unwrap_or("0xCe360FB8D0d12C508BFE7153573D3C4aB476f6A1")
            .parse()
            .expect("Invalid GatewayRegistry contract address");
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

impl<T: Middleware> GatewayRegistry<T> {
    pub fn get(client: Arc<T>) -> Self {
        Self::new(*GATEWAY_REGISTRY_CONTRACT_ADDR, client)
    }
}

impl<T: Middleware> WorkerRegistration<T> {
    pub fn get(client: Arc<T>) -> Self {
        Self::new(*WORKER_REGISTRATION_CONTRACT_ADDR, client)
    }
}

impl<T: Middleware> NetworkController<T> {
    pub fn get(client: Arc<T>) -> Self {
        Self::new(*NETWORK_CONTROLLER_CONTRACT_ADDR, client)
    }
}

impl<T: Middleware> Strategy<T> {
    pub fn get(addr: impl Into<Address>, client: Arc<T>) -> Self {
        Self::new(addr, client)
    }
}

pub async fn multicall<T: Middleware>(client: Arc<T>) -> Result<Multicall<T>, MulticallError<T>> {
    Multicall::new(client, *MULTICALL_CONTRACT_ADDR).await
}
