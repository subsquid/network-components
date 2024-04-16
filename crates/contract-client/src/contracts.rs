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
abigen!(AllocationsViewer, "abi/AllocationsViewer.json");

lazy_static! {
    pub static ref GATEWAY_REGISTRY_CONTRACT_ADDR: Address =
        std::env::var("GATEWAY_REGISTRY_CONTRACT_ADDR")
            .as_deref()
            .unwrap_or("0xC52D401Cf4101e6b20F6F7D51c67f5B1dF9559af")
            .parse()
            .expect("Invalid GatewayRegistry contract address");
    pub static ref WORKER_REGISTRATION_CONTRACT_ADDR: Address =
        std::env::var("WORKER_REGISTRATION_CONTRACT_ADDR")
            .as_deref()
            .unwrap_or("0xCD8e983F8c4202B0085825Cf21833927D1e2b6Dc")
            .parse()
            .expect("Invalid WorkerRegistration contract address");
    pub static ref NETWORK_CONTROLLER_CONTRACT_ADDR: Address =
        std::env::var("NETWORK_CONTROLLER_CONTRACT_ADDR")
            .as_deref()
            .unwrap_or("0x68Fc7E375945d8C8dFb0050c337Ff09E962D976D")
            .parse()
            .expect("Invalid NetworkController contract address");
    pub static ref ALLOCATIONS_VIEWER_CONTRACT_ADDR: Address =
        std::env::var("ALLOCATIONS_VIEWER_CONTRACT_ADDR")
            .as_deref()
            .unwrap_or("0xEf55fB580dc7CA408a725b53F270277E81C1442f")
            .parse()
            .expect("Invalid AllocationsViewer contract address");
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

impl<T: Middleware> AllocationsViewer<T> {
    pub fn get(client: Arc<T>) -> Self {
        Self::new(*ALLOCATIONS_VIEWER_CONTRACT_ADDR, client)
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
