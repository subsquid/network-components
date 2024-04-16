use clap::Args;

use crate::Address;

#[derive(Args)]
pub struct RpcArgs {
    #[arg(
        long,
        env,
        help = "Blockchain RPC URL",
        default_value = "http://127.0.0.1:8545/"
    )]
    pub rpc_url: String,
    #[arg(
        long,
        env,
        help = "Layer 1 blockchain RPC URL. If not provided, rpc_url is assumed to be L1"
    )]
    pub l1_rpc_url: Option<String>,
    #[command(flatten)]
    pub contract_addrs: ContractAddrs,
}

#[derive(Args)]
pub struct ContractAddrs {
    #[arg(long, env)]
    pub gateway_registry_contract_addr: Address,
    #[arg(long, env)]
    pub worker_registration_contract_addr: Address,
    #[arg(long, env)]
    pub network_controller_contract_addr: Address,
    #[arg(long, env)]
    pub allocations_viewer_contract_addr: Address,
    #[arg(long, env)]
    pub multicall_contract_addr: Option<Address>,
}
