use clap::Args;

#[derive(Args, Default)]
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
}
