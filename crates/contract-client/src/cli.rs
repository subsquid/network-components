use clap::Args;
use std::path::PathBuf;

#[derive(Args, Default)]
pub struct RpcArgs {
    #[arg(
        long,
        env,
        help = "Blockchain RPC URL",
        default_value = "http://127.0.0.1:8545/"
    )]
    pub rpc_url: String,
    #[arg(long, env, help = "Raw private key in hex format")]
    pub wallet_key: Option<String>,
    #[arg(
        long,
        env,
        help = "Path to the keystore file (ignored if --wallet is set)"
    )]
    pub keystore_path: Option<PathBuf>,
    #[arg(
        long,
        env,
        help = "Password to decrypt the keystore",
        default_value = ""
    )]
    pub keystore_password: String,
    #[arg(
        long,
        env = "MAX_GET_LOG_BLOCKS",
        help = "Maximum number of blocks to scan for events in a single RPC call",
        default_value = "1000000000"
    )]
    pub max_log_blocks: u32,
}
