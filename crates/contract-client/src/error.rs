use ethers::contract::{ContractError, MulticallError};
use ethers::prelude::{AbiError, Middleware};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Invalid RPC URL: {0:?}")]
    InvalidRpcUrl(#[from] url::ParseError),
    #[error("Invalid Peer ID: {0:?}")]
    InvalidPeerId(#[from] libp2p::identity::ParseError),
    #[error("Contract error: {0}")]
    Contract(String),
    #[error("RPC provider error: {0}")]
    Provider(#[from] ethers::providers::ProviderError),
    #[error("Unsupported RPC protocol")]
    InvalidProtocol,
    #[error("Transaction receipt missing")]
    TxReceiptMissing,
}

impl<M: Middleware> From<ContractError<M>> for ClientError {
    fn from(err: ContractError<M>) -> Self {
        Self::Contract(err.to_string())
    }
}

impl<M: Middleware> From<MulticallError<M>> for ClientError {
    fn from(err: MulticallError<M>) -> Self {
        Self::Contract(err.to_string())
    }
}

impl From<AbiError> for ClientError {
    fn from(err: AbiError) -> Self {
        Self::Contract(err.to_string())
    }
}
