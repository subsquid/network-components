use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, RwLock};

use crate::allocations::AllocationsManager;
use crate::chain_updates::ChainUpdatesHandler;
use contract_client::Client as ContractClient;
use subsquid_network_transport::transport::P2PTransportHandle;
use subsquid_network_transport::{Keypair, PeerId};

use crate::config::{Config, DatasetId};
use crate::network_state::NetworkState;
use crate::query::{Query, QueryResult};
use crate::server::{Message, MsgContent, Server};

pub struct QueryClient {
    network_state: Arc<RwLock<NetworkState>>,
    query_sender: mpsc::Sender<Query>,
}

impl QueryClient {
    pub async fn get_height(&self, dataset_id: &DatasetId) -> Option<u32> {
        self.network_state.read().await.get_height(dataset_id)
    }

    pub async fn find_worker(&self, dataset_id: &DatasetId, start_block: u32) -> Option<PeerId> {
        self.network_state
            .read()
            .await
            .find_worker(dataset_id, start_block)
    }

    pub async fn execute_query(
        &self,
        dataset_id: DatasetId,
        query: String,
        worker_id: PeerId,
        timeout: Option<impl Into<Duration>>,
        profiling: bool,
    ) -> anyhow::Result<QueryResult> {
        let timeout = timeout
            .map(Into::into)
            .unwrap_or(Config::get().default_query_timeout);
        let (result_sender, result_receiver) = oneshot::channel();
        let query = Query {
            dataset_id,
            query,
            worker_id,
            timeout,
            profiling,
            result_sender,
        };
        self.query_sender
            .send(query)
            .await
            .map_err(|_| anyhow::anyhow!("Query server closed"))?;
        result_receiver
            .await
            .map_err(|_| anyhow::anyhow!("Query dropped"))
    }
}

pub async fn get_client(
    keypair: Keypair,
    msg_receiver: mpsc::Receiver<Message>,
    transport_handle: P2PTransportHandle<MsgContent>,
    contract_client: Box<dyn ContractClient>,
    allocations_db_path: PathBuf,
) -> anyhow::Result<QueryClient> {
    let (query_sender, query_receiver) = mpsc::channel(100);
    let network_state = Arc::new(RwLock::new(NetworkState::default()));
    let allocations_manager = Arc::new(RwLock::new(
        AllocationsManager::new(allocations_db_path).await?,
    ));

    let chain_updates_handler = ChainUpdatesHandler::new(
        network_state.clone(),
        allocations_manager.clone(),
        contract_client,
        keypair.public().to_peer_id(),
    );
    chain_updates_handler.spawn().await?;

    let server = Server::new(
        msg_receiver,
        transport_handle,
        query_receiver,
        network_state.clone(),
        allocations_manager,
        keypair,
    );
    tokio::spawn(server.run());

    let client = QueryClient {
        network_state,
        query_sender,
    };
    Ok(client)
}
