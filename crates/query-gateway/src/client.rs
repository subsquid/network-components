use futures::Stream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, RwLock};

use contract_client::Client as ContractClient;
use subsquid_network_transport::util::TaskManager;
use subsquid_network_transport::PeerId;
use subsquid_network_transport::{GatewayEvent, GatewayTransportHandle};

use crate::allocations::AllocationsManager;
use crate::chain_updates::ChainUpdatesHandler;
use crate::config::{Config, DatasetId};
use crate::network_state::NetworkState;
use crate::query::{Query, QueryResult};
use crate::server::Server;

pub struct QueryClient {
    network_state: Arc<RwLock<NetworkState>>,
    query_sender: mpsc::Sender<Query>,
    _task_manager: TaskManager,
}

impl QueryClient {
    pub fn new<S: Stream<Item = GatewayEvent> + Send + Unpin + 'static>(
        network_state: Arc<RwLock<NetworkState>>,
        query_sender: mpsc::Sender<Query>,
        chain_updates_handler: ChainUpdatesHandler,
        server: Server<S>,
    ) -> Self {
        let mut task_manager = TaskManager::default();
        task_manager.spawn(|c| server.run(c));

        let chain_updates_task = move |_| {
            let chain_updates_handler = chain_updates_handler.clone();
            async move {
                chain_updates_handler
                    .pull_chain_updates()
                    .await
                    .unwrap_or_else(|e| log::error!("Error pulling updates from chain: {e:?}"))
            }
        };
        let interval = Config::get().workers_update_interval;
        task_manager.spawn_periodic(chain_updates_task, interval);

        Self {
            network_state,
            query_sender,
            _task_manager: task_manager,
        }
    }

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
            .try_send(query)
            .map_err(|_| anyhow::anyhow!("Cannot send query"))?;
        result_receiver
            .await
            .map_err(|_| anyhow::anyhow!("Query dropped"))
    }
}

pub async fn get_client<S: Stream<Item = GatewayEvent> + Send + Unpin + 'static>(
    local_peer_id: PeerId,
    incoming_messages: S,
    transport_handle: GatewayTransportHandle,
    contract_client: Box<dyn ContractClient>,
    network_state: Arc<RwLock<NetworkState>>,
    allocations_db_path: PathBuf,
) -> anyhow::Result<QueryClient> {
    let (query_sender, query_receiver) = mpsc::channel(1000);

    let allocations_manager = Arc::new(RwLock::new(
        AllocationsManager::new(allocations_db_path).await?,
    ));

    let chain_updates_handler = ChainUpdatesHandler::new(
        network_state.clone(),
        allocations_manager.clone(),
        contract_client,
        local_peer_id,
    );
    chain_updates_handler.pull_chain_updates().await?;

    let server = Server::new(
        local_peer_id,
        incoming_messages,
        transport_handle,
        query_receiver,
        network_state.clone(),
        allocations_manager,
    );

    let client = QueryClient::new(network_state, query_sender, chain_updates_handler, server);
    Ok(client)
}
