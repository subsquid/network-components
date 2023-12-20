use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, RwLock};

use crate::allocations::AllocationsManager;
use contract_client::{AllocationsClient, WorkersClient};
use subsquid_network_transport::{Keypair, PeerId};

use crate::config::{Config, DatasetId};
use crate::network_state::NetworkState;
use crate::query::{Query, QueryResult};
use crate::server::{Message, Server};

pub struct QueryClient {
    network_state: Arc<RwLock<NetworkState>>,
    query_sender: mpsc::Sender<Query>,
    default_query_timeout: Duration,
}

impl QueryClient {
    pub async fn get_dataset_id(&self, dataset: &str) -> Option<DatasetId> {
        self.network_state.read().await.get_dataset_id(dataset)
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
            .unwrap_or(self.default_query_timeout);
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
    config: Config,
    keypair: Keypair,
    msg_receiver: mpsc::Receiver<Message>,
    msg_sender: mpsc::Sender<Message>,
    workers_client: Box<dyn WorkersClient>,
    allocations_client: Box<dyn AllocationsClient>,
    allocations_db_path: PathBuf,
) -> anyhow::Result<QueryClient> {
    let (query_sender, query_receiver) = mpsc::channel(100);
    let network_state = Arc::new(RwLock::new(NetworkState::new(
        config.available_datasets,
        config.worker_inactive_threshold,
        config.worker_greylist_time,
    )));
    let allocations_manager = Arc::new(RwLock::new(
        AllocationsManager::new(
            allocations_client,
            allocations_db_path,
            config.compute_units,
        )
        .await?,
    ));

    let server = Server::new(
        msg_receiver,
        msg_sender,
        query_receiver,
        network_state.clone(),
        allocations_manager,
        keypair,
        config.scheduler_id,
        config.send_metrics,
    );
    tokio::spawn(server.run(
        workers_client,
        config.summary_print_interval,
        config.workers_update_interval,
        config.allocate_interval,
    ));

    let client = QueryClient {
        network_state,
        query_sender,
        default_query_timeout: config.default_query_timeout,
    };
    Ok(client)
}
