use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use collector_utils::Storage;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use sqd_contract_client::Client as ContractClient;
use sqd_network_transport::util::{CancellationToken, TaskManager};
use sqd_network_transport::PortalLogsCollectorEvent::LogQuery;
use sqd_network_transport::{PeerId, PortalLogsCollectorEvent, PortalLogsCollectorTransportHandle};

use crate::collector::PortalLogsCollector;

pub struct Server<T>
where
    T: Storage + Send + Sync + 'static,
{
    _transport_handle: PortalLogsCollectorTransportHandle,
    logs_collector: Arc<PortalLogsCollector<T>>,
    registered_gateways: Arc<Mutex<HashSet<PeerId>>>,
    task_manager: TaskManager,
    event_stream: Box<dyn Stream<Item = PortalLogsCollectorEvent> + Send + Unpin + 'static>,
    collector_index: usize,
    collector_group_size: usize,
    _phantom: PhantomData<T>,
}

impl<T> Server<T>
where
    T: Storage + Send + Sync + 'static,
{
    pub fn new(
        transport_handle: PortalLogsCollectorTransportHandle,
        event_stream: impl Stream<Item = PortalLogsCollectorEvent> + Send + Unpin + 'static,
        logs_collector: PortalLogsCollector<T>,
        collector_index: usize,
        collector_group_size: usize,
    ) -> Self {
        Self {
            _transport_handle: transport_handle,
            logs_collector: Arc::new(logs_collector),
            registered_gateways: Default::default(),
            task_manager: Default::default(),
            event_stream: Box::new(event_stream),
            collector_index,
            collector_group_size,
            _phantom: Default::default(),
        }
    }

    pub async fn run(
        mut self,
        contract_client: Arc<dyn ContractClient>,
        collection_interval: Duration,
        portal_update_interval: Duration,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        log::info!("Starting logs collector server");

        // Get registered gateways from chain
        let gateways = contract_client
            .active_portals()
            .await?
            .into_iter()
            .collect();
        *self.registered_gateways.lock() = gateways;

        self.spawn_portal_update_task(contract_client, portal_update_interval);

        self.spawn_dumping_task(collection_interval);

        self.run_collecting_task(cancellation_token.child_token())
            .await;

        log::info!("Server shutting down");
        self.task_manager.await_stop().await;
        Ok(())
    }

    fn should_process(&self, peer_id: &PeerId) -> bool {
        if let Some(byte) = peer_id.to_bytes().last() {
            (*byte as usize) % self.collector_group_size == self.collector_index
        } else {
            false
        }
    }

    async fn run_collecting_task(&mut self, cancel_token: CancellationToken) {
        loop {
            tokio::select! {
                Some(LogQuery { peer_id, logs }) = self.event_stream.next() => {
                    if !self.should_process(&peer_id) {
                        continue
                    }
                    if self.registered_gateways.lock().contains(&peer_id) {
                        log::debug!("Got log from {peer_id:?}: {logs:?}");
                        self.logs_collector.buffer_logs(peer_id, logs);
                    } else {
                        log::error!("Got unauthorized log from: {peer_id:?}");
                    }
                },
                _ = cancel_token.cancelled() => break,
            };
        }
    }

    fn spawn_dumping_task(&mut self, interval: Duration) {
        let collector = self.logs_collector.clone();
        self.task_manager.spawn_periodic(
            move |_| {
                let local_collector = collector.clone();
                async move {
                    let res = (*local_collector).dump_buffer().await;
                    let _ =
                        res.inspect_err(|err| log::error!("Error while dumping records: {err:?}"));
                }
            },
            interval,
        );
    }

    fn spawn_portal_update_task(
        &mut self,
        contract_client: Arc<dyn ContractClient>,
        interval: Duration,
    ) {
        log::info!("Starting gateway update task");

        let registered_gateways = self.registered_gateways.clone();
        let contract_client: Arc<dyn ContractClient> = contract_client;
        let task = move |_| {
            let registered_gateways = registered_gateways.clone();
            let contract_client = contract_client.clone();
            async move {
                let gateways = match contract_client.active_portals().await {
                    Ok(gateways) => gateways,
                    Err(e) => return log::error!("Error getting registered gateways: {e:?}"),
                };
                *registered_gateways.lock() = gateways.into_iter().collect::<HashSet<PeerId>>();
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }
}
