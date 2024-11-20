use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use collector_utils::Storage;
use futures::StreamExt;
use parking_lot::Mutex;
use sqd_contract_client::Client as ContractClient;
use sqd_messages::LogsRequest;
use sqd_network_transport::util::{CancellationToken, TaskManager};
use sqd_network_transport::{LogsCollectorTransport, PeerId};

use crate::collector::LogsCollector;

const MAX_PAGES: usize = 5;

pub struct Server<T>
where
    T: Storage + Send + Sync + 'static,
{
    transport_handle: Arc<LogsCollectorTransport>,
    logs_collector: LogsCollector<T>,
    registered_workers: Arc<Mutex<HashSet<PeerId>>>,
    task_manager: TaskManager,
}

impl<T> Server<T>
where
    T: Storage + Send + Sync + 'static,
{
    pub fn new(transport: LogsCollectorTransport, logs_collector: LogsCollector<T>) -> Self {
        Self {
            transport_handle: Arc::new(transport),
            logs_collector,
            registered_workers: Default::default(),
            task_manager: Default::default(),
        }
    }

    pub async fn run(
        mut self,
        contract_client: Arc<dyn ContractClient>,
        collection_interval: Duration,
        worker_update_interval: Duration,
        concurrent_workers: usize,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        log::info!("Starting logs collector server");

        // Get registered workers from chain
        let workers = contract_client
            .active_workers()
            .await?
            .into_iter()
            .map(|w| w.peer_id)
            .collect();
        *self.registered_workers.lock() = workers;

        self.spawn_worker_update_task(contract_client, worker_update_interval);
        self.spawn_transport_task();

        self.run_collecting_task(
            collection_interval,
            concurrent_workers,
            cancellation_token.child_token(),
        )
        .await;

        log::info!("Server shutting down");
        self.task_manager.await_stop().await;
        Ok(())
    }

    async fn run_collecting_task(
        &mut self,
        interval: Duration,
        concurrent_jobs: usize,
        cancel_token: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => (),
                _ = cancel_token.cancelled() => break,
            };

            let workers = self.registered_workers.lock().clone();
            log::info!("Collecting logs from {} workers", workers.len());
            let last_timestamps = match self.logs_collector.last_timestamps().await {
                Ok(timestamps) => timestamps,
                Err(e) => {
                    log::warn!("Couldn't read last stored logs: {e:?}");
                    continue;
                }
            };

            futures::stream::iter(workers.into_iter())
                .map(|worker_id| {
                    self.collect_logs(
                        worker_id,
                        last_timestamps
                            .get(&worker_id.to_string())
                            .map(|ts| ts + 1)
                            .unwrap_or(0),
                    )
                })
                .buffer_unordered(concurrent_jobs)
                .take_until(cancel_token.cancelled())
                .collect::<()>()
                .await;

            self.logs_collector
                .dump_buffer()
                .await
                .unwrap_or_else(|e| log::warn!("Couldn't store logs: {e:?}"));
        }
    }

    async fn collect_logs(&self, worker_id: PeerId, mut from_timestamp_ms: u64) {
        let mut last_query_id = None;
        for page in 0..MAX_PAGES {
            if page == 0 {
                log::debug!("Collecting logs from {worker_id:?} since {from_timestamp_ms}");
            } else {
                log::debug!("Collecting more logs from {worker_id:?} since {from_timestamp_ms}");
            }
            let logs = match self
                .transport_handle
                .request_logs(
                    worker_id,
                    LogsRequest {
                        from_timestamp_ms,
                        last_received_query_id: last_query_id,
                    },
                )
                .await
            {
                Ok(logs) => logs,
                Err(e) => {
                    return log::warn!("Error getting logs from {worker_id:?}: {e:?}");
                }
            };

            let Some(last_log) = logs.queries_executed.last() else {
                return;
            };
            last_query_id = last_log.query.as_ref().map(|q| q.query_id.clone());
            from_timestamp_ms = last_log.timestamp_ms;

            self.logs_collector
                .buffer_logs(worker_id, logs.queries_executed);

            if !logs.has_more {
                return;
            }
        }
        log::warn!("Logs from {worker_id} didn't fit in {MAX_PAGES} pages, giving up");
    }

    fn spawn_worker_update_task(
        &mut self,
        contract_client: Arc<dyn ContractClient>,
        interval: Duration,
    ) {
        log::info!("Starting worker update task");
        let registered_workers = self.registered_workers.clone();
        let contract_client: Arc<dyn ContractClient> = contract_client;
        let task = move |_| {
            let registered_workers = registered_workers.clone();
            let contract_client = contract_client.clone();
            async move {
                let workers = match contract_client.active_workers().await {
                    Ok(workers) => workers,
                    Err(e) => return log::error!("Error getting registered workers: {e:?}"),
                };
                *registered_workers.lock() = workers
                    .into_iter()
                    .map(|w| w.peer_id)
                    .collect::<HashSet<PeerId>>();
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }

    fn spawn_transport_task(&mut self) {
        let transport_handle = self.transport_handle.clone();
        self.task_manager.spawn(|cancel_token| async move {
            transport_handle.run(cancel_token).await;
        });
    }
}
