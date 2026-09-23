use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use collector_utils::Storage;
use futures::StreamExt;
use parking_lot::Mutex;
use sqd_contract_client::Client as ContractClient;
use sqd_messages::{LogsRequest, QueryLogs};
use sqd_network_transport::util::{CancellationToken, TaskManager};
use sqd_network_transport::{LogsCollectorTransport, PeerId};
use tokio::time::Instant;
use tokio_util::task::AbortOnDropHandle;

use crate::collector::{rows_from_logs, LogsCollector};

const MAX_PAGES: usize = 5;

/// Where worker logs are requested from: the P2P transport, or a fake in tests.
pub trait LogsSource: Send + Sync + 'static {
    fn request_logs(
        &self,
        worker_id: PeerId,
        request: LogsRequest,
    ) -> impl Future<Output = anyhow::Result<QueryLogs>> + Send;
}

impl LogsSource for LogsCollectorTransport {
    async fn request_logs(
        &self,
        worker_id: PeerId,
        request: LogsRequest,
    ) -> anyhow::Result<QueryLogs> {
        LogsCollectorTransport::request_logs(self, worker_id, request)
            .await
            // The `Debug` form is what has always been logged for these errors.
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }
}

pub struct Server<L, T>
where
    L: LogsSource,
    T: Storage + Send + Sync + 'static,
{
    transport_handle: L,
    logs_collector: LogsCollector<T>,
    registered_workers: Arc<Mutex<HashSet<PeerId>>>,
}

impl<L, T> Server<L, T>
where
    L: LogsSource,
    T: Storage + Send + Sync + 'static,
{
    pub fn new(transport: L, logs_collector: LogsCollector<T>) -> Self {
        Self {
            transport_handle: transport,
            logs_collector,
            registered_workers: Default::default(),
        }
    }

    pub async fn run(
        self,
        contract_client: Arc<dyn ContractClient>,
        collection_interval: Duration,
        backlog_collection_interval: Option<Duration>,
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

        let mut task_manager = TaskManager::default();
        self.spawn_worker_update_task(&mut task_manager, contract_client, worker_update_interval);

        Arc::new(self)
            .run_collecting_task(
                collection_interval,
                backlog_collection_interval,
                concurrent_workers,
                cancellation_token.child_token(),
            )
            .await;

        log::info!("Server shutting down");
        task_manager.await_stop().await;
        Ok(())
    }

    async fn run_collecting_task(
        self: Arc<Self>,
        interval: Duration,
        backlog_interval: Option<Duration>,
        concurrent_jobs: usize,
        cancel_token: CancellationToken,
    ) {
        // A backlog never makes the next round start later than it would anyway.
        let backlog_interval = backlog_interval.map(|backlog| backlog.min(interval));
        match backlog_interval {
            Some(backlog) => log::info!(
                "Collecting logs every {interval:?}, or every {backlog:?} while workers have a backlog"
            ),
            None => log::info!("Collecting logs every {interval:?}"),
        }
        let mut interval = tokio::time::interval(interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // With a backlog the next tick is often already due, and shutdown
                // shouldn't have to win a coin flip against it.
                biased;
                _ = cancel_token.cancelled() => break,
                _ = interval.tick() => (),
            };
            let round_start = Instant::now();

            let workers = self.registered_workers.lock().clone();
            log::info!("Collecting logs from {} workers", workers.len());
            let last_timestamps = match self.logs_collector.last_timestamps().await {
                Ok(timestamps) => timestamps,
                Err(e) => {
                    log::warn!("Couldn't read last stored logs: {e:?}");
                    continue;
                }
            };

            let backlogged_workers = futures::stream::iter(workers)
                .map(|worker_id| {
                    let from_timestamp_ms = last_timestamps
                        .get(&worker_id.to_string())
                        .map(|ts| ts + 1)
                        .unwrap_or(0);
                    // A task per worker, so collecting isn't confined to this thread.
                    let server = self.clone();
                    AbortOnDropHandle::new(tokio::spawn(async move {
                        server.collect_logs(worker_id, from_timestamp_ms).await
                    }))
                })
                .buffer_unordered(concurrent_jobs)
                .take_until(cancel_token.cancelled())
                .fold(0, |backlogged, joined| async move {
                    match joined {
                        Ok(true) => backlogged + 1,
                        Ok(false) => backlogged,
                        Err(e) => {
                            log::error!("Log collection task failed: {e}");
                            backlogged
                        }
                    }
                })
                .await;

            if let Err(e) = self.logs_collector.dump_buffer().await {
                // Don't start the next round early: it would fetch the same logs again,
                // most likely only to fail the same way.
                log::warn!("Couldn't store logs: {e:?}");
                continue;
            }

            if backlogged_workers == 0 {
                continue;
            }
            match backlog_interval {
                Some(backlog_interval) => {
                    let next_round = round_start + backlog_interval;
                    log::info!(
                        "{backlogged_workers} workers have more logs, starting the next round in {:?}",
                        next_round.saturating_duration_since(Instant::now())
                    );
                    interval.reset_at(next_round);
                }
                None => log::info!(
                    "{backlogged_workers} workers have more logs, collecting them next round"
                ),
            }
        }
    }

    /// Returns `true` if the worker may have logs left that this round didn't collect:
    /// it had more than `MAX_PAGES` pages, or the buffer filled up. A failed request
    /// returns `false`, so unreachable workers never make rounds start early.
    async fn collect_logs(&self, worker_id: PeerId, mut from_timestamp_ms: u64) -> bool {
        // Don't even request logs we'd have to drop. The buffer drains every round,
        // so these workers are picked up again next time.
        if self.logs_collector.is_full() {
            log::debug!("Buffer full, skipping log collection from {worker_id}");
            return true;
        }
        let mut last_query_id = None;
        for page in 0..MAX_PAGES {
            if page == 0 {
                log::debug!("Collecting logs from {worker_id} since {from_timestamp_ms}");
            } else {
                log::debug!("Collecting more logs from {worker_id} since {from_timestamp_ms}");
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
                    log::warn!("Error getting logs from {worker_id}: {e:#}");
                    return false;
                }
            };

            let Some(last_log) = logs.queries_executed.last() else {
                return false;
            };
            last_query_id = last_log.query.as_ref().map(|q| q.query_id.clone());
            from_timestamp_ms = last_log.timestamp_ms;

            // Verifying signatures is CPU-bound: keep it off the threads driving the network.
            let rows = tokio::task::spawn_blocking(move || {
                rows_from_logs(worker_id, logs.queries_executed)
            });
            let rows = match rows.await {
                Ok(rows) => rows,
                Err(e) => {
                    log::error!("Couldn't process logs from {worker_id}: {e}");
                    return false;
                }
            };

            if !self.logs_collector.buffer_logs(worker_id, rows) {
                return true;
            }
            if !logs.has_more {
                return false;
            }
            if self.logs_collector.is_full() {
                log::debug!("Buffer full, stopping log collection from {worker_id}");
                return true;
            }
        }
        log::debug!("Logs from {worker_id} didn't fit in {MAX_PAGES} pages, continuing next round");
        true
    }

    fn spawn_worker_update_task(
        &self,
        task_manager: &mut TaskManager,
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
        task_manager.spawn_periodic(task, interval);
    }
}

#[cfg(test)]
mod tests;
