use futures::{Stream, StreamExt};
use itertools::Itertools;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use prometheus_client::registry::Registry;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};

use subsquid_messages::signatures::msg_hash;
use subsquid_messages::{Ping, Pong, RangeSet};
use subsquid_network_transport::util::{CancellationToken, TaskManager};
use subsquid_network_transport::{PeerId, SchedulerEvent, SchedulerTransportHandle};

use crate::cli::Config;
use crate::data_chunk::{chunks_to_worker_state, DataChunk};
use crate::metrics::{MetricsEvent, MetricsWriter};
use crate::metrics_server;
use crate::scheduler::{ChunkStatus, Scheduler};
use crate::scheduling_unit::{SchedulingUnit, UnitId};
use crate::storage::S3Storage;
use crate::worker_state::WorkerState;

const WORKER_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const CHUNKS_SUMMARY_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

pub struct Server<S: Stream<Item = SchedulerEvent> + Send + Unpin + 'static> {
    incoming_events: S,
    incoming_units: Receiver<SchedulingUnit>,
    transport_handle: SchedulerTransportHandle,
    scheduler: Arc<RwLock<Scheduler>>,
    metrics_writer: Arc<RwLock<MetricsWriter>>,
    task_manager: TaskManager,
}

impl<S: Stream<Item = SchedulerEvent> + Send + Unpin + 'static> Server<S> {
    pub fn new(
        incoming_events: S,
        incoming_units: Receiver<SchedulingUnit>,
        transport_handle: SchedulerTransportHandle,
        scheduler: Scheduler,
        metrics_writer: MetricsWriter,
    ) -> Self {
        let scheduler = Arc::new(RwLock::new(scheduler));
        let metrics_writer = Arc::new(RwLock::new(metrics_writer));
        Self {
            incoming_events,
            incoming_units,
            transport_handle,
            scheduler,
            metrics_writer,
            task_manager: Default::default(),
        }
    }

    pub async fn run(
        mut self,
        contract_client: Box<dyn contract_client::Client>,
        storage_client: S3Storage,
        metrics_listen_addr: SocketAddr,
        metrics_registry: Registry,
    ) -> anyhow::Result<()> {
        log::info!("Starting scheduler server");

        // Get worker set immediately to accept pings
        let workers = contract_client.active_workers().await?;
        self.scheduler.write().await.update_workers(workers);

        self.spawn_scheduling_task(contract_client, storage_client.clone())
            .await?;
        self.spawn_worker_monitoring_task();
        self.spawn_metrics_server_task(metrics_listen_addr, metrics_registry);
        self.spawn_jail_inactive_workers_task(storage_client.clone());
        self.spawn_jail_stale_workers_task(storage_client.clone());
        self.spawn_jail_unreachable_workers_task(storage_client);
        self.spawn_regenerate_signatures_task();
        self.spawn_chunks_summary_task();

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        loop {
            tokio::select! {
                Some(ev) = self.incoming_events.next() => self.on_incoming_event(ev).await, // FIXME: blocking event loop
                Some(unit) = self.incoming_units.recv() => self.on_new_unit(unit).await, // FIXME: blocking event loop
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
                else => break
            }
        }

        log::info!("Server shutting down");
        self.task_manager.await_stop().await;
        Ok(())
    }

    async fn on_incoming_event(&mut self, ev: SchedulerEvent) {
        match ev {
            SchedulerEvent::Ping { peer_id, ping } => self.ping(peer_id, ping).await,
            SchedulerEvent::PeerProbed { peer_id, reachable } => self
                .scheduler
                .read()
                .await
                .worker_dialed(peer_id, reachable),
        }
    }

    async fn ping(&mut self, peer_id: PeerId, ping: Ping) {
        log::debug!("Got ping from {peer_id}");
        let ping_hash = msg_hash(&ping);
        let status = self.scheduler.read().await.ping(peer_id, ping.clone());
        self.write_metrics(peer_id, ping).await;
        let pong = Pong {
            ping_hash,
            status: Some(status),
        };
        self.transport_handle
            .send_pong(peer_id, pong)
            .unwrap_or_else(|_| log::error!("Error sending pong: queue full"));
    }

    async fn write_metrics(&mut self, peer_id: PeerId, msg: impl Into<MetricsEvent>) {
        self.metrics_writer
            .write()
            .await
            .write_metrics(Some(peer_id), msg)
            .await
            .unwrap_or_else(|e| log::error!("Error writing metrics: {e:?}"));
    }

    async fn on_new_unit(&self, unit: SchedulingUnit) {
        self.scheduler.write().await.new_unit(unit)
    }

    async fn spawn_scheduling_task(
        &mut self,
        contract_client: Box<dyn contract_client::Client>,
        storage_client: S3Storage,
    ) -> anyhow::Result<()> {
        log::info!("Starting scheduling task");
        let scheduler = self.scheduler.clone();
        let last_epoch = Arc::new(Mutex::new(contract_client.current_epoch().await?));
        let contract_client: Arc<dyn contract_client::Client> = contract_client.into();
        let schedule_interval = Config::get().schedule_interval_epochs;

        let task = move |_| {
            let scheduler = scheduler.clone();
            let contract_client = contract_client.clone();
            let storage_client = storage_client.clone();
            let last_epoch = last_epoch.clone();
            async move {
                // Get current epoch number
                let current_epoch = match contract_client.current_epoch().await {
                    Ok(epoch) => epoch,
                    Err(e) => return log::error!("Error getting epoch: {e:?}"),
                };

                // Update workers every epoch
                let mut last_epoch = last_epoch.lock().await;
                if current_epoch > *last_epoch {
                    match contract_client.active_workers().await {
                        Ok(workers) => scheduler.write().await.update_workers(workers),
                        Err(e) => log::error!("Error getting workers: {e:?}"),
                    }
                    *last_epoch = current_epoch;
                }

                // Schedule chunks every `schedule_interval_epochs`
                let last_schedule_epoch = scheduler.read().await.last_schedule_epoch();
                if current_epoch >= last_schedule_epoch + schedule_interval {
                    let mut scheduler = scheduler.write().await;
                    scheduler.schedule(current_epoch);
                    match scheduler.to_json() {
                        Ok(state) => storage_client.save_scheduler(state).await,
                        Err(e) => log::error!("Error serializing scheduler: {e:?}"),
                    }
                }
            }
        };
        self.task_manager
            .spawn_periodic(task, WORKER_REFRESH_INTERVAL);
        Ok(())
    }

    fn spawn_worker_monitoring_task(&mut self) {
        log::info!("Starting monitoring task");
        let scheduler = self.scheduler.clone();
        let transport_handle = self.transport_handle.clone();
        let interval = Config::get().worker_monitoring_interval;

        let task = move |_| {
            let scheduler = scheduler.clone();
            let transport_handle = transport_handle.clone();
            async move {
                log::info!("Dialing workers...");
                let workers = scheduler.read().await.workers_to_dial();
                for peer_id in workers {
                    transport_handle
                        .probe_peer(peer_id)
                        .unwrap_or_else(|_| log::error!("Cannot probe {peer_id}: queue full"))
                }
            }
        };

        self.task_manager.spawn_periodic(task, interval);
    }

    fn spawn_metrics_server_task(
        &mut self,
        metrics_listen_addr: SocketAddr,
        metrics_registry: Registry,
    ) {
        let scheduler = self.scheduler.clone();
        let task = move |cancel_token: CancellationToken| async move {
            metrics_server::run_server(
                scheduler,
                metrics_listen_addr,
                metrics_registry,
                cancel_token,
            )
            .await
            .unwrap_or_else(|e| log::error!("Metrics server crashed: {e:?}"));
        };
        self.task_manager.spawn(task);
    }

    fn spawn_regenerate_signatures_task(&mut self) {
        let scheduler = self.scheduler.clone();
        let task = move |_| {
            let scheduler = scheduler.clone();
            async move {
                log::info!("Regenerating signatures");
                scheduler.write().await.regenerate_signatures();
            }
        };
        self.task_manager
            .spawn_periodic(task, Config::get().signature_refresh_interval);
    }

    fn spawn_jail_inactive_workers_task(&mut self, storage_client: S3Storage) {
        let timeout = Config::get().worker_inactive_timeout;
        self.spawn_jail_task(storage_client, timeout, |s| s.jail_inactive_workers());
    }

    fn spawn_jail_stale_workers_task(&mut self, storage_client: S3Storage) {
        let timeout = Config::get().worker_stale_timeout;
        self.spawn_jail_task(storage_client, timeout, |s| s.jail_stale_workers());
    }

    fn spawn_jail_unreachable_workers_task(&mut self, storage_client: S3Storage) {
        let timeout = Config::get().worker_unreachable_timeout;
        self.spawn_jail_task(storage_client, timeout, |s| s.jail_unreachable_workers());
    }

    fn spawn_jail_task(
        &mut self,
        storage_client: S3Storage,
        interval: Duration,
        jail_fn: fn(&mut RwLockWriteGuard<Scheduler>) -> bool,
    ) {
        let scheduler = self.scheduler.clone();
        let task = move |_| {
            let scheduler = scheduler.clone();
            let storage_client = storage_client.clone();
            async move {
                let mut scheduler = scheduler.write().await;
                if jail_fn(&mut scheduler) {
                    // If any worker got jailed, save the changed scheduler state
                    match scheduler.to_json() {
                        Ok(state) => storage_client.save_scheduler(state).await,
                        Err(e) => log::error!("Error serializing scheduler: {e:?}"),
                    }
                }
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }

    fn spawn_chunks_summary_task(&mut self) {
        let scheduler = self.scheduler.clone();
        let task = move |_| {
            let scheduler = scheduler.clone();
            async move {
                log::info!("Updating chunks summary");
                let workers = scheduler.read().await.all_workers();
                let units = scheduler.read().await.known_units();
                let summary = build_chunks_summary(workers, units);
                scheduler.write().await.update_chunks_summary(summary);
            }
        };
        self.task_manager
            .spawn_periodic(task, CHUNKS_SUMMARY_REFRESH_INTERVAL);
    }
}

fn find_workers_with_chunk(
    chunk: &DataChunk,
    ranges: &HashMap<String, Vec<(Arc<str>, RangeSet)>>,
) -> Vec<Arc<str>> {
    let ranges = match ranges.get(&chunk.dataset_id) {
        Some(ranges) => ranges,
        None => return vec![],
    };
    ranges
        .iter()
        .filter_map(|(worker_id, ranget_set)| {
            ranget_set
                .includes(chunk.block_range)
                .then_some(worker_id.clone())
        })
        .collect()
}

fn build_chunks_summary(
    workers: Vec<WorkerState>,
    units: HashMap<UnitId, SchedulingUnit>,
) -> HashMap<String, Vec<ChunkStatus>> {
    let assigned_ranges = workers
        .iter()
        .flat_map(|w| {
            let chunks = w.assigned_chunks(&units);
            chunks_to_worker_state(chunks)
                .datasets
                .into_iter()
                .map(|(dataset, ranges)| {
                    (dataset, (<Arc<str>>::from(w.peer_id.to_base58()), ranges))
                })
        })
        .into_group_map();
    let stored_ranges = workers
        .iter()
        .flat_map(|w| {
            w.stored_ranges.iter().map(|(dataset, ranges)| {
                (
                    dataset.clone(),
                    (<Arc<str>>::from(w.peer_id.to_base58()), ranges.clone()),
                )
            })
        })
        .into_group_map();
    units
        .into_values()
        .flatten()
        .map(|chunk| {
            let assigned_to = find_workers_with_chunk(&chunk, &assigned_ranges);
            let downloaded_by = find_workers_with_chunk(&chunk, &stored_ranges);
            let chunk_status = ChunkStatus {
                begin: chunk.block_range.begin,
                end: chunk.block_range.end,
                size_bytes: chunk.size_bytes,
                assigned_to,
                downloaded_by,
            };
            (chunk.dataset_id, chunk_status)
        })
        .into_group_map()
}
