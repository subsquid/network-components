use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use prometheus_client::registry::Registry;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;

use sqd_network_transport::util::{CancellationToken, TaskManager};
use sqd_network_transport::{SchedulerEvent, SchedulerTransportHandle};

use crate::cli::Config;
use crate::scheduler::Scheduler;
use crate::scheduling_unit::{SchedulingUnit, UnitId};
use crate::storage::S3Storage;
use crate::worker_state::WorkerState;
use crate::{metrics_server, prometheus_metrics};
use sqd_messages::assignments::{Assignment, Chunk};

const WORKER_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

pub struct Server {
    incoming_units: Receiver<SchedulingUnit>,
    transport_handle: SchedulerTransportHandle,
    scheduler: Scheduler,
    task_manager: TaskManager,
}

impl Server {
    pub fn new(
        incoming_units: Receiver<SchedulingUnit>,
        transport_handle: SchedulerTransportHandle,
        scheduler: Scheduler,
    ) -> Self {
        Self {
            incoming_units,
            transport_handle,
            scheduler,
            task_manager: Default::default(),
        }
    }

    pub async fn run<S: Stream<Item = SchedulerEvent> + Send + Unpin + 'static>(
        mut self,
        contract_client: Box<dyn sqd_contract_client::Client>,
        storage_client: S3Storage,
        metrics_listen_addr: SocketAddr,
        metrics_registry: Registry,
        incoming_events: S,
    ) -> anyhow::Result<()> {
        log::info!("Starting scheduler server");

        // Get worker set immediately to accept pings
        let workers = contract_client.active_workers().await?;
        self.scheduler.update_workers(workers);

        self.spawn_scheduling_task(contract_client, storage_client.clone())
            .await?;
        self.spawn_worker_monitoring_task();
        self.spawn_metrics_server_task(metrics_listen_addr, metrics_registry);
        self.spawn_jail_inactive_workers_task(storage_client.clone());
        self.spawn_jail_stale_workers_task(storage_client.clone());
        self.spawn_jail_unreachable_workers_task(storage_client.clone());
        self.spawn_assignments_task(storage_client);
        self.spawn_event_processing_task(incoming_events);

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        loop {
            tokio::select! {
                Some(unit) = self.incoming_units.recv() => self.on_new_unit(unit),
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
                else => break
            }
        }

        log::info!("Server shutting down");
        self.task_manager.await_stop().await;
        Ok(())
    }

    fn on_new_unit(&self, unit: SchedulingUnit) {
        self.scheduler.new_unit(unit)
    }

    fn spawn_event_processing_task<S: Stream<Item = SchedulerEvent> + Send + Unpin + 'static>(
        &mut self,
        incoming_events: S,
    ) {
        log::info!("Starting event processing task");
        let scheduler = self.scheduler.clone();
        let num_threads = Config::get().ping_processing_threads;

        let task = move |cancel_token: CancellationToken| {
            incoming_events
                .take_until(cancel_token.cancelled_owned())
                .for_each_concurrent(num_threads, move |ev| {
                    let scheduler = scheduler.clone();
                    async move {
                        let start = Instant::now();
                        let (peer_id, heartbeat) = match ev {
                            SchedulerEvent::PeerProbed { peer_id, reachable } => {
                                return scheduler.worker_dialed(peer_id, reachable)
                            }
                            SchedulerEvent::Heartbeat { peer_id, heartbeat } => {
                                (peer_id, heartbeat)
                            }
                        };

                        log::debug!("Got heartbeat from {peer_id}");
                        scheduler.heartbeat(&peer_id, heartbeat);
                        prometheus_metrics::exec_time("ping", start.elapsed());
                    }
                })
        };
        self.task_manager.spawn(task);
    }

    async fn spawn_scheduling_task(
        &mut self,
        contract_client: Box<dyn sqd_contract_client::Client>,
        storage_client: S3Storage,
    ) -> anyhow::Result<()> {
        log::info!("Starting scheduling task");
        let scheduler = self.scheduler.clone();
        let last_epoch = Arc::new(Mutex::new(contract_client.current_epoch().await?));
        let contract_client: Arc<dyn sqd_contract_client::Client> = contract_client.into();
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
                if current_epoch > *last_epoch.lock() {
                    match contract_client.active_workers().await {
                        Ok(workers) => scheduler.update_workers(workers),
                        Err(e) => log::error!("Error getting workers: {e:?}"),
                    }
                    *last_epoch.lock() = current_epoch;
                }

                // Schedule chunks every `schedule_interval_epochs`
                let last_schedule_epoch = scheduler.last_schedule_epoch();
                if current_epoch >= last_schedule_epoch + schedule_interval {
                    scheduler.schedule(current_epoch);
                    match scheduler.to_json() {
                        Ok(state) => storage_client.save_scheduler(state.clone()).await,
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
                let workers = scheduler.workers_to_dial();
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
        jail_fn: fn(&Scheduler) -> bool,
    ) {
        let scheduler = self.scheduler.clone();
        let task = move |_| {
            let scheduler = scheduler.clone();
            let storage_client = storage_client.clone();
            async move {
                if jail_fn(&scheduler) {
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

    fn spawn_assignments_task(&mut self, storage_client: S3Storage) {
        let scheduler = self.scheduler.clone();
        let task = move |_| {
            let scheduler = scheduler.clone();
            let storage_client = storage_client.clone();
            async move {
                log::info!("Updating assignment");
                let workers = scheduler.all_workers();
                let units = scheduler.known_units();
                let assignment = build_assignment(&workers, &units);
                storage_client.save_assignment(assignment).await;
            }
        };
        self.task_manager
            .spawn_periodic(task, Config::get().assignment_refresh_interval);
    }
}

fn build_assignment(
    workers: &Vec<WorkerState>,
    units: &DashMap<UnitId, SchedulingUnit>,
) -> Assignment {
    let mut assignment: Assignment = Default::default();
    let mut aux: HashMap<String, Vec<String>> = Default::default();
    for (k, unit) in units.clone() {
        let mut local_ids: Vec<String> = Default::default();
        for chunk in unit.chunks {
            let chunk_str = chunk.chunk_str;
            let download_url = chunk.download_url;
            let mut files: HashMap<String, String> = Default::default();
            for filename in chunk.filenames {
                files.insert(filename.clone(), filename);
            }
            let dataset_str = chunk.dataset_id;
            let dataset_id = dataset_str;
            let size_bytes = chunk.size_bytes;
            let chunk = Chunk {
                id: chunk_str.clone(),
                base_url: format!("{download_url}/{chunk_str}"),
                files,
                size_bytes,
                summary: chunk.summary,
            };

            assignment.add_chunk(chunk, dataset_id, download_url);
            local_ids.push(chunk_str);
        }
        aux.insert(k.to_string(), local_ids);
    }

    for worker in workers {
        let peer_id = worker.peer_id;
        let jail_reason = worker.jail_reason.map(|r| r.to_string());
        let mut chunks_idxs: Vec<u64> = Default::default();

        for unit in &worker.assigned_units {
            let unit_id = unit.to_string();
            for chunk_id in aux.get(&unit_id).unwrap() {
                chunks_idxs.push(assignment.chunk_index(chunk_id.to_string()).unwrap());
            }
        }
        chunks_idxs.sort();
        for i in (1..chunks_idxs.len()).rev() {
            chunks_idxs[i] -= chunks_idxs[i - 1];
        }

        assignment.insert_assignment(peer_id, jail_reason, chunks_idxs);
    }
    assignment.regenerate_headers(&Config::get().cloudflare_storage_secret);
    assignment
}
