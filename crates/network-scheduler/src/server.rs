use futures::{Stream, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use prometheus_client::registry::Registry;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, RwLock};

use subsquid_messages::envelope::Msg;
use subsquid_messages::signatures::{msg_hash, SignedMessage};
use subsquid_messages::{Envelope, Ping, Pong, ProstMsg};
use subsquid_network_transport::task_manager::{CancellationToken, TaskManager};
use subsquid_network_transport::transport::P2PTransportHandle;
use subsquid_network_transport::{MsgContent as MsgContentT, PeerId};

use crate::cli::Config;
use crate::metrics::{MetricsEvent, MetricsWriter};
use crate::metrics_server;
use crate::scheduler::Scheduler;
use crate::scheduling_unit::SchedulingUnit;
use crate::storage::S3Storage;

type MsgContent = Box<[u8]>;
type Message = subsquid_network_transport::Message<Box<[u8]>>;

const WORKER_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

pub struct Server<S: Stream<Item = Message> + Send + Unpin + 'static> {
    incoming_messages: S,
    incoming_units: Receiver<SchedulingUnit>,
    transport_handle: P2PTransportHandle<MsgContent>,
    scheduler: Arc<RwLock<Scheduler>>,
    metrics_writer: Arc<RwLock<MetricsWriter>>,
    task_manager: TaskManager,
}

impl<S: Stream<Item = Message> + Send + Unpin + 'static> Server<S> {
    pub fn new(
        incoming_messages: S,
        incoming_units: Receiver<SchedulingUnit>,
        transport_handle: P2PTransportHandle<MsgContent>,
        scheduler: Scheduler,
        metrics_writer: MetricsWriter,
    ) -> Self {
        let scheduler = Arc::new(RwLock::new(scheduler));
        let metrics_writer = Arc::new(RwLock::new(metrics_writer));
        Self {
            incoming_messages,
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

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        loop {
            tokio::select! {
                Some(msg) = self.incoming_messages.next() => self.handle_message(msg).await,
                Some(unit) = self.incoming_units.recv() => self.new_unit(unit).await,
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
                else => break
            }
        }

        log::info!("Server shutting down");
        self.task_manager.await_stop().await;
        Ok(())
    }

    async fn handle_message(&mut self, msg: Message) {
        let peer_id = match msg.peer_id {
            Some(peer_id) => peer_id,
            None => return log::warn!("Dropping anonymous message"),
        };
        let envelope = match Envelope::decode(msg.content.as_slice()) {
            Ok(envelope) => envelope,
            Err(e) => return log::warn!("Error decoding message: {e:?} peer_id={peer_id}"),
        };
        match envelope.msg {
            Some(Msg::Ping(msg)) => self.ping(peer_id, msg).await,
            Some(Msg::QuerySubmitted(msg)) => self.write_metrics(peer_id, msg).await,
            Some(Msg::QueryFinished(msg)) => self.write_metrics(peer_id, msg).await,
            _ => log::debug!("Unexpected msg received: {envelope:?}"),
        };
    }

    async fn ping(&mut self, peer_id: PeerId, mut msg: Ping) {
        log::debug!("Got ping from {peer_id}");
        if !msg
            .worker_id
            .as_ref()
            .is_some_and(|id| *id == peer_id.to_string())
        {
            return log::warn!("Worker ID mismatch in ping");
        }
        if !msg.verify_signature(&peer_id) {
            return log::warn!("Invalid ping signature");
        }
        let ping_hash = msg_hash(&msg);
        let status = self.scheduler.write().await.ping(peer_id, msg.clone());
        self.write_metrics(peer_id, msg).await;
        let pong = Msg::Pong(Pong {
            ping_hash,
            status: Some(status),
        });
        self.send_msg(peer_id, pong).await;
    }

    async fn write_metrics(&mut self, peer_id: PeerId, msg: impl Into<MetricsEvent>) {
        self.metrics_writer
            .write()
            .await
            .write_metrics(Some(peer_id), msg)
            .await
            .unwrap_or_else(|e| log::error!("Error writing metrics: {e:?}"));
    }

    async fn new_unit(&self, unit: SchedulingUnit) {
        self.scheduler.write().await.new_unit(unit)
    }

    async fn send_msg(&mut self, peer_id: PeerId, msg: Msg) {
        let envelope = Envelope { msg: Some(msg) };
        let msg_content = envelope.encode_to_vec().into();
        self.transport_handle
            .send_direct_msg(msg_content, peer_id)
            .unwrap_or_else(|e| log::error!("Error sending message: {e:?}"));
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
                    scheduler.jail_stale_workers();
                    match scheduler.to_json() {
                        Ok(state) => storage_client.save_scheduler(state).await,
                        Err(e) => log::error!("Error serializng scheduler: {e:?}"),
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
        let metrics_writer = self.metrics_writer.clone();
        let transport_handle = self.transport_handle.clone();
        let interval = Config::get().worker_monitoring_interval;

        let task = move |cancel_token: CancellationToken| {
            let scheduler = scheduler.clone();
            let metrics_writer = metrics_writer.clone();
            let transport_handle = transport_handle.clone();
            let cancel_token = cancel_token.clone();
            async move {
                log::info!("Dialing workers...");
                let workers = scheduler.read().await.workers_to_dial();
                let futures = workers.into_iter().map(|worker_id| {
                    let scheduler = scheduler.clone();
                    let transport_handle = transport_handle.clone();
                    async move {
                        log::info!("Dialing worker {worker_id}");
                        match transport_handle.dial_peer(worker_id).await {
                            Ok(res) => scheduler.write().await.worker_dialed(worker_id, res),
                            Err(e) => log::error!("Error dialing worker: {e:?}"),
                        }
                    }
                });

                tokio::select! {
                    _ = futures::future::join_all(futures) => (),
                    _ = cancel_token.cancelled() => return,
                }
                log::info!("Dialing workers complete.");

                let workers = scheduler.read().await.active_workers();
                metrics_writer
                    .write()
                    .await
                    .write_metrics(None, workers)
                    .await
                    .unwrap_or_else(|e| log::error!("Error writing metrics: {e:?}"));
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
        let interval = Config::get().worker_inactive_timeout;
        let scheduler = self.scheduler.clone();
        let task = move |_| {
            let scheduler = scheduler.clone();
            let storage_client = storage_client.clone();
            async move {
                let mut scheduler = scheduler.write().await;
                scheduler.jail_inactive_workers();
                scheduler.jail_stale_workers();
                match scheduler.to_json() {
                    Ok(state) => storage_client.save_scheduler(state).await,
                    Err(e) => log::error!("Error serializng scheduler: {e:?}"),
                }
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }

    fn spawn_jail_stale_workers_task(&mut self, storage_client: S3Storage) {
        let interval = Config::get().worker_stale_timeout;
        let scheduler = self.scheduler.clone();
        let task = move |_| {
            let scheduler = scheduler.clone();
            let storage_client = storage_client.clone();
            async move {
                let mut scheduler = scheduler.write().await;
                scheduler.jail_stale_workers();
                match scheduler.to_json() {
                    Ok(state) => storage_client.save_scheduler(state).await,
                    Err(e) => log::error!("Error serializng scheduler: {e:?}"),
                }
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }

    fn spawn_jail_unreachable_workers_task(&mut self, storage_client: S3Storage) {
        let interval = Config::get().worker_unreachable_timeout;
        let scheduler = self.scheduler.clone();
        let task = move |_| {
            let scheduler = scheduler.clone();
            let storage_client = storage_client.clone();
            async move {
                let mut scheduler = scheduler.write().await;
                scheduler.jail_unreachable_workers();
                scheduler.jail_stale_workers();
                match scheduler.to_json() {
                    Ok(state) => storage_client.save_scheduler(state).await,
                    Err(e) => log::error!("Error serializng scheduler: {e:?}"),
                }
            }
        };
        self.task_manager.spawn_periodic(task, interval);
    }
}
