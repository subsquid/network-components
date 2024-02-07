use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use subsquid_messages::envelope::Msg;
use subsquid_messages::signatures::{msg_hash, SignedMessage};
use subsquid_messages::{Envelope, Ping, Pong, ProstMsg};
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

pub struct Server {
    incoming_messages: Receiver<Message>,
    incoming_units: Receiver<SchedulingUnit>,
    transport_handle: P2PTransportHandle<MsgContent>,
    scheduler: Arc<RwLock<Scheduler>>,
    metrics_writer: Arc<RwLock<MetricsWriter>>,
}

impl Server {
    pub fn new(
        incoming_messages: Receiver<Message>,
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
        }
    }

    pub async fn run(
        mut self,
        contract_client: Box<dyn contract_client::Client>,
        storage_client: S3Storage,
        metrics_listen_addr: SocketAddr,
    ) -> anyhow::Result<()> {
        log::info!("Starting scheduler server");
        let scheduling_task = self
            .spawn_scheduling_task(contract_client, storage_client.clone())
            .await?;
        let monitoring_task = self.spawn_worker_monitoring_task();
        let metrics_server_task = self.spawn_metrics_server_task(metrics_listen_addr);
        let jail_inactive_task = self.spawn_jail_inactive_workers_task(storage_client.clone());
        let jail_stale_task = self.spawn_jail_stale_workers_task(storage_client.clone());
        let jail_unreachable_task = self.spawn_jail_unreachable_workers_task(storage_client);
        loop {
            tokio::select! {
                Some(msg) = self.incoming_messages.recv() => self.handle_message(msg).await,
                Some(unit) = self.incoming_units.recv() => self.new_unit(unit).await,
                else => break
            }
        }
        log::info!("Server shutting down");
        scheduling_task.abort();
        monitoring_task.abort();
        metrics_server_task.abort();
        jail_inactive_task.abort();
        jail_stale_task.abort();
        jail_unreachable_task.abort();
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
        if let Err(e) = self
            .metrics_writer
            .write()
            .await
            .write_metrics(Some(peer_id), msg)
            .await
        {
            log::error!("Error writing metrics: {e:?}");
        }
    }

    async fn new_unit(&self, unit: SchedulingUnit) {
        self.scheduler.write().await.new_unit(unit)
    }

    async fn send_msg(&mut self, peer_id: PeerId, msg: Msg) {
        let envelope = Envelope { msg: Some(msg) };
        let msg_content = envelope.encode_to_vec().into();
        if let Err(e) = self
            .transport_handle
            .send_direct_msg(msg_content, peer_id)
            .await
        {
            log::error!("Error sending message: {e:?}");
        }
    }

    async fn spawn_scheduling_task(
        &self,
        contract_client: Box<dyn contract_client::Client>,
        storage_client: S3Storage,
    ) -> anyhow::Result<JoinHandle<()>> {
        let scheduler = self.scheduler.clone();
        // Get worker set immediately to accept pings
        let workers = contract_client.active_workers().await?;
        let mut last_epoch = contract_client.current_epoch().await?;
        scheduler.write().await.update_workers(workers);
        let interval = Config::get().schedule_interval_epochs;

        Ok(tokio::spawn(async move {
            log::info!("Starting scheduling task");
            loop {
                tokio::time::sleep(WORKER_REFRESH_INTERVAL).await;

                // Get current epoch number
                let current_epoch = match contract_client.current_epoch().await {
                    Ok(epoch) => epoch,
                    Err(e) => {
                        log::error!("Error getting epoch: {e:?}");
                        continue;
                    }
                };
                let mut scheduler_ref = scheduler.write().await;

                // Update workers every epoch
                if current_epoch > last_epoch {
                    match contract_client.active_workers().await {
                        Ok(workers) => scheduler_ref.update_workers(workers),
                        Err(e) => log::error!("Error getting workers: {e:?}"),
                    }
                    last_epoch = current_epoch;
                }

                // Schedule chunks every `schedule_interval_epochs`
                if current_epoch >= scheduler_ref.last_schedule_epoch() + interval {
                    scheduler_ref.schedule(current_epoch);
                    storage_client.save_scheduler(scheduler_ref).await
                }
            }
        }))
    }

    fn spawn_worker_monitoring_task(&self) -> JoinHandle<()> {
        let scheduler = self.scheduler.clone();
        let metrics_writer = self.metrics_writer.clone();
        let transport_handle = self.transport_handle.clone();
        let monitoring_interval = Config::get().worker_monitoring_interval();
        tokio::spawn(async move {
            log::info!("Starting monitoring task");
            loop {
                tokio::time::sleep(monitoring_interval).await;

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
                futures::future::join_all(futures).await;
                log::info!("Dialing workers complete.");

                let workers = scheduler.read().await.active_workers();
                if let Err(e) = metrics_writer
                    .write()
                    .await
                    .write_metrics(None, workers)
                    .await
                {
                    log::error!("Error writing metrics: {e:?}");
                }
            }
        })
    }

    fn spawn_metrics_server_task(&self, metrics_listen_addr: SocketAddr) -> JoinHandle<()> {
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move {
            if let Err(e) = metrics_server::run_server(scheduler, metrics_listen_addr).await {
                log::error!("Metrics server crashed: {e:?}");
            }
        })
    }

    fn spawn_jail_inactive_workers_task(&self, storage_client: S3Storage) -> JoinHandle<()> {
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Config::get().worker_inactive_timeout).await;
                let mut scheduler_ref = scheduler.write().await;
                scheduler_ref.jail_inactive_workers();
                storage_client.save_scheduler(scheduler_ref).await;
            }
        })
    }

    fn spawn_jail_stale_workers_task(&self, storage_client: S3Storage) -> JoinHandle<()> {
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Config::get().worker_stale_timeout).await;
                let mut scheduler_ref = scheduler.write().await;
                scheduler_ref.jail_stale_workers();
                storage_client.save_scheduler(scheduler_ref).await;
            }
        })
    }

    fn spawn_jail_unreachable_workers_task(&self, storage_client: S3Storage) -> JoinHandle<()> {
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Config::get().worker_unreachable_timeout).await;
                let mut scheduler_ref = scheduler.write().await;
                scheduler_ref.jail_unreachable_workers();
                storage_client.save_scheduler(scheduler_ref).await;
            }
        })
    }
}
