use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use subsquid_messages::envelope::Msg;
use subsquid_messages::signatures::{msg_hash, SignedMessage};
use subsquid_messages::{Envelope, Ping, Pong, ProstMsg};
use subsquid_network_transport::{MsgContent, PeerId};

use crate::cli::Config;
use crate::metrics::{MetricsEvent, MetricsWriter};
use crate::metrics_server;
use crate::scheduler::Scheduler;
use crate::scheduling_unit::SchedulingUnit;
use crate::storage::S3Storage;

type Message = subsquid_network_transport::Message<Box<[u8]>>;

pub struct Server {
    incoming_messages: Receiver<Message>,
    incoming_units: Receiver<SchedulingUnit>,
    message_sender: Sender<Message>,
    scheduler: Arc<RwLock<Scheduler>>,
    metrics_writer: Arc<RwLock<MetricsWriter>>,
}

impl Server {
    pub fn new(
        incoming_messages: Receiver<Message>,
        incoming_units: Receiver<SchedulingUnit>,
        message_sender: Sender<Message>,
        scheduler: Scheduler,
        metrics_writer: MetricsWriter,
    ) -> Self {
        let scheduler = Arc::new(RwLock::new(scheduler));
        let metrics_writer = Arc::new(RwLock::new(metrics_writer));
        Self {
            incoming_messages,
            incoming_units,
            message_sender,
            scheduler,
            metrics_writer,
        }
    }

    pub async fn run(
        mut self,
        contract_client: Box<dyn contract_client::Client>,
        storage_client: S3Storage,
        metrics_listen_addr: SocketAddr,
    ) {
        log::info!("Starting scheduler server");
        let scheduling_task = self.spawn_scheduling_task(contract_client, storage_client);
        let monitoring_task = self.spawn_worker_monitoring_task();
        let metrics_server_task = self.spawn_metrics_server_task(metrics_listen_addr);
        let jail_inactive_task = self.spawn_jail_inactive_workers_task();
        let jail_stale_task = self.spawn_jail_stale_workers_task();
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
    }

    async fn handle_message(&mut self, msg: Message) {
        let peer_id = match msg.peer_id {
            Some(peer_id) => peer_id,
            None => return log::warn!("Dropping anonymous message"),
        };
        let envelope = match Envelope::decode(msg.content.as_slice()) {
            Ok(envelope) => envelope,
            Err(e) => return log::warn!("Error decoding message: {e:?}"),
        };
        match envelope.msg {
            Some(Msg::Ping(msg)) => self.ping(peer_id, msg).await,
            Some(Msg::QuerySubmitted(msg)) => self.write_metrics(peer_id, msg).await,
            Some(Msg::QueryFinished(msg)) => self.write_metrics(peer_id, msg).await,
            _ => log::warn!("Unexpected msg received: {envelope:?}"),
        };
    }

    async fn ping(&mut self, peer_id: PeerId, mut msg: Ping) {
        if peer_id.to_string() != msg.worker_id {
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
        let msg = Message {
            peer_id: Some(peer_id),
            topic: None,
            content: envelope.encode_to_vec().into(),
        };
        if let Err(e) = self.message_sender.send(msg).await {
            log::error!("Error sending message: {e:?}");
        }
    }

    fn spawn_scheduling_task(
        &self,
        contract_client: Box<dyn contract_client::Client>,
        storage_client: S3Storage,
    ) -> JoinHandle<()> {
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move {
            log::info!("Starting scheduling task");
            // Get worker set immediately to accept pings
            match contract_client.active_workers().await {
                Ok(workers) => scheduler.write().await.update_workers(workers),
                Err(e) => log::error!("Error getting workers: {e:?}"),
            }
            // Wait some time to get pings and download chunks
            tokio::time::sleep(Config::get().worker_inactive_timeout).await;

            loop {
                match contract_client.active_workers().await {
                    Ok(workers) => scheduler.write().await.update_workers(workers),
                    Err(e) => log::error!("Error getting workers: {e:?}"),
                }
                scheduler.write().await.schedule();
                let _ = storage_client
                    .save_scheduler(scheduler.read().await)
                    .await
                    .map_err(|e| log::error!("Error saving scheduler state: {e:?}"));
                tokio::time::sleep(Config::get().schedule_interval).await;
            }
        })
    }

    fn spawn_worker_monitoring_task(&self) -> JoinHandle<()> {
        let scheduler = self.scheduler.clone();
        let metrics_writer = self.metrics_writer.clone();
        let monitoring_interval = Config::get().worker_inactive_timeout / 2;
        tokio::spawn(async move {
            log::info!("Starting monitoring task");
            loop {
                tokio::time::sleep(monitoring_interval).await;
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

    fn spawn_jail_inactive_workers_task(&self) -> JoinHandle<()> {
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Config::get().worker_inactive_timeout).await;
                scheduler.write().await.jail_inactive_workers();
            }
        })
    }

    fn spawn_jail_stale_workers_task(&self) -> JoinHandle<()> {
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Config::get().worker_stale_timeout).await;
                scheduler.write().await.jail_stale_workers();
            }
        })
    }
}
