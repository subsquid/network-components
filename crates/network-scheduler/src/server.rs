use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use libp2p::core::PublicKey;
use sha3::{Digest, Sha3_256};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use router_controller::messages::envelope::Msg;
use router_controller::messages::{Envelope, Ping, Pong, ProstMsg};
use subsquid_network_transport::{MsgContent, PeerId};

use crate::cli::Config;
use crate::metrics::{MetricsEvent, MetricsWriter};
use crate::metrics_server;
use crate::scheduler::Scheduler;
use crate::scheduling_unit::SchedulingUnit;
use crate::worker_registry::{WorkerRegistry, WORKER_INACTIVE_TIMEOUT};

type Message = subsquid_network_transport::Message<Box<[u8]>>;

pub struct Server {
    incoming_messages: Receiver<Message>,
    incoming_units: Receiver<SchedulingUnit>,
    message_sender: Sender<Message>,
    worker_registry: Arc<RwLock<WorkerRegistry>>,
    scheduler: Arc<RwLock<Scheduler>>,
    metrics_writer: Arc<RwLock<MetricsWriter>>,
    config: Config,
}

impl Server {
    pub fn new(
        incoming_messages: Receiver<Message>,
        incoming_units: Receiver<SchedulingUnit>,
        message_sender: Sender<Message>,
        worker_registry: WorkerRegistry,
        scheduler: Scheduler,
        metrics_writer: MetricsWriter,
        config: Config,
    ) -> Self {
        let worker_registry = Arc::new(RwLock::new(worker_registry));
        let scheduler = Arc::new(RwLock::new(scheduler));
        let metrics_writer = Arc::new(RwLock::new(metrics_writer));
        Self {
            incoming_messages,
            incoming_units,
            message_sender,
            worker_registry,
            scheduler,
            metrics_writer,
            config,
        }
    }

    pub async fn run(mut self, metrics_listen_addr: SocketAddr) {
        log::info!("Starting scheduler server");
        let scheduling_task = self.spawn_scheduling_task();
        let monitoring_task = self.spawn_worker_monitoring_task();
        let metrics_server_task = self.spawn_metrics_server_task(metrics_listen_addr);
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
            Some(Msg::QueryExecuted(msg)) => self.write_metrics(peer_id, msg).await,
            _ => log::warn!("Unexpected msg received: {envelope:?}"),
        };
    }

    async fn ping(&mut self, peer_id: PeerId, mut msg: Ping) {
        if msg.worker_id != peer_id.to_string() {
            return log::warn!(
                "Invalid worker ID in string: {} != {peer_id}",
                msg.worker_id,
            );
        }
        let pubkey = match PublicKey::from_protobuf_encoding(&peer_id.to_bytes()[2..]) {
            Ok(pubkey) => pubkey,
            Err(e) => return log::warn!("Cannot retrieve public key from peer ID: {e:?}"),
        };
        // Need to remove the signature from the struct before encoding
        let signature = std::mem::take(&mut msg.signature);
        let serialized_msg = msg.encode_to_vec();
        if !pubkey.verify(&serialized_msg, &signature) {
            return log::warn!("Invalid ping signature");
        }
        msg.signature = signature;
        let ping_hash = msg_hash(&msg);

        self.worker_registry
            .write()
            .await
            .ping(peer_id, msg.clone())
            .await;
        self.write_metrics(peer_id, msg).await;

        let assigned_state = self.scheduler.read().await.get_worker_state(&peer_id);
        let pong = Msg::Pong(Pong {
            ping_hash,
            assigned_state,
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

    fn spawn_scheduling_task(&self) -> JoinHandle<()> {
        let worker_registry = self.worker_registry.clone();
        let scheduler = self.scheduler.clone();
        let schedule_interval = Duration::from_secs(self.config.schedule_interval_sec);
        tokio::spawn(async move {
            log::info!("Starting scheduling task");
            loop {
                tokio::time::sleep(schedule_interval).await;
                let workers = worker_registry
                    .write()
                    .await
                    .available_workers()
                    .await
                    .into_iter()
                    .map(|w| w.peer_id)
                    .collect();
                scheduler.write().await.schedule(workers);
            }
        })
    }

    fn spawn_worker_monitoring_task(&self) -> JoinHandle<()> {
        let worker_registry = self.worker_registry.clone();
        let metrics_writer = self.metrics_writer.clone();
        let monitoring_interval = WORKER_INACTIVE_TIMEOUT / 2;
        tokio::spawn(async move {
            log::info!("Starting monitoring task");
            loop {
                tokio::time::sleep(monitoring_interval).await;
                let workers = worker_registry.write().await.available_workers().await;
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
        let worker_registry = self.worker_registry.clone();
        let scheduler = self.scheduler.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            if let Err(e) =
                metrics_server::run_server(worker_registry, scheduler, config, metrics_listen_addr)
                    .await
            {
                log::error!("Metrics server crashed: {e:?}");
            }
        })
    }
}

fn msg_hash<M: ProstMsg>(msg: &M) -> Vec<u8> {
    let mut result = [0u8; 32];
    let mut hasher = Sha3_256::default();
    hasher.update(msg.encode_to_vec().as_slice());
    Digest::finalize_into(hasher, result.as_mut_slice().into());
    result.to_vec()
}
