use std::sync::Arc;
use std::time::Duration;

use subsquid_network_transport::{MsgContent, PeerId};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use contract_client::Worker;
use router_controller::messages::envelope::Msg;
use router_controller::messages::{Envelope, ProstMsg};

use crate::metrics::{Metrics, MetricsEvent};
use crate::scheduler::Scheduler;
use crate::scheduling_unit::SchedulingUnit;
use crate::worker_registry::WorkerRegistry;

type Message = subsquid_network_transport::Message<Box<[u8]>>;

pub struct Server {
    incoming_messages: Receiver<Message>,
    worker_updates: Receiver<Vec<Worker>>,
    incoming_units: Receiver<SchedulingUnit>,
    message_sender: Sender<Message>,
    worker_registry: Arc<RwLock<WorkerRegistry>>,
    scheduler: Arc<RwLock<Scheduler>>,
    schedule_interval: Duration,
    metrics_file: File,
}

impl Server {
    pub fn new(
        incoming_messages: Receiver<Message>,
        worker_updates: Receiver<Vec<Worker>>,
        incoming_units: Receiver<SchedulingUnit>,
        message_sender: Sender<Message>,
        schedule_interval: Duration,
        replication_factor: usize,
        metrics_file: File,
    ) -> Self {
        let worker_registry = Arc::new(Default::default());
        let scheduler = Arc::new(RwLock::new(Scheduler::new(replication_factor)));
        Self {
            incoming_messages,
            worker_updates,
            incoming_units,
            message_sender,
            worker_registry,
            scheduler,
            schedule_interval,
            metrics_file,
        }
    }

    pub async fn run(mut self) {
        log::info!("Starting scheduler server");
        let scheduling_task = self.spawn_scheduling_task();
        loop {
            tokio::select! {
                Some(msg) = self.incoming_messages.recv() => self.handle_message(msg).await,
                Some(workers) = self.worker_updates.recv() => self.update_workers(workers).await,
                Some(unit) = self.incoming_units.recv() => self.new_unit(unit).await,
                else => break
            }
        }
        log::info!("Server shutting down");
        scheduling_task.abort()
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
            Some(Msg::Ping(_)) => self.ping(peer_id).await,
            Some(Msg::QuerySubmitted(msg)) => self.save_metrics(peer_id, msg).await,
            Some(Msg::QueryFinished(msg)) => self.save_metrics(peer_id, msg).await,
            Some(Msg::QueryExecuted(msg)) => self.save_metrics(peer_id, msg).await,
            _ => log::warn!("Unexpected msg received: {envelope:?}"),
        };
    }

    async fn ping(&mut self, peer_id: PeerId) {
        self.worker_registry.write().await.ping(peer_id).await;
        let worker_state = self.scheduler.read().await.get_worker_state(&peer_id);
        self.send_msg(peer_id, Msg::StateUpdate(worker_state)).await
    }

    async fn save_metrics(&mut self, peer_id: PeerId, msg: impl Into<MetricsEvent>) {
        let metrics = Metrics::new(peer_id, msg);
        let json_line = match metrics.to_json_line() {
            Err(e) => return log::error!("Invalid metrics: {e:?}"),
            Ok(line) => line,
        };
        let _ = self
            .metrics_file
            .write_all(json_line.as_slice())
            .await
            .map_err(|e| log::error!("Error saving metrics: {e:?}"));
    }

    async fn update_workers(&self, workers: Vec<Worker>) {
        let workers = workers.into_iter().map(|w| w.peer_id);
        self.worker_registry
            .write()
            .await
            .update_workers(workers)
            .await;
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
        let schedule_interval = self.schedule_interval.clone();
        tokio::spawn(async move {
            log::info!("Starting scheduling task");
            loop {
                tokio::time::sleep(schedule_interval).await;
                let workers = worker_registry.read().await.active_workers().await;
                scheduler.write().await.schedule(workers);
            }
        })
    }
}
