mod metrics;

use crate::libp2p_server::metrics::{Metrics, MetricsEvent};
use prost::Message as ProstMsg;
use router_controller::controller::Controller;
use router_controller::messages::{
    envelope::Msg, get_worker_result::Result, Envelope, GetWorker, GetWorkerResult, Ping,
};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use subsquid_network_transport::transport::P2PTransportBuilder;
use subsquid_network_transport::util::get_keypair;
use subsquid_network_transport::{MsgContent, PeerId};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::log;

type Message = subsquid_network_transport::Message<Box<[u8]>>;

pub struct ServerBuilder {
    key_path: Option<PathBuf>,
    listen_addr: Option<String>,
    metrics_path: Option<PathBuf>,
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self {
            key_path: None,
            listen_addr: None,
            metrics_path: None,
        }
    }

    pub fn key_path(mut self, key_path: Option<PathBuf>) -> Self {
        self.key_path = key_path;
        self
    }

    pub fn listen_addr(mut self, listen_addr: String) -> Self {
        self.listen_addr = Some(listen_addr);
        self
    }

    pub fn metrics_path(mut self, metrics_path: PathBuf) -> Self {
        self.metrics_path = Some(metrics_path);
        self
    }

    pub async fn build(self, controller: Arc<Controller>) -> anyhow::Result<Server> {
        let keypair = get_keypair(self.key_path).await?;
        let mut transport_builder = P2PTransportBuilder::from_keypair(keypair);

        if let Some(listen_addr) = self.listen_addr {
            let listen_addr = listen_addr.parse()?;
            transport_builder.listen_on(std::iter::once(listen_addr));
        }

        transport_builder.bootstrap(false);

        let metrics_path = self.metrics_path.unwrap_or("metrics.jsonl".into());
        let metrics_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(metrics_path)
            .await?;

        let (msg_receiver, msg_sender, _) = transport_builder.run().await?;
        Ok(Server {
            controller,
            msg_receiver,
            msg_sender,
            metrics_file,
        })
    }
}

pub struct Server {
    controller: Arc<Controller>,
    msg_receiver: Receiver<Message>,
    msg_sender: Sender<Message>,
    metrics_file: File,
}

impl Server {
    async fn send_msg(&mut self, peer_id: PeerId, msg: Msg) {
        let envelope = Envelope { msg: Some(msg) };
        let msg = Message {
            peer_id: Some(peer_id),
            topic: None,
            content: envelope.encode_to_vec().into(),
        };
        if let Err(e) = self.msg_sender.send(msg).await {
            log::error!("Error sending message: {e:?}");
        }
    }

    pub async fn run(&mut self) {
        while let Some(msg) = self.msg_receiver.recv().await {
            let peer_id = match msg.peer_id {
                Some(peer_id) => peer_id,
                None => {
                    log::warn!("Dropping anonymous message");
                    continue;
                }
            };
            let envelope = match Envelope::decode(msg.content.as_slice()) {
                Ok(envelope) => envelope,
                Err(e) => {
                    log::error!("Error decoding message: {e:?}");
                    continue;
                }
            };

            match envelope.msg {
                Some(Msg::Ping(ping)) => self.ping(peer_id, ping).await,
                Some(Msg::GetWorker(get_worker)) => self.get_worker(peer_id, get_worker).await,
                Some(Msg::QuerySubmitted(msg)) => self.save_metrics(peer_id, msg).await,
                Some(Msg::QueryFinished(msg)) => self.save_metrics(peer_id, msg).await,
                Some(Msg::QueryExecuted(msg)) => self.save_metrics(peer_id, msg).await,
                _ => log::warn!("Unexpected msg received: {envelope:?}"),
            };
        }
    }

    async fn ping(&mut self, peer_id: PeerId, ping: Ping) {
        log::info!("Ping {ping:?}");
        if ping.worker_id != peer_id.to_string() {
            log::warn!(
                "Invalid peer ID in ping message: {} != {}",
                ping.worker_id,
                peer_id
            );
            return;
        }
        let state = self.controller.ping(ping);
        log::info!("Desired state for worker {peer_id}: {state:?}");
        self.send_msg(peer_id, Msg::StateUpdate(state.deref().clone()))
            .await
    }

    async fn get_worker(&mut self, peer_id: PeerId, msg: GetWorker) {
        log::info!("GetWorker {msg:?}");
        let GetWorker {
            query_id,
            dataset,
            start_block,
        } = msg;
        let result = match self.controller.get_worker(&dataset, start_block) {
            Some((worker_id, _, _)) => Result::WorkerId(worker_id),
            None => Result::Error("Not ready to serve requested block".to_string()),
        };
        let response = Msg::GetWorkerResult(GetWorkerResult {
            query_id,
            result: Some(result),
        });
        self.send_msg(peer_id, response).await;
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
}
