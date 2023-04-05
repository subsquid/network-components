use grpc_libp2p::transport::P2PTransportBuilder;
use grpc_libp2p::{MsgContent, PeerId};
use prost::Message as ProstMsg;
use router_controller::controller::Controller;
use router_controller::messages::{
    envelope::Msg, Envelope, GetWorker, GetWorkerResult, Ping, QueryError,
};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::log;

#[derive(Debug)]
struct Content(Box<[u8]>);

impl From<Vec<u8>> for Content {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into_boxed_slice())
    }
}

impl MsgContent for Content {
    fn new(size: usize) -> Self {
        Self(vec![0; size].into_boxed_slice())
    }

    fn as_slice(&self) -> &[u8] {
        self.0.deref()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.0.deref_mut()
    }
}

type Message = grpc_libp2p::Message<Content>;

pub struct Server {
    controller: Arc<Controller>,
    msg_receiver: Receiver<Message>,
    msg_sender: Sender<Message>,
}

impl Server {
    pub async fn new(controller: Arc<Controller>) -> Self {
        let mut transport_builder = P2PTransportBuilder::new(); // TODO: Pass key
        let listen_addr = "/ip4/0.0.0.0/tcp/0".parse().unwrap(); // TODO: Pass listen_addr
        transport_builder.listen_on(std::iter::once(listen_addr));
        let (msg_receiver, msg_sender) = transport_builder.run().await.unwrap(); // FIXME: unwrap

        Self {
            controller,
            msg_receiver,
            msg_sender,
        }
    }

    async fn send_msg(&mut self, peer_id: PeerId, msg: Msg) {
        let envelope = Envelope { msg: Some(msg) };
        let msg = Message {
            peer_id,
            content: envelope.encode_to_vec().into(),
        };
        if let Err(e) = self.msg_sender.send(msg).await {
            log::error!("Error sending message: {e:?}");
        }
    }

    pub async fn run(&mut self) {
        while let Some(msg) = self.msg_receiver.recv().await {
            let envelope = match Envelope::decode(msg.content.as_slice()) {
                Ok(envelope) => envelope,
                Err(e) => {
                    log::error!("Error decoding message: {e:?}");
                    continue;
                }
            };

            match envelope.msg {
                Some(Msg::Ping(ping)) => self.ping(msg.peer_id, ping).await,
                Some(Msg::GetWorker(get_worker)) => self.get_worker(msg.peer_id, get_worker).await,
                _ => log::warn!("Unexpected msg received: {envelope:?}"),
            };
        }
    }

    async fn ping(&mut self, peer_id: PeerId, ping: Ping) {
        let state = self.controller.ping(ping);
        self.send_msg(peer_id, Msg::StateUpdate(state.deref().clone()))
            .await
    }

    async fn get_worker(&mut self, peer_id: PeerId, msg: GetWorker) {
        let response = match self.controller.get_worker(&msg.dataset, msg.start_block) {
            Some((worker_id, _)) => Msg::GetWorkerResult(GetWorkerResult {
                query_id: msg.query_id,
                worker_id,
            }),
            None => Msg::GetWorkerError(QueryError {
                query_id: msg.query_id,
                error: "Not ready to serve requested block".to_string(),
            }),
        };
        self.send_msg(peer_id, response).await;
    }
}
