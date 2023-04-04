use grpc_libp2p::transport::P2PTransportBuilder;
use grpc_libp2p::MsgContent;
use prost::Message as ProstMsg;
use router_controller::controller::Controller;
use router_controller::worker_messages::{envelope::Msg, Envelope};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
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

pub async fn run_server(controller: Arc<Controller>) {
    let mut transport_builder = P2PTransportBuilder::new(); // TODO: Pass key
    let listen_addr = "/ip4/0.0.0.0/tcp/0".parse().unwrap(); // TODO: Pass listen_addr
    transport_builder.listen_on(std::iter::once(listen_addr));
    let (mut msg_receiver, msg_sender) = transport_builder.run::<Content>().await.unwrap();

    while let Some(msg) = msg_receiver.recv().await {
        let envelope = match Envelope::decode(msg.content.as_slice()) {
            Ok(envelope) => envelope,
            Err(e) => {
                log::error!("Error decoding message: {e:?}");
                continue;
            }
        };

        let ping = match envelope.msg {
            Some(Msg::Ping(ping)) => ping,
            _ => {
                log::warn!("Unexpected msg received: {envelope:?}");
                continue;
            }
        };

        let state = controller.ping(ping);

        let envelope = Envelope {
            msg: Some(Msg::StateUpdate(state.deref().clone())),
        };
        let msg = Message {
            peer_id: msg.peer_id,
            content: envelope.encode_to_vec().into(),
        };
        if let Err(e) = msg_sender.send(msg).await {
            log::error!("Error sending message: {e:?}");
        }
    }
}
