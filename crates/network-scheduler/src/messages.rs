use router_controller::messages::{Ping, ProstMsg};
use sha3::{Digest, Sha3_256};
use subsquid_network_transport::{PeerId, PublicKey};

pub fn msg_hash<M: ProstMsg>(msg: &M) -> Vec<u8> {
    let mut result = [0u8; 32];
    let mut hasher = Sha3_256::default();
    hasher.update(msg.encode_to_vec().as_slice());
    Digest::finalize_into(hasher, result.as_mut_slice().into());
    result.to_vec()
}

fn verify_signature(peer_id: &PeerId, msg: &[u8], sig: &[u8]) -> bool {
    match PublicKey::try_decode_protobuf(&peer_id.to_bytes()[2..]) {
        Ok(pubkey) => pubkey.verify(msg, sig),
        Err(e) => {
            log::warn!("Cannot retrieve public key from peer ID: {e:?}");
            false
        }
    }
}

pub trait SignedMessage: ProstMsg + Sized {
    fn detach_signature(&mut self) -> Vec<u8>;
    fn attach_signature(&mut self, signature: Vec<u8>);
    fn verify_signature(&mut self, peer_id: &PeerId) -> bool {
        let sig = self.detach_signature();
        let msg = self.encode_to_vec();
        let result = verify_signature(peer_id, &msg, &sig);
        self.attach_signature(sig);
        result
    }
}

impl SignedMessage for Ping {
    fn detach_signature(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.signature)
    }

    fn attach_signature(&mut self, signature: Vec<u8>) {
        self.signature = signature;
    }
}
