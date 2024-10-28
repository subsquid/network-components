use core::str;
use std::collections::HashMap;

use aws_config::identity;
use crypto_box::{
    aead::{Aead, AeadCore, OsRng},
    SalsaBox, PublicKey, SecretKey
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha512;
use sha2::Digest;
use sha3::digest::generic_array::GenericArray;

use crate::signature::timed_hmac_now;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    pub id: String,
    pub base_url: String,
    pub files: HashMap<String, String>,
    pub size_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub id: String,
    pub base_url: String,
    pub chunks: Vec<Chunk>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Headers {
    worker_id: String,
    worker_signature: String,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct EncryptedHeaders {
    identity: Vec<u8>,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkerAssignment {
    status: String,
    chunks_deltas: Vec<u64>,
    encrypted_headers: EncryptedHeaders,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    datasets: Vec<Dataset>,
    worker_assignments: HashMap<String, WorkerAssignment>,
    #[serde(skip)]
    chunk_map: Option<HashMap<String, u64>>,
}

#[derive(Serialize, Deserialize)]
pub struct NetworkAssignment {
    pub(crate) url: String,
    pub(crate) id: String,
}

#[derive(Serialize, Deserialize)]
pub struct NetworkState {
    pub(crate) network: String,
    pub(crate) assignment: NetworkAssignment
}

impl Assignment {
    pub fn add_chunk(&mut self, chunk: Chunk, dataset_id: String, dataset_url: String) {
        match self.datasets.iter_mut().find(|dataset| dataset.id == dataset_id) {
            Some(dataset) => dataset.chunks.push(chunk),
            None => self.datasets.push(Dataset { id: dataset_id, base_url: dataset_url, chunks: vec![chunk] }),
        }
        self.chunk_map = None
    }
    
    pub fn insert_assignment(&mut self, peer_id: String, status: String, chunks_deltas: Vec<u64>) {
        self.worker_assignments.insert(peer_id.clone(), WorkerAssignment { 
            status, 
            chunks_deltas,
            encrypted_headers: Default::default()
        });
    }

    pub fn dataset_chunks_for_peer_id(&self, peer_id: String) -> Option<Vec<Dataset>> {
        let local_assignment = match self.worker_assignments.get(&peer_id) {
            Some(worker_assignment) => worker_assignment,
            None => {
                return None
            }
        };
        let mut result: Vec<Dataset> = Default::default();
        let mut idxs: Vec<u64> = Default::default();
        let mut cursor = 0;
        for v in &local_assignment.chunks_deltas {
            cursor += v;
            idxs.push(cursor);
        }
        cursor = 0;
        for u in &self.datasets {
            if idxs.is_empty() {
                break;
            }
            let mut filtered_chunks: Vec<Chunk> = Default::default();
            for v in &u.chunks {
                if idxs[0] < cursor {
                    return None; // Malformed diffs
                }
                if idxs[0] == cursor {
                    filtered_chunks.push(v.clone());
                    idxs.remove(0);
                }
                if idxs.is_empty() {
                    break;
                }
                cursor += 1;
            }
            if !filtered_chunks.is_empty() {
                result.push(Dataset {
                    id: u.id.clone(),
                    base_url: u.base_url.clone(),
                    chunks: filtered_chunks
                });
            }
        }
        Some(result)
    }

    pub fn headers_for_peer_id(&self, peer_id: String, secret_key: Vec<u8>) -> Option<HashMap<String, String>> {
        let Some(local_assignment) = self.worker_assignments.get(&peer_id) else {
            return None
        };
        println!("Got assignment");
        let EncryptedHeaders {identity, nonce, ciphertext,} = local_assignment.encrypted_headers.clone();
        let Ok(alice_public_key) = PublicKey::from_slice(identity.as_slice()) else {
            return None
        };
        println!("Got A key");
        let big_slice = Sha512::default().chain_update(secret_key).finalize();
        let Ok(bob_secret_key) = SecretKey::from_slice(&big_slice[00..32]) else {
            return None
        };
        println!("Got B key");
        let bob_public_key_bytes = bob_secret_key.public_key().as_bytes().clone();
        println!("Restored PUB: {:02x?}", bob_public_key_bytes);
        let bob_box = SalsaBox::new(&alice_public_key, &bob_secret_key);
        let generic_nonce = GenericArray::clone_from_slice(&nonce);
        // let Ok(decrypted_plaintext) = bob_box.decrypt(&generic_nonce, &ciphertext[..]) else {
        //     return None
        // };
        let res =  bob_box.decrypt(&generic_nonce, &ciphertext[..]);
        match res {
            Ok(_) => {},
            Err(e) => {
                println!("Error: {:?}", e);
            },
        }
        let decrypted_plaintext = res.unwrap();
        println!("Decrypted");
        let Ok(plaintext_headers) = std::str::from_utf8(&decrypted_plaintext) else {
            return None;
        };
        println!("Converted: {:?}", plaintext_headers);
        let Ok(headers) = serde_json::from_str::<Value>(&plaintext_headers) else {
            return None;
        };
        println!("Intrpreted: {:?}", headers);
        let mut result: HashMap<String, String> = Default::default();
        for (k,v) in headers.as_object().unwrap() {
            result.insert(k.to_string(), v.as_str().unwrap().to_string());
        }
        Some(result)
    }

    pub fn chunk_index(&mut self, chunk_id: String) -> Option<u64> {
        if self.chunk_map.is_none() {
            let mut chunk_map: HashMap<String, u64> = Default::default();
            let mut idx = 0;
            for dataset in &self.datasets {
                for chunk in &dataset.chunks {
                    chunk_map.insert(chunk.id.clone(), idx);
                    idx += 1;
                }
            };
            self.chunk_map = Some(chunk_map);
        };
        self.chunk_map.as_ref().unwrap().get(&chunk_id).cloned()
    }

    pub fn regenerate_headers(&mut self, cloudflare_storage_secret: String, hint: &[u8]) {
        let alice_secret_key = SecretKey::generate(&mut OsRng);
        let alice_public_key_bytes = alice_secret_key.public_key().as_bytes().clone();

        for (worker_id, worker_assignment) in &mut self.worker_assignments {
            let worker_signature = timed_hmac_now(
                worker_id,
                &cloudflare_storage_secret,
            );

            let headers = Headers { 
                worker_id: worker_id.to_string(), 
                worker_signature,
            };
            
            let pub_key = &bs58::decode(worker_id).into_vec().unwrap()[6..];
            let bob_public_key = PublicKey::from_slice(hint).unwrap();
            println!("PK from PeerID: {:02X?}",bob_public_key.as_bytes());
            //secret_key.scalar * public_key.0

            let alice_box = SalsaBox::new(&bob_public_key, &alice_secret_key);
            let nonce = SalsaBox::generate_nonce(&mut OsRng);
            let plaintext = serde_json::to_vec(&headers).unwrap();
            println!("Plaintext: {:?}", str::from_utf8(&plaintext));
            // println!("Original box: {:?}", alice_box);
            let ciphertext = alice_box.encrypt(&nonce, &plaintext[..]).unwrap();


            worker_assignment.encrypted_headers = EncryptedHeaders {
                identity: alice_public_key_bytes.to_vec(),
                nonce: nonce.to_vec(),
                ciphertext,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use sha2::Sha512;
    use sha2::Digest;
    use sqd_network_transport::Keypair;

    use super::*;

    #[test]
    fn it_works() {
        let mut assignment: Assignment = Default::default();
        let keypair = Keypair::generate_ed25519();
        println!("Pub: {:?}", keypair.public());

        let secret_key = keypair.clone().try_into_ed25519().unwrap().secret().as_ref().to_vec();
        println!("Priv: {:02X?}", secret_key);
        let big_slice = Sha512::default().chain_update(secret_key).finalize();
        let Ok(bob_secret_key) = SecretKey::from_slice(&big_slice[00..32]) else {
            return ()
        };
        println!("Restored private: {:02X?}", bob_secret_key.to_bytes());
        let bob_public_key_bytes = bob_secret_key.public_key().as_bytes().clone();
        println!("Restored PUB: {:02x?}", bob_public_key_bytes);

        let peer_id = keypair.public().to_peer_id().to_base58();
        let private_key = keypair.try_into_ed25519().unwrap().secret();
        assignment.insert_assignment(peer_id.clone(), "Ok".to_owned(), Default::default());
        assignment.regenerate_headers("SUPERSECRET".to_owned(), &bob_public_key_bytes);
        println!("HI!");
        let headers = assignment.headers_for_peer_id(peer_id, private_key.as_ref().to_vec());
        println!("Headers: {:?}", headers);

        assert_eq!(4, 4);
    }
}