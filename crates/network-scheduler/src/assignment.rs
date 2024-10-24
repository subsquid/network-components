use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
struct EncryptedHeaders {
    worker_id: String,
    worker_signature: String,
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

    pub fn headers_for_peer_id(&self, peer_id: String) -> Option<HashMap<String, String>> {
        let local_assignment = match self.worker_assignments.get(&peer_id) {
            Some(worker_assignment) => worker_assignment,
            None => {
                return None
            }
        };
        let headers = match serde_json::to_value(&local_assignment.encrypted_headers) {
            Ok(v) => v,
            Err(_) => {
                return None;
            }
        };
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

    pub fn regenerate_headers(&mut self, cloudflare_storage_secret: String) {
        for (worker_id, worker_assignment) in &mut self.worker_assignments {
            let worker_signature = timed_hmac_now(
                worker_id,
                &cloudflare_storage_secret,
            );
            worker_assignment.encrypted_headers = EncryptedHeaders { 
                worker_id: worker_id.to_string(), 
                worker_signature,
            }
        }
    }
}