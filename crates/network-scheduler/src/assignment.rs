use base64::{engine::general_purpose::STANDARD as base64, Engine};
use flate2::read::GzDecoder;
use std::{collections::HashMap, io::Read};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::signature::timed_hmac_now;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    pub id: String,
    pub base_url: String,
    pub files: HashMap<String, String>,
    size_bytes: u64,
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
    fn add_chunk(&mut self, chunk: Chunk, dataset_id: String, dataset_url: String) {
        match self.datasets.iter_mut().find(|dataset| dataset.id == dataset_id) {
            Some(dataset) => dataset.chunks.push(chunk),
            None => self.datasets.push(Dataset { id: dataset_id, base_url: dataset_url, chunks: vec![chunk] }),
        }
        self.chunk_map = None
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
        // let mut result: Vec<Chunk> = Default::default();
        // let mut cursor = 0;
        // for v in &local_assignment.chunks_deltas {
        //     cursor += v;
        //     result.push(flat_chunks[cursor as usize].clone());
        // }
        // Some(result)
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

    pub fn new(scheduler_json: &Value, cloudflare_storage_secret: String) -> Self {
        let mut assignment: Assignment = Default::default();
        let mut aux: HashMap<String, Vec<String>> = Default::default();
        let units = scheduler_json.get("known_units").unwrap().as_object().unwrap();
        for (k, unit) in units {
            let mut local_ids: Vec<String> = Default::default();
            for chunk in unit.get("chunks").unwrap().as_array().unwrap() {
                let chunk_str = chunk.get("chunk_str").unwrap().as_str().unwrap().to_string();
                let download_url = chunk.get("download_url").unwrap().as_str().unwrap().to_string();
                let mut files: HashMap<String, String> = Default::default();
                for file in chunk.get("filenames").unwrap().as_array().unwrap() {
                    let filename = file.as_str().unwrap().to_string();
                    files.insert(filename.clone(), filename);
                }
                let dataset_str = chunk.get("dataset_id").unwrap().as_str().unwrap().to_string();
                let dataset_id = base64.encode(dataset_str);
                let size_bytes = chunk.get("size_bytes").unwrap().as_u64().unwrap();
                let chunk = Chunk {
                    id: chunk_str.clone(),
                    base_url: format!("{download_url}/{chunk_str}"),
                    files,
                    size_bytes,
                };
    
                assignment.add_chunk(chunk, dataset_id, download_url);
                local_ids.push(chunk_str);
            }
            aux.insert(k.clone(), local_ids);
        }
    
        let workers = scheduler_json.get("worker_states").unwrap().as_object().unwrap();
        for (worker_id, data) in workers {
            let peer_id = worker_id.clone();
            let status = match data.get("jail_reason").unwrap().as_str() {
                Some(str) => str.to_string(),
                None => "Ok".to_string()
            };
            let mut chunks_idxs: Vec<u64> = Default::default();
            let units = data.get("assigned_units").unwrap().as_array().unwrap();
            for unit in units {
                let unit_id = unit.as_str().unwrap().to_string();
                for chunk_id in aux.get(&unit_id).unwrap() {
                    chunks_idxs.push(assignment.chunk_index(chunk_id.clone()).unwrap());
                }
            }
            chunks_idxs.sort();
            for i in (1..chunks_idxs.len()).rev() {
                chunks_idxs[i] -= chunks_idxs[i - 1];
            };

            assignment.worker_assignments.insert(peer_id.clone(), WorkerAssignment { 
                status, 
                chunks_deltas: chunks_idxs,
                encrypted_headers: Default::default()
            });
        }

        assignment.regenerate_headers(cloudflare_storage_secret);

        assignment
    }
}