use sha3::{Digest, Sha3_256};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
include!(concat!(env!("OUT_DIR"), "/messages.rs"));

impl Deref for WorkerState {
    type Target = HashMap<String, RangeSet>;

    fn deref(&self) -> &Self::Target {
        &self.datasets
    }
}

impl DerefMut for WorkerState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.datasets
    }
}

impl From<HashMap<String, RangeSet>> for WorkerState {
    fn from(datasets: HashMap<String, RangeSet>) -> Self {
        Self { datasets }
    }
}

impl From<&query_result::Result> for query_finished::Result {
    fn from(result: &query_result::Result) -> Self {
        match result {
            query_result::Result::OkData(data) => Self::Ok(SizeAndHash::compute(data)),
            query_result::Result::BadRequest(err) => Self::BadRequest(err.clone()),
            query_result::Result::ServerError(err) => Self::ServerError(err.clone()),
        }
    }
}

impl SizeAndHash {
    pub fn compute(data: &[u8]) -> Self {
        let size = data.len() as u32;
        let mut hasher = Sha3_256::new();
        hasher.update(data);
        let hash = hasher.finalize();
        Self {
            size,
            sha3_256: hash.to_vec(),
        }
    }
}
