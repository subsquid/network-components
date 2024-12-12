use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

pub use sqd_messages::{Range, RangeSet, data_chunk::DataChunk};

#[derive(serde::Serialize, serde::Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub struct WorkerState {
    pub datasets: HashMap<String, RangeSet>,
}

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
