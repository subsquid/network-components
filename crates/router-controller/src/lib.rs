mod atom;
pub mod controller;
pub mod data_chunk;
pub mod range;

pub mod worker_messages {
    use std::collections::HashMap;
    use std::ops::{Deref, DerefMut};
    include!(concat!(env!("OUT_DIR"), "/worker_messages.rs"));

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
}
