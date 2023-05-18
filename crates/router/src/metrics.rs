use lazy_static::lazy_static;
use prometheus::{opts, register_int_counter_vec, IntCounterVec};

lazy_static! {
    pub static ref DATASET_SYNC_ERRORS: IntCounterVec = register_int_counter_vec!(
        opts!("sqd_dataset_sync_errors", "Dataset synchronization errors"),
        &["dataset"]
    )
    .expect("Can't create a metric");
}
