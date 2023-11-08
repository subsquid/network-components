use lazy_static::lazy_static;
use prometheus::{
    opts, register_int_counter_vec, register_int_gauge_vec, IntCounterVec, IntGaugeVec,
};

lazy_static! {
    pub static ref DATASET_SYNC_ERRORS: IntCounterVec = register_int_counter_vec!(
        opts!("sqd_dataset_sync_errors", "Dataset syncronization errors"),
        &["dataset"]
    )
    .expect("Can't create a metric");
    pub static ref DATASET_HEIGHT: IntGaugeVec =
        register_int_gauge_vec!(opts!("sqd_dataset_height", "Dataset height"), &["dataset"])
            .expect("Can't create a metric");
}
