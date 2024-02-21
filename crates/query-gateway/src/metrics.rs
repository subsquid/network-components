use std::ops::Deref;

use lazy_static::lazy_static;
use prometheus::{register_int_gauge, register_int_gauge_vec, IntGauge, IntGaugeVec, TextEncoder};

lazy_static! {
    static ref ALLOCATED_COMP_UNITS: IntGaugeVec = register_int_gauge_vec!(
        "allocated_comp_units",
        "amount of compute units allocated for this epoch",
        &["worker_id"]
    )
    .unwrap();
    static ref SPENT_COMP_UNITS: IntGaugeVec = register_int_gauge_vec!(
        "spent_comp_units",
        "amount of compute units spent this epoch",
        &["worker_id"]
    )
    .unwrap();
    static ref CURRENT_EPOCH: IntGauge =
        register_int_gauge!("current_epoch", "current epoch number").unwrap();
}

pub fn init_workers<T, S>(workers: T)
where
    T: IntoIterator<Item = S>,
    S: Deref<Target = str>,
{
    for worker_id in workers.into_iter() {
        ALLOCATED_COMP_UNITS.with_label_values(&[&worker_id]).set(0);
        SPENT_COMP_UNITS.with_label_values(&[&worker_id]).set(0);
    }
}

pub fn new_epoch(epoch: u32) {
    CURRENT_EPOCH.set(epoch as i64);
    ALLOCATED_COMP_UNITS.reset();
    SPENT_COMP_UNITS.reset();
}

pub fn update_allocations(allocations: Vec<(String, u32)>) {
    for (worker_id, comp_units) in allocations.into_iter() {
        ALLOCATED_COMP_UNITS
            .with_label_values(&[&worker_id])
            .set(comp_units as i64)
    }
}

pub fn spend_comp_units(worker_id: &str, spent_cus: u32) {
    SPENT_COMP_UNITS
        .with_label_values(&[worker_id])
        .add(spent_cus as i64);
}

pub fn gather_metrics() -> anyhow::Result<String> {
    Ok(TextEncoder::new().encode_to_string(&prometheus::gather())?)
}
