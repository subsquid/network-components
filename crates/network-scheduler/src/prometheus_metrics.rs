use std::collections::HashMap;

use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{linear_buckets, Histogram};
use prometheus_client::registry::Registry;

use crate::scheduling_unit::UnitId;

lazy_static::lazy_static! {
    static ref WORKERS_PER_UNIT: Histogram = Histogram::new(linear_buckets(0.0, 1.0, 100));
    static ref TOTAL_UNITS: Gauge = Default::default();
    static ref ACTIVE_WORKERS: Gauge = Default::default();
    static ref REPLICATION_FACTOR: Gauge = Default::default();
    static ref PARTIALLY_ASSIGNED_UNITS: Gauge = Default::default();
}

pub fn register_metrics(registry: &mut Registry) {
    let registry = registry.sub_registry_with_prefix("scheduler");
    registry.register(
        "workers_per_unit",
        "Number of workers that a scheduling unit is assigned to",
        WORKERS_PER_UNIT.clone(),
    );
    registry.register(
        "total_units",
        "Total number of known units",
        TOTAL_UNITS.clone(),
    );
    registry.register(
        "active_workers",
        "Number of active workers",
        ACTIVE_WORKERS.clone(),
    );
    registry.register(
        "replication_factor",
        "Current replication factor",
        REPLICATION_FACTOR.clone(),
    );
    registry.register(
        "partially_assigned_units",
        "Number of units that are missing some replicas",
        PARTIALLY_ASSIGNED_UNITS.clone(),
    );
}

pub fn units_assigned(counts: HashMap<&UnitId, usize>) {
    for (_unit_id, count) in counts {
        WORKERS_PER_UNIT.observe(count as f64);
    }
}

pub fn total_units(count: usize) {
    TOTAL_UNITS.set(count as i64);
}

pub fn active_workers(count: usize) {
    ACTIVE_WORKERS.set(count as i64);
}

pub fn replication_factor(factor: usize) {
    REPLICATION_FACTOR.set(factor as i64);
}

pub fn partially_assigned_units(count: usize) {
    PARTIALLY_ASSIGNED_UNITS.set(count as i64);
}
