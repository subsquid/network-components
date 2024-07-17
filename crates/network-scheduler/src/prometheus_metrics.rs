use std::time::Duration;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, linear_buckets, Histogram};
use prometheus_client::registry::Registry;

lazy_static::lazy_static! {
    static ref WORKERS_PER_UNIT: Histogram = Histogram::new(linear_buckets(0.0, 1.0, 100));
    static ref TOTAL_UNITS: Gauge = Default::default();
    static ref ACTIVE_WORKERS: Gauge = Default::default();
    static ref REPLICATION_FACTOR: Gauge = Default::default();
    static ref PARTIALLY_ASSIGNED_UNITS: Gauge = Default::default();
    static ref S3_REQUESTS: Counter = Default::default();
    static ref EXEC_TIMES: Family<Vec<(&'static str, &'static str)>, Histogram> = Family::new_with_constructor(
        || Histogram::new(exponential_buckets(0.001, 2.0, 24))
    );
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
    registry.register(
        "s3_requests",
        "Total number of S3 API requests since application start",
        S3_REQUESTS.clone(),
    );
    registry.register(
        "exec_times",
        "Execution times of various procedures (ms)",
        EXEC_TIMES.clone(),
    );
}

pub fn units_assigned(counts: impl IntoIterator<Item = usize>) {
    for count in counts {
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

pub fn s3_request() {
    S3_REQUESTS.inc();
}

pub fn exec_time(procedure: &'static str, duration: Duration) {
    let duration_millis = duration.as_micros() as f64 / 1000.;
    log::trace!("Procedure {procedure} took {duration_millis:.3} ms");
    EXEC_TIMES
        .get_or_create(&vec![("procedure", procedure)])
        .observe(duration_millis);
}
