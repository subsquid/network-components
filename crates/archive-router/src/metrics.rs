use lazy_static::lazy_static;
use prometheus::{opts, register_int_gauge, IntGauge};

lazy_static! {
    pub static ref WORKERS_COUNTER: IntGauge =
        register_int_gauge!(opts!("workers_counter", "Count of active workers"))
            .expect("Can't create a metric");
}
