use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets, histogram_opts, opts, register_histogram, register_int_counter,
    register_int_counter_vec, register_int_gauge_vec, Histogram, IntCounter, IntCounterVec,
    IntGaugeVec,
};

lazy_static! {
    pub static ref NETWORK_ERRORS: IntCounterVec = register_int_counter_vec!(
        opts!("sqd_network_errors", "Dataset HTTP errors"),
        &["network", "type", "status"]
    ).expect("Can't create a metric");
    pub static ref DATASET_SYNC_ERRORS: IntCounterVec = register_int_counter_vec!(
        opts!("sqd_dataset_sync_errors", "Dataset syncronization errors"),
        &["dataset"]
    )
    .expect("Can't create a metric");
    pub static ref DATASET_HEIGHT: IntGaugeVec =
        register_int_gauge_vec!(opts!("sqd_dataset_height", "Dataset height"), &["dataset"])
            .expect("Can't create a metric");

    /// Outcome of every auth-checked request: ok | missing | invalid | fail_open.
    pub static ref AUTH_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!("sqd_v2_auth_total", "v2 archive auth outcomes"),
        &["result"]
    )
    .expect("Can't create a metric");

    /// Time spent in the auth middleware (parse + cache + Network API).
    pub static ref AUTH_LATENCY_SECONDS: Histogram = register_histogram!(histogram_opts!(
        "sqd_v2_auth_latency_seconds",
        "v2 archive auth middleware latency",
        exponential_buckets(0.0005, 2.0, 12).unwrap()
    ))
    .expect("Can't create a metric");

    /// Cache hit broken down by entry state: exists | deleted.
    pub static ref CACHE_HIT_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!("sqd_v2_cache_hit_total", "v2 archive auth cache hits by state"),
        &["state"]
    )
    .expect("Can't create a metric");

    /// Cache miss (UNDEFINED — no entry).
    pub static ref CACHE_MISS_TOTAL: IntCounter =
        register_int_counter!("sqd_v2_cache_miss_total", "v2 archive auth cache misses")
            .expect("Can't create a metric");

    /// Outbound /internal/validate call outcome: ok | deleted | fail_open.
    pub static ref VALIDATE_CALL_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!(
            "sqd_v2_validate_call_total",
            "Outbound /internal/validate calls by outcome"
        ),
        &["result"]
    )
    .expect("Can't create a metric");

    /// Per-key request count. Cardinality is bounded by the top-keys sketch.
    pub static ref REQUESTS_BY_KEY: IntCounterVec = register_int_counter_vec!(
        opts!(
            "sqd_v2_requests_by_key",
            "Requests by API key (top-100 by traffic)"
        ),
        // The label carries the raw `user_id` (from the validate API) or
        // `internal:<ip>` for IP-bypass requests. It is NOT hashed — the
        // value identifies a tenant, never the secret token material, and
        // is intentionally exposed for attribution dashboards. The earlier
        // name `key_id_hash` was misleading; this is the canonical key_id.
        &["key_id"]
    )
    .expect("Can't create a metric");

    /// Worker URLs handed out to clients (Worker:Router ratio canary).
    pub static ref WORKER_URLS_HANDED_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!(
            "sqd_router_worker_urls_handed_total",
            "Worker URLs handed out by Router; compare to Worker incoming RPS"
        ),
        &["dataset"]
    )
    .expect("Can't create a metric");
}
