//! Runs the collection loop against fake workers and storage. Most tests use tokio's
//! paused clock, so the round timings they assert are exact and the tests take no
//! real time.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::ThreadId;
use std::time::Duration;

use async_trait::async_trait;
use collector_utils::{PingRow, QueryExecutedRow, QueryFinishedRow, Storage};
use parking_lot::Mutex;
use sqd_messages::{query_executed, LogsRequest, Query, QueryExecuted, QueryLogs, QueryOkSummary};
use sqd_network_transport::util::CancellationToken;
use sqd_network_transport::{Keypair, PeerId};
use tokio::time::Instant;

use super::{LogsSource, Server, MAX_PAGES};
use crate::collector::LogsCollector;

const INTERVAL: Duration = Duration::from_secs(120);
const PAGE_SIZE: usize = 10;
const LATENCY: Duration = Duration::from_secs(1);
/// Big enough that the buffer never fills up unless a test wants it to.
const LARGE_BUFFER: usize = 1 << 30;
const BASE_TIMESTAMP_MS: u64 = 1_750_000_000_000;

/// Serves logs the way workers do: from `from_timestamp_ms`, or right after
/// `last_received_query_id` when paging within a round, `page_size` at a time.
#[derive(Clone)]
struct FakeWorkers {
    logs: HashMap<PeerId, Vec<QueryExecuted>>,
    unreachable: HashSet<PeerId>,
    page_size: usize,
    latency: Duration,
    requests: Arc<AtomicUsize>,
}

impl FakeWorkers {
    /// One worker per entry, holding that many logs.
    fn with_logs(logs_per_worker: &[usize]) -> Self {
        let logs = logs_per_worker
            .iter()
            .enumerate()
            .map(|(i, &count)| {
                let worker_id = worker(i as u8);
                (worker_id, signed_logs(worker_id, count))
            })
            .collect();
        Self {
            logs,
            unreachable: HashSet::new(),
            page_size: PAGE_SIZE,
            latency: LATENCY,
            requests: Default::default(),
        }
    }

    fn worker_ids(&self) -> HashSet<PeerId> {
        self.logs.keys().chain(&self.unreachable).copied().collect()
    }

    /// Everything the workers have, as `(worker_id, timestamp)` pairs.
    fn all_logs(&self) -> Vec<(String, u64)> {
        let mut all: Vec<_> = self
            .logs
            .iter()
            .flat_map(|(worker_id, logs)| {
                logs.iter()
                    .map(move |log| (worker_id.to_string(), log.timestamp_ms))
            })
            .collect();
        all.sort();
        all
    }
}

impl LogsSource for FakeWorkers {
    async fn request_logs(
        &self,
        worker_id: PeerId,
        request: LogsRequest,
    ) -> anyhow::Result<QueryLogs> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.latency).await;
        if self.unreachable.contains(&worker_id) {
            anyhow::bail!("Timeout(Connect)");
        }
        let logs = &self.logs[&worker_id];
        let start = match request.last_received_query_id {
            Some(id) => {
                logs.iter()
                    .position(|log| log.query.as_ref().unwrap().query_id == id)
                    .unwrap()
                    + 1
            }
            None => logs.partition_point(|log| log.timestamp_ms < request.from_timestamp_ms),
        };
        let end = (start + self.page_size).min(logs.len());
        Ok(QueryLogs {
            queries_executed: logs[start..end].to_vec(),
            has_more: end < logs.len(),
        })
    }
}

/// Keeps stored logs in memory and records when each round started: every round
/// opens with the watermark query.
#[derive(Clone, Default)]
struct FakeStorage(Arc<StorageState>);

#[derive(Default)]
struct StorageState {
    rows: Mutex<Vec<(String, u64)>>,
    round_starts: Mutex<Vec<Instant>>,
    fail_inserts: AtomicBool,
}

impl FakeStorage {
    fn stored_logs(&self) -> Vec<(String, u64)> {
        let mut stored = self.0.rows.lock().clone();
        stored.sort();
        stored
    }
}

#[async_trait]
impl Storage for FakeStorage {
    async fn store_logs<T: Iterator<Item = QueryExecutedRow> + Sized + Send>(
        &self,
        query_logs: T,
    ) -> anyhow::Result<()> {
        if self.0.fail_inserts.load(Ordering::Relaxed) {
            anyhow::bail!("insert failed");
        }
        self.0
            .rows
            .lock()
            .extend(query_logs.map(|row| (row.worker_id().to_owned(), row.worker_timestamp)));
        Ok(())
    }

    async fn store_heartbeats<T: Iterator<Item = PingRow> + Sized + Send>(
        &self,
        _pings: T,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn get_last_stored(&self) -> anyhow::Result<HashMap<String, u64>> {
        self.0.round_starts.lock().push(Instant::now());
        let mut last = HashMap::<String, u64>::new();
        for (worker_id, timestamp) in self.0.rows.lock().iter() {
            let entry = last.entry(worker_id.clone()).or_default();
            *entry = (*entry).max(*timestamp);
        }
        Ok(last)
    }

    async fn store_portal_logs<T: Iterator<Item = QueryFinishedRow> + Sized + Send>(
        &self,
        _portal_logs: T,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }
}

fn worker(n: u8) -> PeerId {
    Keypair::ed25519_from_bytes([n; 32])
        .unwrap()
        .public()
        .to_peer_id()
}

/// `count` validly signed logs, one second apart.
fn signed_logs(worker_id: PeerId, count: usize) -> Vec<QueryExecuted> {
    let client = Keypair::ed25519_from_bytes([255; 32]).unwrap();
    (0..count)
        .map(|i| {
            let timestamp_ms = BASE_TIMESTAMP_MS + i as u64 * 1000;
            let mut query = Query {
                query_id: format!("00000000-0000-4000-8000-{i:012}"),
                dataset: "s3://ethereum-mainnet".to_owned(),
                query: "{}".to_owned(),
                timestamp_ms,
                ..Default::default()
            };
            query.sign(&client, worker_id).unwrap();
            QueryExecuted {
                client_id: client.public().to_peer_id().to_string(),
                query: Some(query),
                timestamp_ms,
                result: Some(query_executed::Result::Ok(QueryOkSummary::default())),
                ..Default::default()
            }
        })
        .collect()
}

fn server<L: LogsSource>(
    workers: L,
    worker_ids: HashSet<PeerId>,
    storage: &FakeStorage,
    max_buffer_size: usize,
) -> Arc<Server<L, FakeStorage>> {
    let collector = LogsCollector::with_limits(storage.clone(), max_buffer_size, max_buffer_size);
    let server = Server::new(workers, collector);
    *server.registered_workers.lock() = worker_ids;
    Arc::new(server)
}

/// Runs the collection loop for `duration` of paused, auto-advancing time and returns
/// the second at which each round started.
async fn run_rounds(
    workers: FakeWorkers,
    storage: &FakeStorage,
    max_buffer_size: usize,
    backlog_interval: Option<Duration>,
    duration: Duration,
) -> Vec<u64> {
    let server = server(
        workers.clone(),
        workers.worker_ids(),
        storage,
        max_buffer_size,
    );

    let start = Instant::now();
    let collecting =
        server.run_collecting_task(INTERVAL, backlog_interval, 30, CancellationToken::new());
    tokio::time::timeout(duration, collecting)
        .await
        .expect_err("the collection loop only stops when cancelled");

    let round_starts = storage.0.round_starts.lock();
    round_starts
        .iter()
        .map(|t| t.duration_since(start).as_secs())
        .collect()
}

// One worker holding 30 pages of logs. A round collects MAX_PAGES = 5 of them and
// takes 5s (1s per page), so draining it takes 6 rounds.
const BACKLOG: usize = 6 * MAX_PAGES * PAGE_SIZE;

#[tokio::test(start_paused = true)]
async fn without_backlog_interval_backlog_drains_one_round_per_interval() {
    let workers = FakeWorkers::with_logs(&[BACKLOG]);
    let expected = workers.all_logs();
    let storage = FakeStorage::default();

    let rounds = run_rounds(workers, &storage, LARGE_BUFFER, None, secs(700)).await;

    assert_eq!(rounds, [0, 120, 240, 360, 480, 600]);
    assert_eq!(storage.stored_logs(), expected);
}

#[tokio::test(start_paused = true)]
async fn backlog_interval_drains_backlog_then_returns_to_collection_interval() {
    let workers = FakeWorkers::with_logs(&[BACKLOG]);
    let expected = workers.all_logs();
    let storage = FakeStorage::default();

    let rounds = run_rounds(workers, &storage, LARGE_BUFFER, Some(secs(10)), secs(300)).await;

    // Drained by the round at 50s instead of 600s, then back to every 120s.
    assert_eq!(rounds, [0, 10, 20, 30, 40, 50, 170, 290]);
    assert_eq!(storage.stored_logs(), expected);
}

#[tokio::test(start_paused = true)]
async fn rounds_longer_than_backlog_interval_run_back_to_back() {
    let workers = FakeWorkers::with_logs(&[BACKLOG]);
    let expected = workers.all_logs();
    let storage = FakeStorage::default();

    let rounds = run_rounds(workers, &storage, LARGE_BUFFER, Some(secs(2)), secs(300)).await;

    // Each round takes 5s, so the next one starts as soon as the previous one ends.
    assert_eq!(rounds, [0, 5, 10, 15, 20, 25, 145, 265]);
    assert_eq!(storage.stored_logs(), expected);
}

#[tokio::test(start_paused = true)]
async fn backlog_interval_is_capped_at_collection_interval() {
    let workers = FakeWorkers::with_logs(&[BACKLOG]);
    let storage = FakeStorage::default();

    let rounds = run_rounds(workers, &storage, LARGE_BUFFER, Some(secs(600)), secs(300)).await;

    assert_eq!(rounds, [0, 120, 240]);
}

#[tokio::test(start_paused = true)]
async fn caught_up_workers_add_no_rounds_or_requests() {
    let mut rounds = Vec::new();
    let mut requests = Vec::new();
    for backlog_interval in [None, Some(secs(10))] {
        // Each worker's logs fit in one page.
        let workers = FakeWorkers::with_logs(&[5, 5, 5]);
        let request_count = workers.requests.clone();
        let storage = FakeStorage::default();
        rounds.push(run_rounds(workers, &storage, LARGE_BUFFER, backlog_interval, secs(300)).await);
        requests.push(request_count.load(Ordering::Relaxed));
    }

    assert_eq!(rounds, [[0, 120, 240], [0, 120, 240]]);
    // One request per worker per round, with or without the backlog interval.
    assert_eq!(requests, [9, 9]);
}

#[tokio::test(start_paused = true)]
async fn unreachable_workers_do_not_start_rounds_early() {
    let mut workers = FakeWorkers::with_logs(&[]);
    workers.unreachable.insert(worker(0));
    let storage = FakeStorage::default();

    let rounds = run_rounds(workers, &storage, LARGE_BUFFER, Some(secs(10)), secs(300)).await;

    assert_eq!(rounds, [0, 120, 240]);
}

#[tokio::test(start_paused = true)]
async fn failed_inserts_do_not_start_rounds_early() {
    let workers = FakeWorkers::with_logs(&[BACKLOG]);
    let storage = FakeStorage::default();
    storage.0.fail_inserts.store(true, Ordering::Relaxed);

    let rounds = run_rounds(workers, &storage, LARGE_BUFFER, Some(secs(10)), secs(300)).await;

    assert_eq!(rounds, [0, 120, 240]);
    assert!(storage.stored_logs().is_empty());
}

#[tokio::test(start_paused = true)]
async fn full_buffer_starts_rounds_early_without_losing_logs() {
    // Three workers with 5 pages each: one round would cover them all, but the
    // buffer only fits 25 logs, so rows are dropped and workers skipped until they
    // are collected in later rounds.
    let workers = FakeWorkers::with_logs(&[50, 50, 50]);
    let expected = workers.all_logs();
    let row_size = QueryExecutedRow::try_from(signed_logs(worker(0), 1).remove(0), worker(0))
        .unwrap()
        .estimated_size();
    let storage = FakeStorage::default();

    let rounds = run_rounds(workers, &storage, 25 * row_size, Some(secs(10)), secs(100)).await;

    // 150 logs at 25 per round take 6 rounds, each started early. A 7th, empty one
    // follows when the last round happens to fill the buffer exactly: which worker
    // is cut off depends on the (random) order they are processed in.
    assert!(matches!(rounds.len(), 6 | 7), "{rounds:?}");
    assert!(rounds.windows(2).all(|w| w[1] - w[0] == 10), "{rounds:?}");
    // Without the backlog interval, only the first 25 would be stored by now.
    assert_eq!(storage.stored_logs(), expected);
}

#[tokio::test(start_paused = true)]
async fn logs_with_invalid_signatures_are_dropped() {
    let mut workers = FakeWorkers::with_logs(&[5]);
    let expected = workers.all_logs();
    // Signed for a different worker.
    let mut forged = signed_logs(worker(1), 3);
    for log in &mut forged {
        log.timestamp_ms += 1_000_000;
        log.query.as_mut().unwrap().timestamp_ms += 1_000_000;
    }
    workers.logs.get_mut(&worker(0)).unwrap().extend(forged);
    let storage = FakeStorage::default();

    run_rounds(workers, &storage, LARGE_BUFFER, None, secs(10)).await;

    assert_eq!(storage.stored_logs(), expected);
}

/// Blocks its thread on every request and records which thread that happened on.
struct BusyWorkers(Arc<Mutex<HashSet<ThreadId>>>);

impl LogsSource for BusyWorkers {
    async fn request_logs(
        &self,
        _worker_id: PeerId,
        _request: LogsRequest,
    ) -> anyhow::Result<QueryLogs> {
        self.0.lock().insert(std::thread::current().id());
        std::thread::sleep(Duration::from_millis(20));
        Ok(QueryLogs::default())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_are_collected_off_the_collection_loop_thread() {
    let threads = Arc::<Mutex<HashSet<ThreadId>>>::default();
    let worker_ids = (0..20).map(worker).collect();
    let server = server(
        BusyWorkers(threads.clone()),
        worker_ids,
        &FakeStorage::default(),
        LARGE_BUFFER,
    );

    // The first round starts immediately; the timeout stops the loop before the second.
    let collecting = server.run_collecting_task(INTERVAL, None, 30, CancellationToken::new());
    tokio::time::timeout(Duration::from_millis(500), collecting)
        .await
        .expect_err("the collection loop only stops when cancelled");

    let threads = threads.lock();
    assert!(
        !threads.contains(&std::thread::current().id()),
        "collected on the collection loop's thread"
    );
    assert!(
        threads.len() > 1,
        "all workers collected on a single thread"
    );
}

/// Not a test: measures one round's CPU work (signature checks and row conversion;
/// the fake transport skips network I/O and protobuf decoding) with the conversion
/// confined to one thread, as the collector ran before, and spread over all cores.
/// `cargo test --release -p logs-collector -- --ignored --nocapture bench_`
#[test]
#[ignore]
fn bench_round_single_vs_multi_thread() {
    let mut workers = FakeWorkers::with_logs(&[1000; 32]);
    workers.page_size = 200;
    workers.latency = Duration::ZERO;
    let rows = workers.logs.values().map(Vec::len).sum::<usize>();

    let mut single = tokio::runtime::Builder::new_current_thread();
    single.max_blocking_threads(1);
    let multi = tokio::runtime::Builder::new_multi_thread();
    for (name, mut builder) in [("1 thread", single), ("all cores", multi)] {
        let runtime = builder.enable_all().build().unwrap();
        let elapsed = runtime.block_on(async {
            let storage = FakeStorage::default();
            let server = server(
                workers.clone(),
                workers.worker_ids(),
                &storage,
                LARGE_BUFFER,
            );
            let start = std::time::Instant::now();
            let collecting =
                server.run_collecting_task(INTERVAL, None, 30, CancellationToken::new());
            let stored = async {
                while storage.0.rows.lock().len() < rows {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::select! {
                _ = collecting => unreachable!(),
                _ = stored => start.elapsed(),
            }
        });
        println!(
            "{name:>9}: {rows} rows in {elapsed:?} ({:.0} rows/s)",
            rows as f64 / elapsed.as_secs_f64()
        );
    }
}

fn secs(secs: u64) -> Duration {
    Duration::from_secs(secs)
}
