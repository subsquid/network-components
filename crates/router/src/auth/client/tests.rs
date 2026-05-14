use super::*;
use crate::auth::clock::TestClock;
use serde_json::json;
use std::sync::OnceLock;
use tokio::sync::{Mutex, MutexGuard};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Serialise tests that read or write VALIDATE_CALL_TOTAL — the
/// counter is process-global and concurrent tests would race on it.
async fn metrics_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

async fn server() -> MockServer {
    MockServer::start().await
}

fn client(server: &MockServer, clock: Arc<dyn Clock>) -> NetworkApiClient {
    NetworkApiClient::with_clock(Some(Url::parse(&server.uri()).unwrap()), clock)
}

#[tokio::test]
async fn validate_200_returns_exists() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"user_id": "u1", "api_key_id": "key1"})),
        )
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    assert_eq!(
        c.validate("sqd_data_abc_xyz").await,
        ValidateResult::Exists {
            user_id: "u1".into(),
            api_key_id: "key1".into(),
            expires_at: None,
        }
    );
}

// New: when the validate API returns an `expires_at` (server-side TTL),
// it must propagate through to ValidateResult so the cache can clamp the
// entry's lifetime below TTL_EXISTS. Server timestamp is unix seconds; we
// project onto the monotonic clock (TestClock here) by computing the
// remaining duration relative to "now" and adding to clock.now().
#[tokio::test]
async fn validate_200_propagates_expires_at() {
    let s = server().await;
    // Server says: token expires 30s from now (unix seconds).
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let exp = now_unix + 30;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"user_id": "u1", "api_key_id": "key1", "expires_at": exp})),
        )
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    match c.validate("sqd_data_abc_xyz").await {
        ValidateResult::Exists {
            user_id,
            api_key_id,
            expires_at: Some(_),
        } => {
            assert_eq!(user_id, "u1");
            assert_eq!(api_key_id, "key1");
            // Specific value depends on TestClock starting reference;
            // the existence of Some(_) is what we assert here, plus the
            // cache test below which exercises the clamping behaviour.
        }
        other => panic!("expected Exists with expires_at, got {:?}", other),
    }
}

// `expires_at` already in the past (server says: this is dead) -> we
// return Some(now) so the cache demotes the entry to Deleted on insert.
#[tokio::test]
async fn validate_200_past_expires_at_returns_now() {
    let s = server().await;
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let past = now_unix - 60;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"user_id": "u1", "api_key_id": "key1", "expires_at": past})),
        )
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    let result = c.validate("sqd_data_abc_xyz").await;
    assert!(
        matches!(
            result,
            ValidateResult::Exists {
                expires_at: Some(_),
                ..
            }
        ),
        "past expires_at must still surface as Some(_) so cache can demote"
    );
}

#[tokio::test]
async fn validate_404_with_json_returns_deleted() {
    let s = server().await;
    // 404 + JSON content-type = the validate API itself answering "deleted".
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(
            ResponseTemplate::new(404)
                .insert_header("content-type", "application/json")
                .set_body_json(json!({"deleted": true})),
        )
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    assert_eq!(c.validate("sqd_data_x_y").await, ValidateResult::Deleted);
}

// 404 WITHOUT a JSON content-type is most likely a misrouted request
// (gateway error page, wrong subpath, etc.) — fail open so a deployment
// misconfig doesn't masquerade as a flood of revoked keys.
#[tokio::test]
async fn validate_404_without_json_fails_open() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(
            ResponseTemplate::new(404)
                .insert_header("content-type", "text/html")
                .set_body_string("<html>404 not found</html>"),
        )
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    assert_eq!(c.validate("sqd_data_x_y").await, ValidateResult::FailOpen);
}

// Bare 404 with no content-type: ambiguous, also fail open. The validate
// API contract requires JSON; anything else looks like misrouting.
#[tokio::test]
async fn validate_404_no_content_type_fails_open() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    assert_eq!(c.validate("sqd_data_x_y").await, ValidateResult::FailOpen);
}

#[tokio::test]
async fn validate_500_returns_fail_open() {
    let s = server().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    assert_eq!(c.validate("sqd_data_x_y").await, ValidateResult::FailOpen);
}

#[tokio::test]
async fn validate_timeout_returns_fail_open() {
    let s = server().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    let start = std::time::Instant::now();
    assert_eq!(c.validate("sqd_data_x_y").await, ValidateResult::FailOpen);
    assert!(
        start.elapsed() < Duration::from_millis(450),
        "must time out within ~250ms; took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn validate_connection_error_returns_fail_open() {
    // Port 1 is reserved/refused on basically every host.
    let c = NetworkApiClient::with_clock(
        Some(Url::parse("http://127.0.0.1:1").unwrap()),
        TestClock::new(),
    );
    assert_eq!(c.validate("sqd_data_x_y").await, ValidateResult::FailOpen);
}

#[tokio::test]
async fn breaker_opens_after_50_consecutive_errors() {
    let s = server().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&s)
        .await;
    let clock = TestClock::new();
    let c = client(&s, clock);
    for _ in 0..50 {
        let _ = c.validate("sqd_data_x_y").await;
    }
    let received_before = s.received_requests().await.unwrap().len();
    assert_eq!(received_before, 50);
    // 51st must short-circuit (no HTTP call).
    assert_eq!(c.validate("sqd_data_x_y").await, ValidateResult::FailOpen);
    let received_after = s.received_requests().await.unwrap().len();
    assert_eq!(
        received_after, 50,
        "breaker must short-circuit the 51st call"
    );
}

#[tokio::test]
async fn breaker_probes_after_30s() {
    let s = server().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&s)
        .await;
    let clock = TestClock::new();
    let c = NetworkApiClient::with_clock(Some(Url::parse(&s.uri()).unwrap()), clock.clone());
    for _ in 0..50 {
        let _ = c.validate("sqd_data_x_y").await;
    }
    // Breaker open. Advance 30s.
    clock.advance(Duration::from_secs(30));
    // Probe should be admitted (and fail again, but the call hits wiremock).
    let _ = c.validate("sqd_data_x_y").await;
    let received = s.received_requests().await.unwrap().len();
    assert_eq!(received, 51, "one probe must be admitted after 30s");
}

#[tokio::test]
async fn breaker_resets_on_success() {
    let s = server().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string(""))
        .up_to_n_times(10)
        .mount(&s)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"user_id": "u", "api_key_id": "key"})),
        )
        .mount(&s)
        .await;
    let clock = TestClock::new();
    let c = client(&s, clock);
    for _ in 0..10 {
        let _ = c.validate("sqd_data_x_y").await;
    }
    // 11th: success -> breaker counter resets.
    assert!(matches!(
        c.validate("sqd_data_x_y").await,
        ValidateResult::Exists { .. }
    ));
    // We can now sustain another 49 errors before breaking.
    // Mount 50 more 500s.
    s.reset().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&s)
        .await;
    for _ in 0..49 {
        let _ = c.validate("sqd_data_x_y").await;
    }
    let received = s.received_requests().await.unwrap().len();
    assert_eq!(
        received, 49,
        "breaker should still be closed after success reset"
    );
}

// 200 OK with malformed body must not cache garbage.
#[tokio::test]
async fn validate_malformed_success_body_is_fail_open() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    assert_eq!(c.validate("sqd_data_x_y").await, ValidateResult::FailOpen);
}

// 200 OK with valid JSON but missing user_id must not be Exists.
#[tokio::test]
async fn validate_200_missing_user_id_is_fail_open() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"other": "field"})))
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    assert_eq!(c.validate("sqd_data_x_y").await, ValidateResult::FailOpen);
}

// half-open: only ONE probe slips through after the open window.
#[tokio::test]
async fn breaker_half_open_admits_exactly_one_probe() {
    let s = server().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&s)
        .await;
    let clock = TestClock::new();
    let c = NetworkApiClient::with_clock(Some(Url::parse(&s.uri()).unwrap()), clock.clone());
    for _ in 0..50 {
        let _ = c.validate("sqd_data_x_y").await;
    }
    clock.advance(Duration::from_secs(30));
    // 10 simultaneous calls after the open window: only 1 probe should be admitted.
    let before = s.received_requests().await.unwrap().len();
    let mut handles = Vec::new();
    let c = Arc::new(c);
    for _ in 0..10 {
        let c = c.clone();
        handles.push(tokio::spawn(
            async move { c.validate("sqd_data_x_y").await },
        ));
    }
    for h in handles {
        let _ = h.await;
    }
    let after = s.received_requests().await.unwrap().len();
    assert_eq!(
        after - before,
        1,
        "exactly one probe must reach the API after the window elapses"
    );
}

/// base_url with a subpath (e.g. behind a reverse proxy at `/api/v2/`)
/// must be preserved when building the validate URL.
#[test]
fn build_url_preserves_subpath_with_trailing_slash() {
    let c = NetworkApiClient::with_clock(
        Some(Url::parse("http://auth.example.com/api/v2/").unwrap()),
        TestClock::new(),
    );
    let joined = c
        .base_url
        .as_ref()
        .unwrap()
        .join("internal/validate")
        .unwrap();
    assert_eq!(
        joined.as_str(),
        "http://auth.example.com/api/v2/internal/validate"
    );
}

/// Without a trailing slash on the input, the client should still
/// resolve to the subpath (we normalise at construction).
#[test]
fn build_url_preserves_subpath_without_trailing_slash() {
    let c = NetworkApiClient::with_clock(
        Some(Url::parse("http://auth.example.com/api/v2").unwrap()),
        TestClock::new(),
    );
    let joined = c
        .base_url
        .as_ref()
        .unwrap()
        .join("internal/validate")
        .unwrap();
    assert_eq!(
        joined.as_str(),
        "http://auth.example.com/api/v2/internal/validate"
    );
}

/// Plain root-only base still resolves correctly.
#[test]
fn build_url_root_base() {
    let c = NetworkApiClient::with_clock(
        Some(Url::parse("http://auth.example.com").unwrap()),
        TestClock::new(),
    );
    let joined = c
        .base_url
        .as_ref()
        .unwrap()
        .join("internal/validate")
        .unwrap();
    assert_eq!(joined.as_str(), "http://auth.example.com/internal/validate");
}

/// End-to-end: validate against a wiremock mounted at a subpath
/// route. Pre-fix, the subpath was silently dropped.
#[tokio::test]
async fn validate_calls_subpath_route() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/internal/validate"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"user_id": "u1", "api_key_id": "key1"})),
        )
        .mount(&s)
        .await;
    let c = NetworkApiClient::with_clock(
        Some(Url::parse(&format!("{}/api/v2/", s.uri())).unwrap()),
        TestClock::new(),
    );
    assert!(matches!(
        c.validate("sqd_data_x").await,
        ValidateResult::Exists { .. }
    ));
    assert_eq!(s.received_requests().await.unwrap().len(), 1);
}

fn validate_call_count(label: &str) -> u64 {
    crate::metrics::VALIDATE_CALL_TOTAL
        .with_label_values(&[label])
        .get()
}

// VALIDATE_CALL_TOTAL must NOT increment when the
// client is disabled (no base_url). No network call → no count.
#[tokio::test]
async fn validate_call_total_silent_when_disabled() {
    let _g = metrics_lock().await;
    let c = NetworkApiClient::with_clock(None, TestClock::new());
    let before = validate_call_count("fail_open");
    let _ = c.validate("sqd_data_x_y").await;
    let _ = c.validate("sqd_data_x_y").await;
    let _ = c.validate("sqd_data_x_y").await;
    assert_eq!(
        validate_call_count("fail_open"),
        before,
        "disabled client must not inflate validate_call_total"
    );
}

// Once the breaker is Open, validate() must short-circuit without
// touching the network. We verify via wiremock's local request log
// (no global metric reference) so the test is independent of any
// other test running in parallel.
#[tokio::test]
async fn validate_short_circuits_when_breaker_open() {
    let s = server().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    for _ in 0..50 {
        let _ = c.validate("sqd_data_x_y").await;
    }
    for _ in 0..10 {
        let _ = c.validate("sqd_data_x_y").await;
    }
    assert_eq!(
        s.received_requests().await.unwrap().len(),
        50,
        "open breaker must not issue any further network calls"
    );
}

// A successful validate hits the wire (verified via wiremock).
#[tokio::test]
async fn validate_issues_a_real_send() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"user_id": "u1", "api_key_id": "key1"})),
        )
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    let _ = c.validate("sqd_data_x_y").await;
    assert_eq!(s.received_requests().await.unwrap().len(), 1);
}

// counter and state mutate under one lock. A success
// followed by a single failure must NOT push the breaker open if the
// success reset the counter, even when the threshold-1 failures
// preceded the success.
#[tokio::test]
async fn success_resets_counter_atomically_with_state() {
    let clock = TestClock::new();
    let breaker = Arc::new(CircuitBreaker::new(
        3,
        Duration::from_secs(30),
        clock as Arc<dyn Clock>,
    ));
    // 2 failures (threshold 3, not yet open).
    breaker.acquire().unwrap().record_failure();
    breaker.acquire().unwrap().record_failure();
    // A success here must reset consecutive_errors to 0.
    breaker.acquire().unwrap().record_success();
    // A subsequent single failure must NOT open the breaker — the
    // streak was broken. Pre-fix, a stale fetch_add could have left
    // n=3 visible to record_failure even after the reset.
    breaker.acquire().unwrap().record_failure();
    assert!(
        breaker.acquire().is_some(),
        "single failure after a success must not open the breaker"
    );
}

// RAII probe guard: dropping a Permit without calling
// record_success/record_failure must record a failure, so the breaker
// never gets stuck in HalfOpen if a probe is cancelled mid-flight.
#[tokio::test]
async fn dropped_permit_records_failure() {
    let clock = TestClock::new();
    let breaker = Arc::new(CircuitBreaker::new(
        3,
        Duration::from_secs(30),
        clock.clone() as Arc<dyn Clock>,
    ));
    // Drop 3 permits without recording → breaker should open.
    for _ in 0..3 {
        let _permit = breaker.acquire();
        // implicit drop → record_failure_internal
    }
    assert!(breaker.acquire().is_none(), "breaker must be open");
}

// cancellation safety: a probe (HalfOpen permit) that is
// dropped without resolution re-opens the breaker for another full
// window, instead of leaving it stuck HalfOpen forever.
#[tokio::test]
async fn cancelled_probe_reopens_breaker() {
    let clock = TestClock::new();
    let breaker = Arc::new(CircuitBreaker::new(
        3,
        Duration::from_secs(30),
        clock.clone() as Arc<dyn Clock>,
    ));
    // Open the breaker via 3 failures.
    for _ in 0..3 {
        breaker.acquire().unwrap().record_failure();
    }
    assert!(breaker.acquire().is_none());
    clock.advance(Duration::from_secs(30));
    // Acquire the probe permit and drop it without recording (simulates
    // client-cancelled future, panic, or shutdown).
    let probe = breaker.acquire().expect("probe should be admitted");
    drop(probe);
    // Breaker must NOT be stuck HalfOpen — should be Open again.
    // After only 30s elapsed (already past the first window), advance
    // another 30s and verify a new probe is admitted exactly once.
    clock.advance(Duration::from_secs(30));
    assert!(breaker.acquire().is_some());
    assert!(
        breaker.acquire().is_none(),
        "second concurrent probe must be denied"
    );
}

// half-open probe failure re-opens the breaker for another full window.
#[tokio::test]
async fn breaker_half_open_failure_reopens() {
    let s = server().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&s)
        .await;
    let clock = TestClock::new();
    let c = NetworkApiClient::with_clock(Some(Url::parse(&s.uri()).unwrap()), clock.clone());
    for _ in 0..50 {
        let _ = c.validate("sqd_data_x_y").await;
    }
    clock.advance(Duration::from_secs(30));
    let _ = c.validate("sqd_data_x_y").await; // probe fails
    let received = s.received_requests().await.unwrap().len();
    // Immediately after a failed probe, breaker is Open again.
    let _ = c.validate("sqd_data_x_y").await;
    assert_eq!(
        s.received_requests().await.unwrap().len(),
        received,
        "breaker must re-open after probe failure"
    );
}

#[tokio::test]
async fn request_body_is_token_only() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .and(body_json(json!({"token": "sqd_data_abc_xyz"})))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"user_id": "u", "api_key_id": "key"})),
        )
        .mount(&s)
        .await;
    let c = client(&s, TestClock::new());
    assert!(matches!(
        c.validate("sqd_data_abc_xyz").await,
        ValidateResult::Exists { .. }
    ));
}
