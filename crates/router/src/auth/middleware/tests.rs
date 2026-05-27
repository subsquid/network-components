use super::*;
use crate::auth::clock::TestClock;
use crate::metrics::{
    AUTH_LATENCY_SECONDS, AUTH_TOTAL, CACHE_HIT_TOTAL, CACHE_MISS_TOTAL, VALIDATE_CALL_TOTAL,
};
use axum::body::Body;
use axum::extract::Extension;
use axum::http::{header, Request, StatusCode};
use axum::middleware::from_fn;
use axum::routing::get;
use axum::Router;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use tower::ServiceExt;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Prometheus counters are process-global; serialise tests that assert on
/// metric deltas so concurrent tests don't shift the baseline.
async fn metrics_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

async fn downstream_handler(req: Request<Body>) -> Response {
    match req.extensions().get::<AuthContext>() {
        Some(ctx) => format!("ok:{}:{}", ctx.user_id, ctx.api_key_id).into_response(),
        None => "ok:no-ctx".into_response(),
    }
}

fn app(state: Arc<AuthState>) -> Router {
    Router::new()
        .route("/test", get(downstream_handler).layer(from_fn(super::auth)))
        .layer(Extension(state))
}

fn req(uri: &str) -> axum::http::request::Builder {
    Request::builder().uri(uri)
}

async fn body_string(resp: Response) -> String {
    let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn assert_no_worker_jwt_header(resp: &Response) {
    assert!(
        resp.headers().get(WORKER_JWT_HEADER).is_none(),
        "{WORKER_JWT_HEADER} must not be present"
    );
}

fn assert_user_id_header(resp: &Response, expected: &str) {
    assert_eq!(
        resp.headers()
            .get(USER_ID_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(expected)
    );
}

fn assert_api_key_id_header(resp: &Response, expected: &str) {
    assert_eq!(
        resp.headers()
            .get(API_KEY_ID_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(expected)
    );
}

fn assert_no_user_id_header(resp: &Response) {
    assert!(
        resp.headers().get(USER_ID_HEADER).is_none(),
        "{USER_ID_HEADER} must not be present"
    );
}

fn assert_no_api_key_id_header(resp: &Response) {
    assert!(
        resp.headers().get(API_KEY_ID_HEADER).is_none(),
        "{API_KEY_ID_HEADER} must not be present"
    );
}

fn worker_jwt(resp: &Response) -> Option<&str> {
    resp.headers()
        .get(WORKER_JWT_HEADER)
        .and_then(|value| value.to_str().ok())
}

fn decode_worker_jwt(token: &str) -> crate::auth::jwt::WorkerJwtClaims {
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3);
    URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap()
}

fn good_validate_mock(user_id: &str) -> Mock {
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "user_id": user_id,
            "api_key_id": "key1"
        })))
}

fn nf_validate_mock() -> Mock {
    // Validate API answer for "key deleted": 404 with a JSON body. The
    // application/json content-type is what tells us this is a real
    // API response (vs a misrouted 404 from a gateway / proxy error
    // page). See `client.rs` for the heuristic.
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(
            ResponseTemplate::new(404)
                .insert_header("content-type", "application/json")
                .set_body_json(serde_json::json!({"deleted": true})),
        )
}

fn count_auth(label: &str) -> u64 {
    AUTH_TOTAL.with_label_values(&[label]).get()
}
fn count_cache_hit(label: &str) -> u64 {
    CACHE_HIT_TOTAL.with_label_values(&[label]).get()
}
fn count_validate(label: &str) -> u64 {
    VALIDATE_CALL_TOTAL.with_label_values(&[label]).get()
}

#[tokio::test]
async fn bearer_header_extracts_key() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u1").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let before_ok = count_auth("ok");
    let resp = app(state.clone())
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let claims = decode_worker_jwt(worker_jwt(&resp).expect("worker jwt header"));
    assert_eq!(claims.u, "u1");
    assert_eq!(claims.k, "key1");
    assert_eq!(claims.exp - claims.iat, 3600);
    assert_user_id_header(&resp, "u1");
    assert_api_key_id_header(&resp, "key1");
    assert_eq!(body_string(resp).await, "ok:u1:key1");
    assert_eq!(count_auth("ok") - before_ok, 1);
}

#[tokio::test]
async fn worker_jwt_expiry_is_capped_by_validate_expires_at() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 120;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "user_id": "u1",
            "api_key_id": "key1",
            "expires_at": exp,
        })))
        .mount(&s)
        .await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let claims = decode_worker_jwt(worker_jwt(&resp).expect("worker jwt header"));
    assert_eq!(claims.exp, exp);
}

#[tokio::test]
async fn expired_validate_deadline_is_denied_immediately() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 1;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "user_id": "u1",
            "api_key_id": "key1",
            "expires_at": exp,
        })))
        .mount(&s)
        .await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_expired_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_no_worker_jwt_header(&resp);
}

#[tokio::test]
async fn worker_jwt_is_reused_for_same_identity() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u1").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let mut tokens = Vec::new();
    for _ in 0..2 {
        let resp = app(state.clone())
            .oneshot(
                req("/test")
                    .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        tokens.push(worker_jwt(&resp).expect("worker jwt header").to_string());
    }

    assert_eq!(tokens[0], tokens[1]);
}

// `Token:` header is NOT a fallback. Treated as missing.
#[tokio::test]
async fn no_token_header_fallback() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u1").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let resp = app(state)
        .oneshot(
            req("/test")
                .header("Token", "sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // No Authorization header -> Missing -> 403 (enforce=true).
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_no_worker_jwt_header(&resp);
    assert_no_user_id_header(&resp);
    assert_no_api_key_id_header(&resp);
    // Wiremock must not have been hit.
    assert_eq!(s.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn non_bearer_scheme_treated_as_missing() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), false, TestClock::new());

    let before = count_auth("missing");
    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Basic dXNlcjpwYXNz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(count_auth("missing") - before, 1);
}

#[tokio::test]
async fn missing_prefix_short_circuits() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let before_invalid = count_auth("invalid");
    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer not_an_sqd_key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(count_auth("invalid") - before_invalid, 1);
    assert_eq!(
        s.received_requests().await.unwrap().len(),
        0,
        "cheap-reject must not call API"
    );
}

#[tokio::test]
async fn missing_token_passes_when_disabled() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), false, TestClock::new());

    let before = count_auth("missing");
    let resp = app(state)
        .oneshot(req("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(count_auth("missing") - before, 1);
}

#[tokio::test]
async fn missing_token_403_when_enabled() {
    let _g = metrics_lock().await;
    let state = AuthState::for_test(None, true, TestClock::new());
    let resp = app(state)
        .oneshot(req("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        resp.headers()
            .get(header::WWW_AUTHENTICATE)
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer realm=\"sqd-archive\""
    );
    let body: Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(body["error"], "CREDENTIALS_INVALID");
    assert!(body["message"].as_str().unwrap().contains("API key"));
}

// the flag changes only the action, not the work.
#[tokio::test]
async fn enforce_disabled_still_does_full_work() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await; // 404 -> Deleted
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), false, TestClock::new());

    let before_calls = count_validate("deleted");
    let before_invalid = count_auth("invalid");
    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // enforce=false -> request passes.
    assert_eq!(resp.status(), StatusCode::OK);
    // ...but Network API was called and the metric incremented.
    assert_eq!(s.received_requests().await.unwrap().len(), 1);
    assert_eq!(count_validate("deleted") - before_calls, 1);
    assert_eq!(count_auth("invalid") - before_invalid, 1);
}

// bad-key flood: one Network API call per 15s.
#[tokio::test]
async fn bad_key_flood_one_call_per_15s() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;
    let clock = TestClock::new();
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, clock.clone());

    for _ in 0..100 {
        let resp = app(state.clone())
            .oneshot(
                req("/test")
                    .header(header::AUTHORIZATION, "Bearer sqd_data_bad_xxx")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }
    assert_eq!(
        s.received_requests().await.unwrap().len(),
        1,
        "100 reqs with same bad key within 15s -> 1 API call"
    );

    // Past the 15s Deleted TTL -> next request re-checks.
    clock.advance(Duration::from_secs(16));
    let _ = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_bad_xxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(s.received_requests().await.unwrap().len(), 2);
}

// Timeout fails open and writes a brief FailedRecently sentinel so a
// burst of concurrent waiters doesn't each issue a 250ms timeout.
// After the sentinel TTL elapses the cache un-poisons.
#[tokio::test]
async fn network_api_timeout_fails_open() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .mount(&s)
        .await;
    let clock = TestClock::new();
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, clock.clone());

    let before_fail = count_auth("fail_open");
    let resp = app(state.clone())
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "fail-open must pass even when enforcing"
    );
    let claims = decode_worker_jwt(worker_jwt(&resp).expect("worker jwt header"));
    assert_eq!(claims.u, "fail-open");
    assert_eq!(claims.k, "fail-open");
    assert_eq!(claims.exp - claims.iat, 3600);
    assert_eq!(count_auth("fail_open") - before_fail, 1);

    // 2nd request within the 1s sentinel: served from cache, no API call.
    let _ = app(state.clone())
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(s.received_requests().await.unwrap().len(), 1);

    // After the sentinel TTL elapses, the cache un-poisons and the next
    // request retries the API (we still recover quickly from outage).
    clock.advance(Duration::from_secs(2));
    let _ = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(s.received_requests().await.unwrap().len(), 2);
}

// cached Deleted denies even during outage.
#[tokio::test]
async fn cached_deleted_denies_during_outage() {
    let _g = metrics_lock().await;
    // No wiremock at all - any call would fail.
    let state = AuthState::for_test(
        Some(Url::parse("http://127.0.0.1:1").unwrap()),
        true,
        TestClock::new(),
    );
    state.cache.put_deleted("sqd_data_abc_xyz".into());

    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_no_worker_jwt_header(&resp);
}

#[tokio::test]
async fn breaker_open_passes_through_without_dial() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&s)
        .await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    // Drive 50 distinct keys to get 50 cache misses + 50 errors -> breaker opens.
    for i in 0..50 {
        let token = format!("Bearer sqd_data_k{i}_xxx");
        let _ = app(state.clone())
            .oneshot(
                req("/test")
                    .header(header::AUTHORIZATION, token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    let before = s.received_requests().await.unwrap().len();
    assert_eq!(before, 50);

    // Next request: breaker open; expect fail-open (200, enforcing or not).
    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_kNEW_xxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let claims = decode_worker_jwt(worker_jwt(&resp).expect("worker jwt header"));
    assert_eq!(claims.u, "fail-open");
    assert_eq!(claims.k, "fail-open");
    assert_eq!(claims.exp - claims.iat, 3600);
    assert_eq!(
        s.received_requests().await.unwrap().len(),
        50,
        "breaker must short-circuit"
    );
}

#[tokio::test]
async fn key_id_in_request_extensions() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("user42").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "ok:user42:key1");
}

#[tokio::test]
async fn cache_miss_then_hit() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let before_miss = CACHE_MISS_TOTAL.get();
    let before_hit = count_cache_hit("exists");
    for _ in 0..2 {
        let resp = app(state.clone())
            .oneshot(
                req("/test")
                    .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    assert_eq!(
        s.received_requests().await.unwrap().len(),
        1,
        "second req must be cached"
    );
    assert_eq!(CACHE_MISS_TOTAL.get() - before_miss, 1);
    assert_eq!(count_cache_hit("exists") - before_hit, 1);
}

#[tokio::test]
async fn auth_latency_histogram_observes() {
    let _g = metrics_lock().await;
    let state = AuthState::for_test(None, true, TestClock::new());
    let before = AUTH_LATENCY_SECONDS.get_sample_count();
    let _ = app(state)
        .oneshot(req("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert!(AUTH_LATENCY_SECONDS.get_sample_count() > before);
}

// full token must never appear in any log line; only key_id may.
#[tokio::test]
#[tracing_test::traced_test]
async fn full_token_never_logged() {
    let s = MockServer::start().await;
    // Force the fail-open warn path so we know a log line was emitted.
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&s)
        .await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());
    let secret = "VERYRANDOMSECRET12345";
    let token = format!("Bearer sqd_data_abc_{secret}");
    let _ = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, &token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        !logs_contain(secret),
        "log captured the random token suffix"
    );
    assert!(
        !logs_contain(&token),
        "log captured the full Authorization header"
    );
}

// concurrent miss flood: the singleflight + cache combine to make
//       N concurrent requests for the same key produce exactly ONE
//       Network API call.
#[tokio::test]
async fn concurrent_miss_flood_coalesces_to_one_call() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    // Hold the response briefly so all 32 racers pile up at the singleflight.
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"user_id": "u1", "api_key_id": "key1"}))
                .set_delay(Duration::from_millis(50)),
        )
        .mount(&s)
        .await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let mut handles = Vec::new();
    for _ in 0..32 {
        let state = state.clone();
        handles.push(tokio::spawn(async move {
            app(state)
                .oneshot(
                    req("/test")
                        .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        }));
    }
    for h in handles {
        let resp = h.await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    assert_eq!(
        s.received_requests().await.unwrap().len(),
        1,
        "32 concurrent misses for the same key must produce exactly 1 API call"
    );
}

// sqd_data_ prefix only — empty token after the prefix is rejected.
#[tokio::test]
async fn empty_token_after_prefix_is_invalid() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());
    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(s.received_requests().await.unwrap().len(), 0);
}

// Malformed success body must never cache a positive (Exists) entry —
// it gets the FailedRecently sentinel like any other parse failure.
#[tokio::test]
async fn malformed_validate_body_does_not_cache_exists() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(200).set_body_string("garbage"))
        .mount(&s)
        .await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());
    let _ = app(state.clone())
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // No Exists / Deleted entry; only the brief FailedRecently sentinel.
    let cached = state.cache.get("sqd_data_abc_xyz");
    assert!(
        matches!(cached, Some(KeyState::FailedRecently) | None),
        "got {cached:?}"
    );
}

// FailOpen sentinel drains the singleflight queue without
// serialising N × 250ms timeouts. With a delayed wiremock, 32 concurrent
// racers should produce at most 1 upstream call (the leader's), with
// followers reading the FailedRecently sentinel.
#[tokio::test]
async fn fail_open_sentinel_drains_queue_without_serialised_timeouts() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .mount(&s)
        .await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let mut handles = Vec::new();
    for _ in 0..32 {
        let state = state.clone();
        handles.push(tokio::spawn(async move {
            app(state)
                .oneshot(
                    req("/test")
                        .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap().status(), StatusCode::OK);
    }
    assert_eq!(
        s.received_requests().await.unwrap().len(),
        1,
        "FailedRecently sentinel must short-circuit all followers"
    );
}

// duplicate Authorization headers are ambiguous
// (RFC 6750 §3.1). Reject as Invalid so a proxy that reorders or
// deduplicates headers can't smuggle a credential.
#[tokio::test]
async fn duplicate_authorization_headers_rejected() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());
    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .header(header::AUTHORIZATION, "Bearer sqd_data_def_uvw")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        s.received_requests().await.unwrap().len(),
        0,
        "duplicate Authorization headers must not reach the API"
    );
}

// Distinct tokens are independent cache entries — a stale entry for
// one token never affects another's lookup.
#[tokio::test]
async fn distinct_tokens_are_isolated_cache_entries() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());
    state.cache.put_deleted("sqd_data_OLD".into());
    // A different token is a different cache key — must validate via API.
    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_NEW")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(s.received_requests().await.unwrap().len(), 1);
}

// CACHE_MISS_TOTAL is incremented exactly once per
// *true* miss. Concurrent followers that wake up to a populated cache
// count as hits, not misses.
#[tokio::test]
async fn cache_miss_counter_does_not_double_count_under_singleflight() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u").mount(&s).await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());

    let before_miss = CACHE_MISS_TOTAL.get();
    let before_hit = count_cache_hit("exists");

    let mut handles = Vec::new();
    for _ in 0..16 {
        let state = state.clone();
        handles.push(tokio::spawn(async move {
            app(state)
                .oneshot(
                    req("/test")
                        .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    // 1 leader = 1 miss; 15 followers = 15 cache hits.
    assert_eq!(CACHE_MISS_TOTAL.get() - before_miss, 1);
    assert_eq!(count_cache_hit("exists") - before_hit, 15);
    assert_eq!(s.received_requests().await.unwrap().len(), 1);
}

// Error paths must never log the token (or any of its bytes).
#[tokio::test]
#[tracing_test::traced_test]
async fn warn_log_does_not_carry_token() {
    let s = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/internal/validate"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&s)
        .await;
    let state = AuthState::for_test(Some(Url::parse(&s.uri()).unwrap()), true, TestClock::new());
    let secret = "VERYSECRETMATERIAL12345";
    let _ = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, format!("Bearer sqd_data_{secret}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(!logs_contain(secret), "warn log must not carry the token");
    assert!(!logs_contain("sqd_data_VERYSECRETMATERIAL"));
}

// ─── IP-allowlist bypass ───────────────────────────────────────────────
//
// These cover the bypass branch added in `decide` (after duplicate-Auth
// rejection, before Bearer extraction). They mirror the empirical
// T1-T4 scenarios captured during prod debugging:
//
//   T1 baseline       : XOFF chain ends with the trusted upstream LB,
//                       second-from-right is the real client.
//   T2 spoof XFF      : client prepends a fake IP; walk-rightmost must
//                       still resolve to the LB-attested real client.
//   T3 direct ClusterIP: no XOFF — peer IP from ConnectInfo is used.
//   T4 spoof both     : same as T2; client-controlled X-Real-IP /
//                       X-Forwarded-For are NEVER consulted, only XOFF.

use ipnet::IpNet;
use std::net::SocketAddr;

fn netv(s: &str) -> IpNet {
    s.parse().unwrap()
}

/// Build an AuthState wired for IP-bypass. `s` provides the validate-API
/// URL that the standard Bearer path uses if we fall through.
fn bypass_state(
    s: &MockServer,
    enforce: bool,
    trusted: Vec<IpNet>,
    allow: Vec<IpNet>,
) -> Arc<AuthState> {
    AuthState::for_test_with_bypass(
        Some(Url::parse(&s.uri()).unwrap()),
        enforce,
        TestClock::new(),
        trusted,
        allow,
    )
}

/// Wrap a request in `ConnectInfo<SocketAddr>` so the middleware can
/// read the connection peer (mimicking what
/// `into_make_service_with_connect_info` does in prod).
fn with_connect_info(mut req: Request<Body>, peer: &str) -> Request<Body> {
    let addr: SocketAddr = peer.parse().unwrap();
    req.extensions_mut().insert(ConnectInfo(addr));
    req
}

// T1-equivalent: chain `<client>, <GLB>` with client in allowlist => bypass.
#[tokio::test]
async fn bypass_xoff_real_client_in_allowlist() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await; // would-be-404 if we fell through

    let state = bypass_state(
        &s,
        true,
        vec![netv("34.149.211.238/32")],
        vec![netv("10.0.0.0/8")],
    );

    let resp = app(state)
        .oneshot(
            req("/test")
                .header(ORIGINAL_FORWARDED_FOR, "10.4.5.5, 34.149.211.238")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let claims = decode_worker_jwt(worker_jwt(&resp).expect("worker jwt header"));
    assert_eq!(claims.u, "internal");
    assert_eq!(claims.k, "internal");
    assert_eq!(claims.exp - claims.iat, 3600);
    assert_user_id_header(&resp, "internal");
    assert_api_key_id_header(&resp, "internal");
    assert_eq!(body_string(resp).await, "ok:internal:internal");
    // Bypass must NOT touch the Network API.
    assert_eq!(s.received_requests().await.unwrap().len(), 0);
}

// T1-equivalent inverse: real client outside allowlist -> standard Bearer
// path runs. With no Authorization header, that yields Missing -> 403.
#[tokio::test]
async fn bypass_xoff_real_client_outside_allowlist() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = bypass_state(
        &s,
        true,
        vec![netv("34.149.211.238/32")],
        vec![netv("10.0.0.0/8")],
    );

    let resp = app(state)
        .oneshot(
            req("/test")
                .header(ORIGINAL_FORWARDED_FOR, "94.43.76.236, 34.149.211.238")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// T2-equivalent (THE security test): client-supplied XFF leftmost is in
// allowlist, but the LB-attested real client (second from right) is not.
// walk-rightmost must surface the real client, NOT the spoof.
#[tokio::test]
async fn bypass_xoff_spoof_leftmost_is_rejected() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = bypass_state(
        &s,
        true,
        vec![netv("34.149.211.238/32")],
        vec![netv("10.0.0.0/8")],
    );

    // Attacker tries to look like 10.4.5.5 by prepending it. GLB then
    // appends real client (94.43.76.236) and itself.
    let resp = app(state)
        .oneshot(
            req("/test")
                .header(
                    ORIGINAL_FORWARDED_FOR,
                    "10.4.5.5, 94.43.76.236, 34.149.211.238",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Walk rightmost: skip 34.149.211.238 (trusted), next is 94.43.76.236
    // -> NOT in allowlist -> standard auth -> Missing -> 403.
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// T3-equivalent: direct ClusterIP path. No XOFF; peer IP from ConnectInfo
// is the client pod IP. If in allowlist -> bypass.
#[tokio::test]
async fn bypass_clusterip_peer_in_allowlist() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = bypass_state(&s, true, vec![], vec![netv("10.0.0.0/8")]);

    let request = with_connect_info(
        req("/test").body(Body::empty()).unwrap(),
        "10.4.1.202:54321",
    );
    let resp = app(state).oneshot(request).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_user_id_header(&resp, "internal");
    assert_api_key_id_header(&resp, "internal");
    assert_eq!(body_string(resp).await, "ok:internal:internal");
}

// ConnectInfo present but peer NOT in allowlist -> standard auth.
#[tokio::test]
async fn bypass_clusterip_peer_outside_allowlist() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = bypass_state(&s, true, vec![], vec![netv("10.0.0.0/8")]);

    let request = with_connect_info(req("/test").body(Body::empty()).unwrap(), "8.8.8.8:443");
    let resp = app(state).oneshot(request).await.unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// T4-equivalent: client smuggles BOTH X-Real-IP and X-Forwarded-For with
// an in-allowlist value. We must ignore those (we only consult XOFF) and
// resolve the real client from the trusted-stripped XOFF chain.
#[tokio::test]
async fn bypass_ignores_x_real_ip_and_xff_after_nginx() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = bypass_state(
        &s,
        true,
        vec![netv("34.149.211.238/32")],
        vec![netv("10.0.0.0/8")],
    );

    let resp = app(state)
        .oneshot(
            req("/test")
                .header("X-Real-IP", "10.4.5.5")
                .header("X-Forwarded-For", "10.4.5.5")
                .header(
                    ORIGINAL_FORWARDED_FOR,
                    "10.4.5.5, 94.43.76.236, 34.149.211.238",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Despite spoofed X-Real-IP / X-Forwarded-For, walk-rightmost on
    // XOFF resolves real client to 94.43.76.236 -> outside allowlist.
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// Empty allowlist disables bypass entirely. Even a "trusted-looking"
// request goes through the standard Bearer path.
#[tokio::test]
async fn bypass_disabled_when_allowlist_empty() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = bypass_state(&s, true, vec![netv("34.149.211.238/32")], vec![]);

    let resp = app(state)
        .oneshot(
            req("/test")
                .header(ORIGINAL_FORWARDED_FOR, "10.4.5.5, 34.149.211.238")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// Malformed XOFF segments are skipped, not used as IPs. With a chain of
// garbage we fall back to ConnectInfo.
#[tokio::test]
async fn bypass_malformed_xoff_falls_back_to_connect_info() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = bypass_state(&s, true, vec![], vec![netv("10.0.0.0/8")]);

    let request = with_connect_info(
        req("/test")
            .header(ORIGINAL_FORWARDED_FOR, "not-an-ip, also-bad")
            .body(Body::empty())
            .unwrap(),
        "10.4.1.202:54321",
    );
    let resp = app(state).oneshot(request).await.unwrap();

    // Malformed entries skipped; chain yields no valid IP; fall back to
    // peer (ConnectInfo) which IS in allowlist.
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "ok:internal:internal");
}

// Chain consisting entirely of trusted IPs -> no real-client extraction
// possible. Don't trust the LB's own IP as "the client" — fall back to
// ConnectInfo (which here is also trusted, so no bypass).
#[tokio::test]
async fn bypass_chain_all_trusted_falls_back() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = bypass_state(
        &s,
        true,
        vec![netv("34.149.211.238/32")],
        vec![netv("10.0.0.0/8")],
    );

    let request = with_connect_info(
        req("/test")
            .header(ORIGINAL_FORWARDED_FOR, "34.149.211.238")
            .body(Body::empty())
            .unwrap(),
        "8.8.8.8:443",
    );
    let resp = app(state).oneshot(request).await.unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// Multiple allowlist CIDRs: any of them matches -> bypass.
#[tokio::test]
async fn bypass_multiple_allowlist_cidrs() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = bypass_state(
        &s,
        true,
        vec![netv("34.149.211.238/32")],
        vec![netv("10.0.0.0/8"), netv("35.1.2.0/29")],
    );

    // Hits second CIDR (dev NAT pool example).
    let resp = app(state)
        .oneshot(
            req("/test")
                .header(ORIGINAL_FORWARDED_FOR, "35.1.2.5, 34.149.211.238")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "ok:internal:internal");
}

// Duplicate-Authorization rejection runs FIRST, even when the source IP
// would otherwise qualify for bypass. The duplicate header is a smuggling
// signal in itself; bypass must not paper over it.
#[tokio::test]
async fn bypass_does_not_override_duplicate_auth_rejection() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u1").mount(&s).await;

    let state = bypass_state(
        &s,
        true,
        vec![netv("34.149.211.238/32")],
        vec![netv("10.0.0.0/8")],
    );

    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_a_b")
                .header(header::AUTHORIZATION, "Bearer sqd_data_c_d")
                .header(ORIGINAL_FORWARDED_FOR, "10.4.5.5, 34.149.211.238")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Duplicate Authorization -> Invalid -> 403 even with allowlisted source.
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// Bypass with a valid Bearer token still bypasses (and uses the
// internal:<ip> user_id, not the token's user_id) — IP precedence is
// intentional: trusted-network identity overrides token identity.
#[tokio::test]
async fn bypass_takes_precedence_over_valid_bearer() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u-from-token").mount(&s).await;

    let state = bypass_state(
        &s,
        true,
        vec![netv("34.149.211.238/32")],
        vec![netv("10.0.0.0/8")],
    );

    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .header(ORIGINAL_FORWARDED_FOR, "10.4.5.5, 34.149.211.238")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "ok:internal:internal");
    // Network API must NOT have been called — we short-circuited on IP.
    assert_eq!(s.received_requests().await.unwrap().len(), 0);
}

// No XOFF and no ConnectInfo -> middleware can't resolve a real IP.
// Bypass cannot fire; standard auth runs (here: no token -> Missing).
#[tokio::test]
async fn bypass_without_xoff_or_connect_info_falls_through() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = bypass_state(&s, true, vec![], vec![netv("10.0.0.0/8")]);

    let resp = app(state)
        .oneshot(req("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// ─── DISABLE_V2_AUTH (kill switch) ──────────────────────────────────────
//
// `disabled=true` short-circuits at the very top of `auth()`: no decide,
// no cache work, no Network API call. One metric sample under the
// `disabled` label so the dashboard shows the switch is engaged.

fn all_ips_v() -> Vec<IpNet> {
    vec![netv("0.0.0.0/0"), netv("::/0")]
}

#[tokio::test]
async fn disable_short_circuits_missing_token() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    // Validate API would 404 if we ever reached it; we shouldn't.
    nf_validate_mock().mount(&s).await;

    // Kill switch on; enforcement also globally on, but should be irrelevant.
    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        true,
        all_ips_v(),
        vec![],
        vec![],
    );

    let before = AUTH_TOTAL.with_label_values(&["disabled"]).get();
    let resp = app(state)
        .oneshot(req("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        AUTH_TOTAL.with_label_values(&["disabled"]).get() - before,
        1,
        "kill switch must increment auth_total{{disabled}} exactly once"
    );
    // Network API must NOT be called.
    assert_eq!(s.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn disable_short_circuits_invalid_token() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        true,
        all_ips_v(),
        vec![],
        vec![],
    );

    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer not_an_sqd_key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Even an obviously invalid token passes when the kill switch is on.
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(s.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn disable_short_circuits_duplicate_authorization() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        true,
        all_ips_v(),
        vec![],
        vec![],
    );

    // Duplicate Authorization is normally a hard 403 (RFC 6750 §3.1).
    // With the kill switch on, even that is suppressed — auth path is OFF.
    let resp = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_a_b")
                .header(header::AUTHORIZATION, "Bearer sqd_data_c_d")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn disable_does_not_emit_decide_metrics() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    good_validate_mock("u1").mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        true,
        all_ips_v(),
        vec![],
        vec![],
    );

    let before_ok = count_auth("ok");
    let before_missing = count_auth("missing");
    let before_lat = AUTH_LATENCY_SECONDS.get_sample_count();

    let _ = app(state)
        .oneshot(
            req("/test")
                .header(header::AUTHORIZATION, "Bearer sqd_data_abc_xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Kill switch path emits exactly the `disabled` counter — neither
    // `ok` (which would imply a full decide), nor the latency histogram.
    assert_eq!(count_auth("ok"), before_ok, "no ok metric on kill switch");
    assert_eq!(count_auth("missing"), before_missing);
    assert_eq!(
        AUTH_LATENCY_SECONDS.get_sample_count(),
        before_lat,
        "kill switch must skip the latency timer entirely"
    );
}

// ─── ENFORCE_V2_AUTH_FOR_IPS (canary by IP) ─────────────────────────────
//
// The list `enforce_for_ips` decides who gets denied on Missing/Invalid:
//   - empty   -> never deny (observe-only mode);
//   - `*`     -> deny everyone (catch-all CIDRs 0.0.0.0/0 + ::/0);
//   - narrow  -> deny only sources matching a CIDR (canary scope).

#[tokio::test]
async fn canary_enforces_for_ip_in_scope() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    // Only enforce for 10.4.0.0/16; everyone else fails open.
    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        false,
        vec![netv("10.4.0.0/16")],
        vec![],
        vec![],
    );

    let request = with_connect_info(req("/test").body(Body::empty()).unwrap(), "10.4.5.5:54321");
    let resp = app(state).oneshot(request).await.unwrap();

    // In-scope source, no token -> Missing -> deny.
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn canary_passes_for_ip_out_of_scope() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        false,
        vec![netv("10.4.0.0/16")],
        vec![],
        vec![],
    );

    let request = with_connect_info(req("/test").body(Body::empty()).unwrap(), "8.8.8.8:443");
    let resp = app(state).oneshot(request).await.unwrap();

    // Out-of-scope source -> fail open even though the policy is "enforce".
    assert_eq!(resp.status(), StatusCode::OK);
    let claims = decode_worker_jwt(worker_jwt(&resp).expect("worker jwt header"));
    assert_eq!(claims.u, "internal");
    assert_eq!(claims.k, "internal");
    assert_eq!(claims.exp - claims.iat, 3600);
}

// In canary mode, the metric still records what WOULD have happened —
// `Missing`/`Invalid` count the same way regardless of whether they were
// actually denied. The dashboard can split denied vs allowed by comparing
// auth_total against handler-side counters.
#[tokio::test]
async fn canary_meters_outcome_even_when_not_enforcing() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        false,
        vec![netv("10.4.0.0/16")],
        vec![],
        vec![],
    );

    let before_missing = count_auth("missing");
    let request = with_connect_info(req("/test").body(Body::empty()).unwrap(), "8.8.8.8:443");
    let _ = app(state).oneshot(request).await.unwrap();

    assert_eq!(
        count_auth("missing") - before_missing,
        1,
        "canary fail-open must still record the would-be outcome"
    );
}

// `*` (wildcard) is parsed by CLI as `0.0.0.0/0,::/0`, which equals
// "enforce for everyone" — same as the legacy `ENFORCE_V2_AUTH=true`.
#[tokio::test]
async fn canary_wildcard_enforces_for_every_source() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        false,
        all_ips_v(),
        vec![],
        vec![],
    );

    // Out-of-RFC1918 source still gets denied — wildcard catches it.
    let request = with_connect_info(req("/test").body(Body::empty()).unwrap(), "8.8.8.8:443");
    let resp = app(state).oneshot(request).await.unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// Wildcard + no ConnectInfo: when the policy is "enforce for everyone",
// the absence of a resolvable IP must NOT degrade to fail-open. The
// catch-all bypass in `should_enforce` (prefix_len == 0) is what handles
// this — without it, every test that uses oneshot() without ConnectInfo
// would silently pass even under a "deny all" policy.
#[tokio::test]
async fn canary_wildcard_enforces_even_without_connect_info() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        false,
        all_ips_v(),
        vec![],
        vec![],
    );

    let resp = app(state)
        .oneshot(req("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// Narrow canary (no catch-all) + missing IP -> fail open.
// This is the safety property: when we can't tell whether a request is
// in scope, we don't pretend it is.
#[tokio::test]
async fn canary_narrow_does_not_enforce_when_ip_unresolvable() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        false,
        vec![netv("10.4.0.0/16")],
        vec![],
        vec![],
    );

    let resp = app(state)
        .oneshot(req("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

// Canary scope reads the same XOFF chain as the bypass path — so the
// real-client extraction is consistent. A request that arrives via nginx
// with XOFF must be checked against `enforce_for_ips` using the
// LB-attested IP, NOT the (untrusted) connection peer (= nginx pod).
#[tokio::test]
async fn canary_scope_reads_xoff_real_client() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        false,
        vec![netv("94.43.0.0/16")],
        vec![netv("34.149.211.238/32")],
        vec![],
    );

    // nginx pod connects in 10.6.x.x; chain shows real client 94.43.x.x.
    let request = with_connect_info(
        req("/test")
            .header(ORIGINAL_FORWARDED_FOR, "94.43.76.236, 34.149.211.238")
            .body(Body::empty())
            .unwrap(),
        "10.6.2.3:443",
    );
    let resp = app(state).oneshot(request).await.unwrap();

    // Real client (94.43.x.x) is in scope -> deny on missing token.
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// Regression for the canary-bypass-via-duplicate-Authorization issue:
// pre-fix, `decide` rejected duplicate Authorization with
// `(Outcome::Invalid, None)` — the real_ip was never resolved. Then in
// `should_enforce`, a narrow-CIDR scope (no catch-all) returned false
// under None real_ip, so the request was allowed despite being in
// canary scope. The fix moves real_ip extraction above the
// duplicate-header check so the IP is always available for scope eval.
#[tokio::test]
async fn canary_narrow_enforces_duplicate_auth_for_in_scope_ip() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    // Narrow canary: enforce ONLY for 10.4.0.0/16. No catch-all — the
    // bypass below is exactly the case the prior code mishandled.
    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        false,
        vec![netv("10.4.0.0/16")],
        vec![],
        vec![],
    );

    // Source IP IS in scope; client smuggles two Authorization headers.
    let request = with_connect_info(
        req("/test")
            .header(header::AUTHORIZATION, "Bearer sqd_data_a_b")
            .header(header::AUTHORIZATION, "Bearer sqd_data_c_d")
            .body(Body::empty())
            .unwrap(),
        "10.4.5.5:54321",
    );
    let resp = app(state).oneshot(request).await.unwrap();

    // Must be 403: duplicate-auth -> Invalid, in-scope IP -> enforce.
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    // Network API must NOT have been called — duplicate-auth short-circuits.
    assert_eq!(s.received_requests().await.unwrap().len(), 0);
}

// Mirror of the regression above, asserting the negative case: out-of-scope
// IP + duplicate-auth still falls through (canary policy says "don't
// enforce for this source"). Confirms the fix didn't accidentally widen
// enforcement to all sources.
#[tokio::test]
async fn canary_narrow_does_not_enforce_duplicate_auth_for_out_of_scope_ip() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        false,
        vec![netv("10.4.0.0/16")],
        vec![],
        vec![],
    );

    let request = with_connect_info(
        req("/test")
            .header(header::AUTHORIZATION, "Bearer sqd_data_a_b")
            .header(header::AUTHORIZATION, "Bearer sqd_data_c_d")
            .body(Body::empty())
            .unwrap(),
        "8.8.8.8:443",
    );
    let resp = app(state).oneshot(request).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

// Internal allowlist still wins over enforce-scope. A source that the
// allowlist accepts must short-circuit to `Ok` regardless of whether it
// would also have been "in canary scope".
#[tokio::test]
async fn canary_internal_allowlist_takes_precedence() {
    let _g = metrics_lock().await;
    let s = MockServer::start().await;
    nf_validate_mock().mount(&s).await;

    let state = AuthState::for_test_full(
        Some(Url::parse(&s.uri()).unwrap()),
        TestClock::new(),
        false,
        all_ips_v(), // would deny everyone…
        vec![],
        vec![netv("10.0.0.0/8")], // …but internal pods bypass.
    );

    let request = with_connect_info(req("/test").body(Body::empty()).unwrap(), "10.4.5.5:54321");
    let resp = app(state).oneshot(request).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_user_id_header(&resp, "internal");
    assert_api_key_id_header(&resp, "internal");
    assert_eq!(body_string(resp).await, "ok:internal:internal");
}
