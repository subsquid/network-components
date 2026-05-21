use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tracing::{error, warn};
use url::Url;

use super::clock::{system_clock, Clock};
use crate::metrics::VALIDATE_CALL_TOTAL;

const TIMEOUT: Duration = Duration::from_millis(250);
const BREAKER_THRESHOLD: u64 = 50;
const BREAKER_OPEN_DURATION: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq)]
pub enum ValidateResult {
    /// `expires_at` is the server-authoritative absolute expiry timestamp,
    /// when present. Forwarded to the cache so a key revoked with a
    /// short server-side TTL doesn't get silently cached for the full
    /// `TTL_EXISTS` (60s) cache TTL.
    Exists {
        user_id: String,
        api_key_id: String,
        expires_at: Option<Instant>,
    },
    Deleted,
    FailOpen,
}

#[derive(Deserialize)]
struct ValidateResponse {
    user_id: String,
    api_key_id: String,
    /// Optional Unix timestamp (seconds) at which the server says this key
    /// stops being valid. Wired through to the cache so the entry expires
    /// at `min(now + TTL_EXISTS, expires_at)`. Server may omit it; the
    /// cache then uses the default 60s TTL.
    #[serde(default)]
    expires_at: Option<u64>,
}

#[derive(Clone, Copy)]
enum OpenState {
    Closed,
    Open {
        until: Instant,
    },
    /// A probe is in flight after the open window elapsed. No other
    /// requests are admitted until the probe records success or failure.
    HalfOpen,
}

/// State + counter live in the SAME mutex so a probe-success that resets
/// the counter and a stale failure that increments it cannot interleave
/// to spuriously open the breaker.
struct State {
    open: OpenState,
    consecutive_errors: u64,
}

pub struct CircuitBreaker {
    state: Mutex<State>,
    threshold: u64,
    open_duration: Duration,
    clock: Arc<dyn Clock>,
}

impl CircuitBreaker {
    pub fn new(threshold: u64, open_duration: Duration, clock: Arc<dyn Clock>) -> Self {
        Self {
            state: Mutex::new(State {
                open: OpenState::Closed,
                consecutive_errors: 0,
            }),
            threshold,
            open_duration,
            clock,
        }
    }

    /// Acquire admission for a request. Returns `Some(Permit)` if the
    /// caller may proceed; the permit MUST be turned into a success or
    /// failure record before it is dropped, otherwise its Drop impl
    /// records a failure. This makes probe acquisition cancellation-safe:
    /// a dropped probe (client disconnect, panic, shutdown) re-opens the
    /// breaker instead of leaving it stuck in HalfOpen forever.
    pub fn acquire(self: &Arc<Self>) -> Option<Permit> {
        let mut state = self.state.lock().unwrap();
        let admit = match state.open {
            OpenState::Closed => true,
            OpenState::HalfOpen => false,
            OpenState::Open { until } => {
                if self.clock.now() >= until {
                    state.open = OpenState::HalfOpen;
                    true
                } else {
                    false
                }
            }
        };
        if admit {
            Some(Permit {
                breaker: Some(self.clone()),
            })
        } else {
            None
        }
    }

    fn record_success_internal(&self) {
        let mut state = self.state.lock().unwrap();
        state.consecutive_errors = 0;
        state.open = OpenState::Closed;
    }

    fn record_failure_internal(&self) {
        let mut state = self.state.lock().unwrap();
        state.consecutive_errors = state.consecutive_errors.saturating_add(1);
        match state.open {
            OpenState::HalfOpen => {
                state.open = OpenState::Open {
                    until: self.clock.now() + self.open_duration,
                };
            }
            OpenState::Closed | OpenState::Open { .. } => {
                if state.consecutive_errors >= self.threshold {
                    state.open = OpenState::Open {
                        until: self.clock.now() + self.open_duration,
                    };
                }
            }
        }
    }
}

/// RAII handle returned by [`CircuitBreaker::acquire`]. Must be consumed
/// via [`Permit::record_success`] / [`Permit::record_failure`]. If
/// dropped without being consumed, records a failure — this keeps the
/// breaker out of permanent HalfOpen on cancellation.
pub struct Permit {
    breaker: Option<Arc<CircuitBreaker>>,
}

impl Permit {
    pub fn record_success(mut self) {
        if let Some(b) = self.breaker.take() {
            b.record_success_internal();
        }
    }

    pub fn record_failure(mut self) {
        if let Some(b) = self.breaker.take() {
            b.record_failure_internal();
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(b) = self.breaker.take() {
            b.record_failure_internal();
        }
    }
}

pub struct NetworkApiClient {
    http: reqwest::Client,
    base_url: Option<Url>,
    breaker: Arc<CircuitBreaker>,
    /// Same clock the breaker uses, kept here so we can project absolute
    /// server-side `expires_at` (unix seconds) onto the monotonic timeline
    /// the cache is keyed on. Tests inject a `TestClock` so deterministic
    /// expiry is observable without `tokio::time::pause`.
    clock: Arc<dyn Clock>,
}

impl NetworkApiClient {
    pub fn new(base_url: Url) -> Self {
        Self::build(Some(base_url), system_clock())
    }

    pub fn disabled() -> Self {
        Self::build(None, system_clock())
    }

    #[cfg(test)]
    pub fn with_clock(base_url: Option<Url>, clock: Arc<dyn Clock>) -> Self {
        Self::build(base_url, clock)
    }

    fn build(base_url: Option<Url>, clock: Arc<dyn Clock>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .build()
            .expect("reqwest client build");
        // Normalise base_url to end with `/` so a relative `join` below
        // preserves any subpath (e.g. `https://auth.example.com/api/v2/`
        // is not silently truncated to root). RFC 3986 absolute-path
        // references with a leading `/` would replace the base path —
        // so we use a relative path on a slash-terminated base instead.
        let base_url = base_url.map(|mut u| {
            if !u.path().ends_with('/') {
                let path = format!("{}/", u.path());
                u.set_path(&path);
            }
            u
        });
        Self {
            http,
            base_url,
            clock: clock.clone(),
            breaker: Arc::new(CircuitBreaker::new(
                BREAKER_THRESHOLD,
                BREAKER_OPEN_DURATION,
                clock,
            )),
        }
    }

    pub async fn validate(&self, token: &str) -> ValidateResult {
        // Local short-circuits (auth disabled, breaker open) return FailOpen
        // WITHOUT incrementing VALIDATE_CALL_TOTAL — that metric measures
        // actual outbound traffic, not every fail-open outcome.
        let Some(base_url) = self.base_url.as_ref() else {
            return ValidateResult::FailOpen;
        };
        let Some(permit) = self.breaker.acquire() else {
            return ValidateResult::FailOpen;
        };
        // Relative path (no leading `/`) so a base_url with a subpath
        // like `/api/v2/` resolves to `/api/v2/internal/validate`.
        let url = match base_url.join("internal/validate") {
            Ok(u) => u,
            Err(_) => {
                error!("failed to build /internal/validate URL");
                permit.record_failure();
                return ValidateResult::FailOpen;
            }
        };
        // From here on we either send a request or hit a transport error
        // partway through; both count as a real validate call attempt.
        let resp = self
            .http
            .post(url)
            .json(&serde_json::json!({ "token": token }))
            .send()
            .await;
        match resp {
            Ok(r) if r.status().is_success() => match r.json::<ValidateResponse>().await {
                Ok(body) => {
                    VALIDATE_CALL_TOTAL.with_label_values(&["ok"]).inc();
                    permit.record_success();
                    let expires_at = body.expires_at.and_then(|exp_unix| {
                        // unix -> Instant requires anchoring on a "now"
                        // pair. We compute the offset between server-time
                        // (unix seconds) and our clock's now() and project
                        // the absolute expiry into the same monotonic
                        // reference frame the cache uses.
                        let now_unix = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .ok()?
                            .as_secs();
                        if exp_unix <= now_unix {
                            // Already expired by server's clock — let the
                            // cache see the past instant and demote it to
                            // `Deleted` immediately (see `put_exists`).
                            return Some(self.clock.now());
                        }
                        let remaining = Duration::from_secs(exp_unix - now_unix);
                        Some(self.clock.now() + remaining)
                    });
                    ValidateResult::Exists {
                        user_id: body.user_id,
                        api_key_id: body.api_key_id,
                        expires_at,
                    }
                }
                Err(err) => {
                    error!(%err, "malformed /internal/validate 200 body");
                    VALIDATE_CALL_TOTAL.with_label_values(&["fail_open"]).inc();
                    permit.record_failure();
                    ValidateResult::FailOpen
                }
            },
            Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => {
                // A bare 404 is ambiguous: the validate endpoint genuinely
                // says "no such key", OR the URL is misconfigured (wrong
                // base path, wrong subpath, missing reverse-proxy route).
                // Treating both as `Deleted` would negatively cache valid
                // keys for 15s under a deployment misconfig. We require a
                // signal that the validate API itself answered: a JSON
                // content-type. HTML/text 404s (typical of misrouted
                // requests through nginx/Cloud LB) fail open instead.
                let is_json = r
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.starts_with("application/json"))
                    .unwrap_or(false);
                if is_json {
                    VALIDATE_CALL_TOTAL.with_label_values(&["deleted"]).inc();
                    permit.record_success();
                    ValidateResult::Deleted
                } else {
                    // Looks like a non-API 404 (misrouted, gateway error
                    // page, etc.). Fail open so a deployment problem
                    // doesn't masquerade as a flood of revoked keys.
                    error!("/internal/validate returned non-JSON 404");
                    VALIDATE_CALL_TOTAL.with_label_values(&["fail_open"]).inc();
                    permit.record_failure();
                    ValidateResult::FailOpen
                }
            }
            Ok(r) => {
                warn!(status = %r.status(), "/internal/validate returned unexpected HTTP status");
                VALIDATE_CALL_TOTAL.with_label_values(&["fail_open"]).inc();
                permit.record_failure();
                ValidateResult::FailOpen
            }
            Err(err) => {
                warn!(%err, "/internal/validate transport failure");
                VALIDATE_CALL_TOTAL.with_label_values(&["fail_open"]).inc();
                permit.record_failure();
                ValidateResult::FailOpen
            }
        }
    }
}

#[cfg(test)]
mod tests;
