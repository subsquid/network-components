use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::extract::ConnectInfo;
use axum::http::{HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use tracing::warn;

use crate::auth::cache::KeyState;
use crate::auth::client::ValidateResult;
use crate::auth::AuthState;
use crate::metrics::{
    AUTH_LATENCY_SECONDS, AUTH_TOTAL, CACHE_HIT_TOTAL, CACHE_MISS_TOTAL, REQUESTS_BY_KEY,
};

const TOKEN_PREFIX: &str = "sqd_data_";
const WORKER_JWT_HEADER: &str = "x-sqd-auth";

const INTERNAL_ID: &str = "internal";
const FAIL_OPEN_ID: &str = "fail-open";

/// Header inspected for the upstream-supplied forwarded chain.
///
/// We deliberately read `X-Original-Forwarded-For` (set by ingress-nginx as a
/// verbatim copy of the inbound `X-Forwarded-For`) rather than the
/// nginx-rewritten `X-Forwarded-For` / `X-Real-IP`. Empirical check
/// (see PR rollout plan) showed nginx with `use-forwarded-headers: true` and
/// default `proxy-real-ip-cidr=0.0.0.0/0` happily promotes a client-supplied
/// leftmost XFF to `X-Real-IP` — i.e. those headers are spoofable. The XOFF
/// preserves the full chain `[client-supplied..., real-client, upstream-LB]`,
/// so we can walk rightmost stripping our own `trusted_ips` and recover the
/// real source IP that the upstream LB observed at TCP handshake.
const ORIGINAL_FORWARDED_FOR: &str = "X-Original-Forwarded-For";

/// Value attached to the request via Extensions on a successful auth pass.
/// Downstream handlers can read validated key identity for audit attribution.
/// IP-allowlist bypass uses the fixed `internal` identity.
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub user_id: String,
    pub api_key_id: String,
    pub expires_at: Option<u64>,
}

#[derive(Debug)]
enum Outcome {
    Ok(AuthContext),
    Missing,
    Invalid,
    FailOpen,
    /// Kill switch active (`DISABLE_V2_AUTH=true`). Counted under its own
    /// label so the dashboard makes it obvious that auth is bypassed
    /// globally, not silently absorbed into `ok`.
    Disabled,
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Outcome::Ok(_) => "ok",
            Outcome::Missing => "missing",
            Outcome::Invalid => "invalid",
            Outcome::FailOpen => "fail_open",
            Outcome::Disabled => "disabled",
        }
    }
}

pub async fn auth<B>(mut req: Request<B>, next: Next<B>) -> Response
where
    B: Send + 'static,
{
    let state = req
        .extensions()
        .get::<Arc<AuthState>>()
        .cloned()
        .expect("AuthState extension is required by auth middleware");

    // Kill switch — short-circuits BEFORE the latency timer / decide so the
    // disabled path is genuinely ~zero work. We still emit one metric
    // sample so dashboards can see the switch is engaged.
    if state.disabled {
        AUTH_TOTAL
            .with_label_values(&[Outcome::Disabled.label()])
            .inc();
        return next.run(req).await;
    }

    let timer = AUTH_LATENCY_SECONDS.start_timer();
    let (outcome, real_ip) = decide(&state, &mut req).await;
    AUTH_TOTAL.with_label_values(&[outcome.label()]).inc();
    timer.observe_duration();

    let allowed = match &outcome {
        Outcome::Ok(_) | Outcome::FailOpen | Outcome::Disabled => true,
        Outcome::Missing | Outcome::Invalid => !should_enforce(&state, real_ip),
    };

    if let Outcome::Ok(ctx) = &outcome {
        // Sketch update + label add/remove are performed under the
        // top-keys lock so that a concurrent eviction can't be silently
        // undone by a stale `with_label_values` call from another task.
        // We label validated requests by user_id (the only token-derived value
        // safe to expose: it identifies the tenant, not the secret material).
        // IP-bypass requests use the bounded synthetic `internal` identity.
        state.top_keys.observe_into(&ctx.user_id, &REQUESTS_BY_KEY);
        req.extensions_mut().insert(ctx.clone());
    }

    if allowed {
        let mut resp = next.run(req).await;
        if let Some((user_id, api_key_id, expires_at)) = worker_jwt_context(&outcome) {
            match &state.worker_jwt_issuer {
                Some(issuer) => match issuer.issue(user_id, api_key_id, expires_at) {
                    Ok(token) => match HeaderValue::from_str(&token) {
                        Ok(value) => {
                            resp.headers_mut().insert(WORKER_JWT_HEADER, value);
                        }
                        Err(_) => {
                            warn!("worker jwt cannot be encoded as response header");
                            return worker_jwt_error();
                        }
                    },
                    Err(err) => {
                        warn!(error = %err, "failed to issue worker jwt");
                        return worker_jwt_error();
                    }
                },
                None => {}
            }
        }
        resp
    } else {
        deny()
    }
}

fn worker_jwt_context(outcome: &Outcome) -> Option<(&str, &str, Option<u64>)> {
    match outcome {
        Outcome::Ok(ctx) => Some((&ctx.user_id, &ctx.api_key_id, ctx.expires_at)),
        Outcome::Missing | Outcome::Invalid => Some((INTERNAL_ID, INTERNAL_ID, None)),
        Outcome::FailOpen => Some((FAIL_OPEN_ID, FAIL_OPEN_ID, None)),
        Outcome::Disabled => None,
    }
}

/// Whether the request should be enforced (denied on `Missing`/`Invalid`).
///
/// One rule: enforce iff the resolved real-client IP matches any CIDR in
/// `state.enforce_for_ips`. Empty list -> never enforce. The wildcard
/// `0.0.0.0/0` + `::/0` (written `*` in the env var) -> enforce for every
/// source. Specific CIDRs -> canary scope.
///
/// When `real_ip` is `None` (no XOFF, no `ConnectInfo`) we still enforce iff
/// the policy is "enforce for everyone" — the `*` shorthand is a catch-all
/// (`prefix_len == 0`) and shouldn't depend on resolving a source IP. In
/// strictly canary mode (only narrow CIDRs), `None` means "can't tell if
/// in scope" and we play it safe by NOT enforcing.
fn should_enforce(state: &AuthState, real_ip: Option<std::net::IpAddr>) -> bool {
    if state.enforce_for_ips.is_empty() {
        return false;
    }
    match real_ip {
        Some(ip) => state.enforce_for_ips.iter().any(|net| net.contains(&ip)),
        None => state
            .enforce_for_ips
            .iter()
            .any(|net| net.prefix_len() == 0),
    }
}

async fn decide<B>(state: &AuthState, req: &mut Request<B>) -> (Outcome, Option<IpAddr>) {
    // 1. Resolve the real client IP up-front. Every Outcome — including
    //    early rejections like duplicate-Authorization — needs to thread it
    //    out so `should_enforce` can decide canary scope correctly.
    //    Pre-fix: real_ip was extracted AFTER the duplicate-header check, so
    //    a request that hit (Outcome::Invalid, None) would pass through
    //    `should_enforce(state, None) == false` under any narrow-CIDR scope
    //    (no catch-all to short-circuit). That was an auth bypass: an
    //    in-scope client could send duplicate Authorization headers and
    //    sneak past enforcement.
    let real_ip = extract_real_client_ip(req, &state.trusted_ips);

    // 2. Pull Authorization header. Bearer scheme only — no Token: fallback.
    //    Multiple Authorization headers are ambiguous (RFC 6750 §3.1
    //    classifies them as invalid_request); reject as Invalid so proxies
    //    or load balancers that reorder headers can't smuggle a credential.
    //    We do this before the IP-bypass check so a request with both a
    //    smuggled extra Authorization AND a trusted source IP is still
    //    rejected — the duplicate header is itself the signal of trouble.
    let auth_headers = req.headers().get_all(axum::http::header::AUTHORIZATION);
    let mut iter = auth_headers.iter();
    let first = iter.next();
    if iter.next().is_some() {
        return (Outcome::Invalid, real_ip);
    }

    // 3. Source-IP bypass. Disabled when allowlist is empty.
    //
    // We attribute every bypass request under a single fixed label
    // (`internal`) instead of `internal:<ip>` per source.
    // Reason: the top-keys sketch (REQUESTS_BY_KEY) is bounded to 100
    // entries; if internal pods come from many distinct IPs (we've seen
    // dozens of /16 pod-CIDR blocks in main GKE), each would claim a
    // sketch slot and could evict real-tenant entries. The IP-level
    // detail is preserved in access logs / traces, not in this metric.
    if !state.internal_allowlist.is_empty() {
        if let Some(ip) = real_ip {
            if state.internal_allowlist.iter().any(|net| net.contains(&ip)) {
                return (
                    Outcome::Ok(AuthContext {
                        user_id: INTERNAL_ID.to_string(),
                        api_key_id: INTERNAL_ID.to_string(),
                        expires_at: None,
                    }),
                    Some(ip),
                );
            }
        }
    }

    let Some(token) = first.and_then(|v| v.to_str().ok()).and_then(extract_bearer) else {
        return (Outcome::Missing, real_ip);
    };

    // 4. Cheap reject: must carry the sqd_data_ scope prefix. The full
    //    `sqd_data_<token>` string is what we use as cache key and what
    //    we send to /internal/validate.
    if !token.starts_with(TOKEN_PREFIX) || token.len() <= TOKEN_PREFIX.len() {
        return (Outcome::Invalid, real_ip);
    }

    // 5. Cache lookup. Maps the cached state to a middleware Outcome,
    //    or falls through to the Network API call on UNDEFINED.
    if let Some(outcome) = lookup(state, token) {
        return (outcome, real_ip);
    }

    // 6. Singleflight + Network API. Concurrent misses for the same token
    //    serialise here; the second waiter re-checks the cache and (typically)
    //    hits the entry the leader wrote — including a brief `FailedRecently`
    //    sentinel that drains the queue without re-issuing 250ms timeouts.
    let _sf = state.inflight.acquire(token).await;
    if let Some(outcome) = lookup(state, token) {
        return (outcome, real_ip);
    }
    CACHE_MISS_TOTAL.inc();
    // VALIDATE_CALL_TOTAL is emitted by `client.validate` itself — only
    // on outcomes that actually attempted a network round-trip. Local
    // short-circuits (auth disabled, breaker open) don't inflate it.
    let outcome = match state.client.validate(token).await {
        ValidateResult::Exists {
            user_id,
            api_key_id,
            expires_at,
        } => {
            let stored = state.cache.put_exists(
                token.to_string(),
                user_id.clone(),
                api_key_id.clone(),
                expires_at,
            );
            if !stored {
                return (Outcome::Invalid, real_ip);
            }
            Outcome::Ok(AuthContext {
                user_id,
                api_key_id,
                expires_at,
            })
        }
        ValidateResult::Deleted => {
            state.cache.put_deleted(token.to_string());
            Outcome::Invalid
        }
        ValidateResult::FailOpen => {
            state.cache.put_failed_recently(token.to_string());
            Outcome::FailOpen
        }
    };
    (outcome, real_ip)
}

/// Cache lookup translated to an Outcome, or `None` if the slot is UNDEFINED.
fn lookup(state: &AuthState, token: &str) -> Option<Outcome> {
    match state.cache.get(token)? {
        KeyState::Exists {
            user_id,
            api_key_id,
            expires_at,
        } => {
            CACHE_HIT_TOTAL.with_label_values(&["exists"]).inc();
            Some(Outcome::Ok(AuthContext {
                user_id,
                api_key_id,
                expires_at,
            }))
        }
        KeyState::Deleted => {
            CACHE_HIT_TOTAL.with_label_values(&["deleted"]).inc();
            Some(Outcome::Invalid)
        }
        KeyState::FailedRecently => {
            CACHE_HIT_TOTAL.with_label_values(&["fail_open"]).inc();
            Some(Outcome::FailOpen)
        }
    }
}

fn worker_jwt_error() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "worker_auth_unavailable"})),
    )
        .into_response()
}

fn extract_bearer(header: &str) -> Option<&str> {
    let mut parts = header.splitn(2, ' ');
    let scheme = parts.next()?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let token = parts.next()?.trim();
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

/// Resolve the real client IP for IP-allowlist purposes.
///
/// The chain `X-Original-Forwarded-For` is laid out left-to-right as:
///
/// ```text
///   [X (?spoof?), ...], CLIENT_IP, GLB
///   └ payload supplied by   │       │
///     the client itself     │       └ trusted upstream LB (in TRUSTED_IPS)
///     (do NOT trust)        │         appended itself last
///                           └ real source IP, written by GLB at TCP handshake;
///                             this is what we want
/// ```
///
/// We walk rightmost-first, skip entries that match `trusted_ips`, and
/// return the first non-trusted IP. If the header is absent or yields no
/// non-trusted IP, fall back to the connection peer (`ConnectInfo`) — that
/// case covers direct in-cluster traffic via ClusterIP, where no XFF
/// chain is present and the peer IS the client pod.
fn extract_real_client_ip<B>(req: &Request<B>, trusted_ips: &[ipnet::IpNet]) -> Option<IpAddr> {
    if let Some(chain) = req
        .headers()
        .get(ORIGINAL_FORWARDED_FOR)
        .and_then(|v| v.to_str().ok())
    {
        // Walk rightmost. The first IP that is NOT in trusted_ips is the
        // real source — anything to the left of it is client-supplied
        // payload (potentially spoofed).
        for segment in chain.split(',').rev() {
            let parsed = match segment.trim().parse::<IpAddr>() {
                Ok(ip) => ip,
                Err(_) => continue,
            };
            if !trusted_ips.iter().any(|net| net.contains(&parsed)) {
                return Some(parsed);
            }
        }
        // All entries matched trusted_ips, or the chain was malformed.
        // Fall through to ConnectInfo — better to short-circuit than to
        // accidentally trust the LB's own IP as "the client".
    }

    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip())
}

fn deny() -> Response {
    let body = Json(json!({
        "error": "CREDENTIALS_INVALID",
        "message": "API key required or invalid. Get one at https://portal.sqd.dev",
        "docs": "https://docs.sqd.dev/v2-keys",
    }));
    let mut resp = (StatusCode::FORBIDDEN, body).into_response();
    resp.headers_mut().insert(
        axum::http::header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer realm=\"sqd-archive\""),
    );
    resp
}

#[cfg(test)]
mod tests;
