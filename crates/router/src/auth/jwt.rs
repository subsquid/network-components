use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};

use super::clock::{system_clock, Clock};

const REFRESH_BEFORE_EXPIRY: Duration = Duration::from_secs(30);
#[cfg(not(test))]
const MAX_CACHED_TOKENS: usize = 10_000;
#[cfg(test)]
const MAX_CACHED_TOKENS: usize = 32;

#[derive(Clone)]
pub struct WorkerJwtIssuer {
    key: EncodingKey,
    ttl: Duration,
    cache: moka::sync::Cache<String, CachedToken>,
    clock: Arc<dyn Clock>,
}

#[derive(Clone, Debug)]
struct CachedToken {
    token: String,
    exp: u64,
    refresh_deadline: Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerJwtClaims {
    pub u: String,
    pub k: String,
    pub iat: u64,
    pub exp: u64,
}

impl WorkerJwtIssuer {
    pub fn from_ed25519_pem_with_ttl(pem: &[u8], ttl: Duration) -> Result<Self, String> {
        Self::try_with_clock(pem, ttl, system_clock())
    }

    fn try_with_clock(
        pem: &[u8],
        ttl: Duration,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, String> {
        if ttl.is_zero() {
            return Err("worker JWT TTL must be greater than zero".to_string());
        }
        let key =
            EncodingKey::from_ed_pem(pem).map_err(|err| format!("invalid Ed25519 key: {err}"))?;
        Ok(Self {
            key,
            ttl,
            cache: moka::sync::Cache::builder()
                .max_capacity(MAX_CACHED_TOKENS as u64)
                .build(),
            clock,
        })
    }

    #[cfg(test)]
    pub fn with_clock(pem: &[u8], ttl: Duration, clock: Arc<dyn Clock>) -> Self {
        Self::try_with_clock(pem, ttl, clock).unwrap()
    }

    pub fn issue(
        &self,
        user_id: &str,
        api_key_id: &str,
        expires_at: Option<u64>,
    ) -> Result<String, String> {
        let now = self.clock.now();
        let iat = now_secs();
        let api_key_id = api_key_id.to_string();
        if let Some(token) = self.cached_token(&api_key_id, now, expires_at) {
            return Ok(token);
        }
        let exp = match expires_at {
            Some(expires_at) => iat.saturating_add(self.ttl.as_secs()).min(expires_at),
            None => iat.saturating_add(self.ttl.as_secs()),
        };
        if exp <= iat {
            return Err("worker JWT expiry deadline has passed".to_string());
        }

        let claims = WorkerJwtClaims {
            u: user_id.to_string(),
            k: api_key_id.clone(),
            iat,
            exp,
        };
        let token = encode(&worker_jwt_header(), &claims, &self.key)
            .map_err(|err| format!("failed to encode worker JWT: {err}"))?;
        self.cache.insert(
            api_key_id,
            CachedToken {
                token: token.clone(),
                exp: claims.exp,
                refresh_deadline: refresh_deadline(now, claims.exp - claims.iat),
            },
        );
        Ok(token)
    }

    fn cached_token(
        &self,
        api_key_id: &str,
        now: Instant,
        expires_at: Option<u64>,
    ) -> Option<String> {
        let max_exp = expires_at.unwrap_or(u64::MAX);
        self.cache
            .get(api_key_id)
            .filter(|cached| now < cached.refresh_deadline && cached.exp <= max_exp)
            .map(|cached| cached.token.clone())
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs()
}

fn refresh_deadline(now: Instant, lifetime_secs: u64) -> Instant {
    now + Duration::from_secs(lifetime_secs).saturating_sub(REFRESH_BEFORE_EXPIRY)
}

fn worker_jwt_header() -> Header {
    Header {
        alg: Algorithm::EdDSA,
        typ: None,
        ..Default::default()
    }
}

#[cfg(test)]
pub mod tests_support {
    pub const TEST_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIH8aSkCaxHQOnBc3SbAkalwpC4wG8z2V/Xfu9T+b3g3F
-----END PRIVATE KEY-----"#;

    pub const TEST_PUBLIC_KEY: &str = r#"-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAEf5FCUWgIB+Rmky06SnyxIZo74sCtFMyFydFp0DObjA=
-----END PUBLIC KEY-----"#;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::clock::TestClock;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use jsonwebtoken::{decode, DecodingKey, Validation};

    fn issuer_with_clock(ttl: Duration, clock: Arc<dyn Clock>) -> WorkerJwtIssuer {
        WorkerJwtIssuer::with_clock(tests_support::TEST_PRIVATE_KEY.as_bytes(), ttl, clock)
    }

    fn decode_claims(token: &str) -> WorkerJwtClaims {
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap()
    }

    #[test]
    fn configured_ttl_sets_expiry() {
        let issuer = issuer_with_clock(Duration::from_secs(42), TestClock::new());

        let claims = decode_claims(&issuer.issue("u1", "key1", None).unwrap());

        assert_eq!(claims.u, "u1");
        assert_eq!(claims.k, "key1");
        assert_eq!(claims.exp - claims.iat, 42);
    }

    #[test]
    fn issued_token_signature_verifies() {
        let issuer = issuer_with_clock(Duration::from_secs(42), TestClock::new());
        let token = issuer.issue("u1", "key1", None).unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.validate_exp = false;
        let decoded = decode::<WorkerJwtClaims>(
            &token,
            &DecodingKey::from_ed_pem(tests_support::TEST_PUBLIC_KEY.as_bytes()).unwrap(),
            &validation,
        )
        .expect("worker JWT signature must verify with issuer public key");
        assert_eq!(decoded.header.alg, Algorithm::EdDSA);
        assert_eq!(decoded.header.typ, None);
        assert_eq!(decoded.claims.u, "u1");
        assert_eq!(decoded.claims.k, "key1");
    }

    #[test]
    fn validation_deadline_caps_expiry() {
        let clock = TestClock::new();
        let deadline = now_secs() + 42;
        let issuer = issuer_with_clock(Duration::from_secs(3600), clock);

        let claims = decode_claims(&issuer.issue("u1", "key1", Some(deadline)).unwrap());

        assert_eq!(claims.exp, deadline);
    }

    #[test]
    fn cached_token_is_not_reused_past_validation_deadline() {
        let clock = TestClock::new();
        let issuer = issuer_with_clock(Duration::from_secs(3600), clock.clone());
        let uncapped = issuer.issue("u1", "key1", None).unwrap();
        let deadline = now_secs() + 42;

        let capped = issuer.issue("u1", "key1", Some(deadline)).unwrap();

        assert_ne!(uncapped, capped);
        assert_eq!(decode_claims(&capped).exp, deadline);
    }

    #[test]
    fn cache_is_capped() {
        let issuer = issuer_with_clock(Duration::from_secs(3600), TestClock::new());

        for i in 0..(MAX_CACHED_TOKENS + 1) {
            issuer.issue("u1", &format!("key{i}"), None).unwrap();
        }

        issuer.cache.run_pending_tasks();
        assert_eq!(issuer.cache.entry_count(), MAX_CACHED_TOKENS as u64);
    }

    #[test]
    fn cache_refresh_uses_injected_clock() {
        let clock = TestClock::new();
        let issuer = issuer_with_clock(Duration::from_secs(3600), clock.clone());
        issuer.issue("u1", "key1", None).unwrap();
        let original_deadline = issuer.cache.get("key1").unwrap().refresh_deadline;

        clock.advance(Duration::from_secs(3600 - REFRESH_BEFORE_EXPIRY.as_secs()));
        issuer.issue("u1", "key1", None).unwrap();
        let refreshed_deadline = issuer.cache.get("key1").unwrap().refresh_deadline;

        assert!(refreshed_deadline > original_deadline);
    }
}
