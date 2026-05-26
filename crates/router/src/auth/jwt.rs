use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use ring::signature::Ed25519KeyPair;
use serde::{Deserialize, Serialize};

const REFRESH_BEFORE_EXPIRY: Duration = Duration::from_secs(30);
#[cfg(not(test))]
const MAX_CACHED_TOKENS: usize = 10_000;
#[cfg(test)]
const MAX_CACHED_TOKENS: usize = 32;

#[derive(Clone)]
pub struct WorkerJwtIssuer {
    key: Arc<Ed25519KeyPair>,
    ttl: Duration,
    cache: Arc<Mutex<HashMap<CacheKey, CachedToken>>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    user_id: String,
    api_key_id: String,
}

#[derive(Clone, Debug)]
struct CachedToken {
    token: String,
    exp: u64,
}

#[derive(Serialize)]
struct WorkerJwtHeader {
    alg: &'static str,
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
        if ttl.is_zero() {
            return Err("worker JWT TTL must be greater than zero".to_string());
        }
        let der = decode_pem(pem, "PRIVATE KEY")?;
        let key = Ed25519KeyPair::from_pkcs8_maybe_unchecked(&der)
            .map_err(|err| format!("invalid Ed25519 key: {err}"))?;
        Ok(Self {
            key: Arc::new(key),
            ttl,
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn issue(
        &self,
        user_id: &str,
        api_key_id: &str,
        expires_at: Option<u64>,
    ) -> Result<String, String> {
        let iat = now_secs();
        let key = CacheKey {
            user_id: user_id.to_string(),
            api_key_id: api_key_id.to_string(),
        };
        if let Some(token) = self.cached_token(&key, iat, expires_at) {
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
            k: api_key_id.to_string(),
            iat,
            exp,
        };
        let header = WorkerJwtHeader { alg: "EdDSA" };
        let header = encode_json(&header)?;
        let payload = encode_json(&claims)?;
        let signing_input = format!("{header}.{payload}");
        let signature = self.key.sign(signing_input.as_bytes());
        let token = format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature));
        self.store_token(key, token.clone(), claims.exp, iat);
        Ok(token)
    }

    fn cached_token(&self, key: &CacheKey, now: u64, expires_at: Option<u64>) -> Option<String> {
        let refresh_at = now.saturating_add(REFRESH_BEFORE_EXPIRY.as_secs());
        let max_exp = expires_at.unwrap_or(u64::MAX);
        self.cache
            .lock()
            .expect("worker JWT cache mutex poisoned")
            .get(key)
            .filter(|cached| cached.exp > refresh_at && cached.exp <= max_exp)
            .map(|cached| cached.token.clone())
    }

    fn store_token(&self, key: CacheKey, token: String, exp: u64, now: u64) {
        let mut cache = self.cache.lock().expect("worker JWT cache mutex poisoned");
        cache.retain(|_, cached| cached.exp > now);
        if !cache.contains_key(&key) && cache.len() >= MAX_CACHED_TOKENS {
            if let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, cached)| cached.exp)
                .map(|(key, _)| key.clone())
            {
                cache.remove(&oldest_key);
            }
        }
        cache.insert(key, CachedToken { token, exp });
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs()
}

fn encode_json<T: Serialize>(value: &T) -> Result<String, String> {
    let bytes =
        serde_json::to_vec(value).map_err(|err| format!("failed to encode JWT JSON: {err}"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub fn decode_pem(pem: &[u8], label: &str) -> Result<Vec<u8>, String> {
    let pem = std::str::from_utf8(pem).map_err(|err| format!("invalid PEM UTF-8: {err}"))?;
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let start = pem
        .find(&begin)
        .ok_or_else(|| format!("missing {begin}"))?
        + begin.len();
    let rest = &pem[start..];
    let finish = rest.find(&end).ok_or_else(|| format!("missing {end}"))?;
    let body: String = rest[..finish]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    STANDARD
        .decode(body)
        .map_err(|err| format!("invalid PEM base64: {err}"))
}

#[cfg(test)]
pub mod tests_support {
    pub const TEST_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIH8aSkCaxHQOnBc3SbAkalwpC4wG8z2V/Xfu9T+b3g3F
-----END PRIVATE KEY-----"#;
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{KeyPair, UnparsedPublicKey, ED25519};

    fn decode_claims(token: &str) -> WorkerJwtClaims {
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap()
    }

    #[test]
    fn configured_ttl_sets_expiry() {
        let issuer = WorkerJwtIssuer::from_ed25519_pem_with_ttl(
            tests_support::TEST_PRIVATE_KEY.as_bytes(),
            Duration::from_secs(42),
        )
        .unwrap();

        let claims = decode_claims(&issuer.issue("u1", "key1", None).unwrap());

        assert_eq!(claims.u, "u1");
        assert_eq!(claims.k, "key1");
        assert_eq!(claims.exp - claims.iat, 42);
    }

    #[test]
    fn issued_token_signature_verifies() {
        let issuer = WorkerJwtIssuer::from_ed25519_pem_with_ttl(
            tests_support::TEST_PRIVATE_KEY.as_bytes(),
            Duration::from_secs(42),
        )
        .unwrap();
        let token = issuer.issue("u1", "key1", None).unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let signature = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        assert_eq!(signature.len(), 64);
        let public_key = UnparsedPublicKey::new(&ED25519, issuer.key.public_key().as_ref());

        public_key
            .verify(signing_input.as_bytes(), &signature)
            .expect("worker JWT signature must verify with issuer public key");
    }

    #[test]
    fn validation_deadline_caps_expiry() {
        let issuer = WorkerJwtIssuer::from_ed25519_pem_with_ttl(
            tests_support::TEST_PRIVATE_KEY.as_bytes(),
            Duration::from_secs(3600),
        )
        .unwrap();
        let deadline = now_secs() + 42;

        let claims = decode_claims(&issuer.issue("u1", "key1", Some(deadline)).unwrap());

        assert_eq!(claims.exp, deadline);
    }

    #[test]
    fn cached_token_is_not_reused_past_validation_deadline() {
        let issuer = WorkerJwtIssuer::from_ed25519_pem_with_ttl(
            tests_support::TEST_PRIVATE_KEY.as_bytes(),
            Duration::from_secs(3600),
        )
        .unwrap();
        let uncapped = issuer.issue("u1", "key1", None).unwrap();
        let deadline = now_secs() + 42;

        let capped = issuer.issue("u1", "key1", Some(deadline)).unwrap();

        assert_ne!(uncapped, capped);
        assert_eq!(decode_claims(&capped).exp, deadline);
    }

    #[test]
    fn cache_is_capped() {
        let issuer = WorkerJwtIssuer::from_ed25519_pem_with_ttl(
            tests_support::TEST_PRIVATE_KEY.as_bytes(),
            Duration::from_secs(3600),
        )
        .unwrap();

        for i in 0..(MAX_CACHED_TOKENS + 1) {
            issuer.issue(&format!("u{i}"), "key1", None).unwrap();
        }

        let cache = issuer.cache.lock().unwrap();
        assert_eq!(cache.len(), MAX_CACHED_TOKENS);
    }
}
