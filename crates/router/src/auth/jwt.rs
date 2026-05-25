use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use ring::rand::SystemRandom;
use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};
use serde::{Deserialize, Serialize};
use serde_json::json;

const ISSUER: &str = "sqd-router";
const AUDIENCE: &str = "sqd-worker";
const DEFAULT_TTL: Duration = Duration::from_secs(60 * 60);
const REFRESH_BEFORE_EXPIRY: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct WorkerJwtIssuer {
    key: Arc<RsaKeyPair>,
    kid: Option<String>,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerJwtClaims {
    pub user_id: String,
    pub api_key_id: String,
    pub iat: u64,
    pub exp: u64,
    pub iss: String,
    pub aud: String,
}

impl WorkerJwtIssuer {
    pub fn from_rsa_pem(pem: &[u8], kid: Option<String>) -> Result<Self, String> {
        Self::from_rsa_pem_with_ttl(pem, kid, DEFAULT_TTL)
    }

    pub fn from_rsa_pem_with_ttl(
        pem: &[u8],
        kid: Option<String>,
        ttl: Duration,
    ) -> Result<Self, String> {
        if ttl.is_zero() {
            return Err("worker JWT TTL must be greater than zero".to_string());
        }
        let key = match decode_pem(pem, "PRIVATE KEY") {
            Ok(der) => RsaKeyPair::from_pkcs8(&der),
            Err(_) => {
                let der = decode_pem(pem, "RSA PRIVATE KEY")?;
                RsaKeyPair::from_der(&der)
            }
        }
        .map_err(|err| format!("invalid RSA key: {err}"))?;
        Ok(Self {
            key: Arc::new(key),
            kid,
            ttl,
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn issue(&self, user_id: &str, api_key_id: &str) -> Result<String, String> {
        let iat = now_secs();
        let key = CacheKey {
            user_id: user_id.to_string(),
            api_key_id: api_key_id.to_string(),
        };
        if let Some(token) = self.cached_token(&key, iat) {
            return Ok(token);
        }

        let claims = WorkerJwtClaims {
            user_id: user_id.to_string(),
            api_key_id: api_key_id.to_string(),
            iat,
            exp: iat.saturating_add(self.ttl.as_secs()),
            iss: ISSUER.to_string(),
            aud: AUDIENCE.to_string(),
        };
        let header = json!({
            "alg": "RS256",
            "typ": "JWT",
            "kid": self.kid.as_ref(),
        });
        let header = encode_json(&header)?;
        let payload = encode_json(&claims)?;
        let signing_input = format!("{header}.{payload}");
        let rng = SystemRandom::new();
        let mut signature = vec![0; self.key.public().modulus_len()];
        self.key
            .sign(&RSA_PKCS1_SHA256, &rng, signing_input.as_bytes(), &mut signature)
            .map_err(|_| "failed to sign worker JWT".to_string())?;
        let token = format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature));
        self.store_token(key, token.clone(), claims.exp, iat);
        Ok(token)
    }

    fn cached_token(&self, key: &CacheKey, now: u64) -> Option<String> {
        let refresh_at = now.saturating_add(REFRESH_BEFORE_EXPIRY.as_secs());
        self.cache
            .lock()
            .expect("worker JWT cache mutex poisoned")
            .get(key)
            .filter(|cached| cached.exp > refresh_at)
            .map(|cached| cached.token.clone())
    }

    fn store_token(&self, key: CacheKey, token: String, exp: u64, now: u64) {
        let mut cache = self.cache.lock().expect("worker JWT cache mutex poisoned");
        cache.retain(|_, cached| cached.exp > now);
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
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDR8qHjLfYFCbIv
7FKY7WceocTHLfW2QJGDF3yY2vrzw7ySxRDE6gqEpApaQs/pfjI+WOxONCO4bIV9
d9GRIkda2j+nkTENGRyelU2uSb20xXDhoWtLfelYGM05OP+2IJb7jIfrFXFdhIFD
7IxH4JsLQavQ8+78c5iPYnjvk6DKMNg4Hx9PqLytDty9clWViw6SJpBJFDmLNIDQ
Mgxk+X5/ODgiLrgPavD9KviQqw28BcvKHopm15vdwNF1EyzecLeZ3OGCL53f/Atl
g1yv6zVLfnKny0mFwYYQUBNBmyk6iqG5vN1I3MhYSR6xwCczvtnFDQn+enyZzgHm
gndORDqFAgMBAAECggEAFwIQ9Wgj4NvLVXl/zcxKnkPv4DzXA4ZM9sGxVmu9JSTA
0DwyK8T9qm6dxymGRoTyyvwMyD8/i5EHtL8s+MukZ+j+ITDbYB+YkCqK7PxI2Cbr
yYEFVKxzpkNwONgDHLmyl4EITjOMjR6d2fdHolg+NZFuBm6330kTIYzoSv0cmbu7
cc2YaxajD9SIYa3gC0NROQ88x9xXt9hT+oMhudj5d3JcyiDxS9J3maqI9WkHwUzW
SQAYtN+L4/jN9vJL/s16lBrxFnFHPud7/l4KkcZOZjT1NSSBBCgldxfTK2wXfCPg
V4Q1ZmQUpkvIgrl5rDYFE9ak/cff/3CLftRClfgruwKBgQDgZSXctadJXIFSB/Oz
FNJAPLG48bvB4qdBO4xiLvFShD3la5DzoTw1oTFmzYuq3rhQOtKD+qN9/7n5uLUD
5BHrg16yDlmz6VbD9FLdU/V+ePF1Jwvl2v9YJ0Q5QGrdltdTilh10nFoLV/Ik9sH
O+QF7EUrxsdkNPS1W9WRe7fT8wKBgQDvhJFTo4PAxp0iaFQXgrwaglPVsS/6qSBY
SyC19+RBYByG3dWEVtsscMe2CaZ4b14quCkxlR6gO+ZTb0voFDN8/b5U2logxvU1
ROIGHgh37zOcRNWsRhOiDyOj8qkQ6HBFmUnLBi8BY5yASH1qtm36kScIAbc/731t
5yNmEpvtpwKBgQCCODCQtJov6I7jm9nAwwSAYriAK0haa73EDVqaX8OLr1J8IMAt
ohPey3xvvDihID610Gz6Sik2pYC3eokRiPkdQ09g5RMJZRAFB3RPHLoKewUkh1RQ
P5aPAbqFvuxFS5QJ1u8e8ND/M9WyAJvKxua8yTAbB3AOpuybkn+Nvc4gIQKBgBuQ
UAEmEiV/Ndod03+ZJfiPAwLWj0Tzbat7idonGveDDgVfRhEixbpJiFIkrimx905H
P0ZbeNjLy+fSKRQeLwa1VNADCNg4zUNCGBjIIAVdW70iFszqi5vczicx587wUOtR
hrJ8lbA9PGdu8C/1qpZpWeqL+AC9mNuq++HlRliFAoGBAI0r7FqWoPVXid4PYbFC
SKH9D7cc6VjJWpxG8sxd0a6IbS2qh+4bGS4IBErtCZIQZC/QJVBFL7Ci53rpxNyl
x+4pvLgz1SGSn2nD6h3/+bhJw+Ak1WEevkdFNFcv6rWxfabDwPCezQz+xxfEXVCs
Nqv/oNzamjjCTlBEKaGe4IjG
-----END PRIVATE KEY-----"#;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_claims(token: &str) -> WorkerJwtClaims {
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap()
    }

    #[test]
    fn configured_ttl_sets_expiry() {
        let issuer = WorkerJwtIssuer::from_rsa_pem_with_ttl(
            tests_support::TEST_PRIVATE_KEY.as_bytes(),
            None,
            Duration::from_secs(42),
        )
        .unwrap();

        let claims = decode_claims(&issuer.issue("u1", "key1").unwrap());

        assert_eq!(claims.exp - claims.iat, 42);
    }
}
