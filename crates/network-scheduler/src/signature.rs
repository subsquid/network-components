use base64::{engine::general_purpose::STANDARD as base64, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use url::form_urlencoded;

// Generate signature with timestamp as required by Cloudflare's is_timed_hmac_valid_v0 function
// https://developers.cloudflare.com/ruleset-engine/rules-language/functions/#hmac-validation
pub fn timed_hmac(message: &str, secret: &str, timestamp: usize) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{}{}", message, timestamp).as_bytes());
    let digest = mac.finalize().into_bytes();
    let token: String =
        form_urlencoded::byte_serialize(base64.encode(digest.as_slice()).as_bytes()).collect();
    format!("{timestamp}-{token}")
}

pub fn timed_hmac_now(message: &str, secret: &str) -> String {
    let timestamp = std::time::UNIX_EPOCH
        .elapsed()
        .unwrap()
        .as_secs()
        .try_into()
        .unwrap();
    timed_hmac(message, secret, timestamp)
}

#[test]
fn test_hmac_sign() {
    let message = "12D3KooWBwbQFT48cNYGPbDwm8rjasbZkc1VMo6rCR6217qr165S";
    let secret = "test_secret";
    let timestamp = 1715662737;
    let expected = "1715662737-E%2BaW1Y5hS587YGeJFKGTnp%2Fhn8rEMmSRlEslPiOQsuE%3D";
    assert_eq!(timed_hmac(message, secret, timestamp), expected);
}
