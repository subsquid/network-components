use std::time::UNIX_EPOCH;

use base64::{prelude::BASE64_URL_SAFE_NO_PAD, Engine};

pub fn timestamp_now_ms() -> u64 {
    UNIX_EPOCH
        .elapsed()
        .expect("Current time should be after 1970")
        .as_millis()
        .try_into()
        .expect("Timestamp should fit in u64")
}

pub fn base64(data: impl AsRef<[u8]>) -> String {
    BASE64_URL_SAFE_NO_PAD.encode(data.as_ref())
}
