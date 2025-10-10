use anyhow::anyhow;
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

pub fn parse_assignment_id(aid: &str) -> anyhow::Result<u64> {
    let mut split = aid.split('_');
    if let Some(tp) = split.next() {
        Ok(chrono::NaiveDateTime::parse_from_str(tp, "%Y%m%dT%H%M%S")?
            .and_utc()
            .timestamp_millis()
            .try_into()?)
    } else {
        Err(anyhow!("no underscore in assignment_id"))
    }
}

pub fn base64(data: impl AsRef<[u8]>) -> String {
    BASE64_URL_SAFE_NO_PAD.encode(data.as_ref())
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};

    #[test]
    fn test_parse_assignment_id() {
        let sample = "20241008T141245_242da92f7d6c";

        let tp = parse_assignment_id(sample).expect("cannot parse sample");

        let expected = Utc.with_ymd_and_hms(2024, 10, 8, 14, 12, 45).unwrap();
        assert_eq!(expected.timestamp_millis() as u64, tp);

        let dt = DateTime::from_timestamp_millis(tp as i64).unwrap();
        assert_eq!(expected, dt);

        let have = format!("{}_{}", dt.format("%Y%m%dT%H%M%S"), "242da92f7d6c");
        assert_eq!(sample, have);
        
    }
}
