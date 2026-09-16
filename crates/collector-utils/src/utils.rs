use anyhow::{anyhow, Context};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentId {
    pub timestamp_ms: u64,
    /// Integer part, only present in the `<time>_<u64>_<hash>` format
    pub number: Option<u64>,
}

/// Supported formats are `<time>_<hash>` and `<time>_<u64>_<hash>`.
pub fn parse_assignment_id(aid: &str) -> anyhow::Result<AssignmentId> {
    let parts: Vec<&str> = aid.split('_').collect();
    let (tp, number) = match parts.as_slice() {
        [tp, _hash] => (tp, None),
        [tp, n, _hash] => {
            let n = n
                .parse::<u64>()
                .context("invalid integer in assignment_id")?;
            (tp, Some(n))
        }
        _ => return Err(anyhow!("unexpected assignment_id format")),
    };
    let timestamp_ms = chrono::NaiveDateTime::parse_from_str(tp, "%FT%T")?
        .and_utc()
        .timestamp_millis()
        .try_into()?;
    Ok(AssignmentId {
        timestamp_ms,
        number,
    })
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
        let hash = "C1A955A7E13FABEC64DCA7965104FA0CBF98C063A6FCB4473E243348CADFAFAE";
        let sample = format!("2025-10-12T12:00:45_{hash}");

        let aid = parse_assignment_id(&sample).expect("cannot parse sample");
        let tp = aid.timestamp_ms;

        let expected = Utc.with_ymd_and_hms(2025, 10, 12, 12, 0, 45).unwrap();
        assert_eq!(expected.timestamp_millis() as u64, tp);
        assert_eq!(aid.number, None);

        let dt = DateTime::from_timestamp_millis(tp as i64).unwrap();
        assert_eq!(expected, dt);

        let have = format!("{}_{}", dt.format("%FT%T"), hash);
        assert_eq!(sample, have);
    }

    #[test]
    fn test_parse_assignment_id_with_integer() {
        let hash = "C1A955A7E13FABEC64DCA7965104FA0CBF98C063A6FCB4473E243348CADFAFAE";
        let sample = format!("2025-10-12T12:00:45_18446744073709551615_{hash}");

        let aid = parse_assignment_id(&sample).expect("cannot parse sample");

        let expected = Utc.with_ymd_and_hms(2025, 10, 12, 12, 0, 45).unwrap();
        assert_eq!(expected.timestamp_millis() as u64, aid.timestamp_ms);
        assert_eq!(aid.number, Some(u64::MAX));
    }

    #[test]
    fn test_parse_assignment_id_invalid() {
        assert!(parse_assignment_id("2025-10-12T12:00:45").is_err());
        assert!(parse_assignment_id("2025-10-12T12:00:45_-1_ABCD").is_err());
        assert!(parse_assignment_id("2025-10-12T12:00:45_abc_ABCD").is_err());
        assert!(parse_assignment_id("2025-10-12T12:00:45_1_2_ABCD").is_err());
    }
}
