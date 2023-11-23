use std::time::UNIX_EPOCH;

pub fn timestamp_now_ms() -> u64 {
    UNIX_EPOCH
        .elapsed()
        .expect("We're after 1970")
        .as_millis()
        .try_into()
        .expect("But not that far")
}
