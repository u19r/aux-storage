use std::time::Duration;

use super::telemetry::{record_provider_count, record_provider_stage, record_read, record_write};

#[test]
fn given_no_recorder_when_telemetry_is_recorded_then_calls_are_no_ops() {
    record_read(1, 128);
    record_write(2, 256);
    record_provider_stage("query", "decode", Duration::from_millis(3));
    record_provider_count("query", "items_fetched", 7);
}
