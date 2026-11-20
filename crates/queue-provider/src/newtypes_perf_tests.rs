use std::str::FromStr;

use alloc_counter::AllocationGuard;

use crate::{
    InvalidMessageId, MessageId,
    constants_tests::{NEWTYPES_PERF_ITERATIONS, NEWTYPES_PERF_MESSAGE_ID_TEXT},
};

fn measure_allocations(
    label: &'static str,
    parse: impl Fn(&str) -> Result<MessageId, InvalidMessageId>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "message_id_parse_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );

    let mut checksum = 0u8;
    for _ in 0..NEWTYPES_PERF_ITERATIONS {
        let id = parse(NEWTYPES_PERF_MESSAGE_ID_TEXT).expect("parse message id");
        checksum ^= id.as_bytes()[15];
    }
    std::hint::black_box(checksum);

    guard.finish()
}

#[test]
fn message_id_parse_does_not_allocate_tests() {
    let report = measure_allocations("message_id_parse_from_str", MessageId::from_str);
    let _ = alloc_counter::emit_report(&report);
    assert_eq!(report.allocation_count, 0);
}
