use std::str::FromStr;

use alloc_counter::AllocationGuard;

use crate::{
    StreamItemId,
    constants_tests::{STREAM_ITEM_HEX, STREAM_ITEM_ID_PERF_ITERATIONS},
};

fn parse_stream_item_id_legacy(value: &str) -> Result<StreamItemId, hex::FromHexError> {
    let decoded = hex::decode(value)?;
    if decoded.len() != 12 {
        return Err(hex::FromHexError::InvalidStringLength);
    }
    let mut bytes = [0u8; 12];
    bytes.copy_from_slice(&decoded);
    Ok(StreamItemId::from(bytes))
}

fn measure_allocations(
    label: &'static str,
    parse: impl Fn(&str) -> Result<StreamItemId, hex::FromHexError>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "stream_item_id_parse_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );

    let mut checksum = 0u8;
    for _ in 0..STREAM_ITEM_ID_PERF_ITERATIONS {
        let id = parse(STREAM_ITEM_HEX).expect("parse stream item id");
        checksum ^= id.as_bytes()[11];
    }
    std::hint::black_box(checksum);

    guard.finish()
}

#[test]
fn stream_item_id_parse_avoids_temporary_decode_allocation_tests() {
    let legacy = measure_allocations("stream_item_id_parse_legacy_hex_decode_vec", |value| {
        parse_stream_item_id_legacy(value)
    });
    let optimized = measure_allocations("stream_item_id_parse_from_str_decode_to_slice", |value| {
        StreamItemId::from_str(value)
    });

    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&optimized);

    assert_eq!(optimized.allocation_count, 0);
    assert!(optimized.allocation_count < legacy.allocation_count);
    assert!(optimized.allocated_bytes < legacy.allocated_bytes);
}
