use std::{str::FromStr, time::Instant};

use alloc_counter::AllocationGuard;

use crate::StreamItemId;

const ITERATIONS: usize = 200_000;
const STREAM_ITEM_HEX: &str = "018f1f612a6f7ac3b9b67f65";

fn parse_stream_item_id_legacy(value: &str) -> Result<StreamItemId, hex::FromHexError> {
    let decoded = hex::decode(value)?;
    if decoded.len() != 12 {
        return Err(hex::FromHexError::InvalidStringLength);
    }
    let mut bytes = [0u8; 12];
    bytes.copy_from_slice(&decoded);
    Ok(StreamItemId::from(bytes))
}

fn measure_runtime(
    label: &str,
    parse: impl Fn(&str) -> Result<StreamItemId, hex::FromHexError>,
) -> f64 {
    let started = Instant::now();
    let mut checksum = 0u8;

    for _ in 0..ITERATIONS {
        let id = parse(STREAM_ITEM_HEX).expect("parse stream item id");
        checksum ^= id.as_bytes()[11];
    }

    let elapsed = started.elapsed();
    let ns_per_iter = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!(
        "{label} iterations={ITERATIONS} checksum={checksum} elapsed_ms={:.3} \
         ns_per_iter={ns_per_iter:.2}",
        elapsed.as_secs_f64() * 1_000.0,
    );
    ns_per_iter
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
    for _ in 0..ITERATIONS {
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

    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&optimized);

    assert_eq!(optimized.allocation_count, 0);
    assert!(optimized.allocation_count < legacy.allocation_count);
    assert!(optimized.allocated_bytes < legacy.allocated_bytes);
}

#[test]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture before/after parse changes"]
fn stream_item_id_parse_runtime_perf_probe() {
    let legacy = measure_runtime("stream_item_id_parse_legacy_hex_decode_vec", |value| {
        parse_stream_item_id_legacy(value)
    });
    let optimized = measure_runtime("stream_item_id_parse_from_str_decode_to_slice", |value| {
        StreamItemId::from_str(value)
    });

    assert!(
        optimized < legacy * 0.85,
        "expected >=15% runtime win, legacy={legacy:.2}ns optimized={optimized:.2}ns"
    );
}
