use std::hint::black_box;

use alloc_counter::{AllocationGuard, AllocationReport};

use crate::{BootstrapCursorName, CursorName};

const TABLE_NAME: &str = "orders/table";
const PEER_REGION: &str = "eu-west-2";
const GENERATION: &str = "bootstrap-generation-0001";
const ITERATIONS: usize = 128;

#[test]
fn given_bootstrap_identity_when_building_then_owned_encoding_allocations_drop() {
    let legacy = measure("legacy-build", || {
        let table_name = TABLE_NAME.to_string();
        let peer_region = PEER_REGION.to_string();
        let generation = GENERATION.to_string();
        let encoded = format!(
            "{}{}:{}:{}",
            crate::constants::BOOTSTRAP_CURSOR_PREFIX,
            legacy_encode(&table_name),
            legacy_encode(&peer_region),
            legacy_encode(&generation),
        );
        let cursor_name = CursorName::new(&encoded);
        black_box((cursor_name, table_name, peer_region, generation));
    });
    let current = measure("current-build", || {
        black_box(
            BootstrapCursorName::try_new(TABLE_NAME, PEER_REGION, GENERATION)
                .expect("identity should be valid"),
        );
    });

    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&current);
    assert!(current.allocation_count < legacy.allocation_count);
    assert!(current.allocated_bytes < legacy.allocated_bytes);
}

#[test]
fn given_bootstrap_identity_when_parsing_then_reencoding_allocations_drop() {
    let identity = BootstrapCursorName::try_new(TABLE_NAME, PEER_REGION, GENERATION)
        .expect("identity should be valid");
    let legacy = measure("legacy-parse", || {
        let mut components = identity
            .cursor_name()
            .as_str()
            .get(crate::constants::BOOTSTRAP_CURSOR_PREFIX.len()..)
            .expect("ASCII prefix ends on a character boundary")
            .split(':');
        let table_name = legacy_decode(components.next().expect("table"));
        let peer_region = legacy_decode(components.next().expect("peer"));
        let generation = legacy_decode(components.next().expect("generation"));
        let encoded = format!(
            "{}{}:{}:{}",
            crate::constants::BOOTSTRAP_CURSOR_PREFIX,
            legacy_encode(&table_name),
            legacy_encode(&peer_region),
            legacy_encode(&generation),
        );
        let parsed = CursorName::new(&encoded);
        assert_eq!(parsed, *identity.cursor_name());
        black_box((parsed, table_name, peer_region, generation));
    });
    let current = measure("current-parse", || {
        black_box(BootstrapCursorName::parse(identity.cursor_name()).expect("identity parses"));
    });

    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&current);
    assert!(current.allocation_count < legacy.allocation_count);
    assert!(current.allocated_bytes < legacy.allocated_bytes);
}

fn measure(label: &'static str, operation: impl Fn()) -> AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "protected_cursor_identity_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..ITERATIONS {
        operation();
    }
    guard.finish()
}

fn legacy_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn legacy_decode(value: &str) -> String {
    let mut decoded = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().as_chunks::<2>().0 {
        decoded.push((legacy_hex(pair[0]) << 4) | legacy_hex(pair[1]));
    }
    String::from_utf8(decoded).expect("legacy identity is UTF-8")
}

fn legacy_hex(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => unreachable!("fixture contains canonical lowercase hex"),
    }
}
