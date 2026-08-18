use foundationdb::tuple::{Element, pack, unpack};

use super::keyspace::{encode_prefix, prefix_bytes, strip_prefix};

#[test]
fn given_configured_prefix_when_composing_tuple_then_prefix_is_the_first_element() {
    let prefix = encode_prefix(Some(b"pd/"));
    let logical = pack(&(2_i64, "item"));
    let physical = prefix_bytes(&prefix, &logical);
    let elements = unpack::<Vec<Element<'_>>>(&physical).expect("physical tuple");

    assert_eq!(
        physical,
        vec![
            0x01, 0x70, 0x64, 0x2f, 0x00, 0x15, 0x02, 0x02, 0x69, 0x74, 0x65, 0x6d, 0x00,
        ]
    );
    assert!(matches!(&elements[0], Element::Bytes(value) if value.as_ref() == b"pd/"));
    assert!(matches!(elements[1], Element::Int(2)));
    assert!(matches!(&elements[2], Element::String(value) if value == "item"));
    assert_eq!(strip_prefix(&physical, &prefix), logical);
}

#[test]
fn given_no_configured_prefix_when_composing_tuple_then_first_element_is_nil() {
    let prefix = encode_prefix(None);
    let physical = prefix_bytes(&prefix, &pack(&(2_i64,)));
    let elements = unpack::<Vec<Element<'_>>>(&physical).expect("physical tuple");

    assert!(matches!(elements[0], Element::Nil));
    assert!(matches!(elements[1], Element::Int(2)));
}

#[test]
#[ignore = "allocation counters require an isolated test process"]
fn composing_physical_keys_allocates_only_the_output_buffer() {
    const ITERATIONS: u64 = 128;
    let prefix = encode_prefix(Some(b"pd/"));
    let logical = pack(&(2_i64, "item", 42_i64));
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "composing_physical_keys_allocates_only_the_output_buffer",
        file!(),
        line!(),
        Some("fdb_physical_key_prefix"),
    );

    for _ in 0..ITERATIONS {
        std::hint::black_box(prefix_bytes(&prefix, &logical));
    }

    let report = guard.finish();
    alloc_counter::emit_report(&report);
    assert_eq!(report.allocation_count, ITERATIONS, "{report:?}");
}
