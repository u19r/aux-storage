use std::{borrow::Cow, collections::HashMap};

use foundationdb::tuple::{Bytes, Element, pack, unpack};
use storage_types::{
    AttributeValue, INDEXED_VALUE_LZ4_HEADER, INDEXED_VALUE_RAW_HEADER, IndexedWireItem,
    IndexerDeclaration, MAX_INDEXERS_CAPACITY, MaxIndexers, StorageEnum, WireItem,
    context::WrappedError as _,
};

use super::item_encoding::{
    decode_indexed_wire_item, decode_wire_item_with_indexers_from_storage_bytes,
    encode_indexed_wire_item, encode_wire_item_storage_bytes, foundationdb_compressed_is_smaller,
};
use crate::sorted_kv_store::ItemValueCodec;

fn declaration(names: Vec<String>) -> IndexerDeclaration {
    IndexerDeclaration::try_new(
        names,
        MaxIndexers::try_new(MAX_INDEXERS_CAPACITY).expect("maximum capacity"),
    )
    .expect("declaration")
}

fn tuple_elements(bytes: &[u8]) -> Vec<Element<'_>> {
    unpack(bytes).expect("FoundationDB tuple")
}

fn tuple_header(elements: &[Element<'_>]) -> u8 {
    match elements.first() {
        Some(Element::Bytes(header)) if header.len() == 1 => header[0],
        element => panic!("unexpected tuple header: {element:?}"),
    }
}

#[test]
fn given_indexed_values_when_encoded_for_foundationdb_then_tuple_is_exact_and_unpadded() {
    let item = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("entity\0顧客".to_string()),
        ),
        ("first".to_string(), AttributeValue::S("a\0b".to_string())),
        ("last".to_string(), AttributeValue::S("終".to_string())),
    ]);
    let names = vec![
        "first".to_string(),
        "missing".to_string(),
        "last".to_string(),
    ];
    let indexed = IndexedWireItem::extract(&item, &declaration(names.clone())).expect("extract");
    let encoded = encode_indexed_wire_item(ItemValueCodec::FoundationDbTuple, &indexed)
        .expect("encode FoundationDB tuple");
    let elements = tuple_elements(&encoded);

    assert_eq!(elements.len(), 5, "tuple must not pad to table capacity");
    assert_eq!(tuple_header(&elements), INDEXED_VALUE_RAW_HEADER);
    assert!(matches!(&elements[2], Element::Bytes(value) if value.as_ref() == b"a\0b"));
    assert!(matches!(&elements[3], Element::Nil));
    assert!(matches!(&elements[4], Element::Bytes(value) if value.as_ref() == "終".as_bytes()));

    let (decoded, decoded_names) = decode_wire_item_with_indexers_from_storage_bytes(
        ItemValueCodec::FoundationDbTuple,
        &encoded,
        MaxIndexers::try_new(MAX_INDEXERS_CAPACITY).expect("capacity"),
    )
    .expect("decode FoundationDB tuple");
    assert_eq!(decoded.to_attribute_map().expect("logical item"), item);
    assert_eq!(decoded_names, names);
}

#[test]
fn given_empty_and_maximum_declarations_when_encoded_then_tuple_lengths_are_exact() {
    let empty = IndexedWireItem::extract(&HashMap::new(), &declaration(Vec::new()))
        .expect("empty declaration");
    let empty_encoded =
        encode_indexed_wire_item(ItemValueCodec::FoundationDbTuple, &empty).expect("encode empty");
    assert_eq!(tuple_elements(&empty_encoded).len(), 2);

    let names = (0..MAX_INDEXERS_CAPACITY)
        .map(|ordinal| format!("field_{ordinal}"))
        .collect::<Vec<_>>();
    let maximum = IndexedWireItem::extract(&HashMap::new(), &declaration(names))
        .expect("maximum declaration");
    let maximum_encoded = encode_indexed_wire_item(ItemValueCodec::FoundationDbTuple, &maximum)
        .expect("encode maximum");
    let elements = tuple_elements(&maximum_encoded);
    assert_eq!(elements.len(), 2 + usize::from(MAX_INDEXERS_CAPACITY));
    assert!(
        elements
            .iter()
            .skip(2)
            .all(|element| matches!(element, Element::Nil))
    );
}

#[test]
fn given_compressible_payload_when_encoded_then_lz4_header_round_trips() {
    let item = HashMap::from([(
        "payload".to_string(),
        AttributeValue::S("compressible".repeat(512)),
    )]);
    let wire = WireItem::from_attribute_map(&item).expect("wire item");
    let encoded = encode_wire_item_storage_bytes(
        ItemValueCodec::FoundationDbTuple,
        &wire,
        None,
        MaxIndexers::ZERO,
    )
    .expect("encode compressed tuple");

    assert_eq!(
        tuple_header(&tuple_elements(&encoded)),
        INDEXED_VALUE_LZ4_HEADER
    );
    assert_eq!(
        decode_indexed_wire_item(ItemValueCodec::FoundationDbTuple, &encoded)
            .expect("decode compressed tuple")
            .to_attribute_map()
            .expect("logical item"),
        item
    );
}

#[test]
fn given_equal_packed_payload_sizes_when_selecting_codec_then_raw_wins() {
    let bytes = b"same\0bytes";
    assert!(!foundationdb_compressed_is_smaller(bytes, bytes));
}

#[test]
fn given_malformed_foundationdb_tuple_when_decoded_then_corruption_is_reported() {
    let cases = [
        pack(&vec![
            Element::Bytes(Bytes::from(vec![INDEXED_VALUE_RAW_HEADER])),
            Element::String(Cow::Borrowed("not bytes")),
        ]),
        pack(&vec![
            Element::Bytes(Bytes::from(vec![INDEXED_VALUE_RAW_HEADER])),
            Element::Bytes(Bytes::from(br#"{}"#.to_vec())),
            Element::Int(1),
        ]),
    ];

    for encoded in cases {
        let error = decode_indexed_wire_item(ItemValueCodec::FoundationDbTuple, &encoded)
            .expect_err("malformed tuple must fail");
        assert!(matches!(
            error.to_enum(),
            StorageEnum::InternalServerError { message }
                if message.starts_with("stored_item_corruption:")
        ));
    }
}

#[test]
#[ignore = "performance evidence; run in an isolated test process"]
fn given_realistic_items_when_measuring_indexer_codecs_then_emit_comparable_evidence() {
    const ITERATIONS: u64 = 1_000;
    let names = (0..MAX_INDEXERS_CAPACITY)
        .map(|ordinal| format!("indexer_{ordinal}"))
        .collect::<Vec<_>>();
    for payload_kind in ["raw", "compressible"] {
        for declaration_count in [0_usize, 2, 4, 16, 32] {
            let mut item = HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S(format!("entity#{:084}", 7)),
                ),
                (
                    "sk".to_string(),
                    AttributeValue::S("model#0001".to_string()),
                ),
                ("number".to_string(), AttributeValue::N("42".to_string())),
            ]);
            for ordinal in 0..MAX_INDEXERS_CAPACITY {
                if ordinal % 2 == 0 {
                    item.insert(
                        names[usize::from(ordinal)].clone(),
                        AttributeValue::S(format!("value#{ordinal:02}")),
                    );
                }
            }
            for ordinal in 0_usize..7 {
                let payload = if payload_kind == "compressible" {
                    format!("field-{ordinal}-{}", "repeat".repeat(24))
                } else {
                    (0..145)
                        .map(|offset| char::from(33 + ((ordinal * 47 + offset * 31) % 90) as u8))
                        .collect()
                };
                item.insert(format!("payload_{ordinal}"), AttributeValue::S(payload));
            }
            let wire = WireItem::from_attribute_map(&item).expect("wire item");
            let declaration = &names[..declaration_count];
            for (codec_name, codec) in [
                ("rocksdb", ItemValueCodec::RocksDbEnvelope),
                ("foundationdb", ItemValueCodec::FoundationDbTuple),
            ] {
                let encoded = encode_wire_item_storage_bytes(
                    codec,
                    &wire,
                    Some(declaration),
                    MaxIndexers::try_new(MAX_INDEXERS_CAPACITY).expect("capacity"),
                )
                .expect("encode sample");
                let guard = alloc_counter::AllocationGuard::start(
                    module_path!(),
                    "indexer_codec_performance_evidence",
                    file!(),
                    line!(),
                    None,
                );
                let started = std::time::Instant::now();
                for _ in 0..ITERATIONS {
                    let encoded = encode_wire_item_storage_bytes(
                        codec,
                        std::hint::black_box(&wire),
                        Some(std::hint::black_box(declaration)),
                        MaxIndexers::try_new(MAX_INDEXERS_CAPACITY).expect("capacity"),
                    )
                    .expect("encode");
                    let decoded = decode_wire_item_with_indexers_from_storage_bytes(
                        codec,
                        std::hint::black_box(&encoded),
                        MaxIndexers::try_new(MAX_INDEXERS_CAPACITY).expect("capacity"),
                    )
                    .expect("decode");
                    std::hint::black_box(decoded);
                }
                let elapsed = started.elapsed();
                let allocations = guard.finish();
                println!(
                    "{{\"codec\":\"{codec_name}\",\"payload\":\"{payload_kind}\",\"declarations\":\
                     {declaration_count},\"iterations\":{ITERATIONS},\"physical_bytes\":{},\"\
                     ns_per_round_trip\":{},\"allocations_per_round_trip\":{:.3},\"\
                     allocated_bytes_per_round_trip\":{:.1}}}",
                    encoded.len(),
                    elapsed.as_nanos() / u128::from(ITERATIONS),
                    allocations.allocation_count as f64 / ITERATIONS as f64,
                    allocations.allocated_bytes as f64 / ITERATIONS as f64,
                );
            }
        }
    }
}
