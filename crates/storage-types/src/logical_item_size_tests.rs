use std::{cell::Cell, collections::HashMap, time::Instant};

use alloc_counter::AllocationGuard;

use crate::{
    AttributeValue, LogicalItemSize, WireItem, WireItemKeyAttributes,
    constants::{MAX_ATTRIBUTE_NAME_BYTES, MAX_ATTRIBUTE_NESTING_DEPTH, MAX_ITEM_SIZE_BYTES},
};

#[test]
fn given_attribute_name_when_validating_then_error_label_is_only_built_on_failure() {
    let calls = Cell::new(0);
    let field = || {
        calls.set(calls.get() + 1);
        "Item attribute name".to_owned()
    };
    assert!(crate::dynamodb_limits::validate_attribute_name("pk", field).is_ok());
    assert_eq!(calls.get(), 0);

    let error = crate::dynamodb_limits::validate_attribute_name("", field).unwrap_err();
    assert_eq!(error, "Item attribute name cannot be empty");
    assert_eq!(calls.get(), 1);

    let nested = AttributeValue::M(HashMap::from([(
        String::new(),
        AttributeValue::S("value".to_owned()),
    )]));
    let error = LogicalItemSize::try_from_entries([("pk", &nested)])
        .unwrap_err()
        .to_string();
    assert!(error.contains("Item nested attribute name cannot be empty"));
}

#[test]
fn given_valid_attributes_when_calculating_logical_size_then_decoded_value_rules_apply() {
    let binary =
        LogicalItemSize::try_from_entries([("bin", &AttributeValue::B("AQID".to_owned()))])
            .expect("valid binary attribute");
    assert_eq!(u64::from(binary), "bin".len() as u64 + 3);

    let number =
        LogicalItemSize::try_from_entries([("number", &AttributeValue::N("123".to_owned()))])
            .expect("valid number attribute");
    assert_eq!(u64::from(number), "number".len() as u64 + 3);

    let nested = AttributeValue::M(HashMap::from([(
        "child".to_owned(),
        AttributeValue::L(vec![
            AttributeValue::BOOL(true),
            AttributeValue::N("12".to_owned()),
        ]),
    )]));
    let nested_size =
        LogicalItemSize::try_from_entries([("nested", &nested)]).expect("valid nested attribute");
    assert_eq!(u64::from(nested_size), 23);
}

#[test]
fn given_invalid_attributes_when_calculating_logical_size_then_validation_rejects_them() {
    assert!(
        LogicalItemSize::try_from_entries([(
            "binary",
            &AttributeValue::B("not-base64".to_owned()),
        )])
        .is_err()
    );
    assert!(
        LogicalItemSize::try_from_entries([(
            "number",
            &AttributeValue::N("not-a-number".to_owned()),
        )])
        .is_err()
    );
    assert!(
        LogicalItemSize::try_from_entries([("", &AttributeValue::S("value".to_owned()),)]).is_err()
    );
    let too_long_name = "x".repeat(MAX_ATTRIBUTE_NAME_BYTES + 1);
    assert!(
        LogicalItemSize::try_from_entries([(
            too_long_name.as_str(),
            &AttributeValue::S("value".to_owned()),
        )])
        .is_err()
    );

    let mut nested = AttributeValue::S("value".to_owned());
    for _ in 0..=MAX_ATTRIBUTE_NESTING_DEPTH {
        nested = AttributeValue::L(vec![nested]);
    }
    assert!(LogicalItemSize::try_from_entries([("nested", &nested)]).is_err());
}

#[test]
fn given_boundary_sizes_when_converting_logical_size_then_positive_bounds_are_enforced() {
    let value = AttributeValue::S("x".repeat(MAX_ITEM_SIZE_BYTES - "pk".len()));
    let max = LogicalItemSize::try_from_entries([("pk", &value)]).expect("maximum item size");
    assert_eq!(u64::from(max), MAX_ITEM_SIZE_BYTES as u64);
    assert_eq!(i64::from(max), MAX_ITEM_SIZE_BYTES as i64);

    let oversized = AttributeValue::S("x".repeat(MAX_ITEM_SIZE_BYTES - "pk".len() + 1));
    assert!(LogicalItemSize::try_from_entries([("pk", &oversized)]).is_err());
    assert!(
        LogicalItemSize::try_from_entries(std::iter::empty::<(&str, &AttributeValue)>()).is_err()
    );
    assert!(LogicalItemSize::try_from(0_u64).is_err());
    assert!(LogicalItemSize::try_from(-1_i64).is_err());
    assert!(LogicalItemSize::try_from((MAX_ITEM_SIZE_BYTES as u64) + 1).is_err());
    assert!(LogicalItemSize::try_from(u64::MAX).is_err());
}

#[test]
fn given_dynamo_json_when_calculating_wire_item_size_then_structure_is_validated() {
    let item = WireItem::dynamo_json(br#"{"pk":{"S":"x"},"data":{"B":"AQID"}}"#.to_vec());
    assert_eq!(
        u64::from(item.logical_size().expect("Dynamo JSON size")),
        10
    );

    let escaped_name = WireItem::dynamo_json(br#"{"a\u0062":{"S":"x"}}"#.to_vec());
    assert_eq!(
        u64::from(
            escaped_name
                .logical_size()
                .expect("escaped attribute name size")
        ),
        3
    );

    let trailing_data = WireItem::dynamo_json(br#"{"pk":{"S":"x"}} trailing"#.to_vec());
    assert!(trailing_data.logical_size().is_err());

    let duplicate_name = WireItem::dynamo_json(br#"{"pk":{"S":"x"},"pk":{"S":"y"}}"#.to_vec());
    assert!(duplicate_name.logical_size().is_err());
}

#[test]
fn given_local_split_when_calculating_wire_item_size_then_canonical_keys_are_counted() {
    let primary_key = WireItemKeyAttributes::new(
        "pk".to_owned(),
        AttributeValue::S("primary".to_owned()),
        None,
        None,
    );
    let secondary_key = WireItemKeyAttributes::new(
        "pk".to_owned(),
        AttributeValue::S("secondary".to_owned()),
        None,
        None,
    );
    let blob = br#"{"pk":{"S":"blob"},"data":{"S":"v"}}"#.to_vec();
    let item = WireItem::local_split(primary_key, Some(secondary_key), Some(blob));

    assert_eq!(
        u64::from(item.logical_size().expect("LocalSplit size")),
        "pk".len() as u64 + "secondary".len() as u64 + "data".len() as u64 + 1
    );
    let attributes = item.to_attribute_map().expect("canonical LocalSplit image");
    assert_eq!(
        attributes.get("pk"),
        Some(&AttributeValue::S("secondary".to_owned()))
    );
    assert_eq!(
        attributes.get("data"),
        Some(&AttributeValue::S("v".to_owned()))
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_valid_items_when_measuring_logical_size_then_report_allocations_and_time() {
    const ITERATIONS: usize = 20_000;
    let scalar = AttributeValue::S("value".to_owned());
    let nested = AttributeValue::M(HashMap::from([(
        "child".to_owned(),
        AttributeValue::S("value".to_owned()),
    )]));

    for (label, value) in [("scalar", &scalar), ("nested", &nested)] {
        let guard = AllocationGuard::start(
            module_path!(),
            "given_valid_items_when_measuring_logical_size_then_report_allocations_and_time",
            file!(),
            line!(),
            Some(label),
        );
        let mut total = 0_u64;
        for _ in 0..ITERATIONS {
            let size = LogicalItemSize::try_from_entries_with_field(
                [("item", std::hint::black_box(value))],
                "Item",
            )
            .unwrap();
            total += u64::from(size);
        }
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert!(total > 0);

        let started = Instant::now();
        for _ in 0..ITERATIONS {
            let size = LogicalItemSize::try_from_entries_with_field(
                [("item", std::hint::black_box(value))],
                "Item",
            )
            .unwrap();
            std::hint::black_box(size);
        }
        println!(
            "logical_item_size label={label} iterations={ITERATIONS} elapsed_ns={}",
            started.elapsed().as_nanos()
        );
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
#[alloc_counter::count_allocations(label = "logical_item_size_scalar")]
fn given_scalar_item_when_counting_allocations_then_size_is_stable() {
    let value = AttributeValue::S("value".to_owned());
    let mut total = 0_u64;
    for _ in 0..1_000 {
        total += u64::from(
            LogicalItemSize::try_from_entries([("item", std::hint::black_box(&value))]).unwrap(),
        );
    }
    assert_eq!(total, 9_000);
}

#[test]
fn given_binary_encodings_when_sized_then_padding_and_invalid_values_are_preserved() {
    for (encoded, length) in [
        ("", 0),
        ("AA==", 1),
        ("AA", 1),
        ("AAA=", 2),
        ("AAA", 2),
        ("AAAA", 3),
    ] {
        let value = AttributeValue::B(encoded.to_owned());
        assert_eq!(
            u64::from(LogicalItemSize::try_from_entries([("b", &value)]).unwrap()),
            length + 1
        );
    }
    for encoded in ["A", "AA=", "!AAA", "AB==", "AAA==", "AA==x"] {
        let error =
            LogicalItemSize::try_from_entries([("b", &AttributeValue::B(encoded.to_owned()))])
                .unwrap_err();
        assert!(
            error.to_string().contains("invalid base64"),
            "{encoded}: {error}"
        );
    }
}

#[test]
fn given_binary_lengths_when_sized_then_match_decoding_and_item_limit() {
    use base64::{
        Engine as _,
        engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
    };
    for length in 0..257 {
        for encoded in [
            STANDARD.encode(vec![0; length]),
            STANDARD_NO_PAD.encode(vec![0; length]),
        ] {
            let value = AttributeValue::B(encoded);
            assert_eq!(
                u64::from(LogicalItemSize::try_from_entries([("b", &value)]).unwrap()),
                length as u64 + 1
            );
        }
    }
    let maximum = MAX_ITEM_SIZE_BYTES;
    let exact = AttributeValue::B(STANDARD.encode(vec![0; maximum - 1]));
    assert_eq!(
        u64::from(LogicalItemSize::try_from_entries([("b", &exact)]).unwrap()),
        MAX_ITEM_SIZE_BYTES as u64
    );
    let oversized = AttributeValue::B(STANDARD.encode(vec![0; maximum]));
    assert!(
        LogicalItemSize::try_from_entries([("b", &oversized)])
            .unwrap_err()
            .to_string()
            .contains("maximum allowed size")
    );
    let invalid = AttributeValue::L(vec![AttributeValue::B("!AAA".to_owned())]);
    assert!(
        LogicalItemSize::try_from_entries([("b", &invalid)])
            .unwrap_err()
            .to_string()
            .contains("invalid base64")
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_binary_item_shapes_when_sized_repeatedly_then_report_cpu_and_allocations() {
    use base64::{
        Engine as _,
        engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
    };
    const ITERATIONS: usize = 20_000;
    let fixtures = [
        ("string", AttributeValue::S("x".repeat(128))),
        (
            "binary_small",
            AttributeValue::B(STANDARD.encode([0u8; 128])),
        ),
        (
            "binary_large",
            AttributeValue::B(STANDARD.encode(vec![0u8; 65536])),
        ),
        (
            "binary_unpadded",
            AttributeValue::B(STANDARD_NO_PAD.encode(vec![0u8; 1025])),
        ),
        (
            "binary_set",
            AttributeValue::BS((0..8).map(|n| STANDARD.encode(vec![n; 1024])).collect()),
        ),
        (
            "binary_nested",
            AttributeValue::M(HashMap::from([(
                "children".to_owned(),
                AttributeValue::L(
                    (0..8)
                        .map(|n| AttributeValue::B(STANDARD.encode(vec![n; 1024])))
                        .collect(),
                ),
            )])),
        ),
    ];
    for (label, value) in fixtures {
        let expected = LogicalItemSize::try_from_entries([("payload", &value)]).unwrap();
        let guard = AllocationGuard::start(
            module_path!(),
            "binary_item_size",
            file!(),
            line!(),
            Some(label),
        );
        let cpu_started = cpu_time::ProcessTime::try_now().unwrap();
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            let size =
                LogicalItemSize::try_from_entries([("payload", std::hint::black_box(&value))])
                    .unwrap();
            assert_eq!(size, expected);
            std::hint::black_box(size);
        }
        eprintln!(
            "binary_item_size label={label} iterations={ITERATIONS} elapsed_ns={} cpu_ns={}",
            started.elapsed().as_nanos(),
            cpu_started.try_elapsed().unwrap().as_nanos()
        );
        alloc_counter::emit_report(&guard.finish()).unwrap();
    }
}
