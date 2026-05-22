use alloc_counter::AllocationGuard;
use storage_types::{AttributeValue, WireItem, WireItemKeyAttributes};

use crate::raw_dynamodb_response::{wire_item_json_len_upper_bound, write_wire_item_array};

const ITEM_COUNT: usize = 64;
const ITERATIONS: usize = 128;

#[test]
fn local_split_wire_item_serialization_composes_safe_json_without_map_decode_tests() {
    let items = local_split_items(false);
    let legacy = measure_local_split_serialization("legacy_map_decode", &items, |items| {
        legacy_write_wire_item_array(items)
    });
    alloc_counter::emit_report(&legacy);
    let optimized = measure_local_split_serialization("direct_compose", &items, |items| {
        let mut out = Vec::with_capacity(16 * 1024);
        write_wire_item_array(&mut out, items.to_vec())?;
        Ok(out)
    });
    alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count < legacy.allocation_count,
        "direct composition should reduce allocation count: optimized={} legacy={}",
        optimized.allocation_count,
        legacy.allocation_count
    );
    assert!(
        optimized.allocated_bytes < legacy.allocated_bytes,
        "direct composition should reduce allocated bytes: optimized={} legacy={}",
        optimized.allocated_bytes,
        legacy.allocated_bytes
    );
}

#[test]
fn local_split_wire_item_serialization_falls_back_when_blob_duplicates_key_tests() {
    let item = local_split_item(0, true);
    let mut out = Vec::new();
    write_wire_item_array(&mut out, vec![item]).expect("serialize local split item");
    let decoded: serde_json::Value =
        serde_json::from_slice(&out).expect("decode serialized local split array");

    assert_eq!(decoded[0]["pk"]["S"], "blob-overrides-key");
    assert_eq!(decoded[0]["payload"]["S"], "payload-0000");
}

#[test]
fn wire_item_json_len_upper_bound_covers_serialized_item_bytes_tests() {
    for item in [
        local_split_item(0, false),
        local_split_item(1, true),
        WireItem::dynamo_json(br#"{"pk":{"S":"tenant#0000"},"payload":{"S":"raw"}}"#.to_vec()),
    ] {
        let mut out = Vec::new();
        write_wire_item_array(&mut out, vec![item.clone()]).expect("serialize wire item array");
        assert!(wire_item_json_len_upper_bound(&item).expect("measure wire item") >= out.len() - 2);
    }
}

fn measure_local_split_serialization(
    label: &'static str,
    items: &[WireItem],
    serialize: impl Fn(&[WireItem]) -> serde_json::Result<Vec<u8>>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "local_split_wire_item_serialization_composes_safe_json_without_map_decode_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..ITERATIONS {
        let bytes = serialize(items).expect("serialize local split items");
        assert!(bytes.len() > ITEM_COUNT * 512);
    }
    let report = guard.finish();

    let bytes = serialize(items).expect("serialize local split items");
    let decoded: serde_json::Value =
        serde_json::from_slice(&bytes).expect("decode local split items");
    assert_eq!(decoded[0]["pk"]["S"], "tenant#0000");
    assert_eq!(decoded[0]["payload"]["S"], "payload-0000");

    report
}

fn legacy_write_wire_item_array(items: &[WireItem]) -> serde_json::Result<Vec<u8>> {
    let mut out = Vec::new();
    out.push(b'[');
    for (index, item) in items.iter().cloned().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        let map = item
            .into_attribute_map()
            .map_err(crate::raw_dynamodb_response::storage_error_to_json_error)?;
        serde_json::to_writer(&mut out, &map)?;
    }
    out.push(b']');
    Ok(out)
}

fn local_split_items(duplicate_key_in_blob: bool) -> Vec<WireItem> {
    (0..ITEM_COUNT)
        .map(|index| local_split_item(index, duplicate_key_in_blob))
        .collect()
}

fn local_split_item(index: usize, duplicate_key_in_blob: bool) -> WireItem {
    let primary_key = WireItemKeyAttributes::new(
        "pk".to_string(),
        AttributeValue::S(format!("tenant#{index:04}")),
        Some("sk".to_string()),
        Some(AttributeValue::S(format!("item#{index:04}"))),
    );
    let blob = if duplicate_key_in_blob {
        format!(
            r#"{{"pk":{{"S":"blob-overrides-key"}},"payload":{{"S":"payload-{index:04}"}},"body":{{"S":"{}"}}}}"#,
            "x".repeat(768)
        )
    } else {
        format!(
            r#"{{"payload":{{"S":"payload-{index:04}"}},"body":{{"S":"{}"}},"counter":{{"N":"{}"}}}}"#,
            "x".repeat(768),
            index
        )
    };
    WireItem::local_split(primary_key, None, Some(blob.into_bytes()))
}
