use std::{hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use serde::Serialize;
use storage_types::{AttributeMap, GetItemResponse, WireItem};

use crate::get_item_response::into_json_bytes;

const ITERATIONS: usize = 10_000;
const ITEM_JSON: &[u8] = br#"{"pk":{"S":"tenant#001"},"sk":{"S":"item#001"},"payload":{"S":"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},"counter":{"N":"42"},"active":{"BOOL":true}}"#;

#[test]
fn get_item_wire_serde_preserves_json_shape_and_rejects_non_objects() {
    let response = GetItemResponse {
        item: Some(WireItem::dynamo_json(ITEM_JSON.to_vec())),
    };
    let json = serde_json::to_vec(&response).unwrap();
    let actual: serde_json::Value = serde_json::from_slice(&json).unwrap();
    let expected: serde_json::Value = serde_json::from_slice(ITEM_JSON).unwrap();
    assert_eq!(actual["Item"], expected);
    let decoded: GetItemResponse = serde_json::from_slice(&json).unwrap();
    assert!(matches!(decoded.item, Some(WireItem::DynamoJson { .. })));
    assert!(serde_json::from_str::<GetItemResponse>(r#"{"Item":42}"#).is_err());
    assert!(serde_json::from_str::<GetItemResponse>(r#"{"Item":[]}"#).is_err());
}

#[derive(Clone, Serialize)]
struct MapResponse {
    #[serde(rename = "Item")]
    item: AttributeMap,
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_get_item_wire_image_when_cloned_or_generic_serialized_then_report_allocations_and_time() {
    let map = MapResponse {
        item: serde_json::from_slice(ITEM_JSON).unwrap(),
    };
    let wire = GetItemResponse {
        item: Some(WireItem::dynamo_json(ITEM_JSON.to_vec())),
    };
    for label in ["map_clone", "wire_clone", "map_serde", "wire_serde"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "given_get_item_wire_image_when_cloned_or_generic_serialized_then_report_allocations_and_time",
            file!(),
            line!(),
            Some(label),
        );
        let started = Instant::now();
        let mut bytes = 0_usize;
        for _ in 0..ITERATIONS {
            bytes += match label {
                "map_clone" => black_box(map.clone()).item.len(),
                "wire_clone" => black_box(wire.clone()).item.unwrap().payload_len(),
                "map_serde" => serde_json::to_vec(black_box(&map)).unwrap().len(),
                _ => serde_json::to_vec(black_box(&wire)).unwrap().len(),
            };
        }
        let elapsed = started.elapsed();
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert!(bytes > 0);
        println!(
            "get_item_generic label={label} iterations={ITERATIONS} elapsed_ns={}",
            elapsed.as_nanos()
        );
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_get_item_wire_image_when_serializing_response_then_report_allocations_and_time() {
    for label in ["attribute_map", "wire_item"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "given_get_item_wire_image_when_serializing_response_then_report_allocations_and_time",
            file!(),
            line!(),
            Some(label),
        );
        let mut bytes = 0_usize;
        for _ in 0..ITERATIONS {
            bytes += response_bytes(black_box(label)).len();
        }
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        assert!(bytes > 0);

        let started = Instant::now();
        for _ in 0..ITERATIONS {
            bytes += response_bytes(black_box(label)).len();
        }
        println!(
            "get_item_response label={label} iterations={ITERATIONS} elapsed_ns={}",
            started.elapsed().as_nanos()
        );
        black_box(bytes);
    }
}

fn response_bytes(label: &str) -> Vec<u8> {
    let wire_item = WireItem::dynamo_json(ITEM_JSON.to_vec());
    if label == "attribute_map" {
        let item = AttributeMap::try_from(wire_item).unwrap();
        serde_json::to_vec(&MapResponse { item }).unwrap()
    } else {
        into_json_bytes(GetItemResponse {
            item: Some(wire_item),
        })
        .unwrap()
    }
}
