use std::{collections::HashMap, time::Instant};

use serde::Deserialize;

use crate::{
    AttributeMap, AttributeValue, BatchGetItemResponse, GetItemResponse, QueryResponse, TableName,
};

const ITERATIONS: usize = 200_000;

fn get_item_response_json() -> Vec<u8> {
    br#"{
        "Item": {
            "pk": {"S": "tenant#001"},
            "sk": {"S": "item#000000000001"},
            "payload": {"S": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},
            "counter": {"N": "42"},
            "active": {"BOOL": true},
            "gsi1pk": {"S": "group#001"},
            "gsi1sk": {"S": "sort#000000000001"}
        }
    }"#
    .to_vec()
}

fn query_response_json() -> Vec<u8> {
    br#"{
        "Items": [
            {
                "pk": {"S": "tenant#001"},
                "sk": {"S": "item#000000000001"},
                "payload": {"S": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},
                "counter": {"N": "42"},
                "active": {"BOOL": true},
                "gsi1pk": {"S": "group#001"},
                "gsi1sk": {"S": "sort#000000000001"}
            },
            {
                "pk": {"S": "tenant#001"},
                "sk": {"S": "item#000000000002"},
                "payload": {"S": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},
                "counter": {"N": "43"},
                "active": {"BOOL": true},
                "gsi1pk": {"S": "group#001"},
                "gsi1sk": {"S": "sort#000000000002"}
            }
        ],
        "Count": 2,
        "ScannedCount": 2
    }"#
    .to_vec()
}

fn batch_get_item_response_json() -> Vec<u8> {
    br#"{
        "Responses": {
            "bench-table": [
                {
                    "pk": {"S": "tenant#001"},
                    "sk": {"S": "item#000000000001"},
                    "payload": {"S": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},
                    "counter": {"N": "42"},
                    "active": {"BOOL": true},
                    "gsi1pk": {"S": "group#001"},
                    "gsi1sk": {"S": "sort#000000000001"}
                },
                {
                    "pk": {"S": "tenant#001"},
                    "sk": {"S": "item#000000000002"},
                    "payload": {"S": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},
                    "counter": {"N": "43"},
                    "active": {"BOOL": true},
                    "gsi1pk": {"S": "group#001"},
                    "gsi1sk": {"S": "sort#000000000002"}
                }
            ]
        }
    }"#
    .to_vec()
}

fn attributes_response_json() -> Vec<u8> {
    br#"{
        "Attributes": {
            "pk": {"S": "tenant#001"},
            "sk": {"S": "item#000000000001"},
            "payload": {"S": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"},
            "counter": {"N": "42"},
            "active": {"BOOL": true},
            "gsi1pk": {"S": "group#001"},
            "gsi1sk": {"S": "sort#000000000001"}
        }
    }"#
    .to_vec()
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyGetItemResponse {
    item: Option<HashMap<String, AttributeValue>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyQueryResponse {
    items: Option<Vec<HashMap<String, AttributeValue>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyBatchGetItemResponse {
    responses: Option<HashMap<TableName, Vec<HashMap<String, AttributeValue>>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LegacyAttributesResponse {
    attributes: Option<HashMap<String, AttributeValue>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AttributeMapAttributesResponse {
    attributes: Option<AttributeMap>,
}

fn measure<T>(label: &str, body: &[u8], iterations: usize, count: impl Fn(&T) -> usize) -> f64
where T: for<'de> Deserialize<'de> {
    let start = Instant::now();
    let mut decoded_items = 0usize;

    for _ in 0..iterations {
        let response: T = serde_json::from_slice(body).expect("deserialize");
        decoded_items += count(&response);
    }

    let elapsed = start.elapsed();
    let ns_per_iter = elapsed.as_nanos() as f64 / iterations as f64;
    println!(
        "{label} iterations={iterations} decoded_items={decoded_items} elapsed_ms={} \
         ns_per_iter={ns_per_iter:.1}",
        elapsed.as_millis()
    );
    ns_per_iter
}

#[test]
#[ignore = "manual perf probe; run with --nocapture before/after response map changes"]
fn get_item_response_deserialization_perf_probe() {
    let body = get_item_response_json();
    measure::<GetItemResponse>("get_item_attribute_map", &body, ITERATIONS, |response| {
        response.item.as_ref().map_or(0, AttributeMap::len)
    });
}

#[test]
#[ignore = "manual perf probe; compares legacy HashMap and AttributeMap response shapes"]
fn read_response_deserialization_perf_comparison() {
    let get_item = get_item_response_json();
    let query = query_response_json();
    let batch_get = batch_get_item_response_json();
    let attributes = attributes_response_json();

    let legacy_get =
        measure::<LegacyGetItemResponse>("legacy_get_item_hashmap", &get_item, ITERATIONS, |r| {
            r.item.as_ref().map_or(0, HashMap::len)
        });
    let current_get = measure::<GetItemResponse>(
        "current_get_item_attribute_map",
        &get_item,
        ITERATIONS,
        |r| r.item.as_ref().map_or(0, AttributeMap::len),
    );

    let legacy_query =
        measure::<LegacyQueryResponse>("legacy_query_hashmap", &query, ITERATIONS, |r| {
            r.items
                .as_ref()
                .map_or(0, |items| items.iter().map(HashMap::len).sum())
        });
    let current_query =
        measure::<QueryResponse>("current_query_attribute_map", &query, ITERATIONS, |r| {
            r.items
                .as_ref()
                .map_or(0, |items| items.iter().map(AttributeMap::len).sum())
        });

    let legacy_batch = measure::<LegacyBatchGetItemResponse>(
        "legacy_batch_get_hashmap",
        &batch_get,
        ITERATIONS,
        |r| {
            r.responses.as_ref().map_or(0, |tables| {
                tables
                    .values()
                    .flat_map(|items| items.iter())
                    .map(HashMap::len)
                    .sum()
            })
        },
    );
    let current_batch = measure::<BatchGetItemResponse>(
        "current_batch_get_attribute_map",
        &batch_get,
        ITERATIONS,
        |r| {
            r.responses.as_ref().map_or(0, |tables| {
                tables
                    .values()
                    .flat_map(|items| items.iter())
                    .map(AttributeMap::len)
                    .sum()
            })
        },
    );

    let legacy_attributes = measure::<LegacyAttributesResponse>(
        "legacy_attributes_hashmap",
        &attributes,
        ITERATIONS,
        |r| r.attributes.as_ref().map_or(0, HashMap::len),
    );
    let current_attributes = measure::<AttributeMapAttributesResponse>(
        "current_attributes_attribute_map",
        &attributes,
        ITERATIONS,
        |r| r.attributes.as_ref().map_or(0, AttributeMap::len),
    );

    println!(
        "speedup get_item={:.1}% query={:.1}% batch_get={:.1}% attributes={:.1}%",
        (legacy_get - current_get) / legacy_get * 100.0,
        (legacy_query - current_query) / legacy_query * 100.0,
        (legacy_batch - current_batch) / legacy_batch * 100.0,
        (legacy_attributes - current_attributes) / legacy_attributes * 100.0,
    );
}

#[test]
fn attribute_map_preserves_dynamodb_json_map_shape() {
    let mut item = AttributeMap::new();
    item.insert("pk", AttributeValue::S("tenant#001".to_string()));
    item.insert("counter", AttributeValue::N("42".to_string()));

    let encoded = serde_json::to_value(&item).expect("serialize");
    assert_eq!(
        encoded,
        serde_json::json!({
            "pk": {"S": "tenant#001"},
            "counter": {"N": "42"}
        })
    );

    let decoded: AttributeMap = serde_json::from_value(encoded).expect("deserialize");
    assert_eq!(
        decoded.get("pk"),
        Some(&AttributeValue::S("tenant#001".to_string()))
    );
    assert_eq!(decoded.len(), 2);
}
