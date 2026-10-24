use std::{borrow::Cow, hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use serde::{Deserialize, Serialize};
use storage_derive::WireItemEncode;

use crate::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeySchemaElement, KeyType, StorageError,
    StorageResult, StoredTableInfo, TableName, TableStatus, TimestampMillis, TryFromWireItem,
    TryIntoWireItem, WireItem, WireItemKeyAttributes, constants_tests::WIRE_ITEM_ALLOC_ITERATIONS,
    to_hashmap,
};

fn legacy_timestamp_alias(field: &str) -> Option<&'static str> {
    match field {
        "created_at" => Some("c_at"),
        "c_at" => Some("created_at"),
        "updated_at" => Some("u_at"),
        "u_at" => Some("updated_at"),
        "expires_at" => Some("e_at"),
        "e_at" => Some("expires_at"),
        _ => None,
    }
}

fn legacy_scalar_fields<'a>(
    bytes: &'a [u8],
    fields: &[&str],
) -> StorageResult<Vec<Option<Cow<'a, str>>>> {
    let mut values = crate::wire_item::projection::parse_dynamo_scalar_fields(bytes, fields)?;
    for (index, field) in fields.iter().enumerate() {
        if values[index].is_none()
            && let Some(alias) = legacy_timestamp_alias(field)
        {
            let alias_value =
                crate::wire_item::projection::parse_dynamo_scalar_fields(bytes, &[alias])?;
            values[index] = alias_value.into_iter().next().flatten();
        }
    }
    Ok(values)
}

fn aliased_projection_fixture() -> (WireItem, Vec<u8>, [&'static str; 4]) {
    let bytes = br#"{
        "created_at":{"N":"100"},
        "c_at":{"N":"200"},
        "updated_at":{"N":"300"},
        "e_at":{"N":"600"},
        "unrelated":{"S":"ignored"}
    }"#
    .to_vec();
    (
        WireItem::dynamo_json(bytes.clone()),
        bytes,
        ["created_at", "created_at", "updated_at", "expires_at"],
    )
}

fn measure_aliased_projection(
    label: &'static str,
    baseline: bool,
) -> alloc_counter::AllocationReport<'static> {
    let (wire_item, bytes, fields) = aliased_projection_fixture();
    let guard = AllocationGuard::start(
        module_path!(),
        "wire_item_aliased_scalar_projection",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..WIRE_ITEM_ALLOC_ITERATIONS {
        let values = if baseline {
            legacy_scalar_fields(bytes.as_slice(), &fields)
        } else {
            wire_item.scalar_attributes(&fields)
        }
        .unwrap();
        assert_eq!(values[0].as_deref(), Some("100"));
        assert_eq!(values[1].as_deref(), Some("200"));
        assert_eq!(values[2].as_deref(), Some("300"));
        assert_eq!(values[3].as_deref(), Some("600"));
    }
    guard.finish()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct NonKeyPayload {
    status: String,
    table_id: String,
    revision: u64,
    attempts: u32,
    enabled: bool,
    note: String,
}

fn sample_local_split_parts() -> (WireItemKeyAttributes, Vec<u8>) {
    let key_attributes = WireItemKeyAttributes::new(
        "pk".to_string(),
        AttributeValue::S("org#o_123".to_string()),
        Some("sk".to_string()),
        Some(AttributeValue::S("user#u_456".to_string())),
    );
    let non_key_payload = NonKeyPayload {
        status: "active".to_string(),
        table_id: "t_789".to_string(),
        revision: 42,
        attempts: 3,
        enabled: true,
        note: "no eager parse on read".to_string(),
    };
    let non_key_attributes = match to_hashmap(&non_key_payload) {
        Ok(map) => map,
        Err(err) => panic!("serialize non-key payload to attribute map: {err}"),
    };
    let blob = match serde_json::to_vec(&non_key_attributes) {
        Ok(blob) => blob,
        Err(err) => panic!("serialize non-key attributes to blob: {err}"),
    };
    (key_attributes, blob)
}

fn measure_lazy_local_split_baseline() -> alloc_counter::AllocationReport<'static> {
    let (key_attributes, blob) = sample_local_split_parts();
    let guard = AllocationGuard::start(
        module_path!(),
        "wire_item_local_split_lazy_read_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..WIRE_ITEM_ALLOC_ITERATIONS {
        let wire_item = WireItem::local_split(key_attributes.clone(), None, Some(blob.clone()));
        assert!(matches!(wire_item, WireItem::LocalSplit { .. }));
        assert!(wire_item.payload_len() > 0);
    }
    guard.finish()
}

fn sample_table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("alloc_wire_item"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        max_indexers: crate::MaxIndexers::ZERO,
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: crate::StreamRetentionDuration::default(),
        default_item_stream_duration: crate::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

fn sample_dynamo_wire_item() -> WireItem {
    let item = std::collections::HashMap::from([
        ("pk".to_string(), AttributeValue::S("ORG#001".to_string())),
        ("sk".to_string(), AttributeValue::S("USER#abc".to_string())),
        (
            "status".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        ("attempts".to_string(), AttributeValue::N("7".to_string())),
        ("enabled".to_string(), AttributeValue::BOOL(true)),
        (
            "metadata".to_string(),
            AttributeValue::M(std::collections::HashMap::from([
                ("source".to_string(), AttributeValue::S("alloc".to_string())),
                (
                    "tags".to_string(),
                    AttributeValue::L(vec![
                        AttributeValue::S("alpha".to_string()),
                        AttributeValue::S("beta".to_string()),
                    ]),
                ),
            ])),
        ),
    ]);
    let bytes = match serde_json::to_vec(&item) {
        Ok(bytes) => bytes,
        Err(err) => panic!("serialize dynamo wire test item: {err}"),
    };
    WireItem::dynamo_json(bytes)
}

fn sample_dynamo_wire_item_with_attribute_count(count: usize) -> WireItem {
    let item = (0..count)
        .map(|index| {
            (
                format!("attribute-{index}"),
                AttributeValue::S(format!("value-{index}")),
            )
        })
        .collect::<std::collections::HashMap<_, _>>();
    WireItem::dynamo_json(serde_json::to_vec(&item).unwrap())
}

fn legacy_dynamo_json_to_attribute_map(item: &WireItem) -> StorageResult<crate::AttributeMap> {
    item.clone().into_attribute_map()
}

fn measure_dynamo_json_map_decode(
    label: &'static str,
    item: &WireItem,
    decode: fn(&WireItem) -> StorageResult<crate::AttributeMap>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "wire_item_dynamo_json_map_decode",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..WIRE_ITEM_ALLOC_ITERATIONS {
        let decoded = decode(item).unwrap();
        assert_eq!(decoded.len(), 100);
        std::hint::black_box(decoded);
    }
    guard.finish()
}

fn measure_last_evaluated_key_projection_baseline() -> alloc_counter::AllocationReport<'static> {
    let table_info = sample_table_info();
    let wire_item = sample_dynamo_wire_item();
    let guard = AllocationGuard::start(
        module_path!(),
        "wire_item_last_evaluated_key_projection_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..WIRE_ITEM_ALLOC_ITERATIONS {
        let lek = match wire_item.last_evaluated_key(&table_info, &None) {
            Ok(value) => value,
            Err(err) => panic!("build projected last evaluated key: {err}"),
        };
        assert!(lek.is_some());
    }
    guard.finish()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BundleManifestViewModel {
    manifest_json: String,
    base_schema_json: String,
}

impl TryFromWireItem for BundleManifestViewModel {
    fn try_from_wire_item(item: &WireItem) -> StorageResult<Self> {
        let values = item.scalar_attributes(&["manifest_json", "base_schema_json"])?;
        let manifest_json = values
            .first()
            .and_then(|value| value.as_ref())
            .map(|value| value.to_string())
            .ok_or_else(|| crate::StorageError::internal(&"missing manifest_json"))?;
        let base_schema_json = values
            .get(1)
            .and_then(|value| value.as_ref())
            .map(|value| value.to_string())
            .ok_or_else(|| crate::StorageError::internal(&"missing base_schema_json"))?;
        Ok(Self {
            manifest_json,
            base_schema_json,
        })
    }
}

fn sample_manifest_wire_item() -> WireItem {
    let item = std::collections::HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("ZB#V#0000000042".to_string()),
        ),
        ("sk".to_string(), AttributeValue::S("M".to_string())),
        (
            "manifest_json".to_string(),
            AttributeValue::S("{\"version\":42}".to_string()),
        ),
        (
            "base_schema_json".to_string(),
            AttributeValue::S("{\"entities\":{}}".to_string()),
        ),
        (
            "compile_time_ms".to_string(),
            AttributeValue::N("19".to_string()),
        ),
        (
            "compiled_at".to_string(),
            AttributeValue::S("2026-02-15T00:00:00Z".to_string()),
        ),
    ]);
    let bytes = match serde_json::to_vec(&item) {
        Ok(bytes) => bytes,
        Err(err) => panic!("serialize manifest wire test item: {err}"),
    };
    WireItem::dynamo_json(bytes)
}

fn measure_manifest_view_decode_baseline() -> alloc_counter::AllocationReport<'static> {
    let wire_item = sample_manifest_wire_item();
    let guard = AllocationGuard::start(
        module_path!(),
        "wire_item_manifest_view_decode_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..WIRE_ITEM_ALLOC_ITERATIONS {
        let view = match wire_item.try_decode::<BundleManifestViewModel>() {
            Ok(view) => view,
            Err(err) => panic!("decode manifest view model: {err}"),
        };
        assert!(!view.manifest_json.is_empty());
        assert!(!view.base_schema_json.is_empty());
    }
    guard.finish()
}

#[test]
fn lazy_local_split_read_allocation_baseline_tests() {
    // Snapshot (2026-02-18, `cargo test -p storage-types wire_item_alloc_tests
    // -- --nocapture`): allocation_count=5127, allocated_bytes=184229.
    let report = measure_lazy_local_split_baseline();
    let _ = alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[test]
fn wire_item_last_evaluated_key_projection_allocation_baseline_tests() {
    // Snapshot (2026-02-18, `cargo test -p storage-types wire_item_alloc_tests
    // -- --nocapture`): allocation_count=9223, allocated_bytes=192394.
    let report = measure_last_evaluated_key_projection_baseline();
    let _ = alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[test]
fn wire_item_manifest_view_decode_allocation_baseline_tests() {
    // Snapshot (2026-02-18, `cargo test -p storage-types wire_item_alloc_tests
    // -- --nocapture`): allocation_count=7176, allocated_bytes=143805.
    let report = measure_manifest_view_decode_baseline();
    let _ = alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, WireItemEncode)]
struct WriteWireFixture {
    pk: String,
    sk: String,
    entity_type: String,
    status: String,
    revision: u64,
    active: bool,
}

fn sample_write_wire_fixture() -> WriteWireFixture {
    WriteWireFixture {
        pk: "ORG#001".to_string(),
        sk: "USER#abc".to_string(),
        entity_type: "FIXTURE".to_string(),
        status: "active".to_string(),
        revision: 7,
        active: true,
    }
}

fn measure_write_wire_encode_baseline() -> alloc_counter::AllocationReport<'static> {
    let fixture = sample_write_wire_fixture();
    let guard = AllocationGuard::start(
        module_path!(),
        "wire_item_write_encode_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..WIRE_ITEM_ALLOC_ITERATIONS {
        let wire = match fixture.try_into_wire_item() {
            Ok(item) => item,
            Err(err) => panic!("encode fixture wire item: {err}"),
        };
        assert!(wire.payload_len() > 0);
    }
    guard.finish()
}

fn realistic_local_split_wire_item() -> WireItem {
    let key_attributes = WireItemKeyAttributes::new(
        "pk".to_string(),
        AttributeValue::S("tenant#00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001".to_string()),
        Some("sk".to_string()),
        Some(AttributeValue::S(
            "item#0001#sort-key-component-with-realistic-dynamodb-length-000000000000000000000000000000".to_string(),
        )),
    );
    let mut non_keys = std::collections::HashMap::with_capacity(8);
    non_keys.insert(
        "ttl".to_string(),
        AttributeValue::N("2200000000".to_string()),
    );
    non_keys.insert(
        "status".to_string(),
        AttributeValue::S("active".to_string()),
    );
    non_keys.insert("attempts".to_string(), AttributeValue::N("7".to_string()));
    non_keys.insert("payload".to_string(), AttributeValue::S("x".repeat(1_100)));
    non_keys.insert(
        "category".to_string(),
        AttributeValue::S("category#1".to_string()),
    );
    non_keys.insert(
        "owner".to_string(),
        AttributeValue::S("owner#2".to_string()),
    );
    non_keys.insert(
        "gsi0pk".to_string(),
        AttributeValue::S(format!("gsi0#partition#{:092}", 1)),
    );
    non_keys.insert(
        "gsi0sk".to_string(),
        AttributeValue::S(format!("gsi0#sort#{:092}", 1)),
    );
    let blob = serde_json::to_vec(&non_keys).unwrap();
    WireItem::local_split(key_attributes, None, Some(blob))
}

#[test]
fn borrowed_local_split_map_matches_owned_decode_and_keeps_corruption_errors() {
    let item = realistic_local_split_wire_item();
    let borrowed = item.to_attribute_map().unwrap();
    let owned = item.clone().into_attribute_map().unwrap();
    assert_eq!(borrowed, owned);

    let primary = WireItemKeyAttributes::new(
        "pk".to_string(),
        AttributeValue::S("primary".to_string()),
        None,
        None,
    );
    let secondary = WireItemKeyAttributes::new(
        "sk".to_string(),
        AttributeValue::S("secondary".to_string()),
        None,
        None,
    );
    for blob in [
        b"{}".to_vec(),
        br#"{"pk":{"S":"residual"},"payload":{"N":"42"}}"#.to_vec(),
    ] {
        let item = WireItem::local_split(primary.clone(), Some(secondary.clone()), Some(blob));
        assert_eq!(
            item.to_attribute_map().unwrap(),
            item.into_attribute_map().unwrap(),
        );
    }

    let WireItem::LocalSplit {
        primary_key,
        secondary_key,
        ..
    } = item
    else {
        panic!("expected LocalSplit fixture");
    };
    let invalid = WireItem::local_split(primary_key, secondary_key, Some(b"{".to_vec()));
    assert_eq!(
        invalid.to_attribute_map().unwrap_err().to_string(),
        invalid.into_attribute_map().unwrap_err().to_string(),
    );
}

fn legacy_local_split_into_attribute_map(item: WireItem) -> StorageResult<crate::AttributeMap> {
    match item {
        WireItem::DynamoJson { data } => serde_json::from_slice::<
            std::collections::HashMap<String, AttributeValue>,
        >(data.as_slice())
        .map(crate::AttributeMap::from)
        .map_err(|err| {
            StorageError::internal(&format!("decode wire dynamo json item into map: {err}"))
        }),
        WireItem::LocalSplit {
            primary_key,
            secondary_key,
            non_key_attributes_blob,
        } => {
            let mut attributes = std::collections::HashMap::with_capacity(4);
            attributes.insert(primary_key.hash_key_name.into_owned(), primary_key.hash_key);
            if let (Some(name), Some(value)) = (primary_key.sort_key_name, primary_key.sort_key) {
                attributes.insert(name.into_owned(), value);
            }
            if let Some(secondary_key) = secondary_key {
                attributes.insert(
                    secondary_key.hash_key_name.into_owned(),
                    secondary_key.hash_key,
                );
                if let (Some(name), Some(value)) =
                    (secondary_key.sort_key_name, secondary_key.sort_key)
                {
                    attributes.insert(name.into_owned(), value);
                }
            }
            if let Some(blob) = non_key_attributes_blob
                && !blob.is_empty()
            {
                let non_keys = serde_json::from_slice::<
                    std::collections::HashMap<String, AttributeValue>,
                >(blob.as_slice())
                .map_err(|err| {
                    StorageError::internal(&format!(
                        "decode sqlite non-key attributes blob into map: {err}"
                    ))
                })?;
                attributes.extend(non_keys);
            }
            Ok(attributes.into())
        }
    }
}

fn measure_local_split_map_decode(
    label: &'static str,
    decode: impl Fn(WireItem) -> StorageResult<crate::AttributeMap>,
) -> alloc_counter::AllocationReport<'static> {
    let wire_item = realistic_local_split_wire_item();
    let guard = AllocationGuard::start(
        module_path!(),
        "wire_item_realistic_local_split_map_decode",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..WIRE_ITEM_ALLOC_ITERATIONS {
        let map = decode(wire_item.clone()).unwrap();
        assert_eq!(map.len(), 10);
    }
    guard.finish()
}

#[test]
fn wire_item_realistic_local_split_map_decode_allocations_drop_tests() {
    let legacy = measure_local_split_map_decode(
        "wire_item_realistic_local_split_decode_legacy_capacity",
        legacy_local_split_into_attribute_map,
    );
    let optimized = measure_local_split_map_decode(
        "wire_item_realistic_local_split_decode_sized_capacity",
        WireItem::into_attribute_map,
    );

    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&optimized);

    assert!(optimized.allocated_bytes < legacy.allocated_bytes);
}

#[test]
fn wire_item_dynamo_json_map_decode_preserves_shapes_errors_and_avoids_payload_clone() {
    for count in [1, 10, 100] {
        let item = sample_dynamo_wire_item_with_attribute_count(count);
        assert_eq!(item.to_attribute_map().unwrap().len(), count);
        assert_eq!(
            legacy_dynamo_json_to_attribute_map(&item).unwrap().len(),
            count
        );
    }

    let malformed = WireItem::dynamo_json(br#"{"#.to_vec());
    assert_eq!(
        malformed.to_attribute_map().unwrap_err().to_string(),
        legacy_dynamo_json_to_attribute_map(&malformed)
            .unwrap_err()
            .to_string()
    );

    let item = sample_dynamo_wire_item_with_attribute_count(100);
    let legacy = measure_dynamo_json_map_decode(
        "legacy_clone_payload",
        &item,
        legacy_dynamo_json_to_attribute_map,
    );
    let candidate =
        measure_dynamo_json_map_decode("borrowed_payload", &item, WireItem::to_attribute_map);
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(candidate.allocation_count < legacy.allocation_count);
    assert!(candidate.allocated_bytes < legacy.allocated_bytes);
}

#[test]
fn wire_item_write_encode_allocation_baseline_tests() {
    // Snapshot (2026-02-18, `cargo test -p storage-types wire_item_alloc_tests
    // -- --nocapture`): allocation_count=11313, allocated_bytes=557993.
    let report = measure_write_wire_encode_baseline();
    let _ = alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[test]
#[ignore = "manual differential allocation probe"]
fn aliased_scalar_projection_allocations_drop_tests() {
    let legacy = measure_aliased_projection("legacy_reparse", true);
    let optimized = measure_aliased_projection("single_scan", false);

    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&optimized);
    assert!(optimized.allocation_count < legacy.allocation_count);
    assert!(optimized.allocated_bytes < legacy.allocated_bytes);
}

#[test]
#[ignore = "manual differential CPU probe"]
fn aliased_scalar_projection_cpu_probe_tests() {
    let (wire_item, bytes, fields) = aliased_projection_fixture();
    let start = Instant::now();
    let mut legacy_checksum = 0usize;
    for _ in 0..WIRE_ITEM_ALLOC_ITERATIONS {
        let values = legacy_scalar_fields(bytes.as_slice(), &fields).unwrap();
        legacy_checksum = legacy_checksum.wrapping_add(black_box(values.len()));
    }
    let legacy_ns = start.elapsed().as_nanos();

    let start = Instant::now();
    let mut optimized_checksum = 0usize;
    for _ in 0..WIRE_ITEM_ALLOC_ITERATIONS {
        let values = wire_item.scalar_attributes(&fields).unwrap();
        optimized_checksum = optimized_checksum.wrapping_add(black_box(values.len()));
    }
    let optimized_ns = start.elapsed().as_nanos();

    println!(
        "aliased_scalar_projection_cpu legacy_ns={legacy_ns} optimized_ns={optimized_ns} \
         iterations={WIRE_ITEM_ALLOC_ITERATIONS} checksum={legacy_checksum}/{optimized_checksum}"
    );
    assert_eq!(legacy_checksum, optimized_checksum);
}

#[test]
#[ignore = "manual differential allocation and CPU probe"]
fn given_mixed_wire_items_when_compared_then_borrowed_values_reduce_allocations() {
    let (keys, blob) = sample_local_split_parts();
    let left = WireItem::local_split(keys, None, Some(blob));
    let right = WireItem::dynamo_json(left.dynamo_json_bytes().unwrap().into_owned());
    for label in ["attribute_maps", "borrowed_attributes"] {
        let guard = AllocationGuard::start(
            module_path!(),
            "wire_item_comparison",
            file!(),
            line!(),
            Some(label),
        );
        let start = Instant::now();
        for _ in 0..WIRE_ITEM_ALLOC_ITERATIONS {
            let equal = if label == "attribute_maps" {
                let left = black_box(&left).to_attribute_map().unwrap();
                let right = black_box(&right).to_attribute_map().unwrap();
                left.len() == right.len()
                    && left
                        .iter()
                        .all(|(name, value)| right.get(name) == Some(value))
            } else {
                black_box(&left)
                    .has_same_attributes(black_box(&right))
                    .unwrap()
            };
            assert!(black_box(equal));
        }
        let elapsed = start.elapsed();
        let report = guard.finish();
        println!(
            "{label}: allocations={} bytes={} elapsed_ns={} \
             iterations={WIRE_ITEM_ALLOC_ITERATIONS}",
            report.allocation_count,
            report.allocated_bytes,
            elapsed.as_nanos(),
        );
        let _ = alloc_counter::emit_report(&report);
    }
}
