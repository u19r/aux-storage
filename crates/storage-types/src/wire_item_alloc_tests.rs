use alloc_counter::AllocationGuard;
use serde::{Deserialize, Serialize};
use storage_derive::WireItemEncode;

use crate::{
    AttributeDefinition, AttributeMap, AttributeValue, KeyAttributeType, KeySchemaElement, KeyType,
    StorageResult, StoredTableInfo, TableName, TableStatus, TimestampMillis, TryFromWireItem,
    TryIntoWireItem, WireItem, constants_tests::WIRE_ITEM_ALLOC_ITERATIONS,
};

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
        stream_enabled_at: None,
        table_stream_duration: crate::StreamRetentionDuration::default(),
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
            AttributeValue::M(AttributeMap::from(std::collections::HashMap::from([
                ("source".to_string(), AttributeValue::S("alloc".to_string())),
                (
                    "tags".to_string(),
                    AttributeValue::L(vec![
                        AttributeValue::S("alpha".to_string()),
                        AttributeValue::S("beta".to_string()),
                    ]),
                ),
            ]))),
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
