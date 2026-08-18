use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard, OnceLock},
};

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeValue, ItemKey, StreamItemId, StreamName, TableName, TimestampMillis, WireItem,
};
use stream_provider::{StoredStreamPointer, StreamDataType};

use crate::{
    keyspace::{compact::TableStorageId, table_identity::TableIdentity},
    storage_provider::encode_wire_item_storage_bytes,
    stream::{
        helpers::{StreamEntryContext, create_item_update_stream_entries_wire_encoded},
        item_codec::encode_stored_stream_item_parts,
    },
};

const ITEM_COUNT: usize = 96;

fn alloc_suite_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    match LOCK.get_or_init(|| Mutex::new(())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn sample_item(index: usize) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S("ORG#ALLOC".to_string())),
        (
            "sk".to_string(),
            AttributeValue::S(format!("ITEM#{index:04}")),
        ),
        (
            "entity_type".to_string(),
            AttributeValue::S("ALLOC_PROFILE".to_string()),
        ),
        ("revision".to_string(), AttributeValue::N(index.to_string())),
        (
            "ttl".to_string(),
            AttributeValue::N((2_200_000_000_u64 + u64::try_from(index).unwrap_or(0)).to_string()),
        ),
        (
            "payload".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "status".to_string(),
                    AttributeValue::S("active".to_string()),
                ),
                (
                    "flags".to_string(),
                    AttributeValue::L(vec![
                        AttributeValue::S("stream".to_string()),
                        AttributeValue::S("ttl".to_string()),
                    ]),
                ),
            ])),
        ),
    ])
}

fn sample_items() -> Vec<HashMap<String, AttributeValue>> {
    (0..ITEM_COUNT).map(sample_item).collect()
}

fn sample_wire_items() -> Vec<WireItem> {
    sample_items()
        .into_iter()
        .map(|item| WireItem::from_attribute_map(&item).expect("wire item"))
        .collect()
}

fn item_key(table_name: &TableName, index: usize) -> ItemKey {
    ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("ORG#ALLOC".to_string()),
        Some(AttributeValue::S(format!("ITEM#{index:04}"))),
    )
}

fn table_identity(table_name: &TableName) -> TableIdentity {
    TableIdentity::new(TableStorageId::new(1), table_name.clone(), Vec::new())
}

fn measure_stream_envelope_wire_insert_baseline() -> alloc_counter::AllocationReport<'static> {
    let table_name = TableName::new("alloc_stream_wire_baseline");
    let wire_items = sample_wire_items();
    let encoded = wire_items
        .iter()
        .map(|item| {
            encode_wire_item_storage_bytes(
                crate::sorted_kv_store::ItemValueCodec::RocksDbEnvelope,
                item,
                None,
                storage_types::MaxIndexers::ZERO,
            )
            .expect("encode wire storage bytes")
        })
        .collect::<Vec<_>>();

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_stream_envelope_wire_insert_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    let table_identity = table_identity(&table_name);
    for (index, item_bytes) in encoded.iter().enumerate() {
        let key = item_key(&table_name, index);
        let _entries = create_item_update_stream_entries_wire_encoded(
            StreamEntryContext {
                table_identity: &table_identity,
                table_name: &table_name,
                item_key: &key,
                indexers: &[],
                old_indexers: None,
            },
            item_bytes.as_slice(),
            None,
            StreamItemId::random(),
            false,
            None,
        )
        .expect("create wire stream envelope entries");
    }
    guard.finish()
}

fn measure_stream_envelope_wire_update_embedded() -> alloc_counter::AllocationReport<'static> {
    let table_name = TableName::new("alloc_stream_wire_update");
    let wire_items = sample_wire_items();
    let encoded = wire_items
        .iter()
        .map(|item| {
            encode_wire_item_storage_bytes(
                crate::sorted_kv_store::ItemValueCodec::RocksDbEnvelope,
                item,
                None,
                storage_types::MaxIndexers::ZERO,
            )
            .expect("encode wire storage bytes")
        })
        .collect::<Vec<_>>();

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_stream_envelope_wire_update_embedded",
        file!(),
        line!(),
        Some("component"),
    );
    let table_identity = table_identity(&table_name);
    for (index, item_bytes) in encoded.iter().enumerate() {
        let key = item_key(&table_name, index);
        let _entries = create_item_update_stream_entries_wire_encoded(
            StreamEntryContext {
                table_identity: &table_identity,
                table_name: &table_name,
                item_key: &key,
                indexers: &[],
                old_indexers: None,
            },
            item_bytes.as_slice(),
            Some(item_bytes.as_slice()),
            StreamItemId::random(),
            false,
            None,
        )
        .expect("create wire update stream envelope entries");
    }
    guard.finish()
}

fn measure_stream_envelope_pointer_payload_encode_stage() -> alloc_counter::AllocationReport<'static>
{
    let table_name = TableName::new("alloc_stream_pointer_payload");
    let guard = AllocationGuard::start(
        module_path!(),
        "kv_stream_envelope_pointer_payload_encode",
        file!(),
        line!(),
        Some("component"),
    );
    for index in 0..ITEM_COUNT {
        let key = item_key(&table_name, index);
        let item_stream = StreamName::table_item_stream(&table_name, &key).expect("item stream");
        let pointer = StoredStreamPointer::pointer(
            item_stream,
            table_name.clone(),
            storage_types::ItemStreamVersion::new(1),
        );
        let _pointer_payload = storage_types::storage_serde::to_bytes(&pointer)
            .expect("encode stored pointer payload");
    }
    guard.finish()
}

fn measure_stream_envelope_outer_record_encode_stage() -> alloc_counter::AllocationReport<'static> {
    let table_name = TableName::new("alloc_stream_outer_record");
    let created_at = TimestampMillis::now();
    let pointer_payloads = (0..ITEM_COUNT)
        .map(|index| {
            let key = item_key(&table_name, index);
            let item_stream =
                StreamName::table_item_stream(&table_name, &key).expect("item stream");
            let pointer = StoredStreamPointer::pointer(
                item_stream,
                table_name.clone(),
                storage_types::ItemStreamVersion::new(1),
            );
            storage_types::storage_serde::to_bytes(&pointer).expect("encode stored pointer payload")
        })
        .collect::<Vec<_>>();

    let guard = AllocationGuard::start(
        module_path!(),
        "kv_stream_envelope_outer_record_encode",
        file!(),
        line!(),
        Some("component"),
    );
    for payload in &pointer_payloads {
        let _bytes = encode_stored_stream_item_parts(
            None,
            payload.as_slice(),
            StreamDataType::StreamPointer,
            created_at,
        )
        .expect("encode outer stream record");
    }
    guard.finish()
}

#[test]
fn kv_stream_envelope_insert_allocation_baseline_tests() {
    // Snapshot (2026-02-18, `cargo test -p kv stream_helpers_alloc_tests --
    // --nocapture`): allocation_count=3841, allocated_bytes=1133005.
    let _suite_lock = alloc_suite_lock();
    let report = measure_stream_envelope_wire_insert_baseline();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[test]
fn kv_stream_envelope_insert_vs_update_component_allocation_profile_tests() {
    // Snapshot (2026-02-18, `cargo test -p kv stream_helpers_alloc_tests --
    // --nocapture`): insert: allocation_count=3840, allocated_bytes=1132990
    // update_embedded: allocation_count=4320, allocated_bytes=1722566
    let _suite_lock = alloc_suite_lock();
    let insert = measure_stream_envelope_wire_insert_baseline();
    let update = measure_stream_envelope_wire_update_embedded();

    alloc_counter::emit_report(&insert);
    alloc_counter::emit_report(&update);

    assert!(insert.allocation_count > 0);
    assert!(update.allocation_count > 0);
    assert!(insert.allocated_bytes > 0);
    assert!(update.allocated_bytes > 0);
}

#[test]
fn kv_stream_envelope_serialization_component_breakdown_tests() {
    // Snapshot (2026-02-18, `cargo test -p kv stream_helpers_alloc_tests --
    // --nocapture`): pointer_payload: allocation_count=1829,
    // allocated_bytes=965098 outer_record: allocation_count=96,
    // allocated_bytes=23051
    let _suite_lock = alloc_suite_lock();
    let pointer_payload = measure_stream_envelope_pointer_payload_encode_stage();
    let outer_record = measure_stream_envelope_outer_record_encode_stage();

    alloc_counter::emit_report(&pointer_payload);
    alloc_counter::emit_report(&outer_record);

    assert!(pointer_payload.allocation_count > 0);
    assert!(outer_record.allocation_count > 0);
    assert!(pointer_payload.allocated_bytes > 0);
    assert!(outer_record.allocated_bytes > 0);
}
