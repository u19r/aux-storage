use std::hint::black_box;

use alloc_counter::AllocationGuard;
use storage_provider::ItemPrimaryEffects;
use storage_types::{AttributeValue, ItemProvenance, ItemRevision, KeyAttributes};

use crate::{
    SqlEngine, item_tombstone::sql_statements::tombstone_upsert_statement, test_support::table,
};

#[test]
fn given_tombstone_write_when_rendered_then_preserve_sql_and_bind_order() {
    let table = table();
    let key = KeyAttributes::from_iter([("pk".to_owned(), AttributeValue::S("key".to_owned()))]);
    let effects = ItemPrimaryEffects::for_delete(
        None,
        ItemRevision::try_from(7u64).unwrap(),
        ItemProvenance::default(),
    )
    .unwrap();
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        let (sql, binds) = tombstone_upsert_statement(engine, &table, &key, &effects)
            .unwrap()
            .into_parts();
        assert_eq!(
            sql,
            format!(
                "INSERT INTO item_tombstones (table_id, key_json, revision, provenance) VALUES \
                 ({prefix}1, {prefix}2, {prefix}3, {prefix}4) ON CONFLICT (table_id, key_json) DO \
                 UPDATE SET revision = excluded.revision, provenance = excluded.provenance"
            )
        );
        assert!(matches!(&binds[0],crate::SqlValue::Blob(value) if value == &[1;16]));
        assert!(
            matches!(&binds[1],crate::SqlValue::Text(value) if value == r#"{"pk":{"S":"key"}}"#)
        );
        assert!(matches!(&binds[2], crate::SqlValue::Integer(7)));
        assert!(
            matches!(&binds[3],crate::SqlValue::Blob(value) if value == ItemProvenance::LOCAL_BYTES)
        );
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_tombstone_write_when_constructed_then_report_cpu_and_allocations() {
    let table = table();
    let key = KeyAttributes::from_iter([("pk".to_owned(), AttributeValue::S("key".to_owned()))]);
    let effects = ItemPrimaryEffects::for_delete(
        None,
        ItemRevision::try_from(7u64).unwrap(),
        ItemProvenance::default(),
    )
    .unwrap();
    for (engine, name) in [
        (SqlEngine::Sqlite, "sqlite"),
        (SqlEngine::Turso, "turso"),
        (SqlEngine::Postgres, "postgres"),
    ] {
        for _ in 0..100 {
            drop(black_box(
                tombstone_upsert_statement(engine, &table, &key, &effects).unwrap(),
            ));
        }
        let guard = AllocationGuard::start(
            module_path!(),
            "tombstone_sql",
            file!(),
            line!(),
            Some(name),
        );
        let started = cpu_time::ProcessTime::now();
        for _ in 0..100_000 {
            drop(black_box(
                tombstone_upsert_statement(
                    black_box(engine),
                    black_box(&table),
                    black_box(&key),
                    black_box(&effects),
                )
                .unwrap(),
            ));
        }
        let cpu = started.elapsed();
        alloc_counter::emit_report(&guard.finish()).unwrap();
        eprintln!(
            "gsi_sql label={name}_tombstone iterations=100000 cpu_ns={}",
            cpu.as_nanos()
        );
    }
}

#[test]
fn given_stored_item_when_observed_for_write_then_image_and_ownership_metadata_are_preserved() {
    let image = format!(
        r#"{{"pk":{{"S":"key"}},"payload":{{"S":"{}"}}}}"#,
        "x".repeat(4096)
    );
    let row = crate::SqlRow::new(vec![
        crate::SqlValue::Text(image.clone()),
        crate::SqlValue::Text("[]".to_owned()),
        crate::SqlValue::Integer(7),
        crate::SqlValue::Blob(ItemProvenance::LOCAL_BYTES.to_vec()),
        crate::SqlValue::Integer(42),
    ]);
    let observation = crate::item_write::decode_live(
        row,
        storage_types::StoredWireItem {
            item: storage_types::WireItem::dynamo_json(image.as_bytes().to_vec()),
            indexers: Vec::new(),
            source: None,
        },
    )
    .unwrap();
    let storage_provider::ItemWriteObservation::Live {
        stored,
        revision,
        provenance,
        ttl_epoch,
    } = observation
    else {
        panic!("live item expected");
    };
    assert_eq!(stored.item.to_dynamo_json_string().unwrap(), image);
    assert!(stored.indexers.is_empty());
    assert_eq!(revision, ItemRevision::try_from(7_u64).unwrap());
    assert_eq!(provenance.as_bytes(), ItemProvenance::LOCAL_BYTES);
    assert_eq!(ttl_epoch, Some(42));
}

#[test]
fn given_invalid_stored_metadata_when_observed_for_write_then_physical_errors_are_rejected() {
    for row in [
        vec![],
        vec![
            crate::SqlValue::Text("{}".to_owned()),
            crate::SqlValue::Text("[]".to_owned()),
            crate::SqlValue::Integer(-1),
            crate::SqlValue::Blob(ItemProvenance::LOCAL_BYTES.to_vec()),
            crate::SqlValue::Null,
        ],
        vec![
            crate::SqlValue::Text("{}".to_owned()),
            crate::SqlValue::Text("[]".to_owned()),
            crate::SqlValue::Integer(1),
            crate::SqlValue::Blob(vec![]),
            crate::SqlValue::Null,
        ],
        vec![
            crate::SqlValue::Text("{}".to_owned()),
            crate::SqlValue::Text("[]".to_owned()),
            crate::SqlValue::Integer(1),
            crate::SqlValue::Blob(ItemProvenance::LOCAL_BYTES.to_vec()),
            crate::SqlValue::Text("invalid".to_owned()),
        ],
    ] {
        assert!(
            crate::item_write::decode_live(
                crate::SqlRow::new(row),
                storage_types::StoredWireItem {
                    item: storage_types::WireItem::dynamo_json(b"{}".to_vec()),
                    indexers: Vec::new(),
                    source: None
                }
            )
            .is_err()
        );
    }
}
