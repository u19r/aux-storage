use std::{hint::black_box, sync::Arc};

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeDefinition, AttributeValue, BoundTableInfo, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType,
};

use crate::{
    SqlEngine,
    item_read::{point_read_statement, primary_key_predicate},
    test_support::table,
};

fn keyed_table(count: usize, width: usize, numeric: bool) -> (BoundTableInfo, KeyAttributes) {
    let mut table = table();
    let info = Arc::make_mut(&mut table.info);
    let mut key = KeyAttributes::with_capacity(count);
    for index in 0..count {
        let name = format!("key_{index}_{}", "x".repeat(width));
        info.key_schema.push(KeySchemaElement {
            attribute_name: name.clone(),
            key_type: if index == 0 {
                KeyType::Hash
            } else {
                KeyType::Range
            },
        });
        info.attribute_definitions.push(AttributeDefinition {
            attribute_name: name.clone(),
            attribute_type: if numeric {
                KeyAttributeType::N
            } else {
                KeyAttributeType::S
            },
        });
        key.insert(
            name,
            if numeric {
                AttributeValue::N("123.45".to_owned())
            } else {
                AttributeValue::S("value".to_owned())
            },
        );
    }
    (table, key)
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_primary_predicate_shapes_when_constructed_then_report_cpu_and_allocations() {
    for (engine, name) in [
        (SqlEngine::Sqlite, "sqlite"),
        (SqlEngine::Turso, "turso"),
        (SqlEngine::Postgres, "postgres"),
    ] {
        for (count, width, numeric) in [(1, 0, false), (2, 0, false), (2, 0, true), (2, 128, false)]
        {
            let (table, key) = keyed_table(count, width, numeric);
            for outer in [false, true] {
                let label = format!("{name}_{count}_{width}_{numeric}_{outer}");
                report(&label, || {
                    if outer {
                        drop(black_box(
                            point_read_statement(
                                black_box(engine),
                                black_box(&table),
                                black_box(&key),
                                "item_bytes",
                            )
                            .unwrap(),
                        ));
                    } else {
                        drop(black_box(
                            primary_key_predicate(
                                black_box(engine),
                                black_box(&table),
                                black_box(&key),
                            )
                            .unwrap(),
                        ));
                    }
                });
            }
        }
    }
}

fn report(label: &str, mut render: impl FnMut()) {
    for _ in 0..100 {
        render();
    }
    let guard = AllocationGuard::start(
        module_path!(),
        "primary_predicate",
        file!(),
        line!(),
        Some(label),
    );
    let started = cpu_time::ProcessTime::now();
    for _ in 0..100_000 {
        render();
    }
    let cpu = started.elapsed();
    alloc_counter::emit_report(&guard.finish()).unwrap();
    eprintln!(
        "gsi_sql label={label} iterations=100000 cpu_ns={}",
        cpu.as_nanos()
    );
}

#[test]
fn given_primary_predicate_when_rendered_then_preserve_order_types_and_missing_key_error() {
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        let (mut table, key) = keyed_table(2, 0, false);
        Arc::make_mut(&mut table.info).key_schema.reverse();
        let (sql, binds) = primary_key_predicate(engine, &table, &key).unwrap();
        assert_eq!(
            sql,
            format!("\"key_0_\" = {prefix}1 AND \"key_1_\" = {prefix}2")
        );
        assert_eq!(binds.len(), 2);
        let (table, key) = keyed_table(2, 0, true);
        let (sql, binds) = primary_key_predicate(engine, &table, &key).unwrap();
        let first = if engine == SqlEngine::Postgres {
            format!("CAST({prefix}1 AS TEXT)::NUMERIC")
        } else {
            format!("{prefix}1")
        };
        assert!(sql.starts_with(&format!("\"key_0_\" = {first}")));
        assert_eq!(binds.len(), 2);
        assert!(primary_key_predicate(engine, &table, &KeyAttributes::new()).is_err());
    }
}
