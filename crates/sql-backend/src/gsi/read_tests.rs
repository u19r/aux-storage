use std::sync::Arc;

use storage_types::{GlobalSecondaryIndex, IndexName, Projection, ProjectionType};

use super::read::{decode_gsi_lifecycle, gsi_lifecycle_statement};
use crate::{SqlEngine, SqlQueryPage, SqlRow, SqlValue, test_support::table};

fn row(table_name: &str, phase: &str) -> SqlRow {
    SqlRow::new(vec![
        SqlValue::Text(table_name.to_owned()),
        SqlValue::Text("by-status".to_owned()),
        SqlValue::Blob(vec![1]),
        SqlValue::Text(phase.to_owned()),
    ])
}

fn indexed_table() -> storage_types::BoundTableInfo {
    let mut table = table();
    Arc::make_mut(&mut table.info).global_secondary_indexes = Some(vec![GlobalSecondaryIndex {
        index_name: IndexName::new("by-status"),
        key_schema: vec![],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
    }]);
    table
}

#[test]
fn given_gsi_foreground_read_when_decoding_then_only_identity_and_phase_are_required() {
    let table = indexed_table();
    for engine in [SqlEngine::Sqlite, SqlEngine::Turso, SqlEngine::Postgres] {
        let (sql, _) = gsi_lifecycle_statement(engine, &table)
            .unwrap()
            .into_parts();
        assert!(sql.starts_with("SELECT table_name, index_name, incarnation, phase FROM"));
    }
    for phase in ["scanning", "catching-up", "active", "blocked"] {
        let mut page = SqlQueryPage::new();
        page.rows.push(row("table", phase));
        let lifecycle = decode_gsi_lifecycle(page, &table).unwrap();
        assert_eq!(
            lifecycle.writable_targets().len(),
            usize::from(phase != "blocked")
        );
    }
}

#[test]
fn given_invalid_gsi_status_rows_when_decoding_then_catalog_and_physical_invariants_are_enforced() {
    let table = indexed_table();
    for rows in [
        vec![],
        vec![row("another-table", "active")],
        vec![row("table", "unknown")],
        vec![row("table", "active"), row("table", "active")],
        vec![SqlRow::new(vec![])],
        vec![SqlRow::new(vec![
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ])],
    ] {
        let mut page = SqlQueryPage::new();
        page.rows = rows;
        assert!(decode_gsi_lifecycle(page, &table).is_err());
    }
    let mut page = SqlQueryPage::new();
    page.rows.push(row("table", "active"));
    page.more = true;
    assert!(decode_gsi_lifecycle(page, &table).is_err());
}
