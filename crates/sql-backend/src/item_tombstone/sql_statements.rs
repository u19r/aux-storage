use storage_provider::{ItemPrimaryEffects, ItemTombstoneScan, canonical_revision_key};
use storage_types::{BoundTableInfo, KeyAttributes, StorageResult};

use crate::{
    SqlLayout, SqlStatement, SqlValue,
    sql_template::sql_template,
    write_batch::{PreparedUpsertRow, UpsertShape},
};

pub(crate) fn tombstone_upsert_row(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    key: &KeyAttributes,
    effects: &ItemPrimaryEffects,
) -> StorageResult<PreparedUpsertRow> {
    let relation = engine.relation_name("item_tombstones").into_owned();
    let shape = UpsertShape::try_new(
        vec![
            "table_id".to_owned(),
            "key_json".to_owned(),
            "revision".to_owned(),
            "provenance".to_owned(),
        ],
        vec![false; 4],
        vec!["table_id".to_owned(), "key_json".to_owned()],
        false,
    )?;
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(4)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    binds.push(SqlValue::try_from(crate::row_view::SqlValueRef::Blob(
        table.history_id.as_ref(),
    ))?);
    binds.push(SqlValue::Text(canonical_revision_key(key)?));
    binds.push(SqlValue::Integer(i64::try_from(effects.revision())?));
    binds.push(SqlValue::try_from(crate::row_view::SqlValueRef::Blob(
        effects.provenance().as_bytes(),
    ))?);
    Ok(PreparedUpsertRow {
        relation,
        shape,
        binds,
    })
}

pub(crate) fn delete_tombstone_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<SqlStatement> {
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(2)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    binds.push(SqlValue::try_from(crate::row_view::SqlValueRef::Blob(
        table.history_id.as_ref(),
    ))?);
    binds.push(SqlValue::Text(canonical_revision_key(key)?));
    Ok(SqlStatement::new(
        format!(
            "DELETE FROM {relation} WHERE table_id = {} AND key_json = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation = engine.relation_name("item_tombstones"),
        ),
        binds,
    ))
}

pub(super) fn scan_statement(
    engine: impl SqlLayout,
    scan: &ItemTombstoneScan,
) -> StorageResult<SqlStatement> {
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(1 + usize::from(scan.after().is_some()))
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut sql = sql_template!(engine,
        "SELECT key_json, revision, provenance FROM "["item_tombstones"],
        " WHERE table_id = " {1}
    )
    .into_owned();
    binds.push(SqlValue::try_from(crate::row_view::SqlValueRef::Blob(
        scan.table().history_id.as_ref(),
    ))?);
    if let Some(after) = scan.after() {
        sql.push_str(&format!(
            " AND key_json > {}",
            engine.engine().placeholder(2)
        ));
        binds.push(SqlValue::try_from(crate::row_view::SqlValueRef::Text(
            after,
        ))?);
    }
    sql.push_str(&format!(" ORDER BY key_json LIMIT {}", scan.probe_limit()));
    Ok(SqlStatement::new(sql, binds))
}
