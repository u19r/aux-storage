use storage_provider::{ItemPrimaryEffects, ItemTombstoneScan, canonical_revision_key};
use storage_types::{BoundTableInfo, KeyAttributes, StorageResult};

use crate::{
    SqlLayout, SqlStatement, SqlValue, item_read::primary_key_predicate, sql_template::sql_template,
};

pub(crate) fn tombstone_upsert_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    key: &KeyAttributes,
    effects: &ItemPrimaryEffects,
) -> StorageResult<SqlStatement> {
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(4)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let sql = sql_template!(engine,
        "INSERT INTO "["item_tombstones"],
        " (table_id, key_json, revision, provenance) VALUES (" {1},
        ", " {2}, ", " {3}, ", " {4},
        ") ON CONFLICT (table_id, key_json) DO UPDATE SET revision = excluded.revision, provenance = excluded.provenance"
    );
    binds.push(SqlValue::try_from(crate::row_view::SqlValueRef::Blob(
        table.history_id.as_ref(),
    ))?);
    binds.push(SqlValue::Text(canonical_revision_key(key)?));
    binds.push(SqlValue::Integer(i64::try_from(effects.revision())?));
    binds.push(SqlValue::try_from(crate::row_view::SqlValueRef::Blob(
        effects.provenance().as_bytes(),
    ))?);
    Ok(SqlStatement::new(sql, binds))
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

pub(crate) fn deleted_observation_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<SqlStatement> {
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(2)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let key_json = canonical_revision_key(key)?;
    let sql = format!(
        "SELECT revision, provenance FROM {relation} WHERE table_id = {} AND key_json = {} LIMIT 1",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation = engine.relation_name("item_tombstones"),
    );
    binds.push(SqlValue::try_from(crate::row_view::SqlValueRef::Blob(
        table.history_id.as_ref(),
    ))?);
    binds.push(SqlValue::Text(key_json));
    Ok(SqlStatement::new(sql, binds))
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

pub(crate) fn primary_delete_statements(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    key: &KeyAttributes,
    effects: &ItemPrimaryEffects,
) -> StorageResult<Vec<SqlStatement>> {
    let mut statements = Vec::new();
    statements
        .try_reserve_exact(2)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let (predicate, binds) = primary_key_predicate(engine, table, key)?;
    let physical = engine.item_table_name(&table.info.table_name, table.history_id);
    let primary = SqlStatement::new(
        format!("DELETE FROM \"{physical}\" WHERE {predicate}"),
        binds,
    );
    let tombstone = tombstone_upsert_statement(engine, table, key, effects)?;
    statements.push(primary);
    statements.push(tombstone);
    Ok(statements)
}
