//! Native GSI column updates, index validity, and exact job transitions.

use storage_provider::maintenance::{NativeGsiFill, NativeGsiRowVersion, NativeGsiTransition};
use storage_types::{AttributeName, BoundGsi, BoundTableInfo, StorageError, StorageResult};

use crate::{SqlEngine, SqlLayout, SqlStatement, SqlValue, item_read::encode_key_value};

pub(in crate::gsi) fn fill_row_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    index: &BoundGsi,
    fill: NativeGsiFill,
) -> StorageResult<SqlStatement> {
    let values = fill
        .values
        .ok_or_else(|| StorageError::internal("Native fill has no values"))?;
    let mut binds = Vec::new();
    let mut assignments = Vec::new();
    for column in &index.columns {
        binds.push(
            values
                .get(&column.attribute)
                .map(|value| encode_key_value(engine, value))
                .transpose()?
                .unwrap_or(SqlValue::Null),
        );
        let placeholder = if engine.engine() == SqlEngine::Postgres
            && column.kind == storage_types::KeyAttributeType::N
        {
            engine.engine().numeric_placeholder(binds.len())
        } else {
            engine.engine().placeholder(binds.len())
        };
        assignments.push(format!(
            "\"{}\" = {placeholder}",
            crate::item_schema::key_column_identifier(column)
        ));
    }
    let predicate = match fill.version {
        NativeGsiRowVersion::Postgres { ctid, xmin } => {
            binds.extend([SqlValue::Text(ctid), SqlValue::Text(xmin)]);
            format!(
                "ctid = CAST({} AS TEXT)::tid AND xmin::text = {}",
                engine.engine().placeholder(binds.len() - 1),
                engine.engine().placeholder(binds.len())
            )
        }
        NativeGsiRowVersion::Serialized => {
            let mut predicates = Vec::new();
            for element in storage_types::KeySchemaElement::ordered(&table.key_schema) {
                let value = fill
                    .key
                    .get(&element.attribute_name)
                    .ok_or_else(StorageError::invalid_or_missing_key)?;
                binds.push(encode_key_value(engine, value)?);
                predicates.push(format!(
                    "\"{}\" = {}",
                    AttributeName::new(&element.attribute_name).sanitized(),
                    engine.engine().placeholder(binds.len())
                ));
            }
            predicates.join(" AND ")
        }
    };
    Ok(SqlStatement::new(
        format!(
            "UPDATE \"{}\" SET {} WHERE {predicate}",
            engine.item_table_name(table.history_id),
            assignments.join(", ")
        ),
        binds,
    ))
}

pub(crate) fn index_state_statement(
    engine: impl SqlLayout,
    history: storage_types::TableHistoryId,
    index: &BoundGsi,
) -> SqlStatement {
    let name = engine.gsi_index_name(history, index);
    let sql = if engine.engine() == SqlEngine::Postgres {
        // The DDL session holds the session advisory lock of the index name
        // while it builds or drops the index. An INVALID index whose lock is
        // held elsewhere is a build in progress (2), not a failed build (0).
        format!(
            "SELECT CASE WHEN indisvalid THEN 1 WHEN \
             pg_try_advisory_xact_lock(hashtextextended({0}, 0)) THEN 0 ELSE 2 END FROM pg_index \
             WHERE indexrelid = to_regclass({0})",
            engine.engine().placeholder(1)
        )
    } else {
        format!(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = {}",
            engine.engine().placeholder(1)
        )
    };
    SqlStatement::new(sql, vec![SqlValue::Text(name)])
}

pub(in crate::gsi) fn native_transition_statement(
    engine: impl SqlLayout,
    transition: &NativeGsiTransition,
) -> StorageResult<SqlStatement> {
    let old = storage_backfill::GsiStateRecord::try_from(&transition.expected.state)?;
    let next = storage_backfill::GsiStateRecord::try_from(&transition.next.state)?;
    Ok(SqlStatement::new(
        format!(
            "UPDATE {} SET phase = {}, state = {}, incarnation = {} WHERE table_id = {} AND \
             index_name = {} AND incarnation = {} AND phase = {} AND state = {}",
            engine.relation_name("sys_gsi_builds"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            engine.engine().placeholder(5),
            engine.engine().placeholder(6),
            engine.engine().placeholder(7),
            engine.engine().placeholder(8)
        ),
        vec![
            SqlValue::Text(next.phase.to_owned()),
            SqlValue::Blob(next.state),
            SqlValue::Blob(transition.next.target.incarnation.as_ref().to_vec()),
            SqlValue::Blob(transition.expected.target.history_id.as_ref().to_vec()),
            SqlValue::Text(transition.expected.target.index_name.to_string()),
            SqlValue::Blob(transition.expected.target.incarnation.as_ref().to_vec()),
            SqlValue::Text(old.phase.to_owned()),
            SqlValue::Blob(old.state),
        ],
    ))
}

/// Metadata-only update of a current source row protected by the TTL fill
/// attempt.
pub(in crate::gsi) fn ttl_metadata_statement(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    row: storage_provider::maintenance::PrimaryTtlMetadata,
) -> StorageResult<SqlStatement> {
    let ttl_key = crate::item_schema::ttl_key_identifier();
    let (epoch, token) = match row.ttl {
        Some(key) => (
            SqlValue::Integer(key.ttl_seconds()),
            SqlValue::Blob(key.encode_ordered_bytes()?),
        ),
        None => (SqlValue::Null, SqlValue::Null),
    };
    let mut binds = vec![epoch, token];
    let mut predicates = Vec::new();
    for element in storage_types::KeySchemaElement::ordered(&table.key_schema) {
        let value = row
            .key
            .get(&element.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        binds.push(encode_key_value(engine, value)?);
        let placeholder = if engine.engine() == SqlEngine::Postgres
            && matches!(value, storage_types::AttributeValue::N(_))
        {
            engine.engine().numeric_placeholder(binds.len())
        } else {
            engine.engine().placeholder(binds.len())
        };
        predicates.push(format!(
            "\"{}\" = {placeholder}",
            AttributeName::new(&element.attribute_name).sanitized()
        ));
    }
    Ok(SqlStatement::new(
        format!(
            "UPDATE \"{}\" SET __aux_ttl_epoch = {}, {ttl_key} = {} WHERE {}",
            engine.item_table_name(table.history_id),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            predicates.join(" AND ")
        ),
        binds,
    ))
}
