//! Physical SQL rows for one shared SystemStream split decision.

use storage_provider::SYSTEM_STREAM_SNAPSHOT_BYTES;
use storage_types::{ReadProtection, StorageEnum, StorageError, StorageResult, storage_serde};
use stream_provider::{
    SystemStreamHistory, SystemStreamSplitOperationId, SystemStreamSplitReceipt,
};

use crate::{
    SqlAttempt, SqlEngine, SqlLayout, SqlStatement, SqlValue, stream_checkpoint,
    stream_history::{decode_history_page, history_statement},
};

pub(crate) async fn read_split_state<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    operation_id: &SystemStreamSplitOperationId,
) -> StorageResult<(SystemStreamHistory, Option<SystemStreamSplitReceipt>)> {
    let receipt = SqlStatement::new(
        format!(
            "SELECT receipt_blob FROM {relation_sys_system_stream_splits} WHERE scope_key = \
             'system' AND operation_id = {}",
            engine.engine().placeholder(1),
            relation_sys_system_stream_splits = engine.relation_name("sys_system_stream_splits"),
        ),
        vec![SqlValue::Text(operation_id.as_str().to_owned())],
    );
    let [history, receipt] = attempt
        .query_batch_protected(
            vec![
                (
                    history_statement(engine, crate::stream_history::HistoryRead::Reader),
                    ReadProtection::Snapshot,
                ),
                (receipt, ReadProtection::Snapshot),
            ],
            SYSTEM_STREAM_SNAPSHOT_BYTES,
        )
        .await?
        .try_into()
        .map_err(|_| unavailable())?;
    let history = decode_history_page(history, engine)?;
    if receipt.more || receipt.rows.len() > 1 {
        return Err(unavailable());
    }
    let receipt = receipt
        .rows
        .first()
        .map(|row| match row.value(crate::ColumnIndex::new(0))? {
            SqlValue::Blob(bytes) => storage_serde::from_bytes(bytes),
            _ => Err(unavailable()),
        })
        .transpose()?;
    Ok((history, receipt))
}

pub(crate) async fn write_split<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    receipt: &SystemStreamSplitReceipt,
) -> StorageResult<()> {
    let split = &receipt.split;
    let scope = split.source_scope();
    let parent = split.sealed_parent();
    let generation = i64::try_from(scope.generation()).map_err(|_| unavailable())?;
    let sealed_after = i64::try_from(receipt.sealed_after_offset).map_err(|_| unavailable())?;
    let protected = attempt
        .query(
            SqlStatement::new(
                format!(
                    "SELECT 1 FROM {relation_sys_dynamo_stream_protected_frontier} WHERE scope_id \
                     = {} AND generation = {} AND shard_id = {} AND lineage_id = {}",
                    engine.engine().placeholder(1),
                    engine.engine().placeholder(2),
                    engine.engine().placeholder(3),
                    engine.engine().placeholder(4),
                    relation_sys_dynamo_stream_protected_frontier =
                        engine.relation_name("sys_dynamo_stream_protected_frontier"),
                ),
                vec![
                    SqlValue::Text(scope.scope_id().as_str().to_owned()),
                    SqlValue::Integer(generation),
                    SqlValue::Text(parent.shard_id().as_str().to_owned()),
                    SqlValue::Text(parent.lineage_id().as_str().to_owned()),
                ],
            ),
            ReadProtection::Conflict,
            SYSTEM_STREAM_SNAPSHOT_BYTES,
        )
        .await?;
    if protected.more || protected.rows.len() > 1 {
        return Err(unavailable());
    }
    if engine.engine() == SqlEngine::Postgres {
        attempt
            .execute(SqlStatement::new(
                crate::sql_template::sql_template!(
                    engine,
                    "UPDATE "["sys_system_stream_sequences"],
                    " SET next_offset = $1 WHERE scope_key = 'system' AND shard_id = $2"
                ),
                vec![
                    SqlValue::Integer(sealed_after),
                    SqlValue::Text(parent.shard_id().as_str().to_owned()),
                ],
            ))
            .await?;
    }
    let mut writes = vec![SqlStatement::new(
        format!(
            "UPDATE {relation_sys_system_stream_lineage} SET sealed = TRUE, sealed_after_offset = \
             {} WHERE scope_key = 'system' AND shard_id = {} AND lineage_id = {} AND sealed = \
             FALSE AND sealed_after_offset IS NULL AND EXISTS (SELECT 1 FROM \
             {relation_sys_system_stream_sequences} WHERE scope_key = 'system' AND shard_id = {} \
             AND next_offset = {})",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            engine.engine().placeholder(5),
            relation_sys_system_stream_lineage = engine.relation_name("sys_system_stream_lineage"),
            relation_sys_system_stream_sequences =
                engine.relation_name("sys_system_stream_sequences"),
        ),
        vec![
            SqlValue::Integer(sealed_after),
            SqlValue::Text(parent.shard_id().as_str().to_owned()),
            SqlValue::Text(parent.lineage_id().as_str().to_owned()),
            SqlValue::Text(parent.shard_id().as_str().to_owned()),
            SqlValue::Integer(sealed_after),
        ],
    )];
    for child in split.children() {
        let lineage = child.lineage();
        let initial = i64::try_from(child.initial_sequence().get()).map_err(|_| unavailable())?;
        writes.push(SqlStatement::new(
            format!(
                "INSERT INTO {relation_sys_system_stream_lineage} (scope_key, shard_id, \
                 lineage_id, parent_lineage_id, sealed, initial_sequence) VALUES ('system', {}, \
                 {}, {}, FALSE, {})",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                engine.engine().placeholder(3),
                engine.engine().placeholder(4),
                relation_sys_system_stream_lineage =
                    engine.relation_name("sys_system_stream_lineage"),
            ),
            vec![
                SqlValue::Text(lineage.shard_id().as_str().to_owned()),
                SqlValue::Text(lineage.lineage_id().as_str().to_owned()),
                SqlValue::Text(parent.lineage_id().as_str().to_owned()),
                SqlValue::Integer(initial),
            ],
        ));
        writes.push(SqlStatement::new(
            format!(
                "INSERT INTO {relation_sys_system_stream_sequences} (scope_key, shard_id, \
                 next_offset) VALUES ('system', {}, {})",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                relation_sys_system_stream_sequences =
                    engine.relation_name("sys_system_stream_sequences"),
            ),
            vec![
                SqlValue::Text(lineage.shard_id().as_str().to_owned()),
                SqlValue::Integer(initial),
            ],
        ));
        if !protected.rows.is_empty() {
            let frontier = child.initial_frontier().map_err(|_| unavailable())?;
            writes.push(stream_checkpoint::protect_frontier(
                engine, scope, &frontier,
            )?);
        }
    }
    writes.push(SqlStatement::new(
        format!(
            "INSERT INTO {relation_sys_system_stream_splits} (scope_key, operation_id, \
             receipt_blob) VALUES ('system', {}, {})",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_system_stream_splits = engine.relation_name("sys_system_stream_splits"),
        ),
        vec![
            SqlValue::Text(split.operation_id().as_str().to_owned()),
            SqlValue::Blob(storage_serde::to_bytes(receipt)?),
        ],
    ));
    // Publish a topology change without changing the scope incarnation.
    // The split's data-table fence drains prior writers before the tail read.
    // Later writers observe the new route after acquiring their compatible
    // fence.
    writes.push(SqlStatement::new(
        format!(
            "UPDATE {relation_sys_system_stream_scope} SET scope_id = scope_id WHERE scope_key = \
             'system' AND scope_id = {} AND generation = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_system_stream_scope = engine.relation_name("sys_system_stream_scope"),
        ),
        vec![
            SqlValue::Text(scope.scope_id().as_str().to_owned()),
            SqlValue::Integer(generation),
        ],
    ));
    if attempt
        .execute_batch(writes)
        .await?
        .into_iter()
        .any(|changed| changed != 1)
    {
        return Err(unavailable());
    }
    Ok(())
}

fn unavailable() -> StorageError {
    StorageEnum::HistoryUnavailable.into()
}
