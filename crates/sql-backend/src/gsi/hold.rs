//! Indexed private history-hold observations for SystemStream retention.

use storage_backfill::GsiBuildTarget;
use storage_provider::CATALOG_READ_BYTES;
use storage_types::{
    IndexName, StorageError, StorageResult, SystemStreamPosition, SystemStreamScope, TableName,
    maintenance::TargetIncarnation,
};

use super::{read::decode_build_row, sql_statements};
use crate::{ReadProtection, SqlAttempt, SqlLayout, SqlValue, stream_checkpoint};

/// Read one minimum hold per bounded catalog lineage, then batch keyed build
/// observations before giving the shared trim policy a consumed position.
pub(crate) async fn load_trim_protection_rows<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    scope: &SystemStreamScope,
    lineages: &[SystemStreamPosition],
) -> StorageResult<(Vec<SystemStreamPosition>, Vec<SystemStreamPosition>)> {
    let mut statements = vec![stream_checkpoint::protected_frontiers_statement(
        engine, scope,
    )?];
    statements.extend(
        lineages
            .iter()
            .map(|lineage| sql_statements::minimum_hold_statement(engine, scope, lineage))
            .collect::<StorageResult<Vec<_>>>()?,
    );
    let pages = attempt
        .query_batch(statements, ReadProtection::Conflict, CATALOG_READ_BYTES)
        .await?;
    if pages.len() != lineages.len() + 1 {
        return Err(StorageError::internal("SQL GSI hold scan count differs"));
    }
    let mut pages = pages.into_iter();
    let frontiers =
        stream_checkpoint::decode_protected_frontiers(pages.next().ok_or_else(invalid_hold)?)?;
    let mut positions = Vec::new();
    let mut targets = Vec::new();
    for (page, lineage) in pages.zip(lineages) {
        if page.more || page.rows.len() > 1 {
            return Err(StorageError::internal(
                "SQL GSI minimum hold scan is unbounded",
            ));
        }
        let Some(row) = page.rows.into_iter().next() else {
            continue;
        };
        let [table, index, incarnation, offset]: [SqlValue; 4] =
            row.into_values().try_into().map_err(|_| invalid_hold())?;
        let (
            SqlValue::Text(table),
            SqlValue::Text(index),
            SqlValue::Blob(incarnation),
            SqlValue::Integer(offset),
        ) = (table, index, incarnation, offset)
        else {
            return Err(invalid_hold());
        };
        let offset = u64::try_from(offset).map_err(|_| invalid_hold())?;
        let position = SystemStreamPosition::try_new(
            lineage.shard_id().as_str(),
            lineage.lineage_id().as_str(),
            format!("{offset:020}"),
        )
        .map_err(|_| invalid_hold())?;
        targets.push(GsiBuildTarget {
            table_name: TableName::new(&table),
            index_name: IndexName::new(&index),
            incarnation: TargetIncarnation::try_from(incarnation).map_err(|_| invalid_hold())?,
        });
        positions.push(position);
    }
    let statements = targets
        .iter()
        .map(|target| sql_statements::build_for_hold_statement(engine, target))
        .collect();
    let pages = attempt
        .query_batch(statements, ReadProtection::Conflict, CATALOG_READ_BYTES)
        .await?;
    if pages.len() != positions.len() {
        return Err(StorageError::internal("SQL GSI hold build count differs"));
    }
    for ((page, target), position) in pages.into_iter().zip(targets).zip(&positions) {
        if page.more || page.rows.len() != 1 {
            return Err(invalid_hold());
        }
        let build = decode_build_row(page.rows.into_iter().next().ok_or_else(invalid_hold)?)?;
        if build.target != target {
            return Err(invalid_hold());
        }
        build.validate_private_hold(scope, position)?;
    }
    Ok((frontiers, positions))
}

fn invalid_hold() -> StorageError {
    StorageError::internal("SQL GSI private history hold is invalid")
}
