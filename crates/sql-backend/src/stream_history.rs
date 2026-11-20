//! Protected SystemStream allocation bounds on the caller's SQL attempt.

use std::num::NonZeroU64;

use storage_provider::{SYSTEM_STREAM_SNAPSHOT_BYTES, SYSTEM_STREAM_SNAPSHOT_ROWS};
use storage_types::{
    SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES, StorageEnum, StorageResult, SystemStreamCheckpoint,
    SystemStreamPosition, SystemStreamScope,
};
use stream_provider::{
    SystemStreamHistory, SystemStreamHistoryBounds, SystemStreamLineage, SystemStreamLineageCatalog,
};

use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlRow, SqlStatement, SqlValue,
};

/// Install the existing scope, lineage, and sequence authority layout. A
/// partial pre-existing authority is rejected instead of bootstrapped over.
pub async fn initialize_system_stream<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    for sql in authority_schema(engine) {
        attempt.execute(SqlStatement::new(sql, vec![])).await?;
    }
    Ok(())
}

/// Insert the system authority after `initialize_system_stream` when the
/// database has none.
pub(crate) async fn seed_system_stream<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    initial_stream_lineages: storage_types::InitialStreamLineages,
) -> StorageResult<()> {
    let scope = attempt
        .query(
            SqlStatement::new(
                crate::sql_template::sql_template!(
                    engine,
                    "SELECT scope_id FROM "["sys_system_stream_scope"],
                    " WHERE scope_key = 'system'"
                )
                .into_owned(),
                vec![],
            ),
            ReadProtection::Snapshot,
            SYSTEM_STREAM_SNAPSHOT_BYTES,
        )
        .await?;
    if scope.more || scope.rows.len() > 1 {
        return Err(unavailable());
    }
    if scope.rows.is_empty() {
        reject_partial_authority(attempt, engine).await?;
        insert_initial_authority(attempt, engine, initial_stream_lineages).await?;
    }
    capture_high(attempt, engine).await?;
    Ok(())
}

fn authority_schema(engine: impl SqlLayout) -> [String; 4] {
    let int = engine.engine().integer_type();
    let sealed = engine.engine().boolean_column("sealed");
    [
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_system_stream_scope} (scope_key TEXT \
             PRIMARY KEY, version {int} NOT NULL, scope_id TEXT NOT NULL, generation {int} NOT \
             NULL, transaction_offset {int} NOT NULL DEFAULT 0 CHECK (transaction_offset >= 0)){}",
            engine.engine().table_suffix(),
            relation_sys_system_stream_scope = engine.relation_name("sys_system_stream_scope"),
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_system_stream_lineage} (scope_key TEXT NOT \
             NULL, shard_id TEXT NOT NULL, lineage_id TEXT NOT NULL, parent_lineage_id TEXT, \
             sealed {sealed}, sealed_after_offset {int}, initial_sequence {int} NOT NULL DEFAULT \
             1 CHECK (initial_sequence > 0), retired_offset {int} NOT NULL DEFAULT 0, PRIMARY KEY \
             (scope_key, shard_id, lineage_id)){}",
            engine.engine().table_suffix(),
            relation_sys_system_stream_lineage = engine.relation_name("sys_system_stream_lineage"),
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_system_stream_sequences} (scope_key TEXT \
             NOT NULL, shard_id TEXT PRIMARY KEY, next_offset {int} NOT NULL CHECK (next_offset > \
             0)){}",
            engine.engine().table_suffix(),
            relation_sys_system_stream_sequences =
                engine.relation_name("sys_system_stream_sequences"),
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_system_stream_splits} (scope_key TEXT NOT \
             NULL, operation_id {} NOT NULL, receipt_blob {} NOT NULL, PRIMARY KEY (scope_key, \
             operation_id)){}",
            engine.engine().text_type(),
            engine.engine().blob_type(),
            engine.engine().table_suffix(),
            relation_sys_system_stream_splits = engine.relation_name("sys_system_stream_splits"),
        ),
    ]
}

async fn reject_partial_authority<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let sql = crate::sql_template::sql_template!(
        engine,
        "SELECT (SELECT COUNT(*) FROM "["sys_system_stream_lineage"],
        "), (SELECT COUNT(*) FROM "["sys_system_stream_sequences"],
        ")"
    )
    .into_owned();
    let page = attempt
        .query(
            SqlStatement::new(sql, vec![]),
            ReadProtection::Snapshot,
            SYSTEM_STREAM_SNAPSHOT_BYTES,
        )
        .await?;
    if page.more || page.rows.len() != 1 {
        return Err(unavailable());
    }
    let row = &page.rows[0];
    if integer(row, 0)? != 0 || integer(row, 1)? != 0 {
        return Err(unavailable());
    }
    Ok(())
}

async fn insert_initial_authority<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    initial_stream_lineages: storage_types::InitialStreamLineages,
) -> StorageResult<()> {
    let scope_id = uuid::Uuid::new_v4().to_string();
    let scope_sql = format!(
        "INSERT INTO {relation_sys_system_stream_scope} (scope_key, version, scope_id, \
         generation) VALUES ({}, {}, {}, {})",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        engine.engine().placeholder(4),
        relation_sys_system_stream_scope = engine.relation_name("sys_system_stream_scope"),
    );
    attempt
        .execute(SqlStatement::new(
            scope_sql,
            vec![
                SqlValue::Text("system".to_owned()),
                SqlValue::Integer(1),
                SqlValue::Text(scope_id.clone()),
                SqlValue::Integer(1),
            ],
        ))
        .await?;
    let scope = SystemStreamScope::try_new(scope_id, 1).map_err(|_| unavailable())?;
    let catalog = SystemStreamLineageCatalog::initial_roots(scope, initial_stream_lineages)
        .map_err(|_| unavailable())?;
    for lineage in catalog.iter() {
        let lineage_sql = format!(
            "INSERT INTO {relation_sys_system_stream_lineage} (scope_key, shard_id, lineage_id, \
             sealed) VALUES ({}, {}, {}, {})",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            relation_sys_system_stream_lineage = engine.relation_name("sys_system_stream_lineage"),
        );
        attempt
            .execute(SqlStatement::new(
                lineage_sql,
                vec![
                    SqlValue::Text("system".to_owned()),
                    SqlValue::Text(lineage.shard_id().as_str().to_owned()),
                    SqlValue::Text(lineage.lineage_id().as_str().to_owned()),
                    SqlValue::Boolean(false),
                ],
            ))
            .await?;
        if engine.engine() == SqlEngine::Postgres {
            continue;
        }
        let sequence_sql = format!(
            "INSERT INTO {relation_sys_system_stream_sequences} (scope_key, shard_id, \
             next_offset) VALUES ({}, {}, {})",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            relation_sys_system_stream_sequences =
                engine.relation_name("sys_system_stream_sequences"),
        );
        attempt
            .execute(SqlStatement::new(
                sequence_sql,
                vec![
                    SqlValue::Text("system".to_owned()),
                    SqlValue::Text(lineage.shard_id().as_str().to_owned()),
                    SqlValue::Integer(1),
                ],
            ))
            .await?;
    }
    Ok(())
}

/// Capture the high frontier after protecting the native stream authority.
/// Callers stage table and GSI effects before settling this same attempt.
pub async fn capture_high<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<SystemStreamCheckpoint> {
    read_history(attempt, engine)
        .await?
        .capture_high()
        .map_err(|_| unavailable())
}

pub(crate) fn sequence_advance_statement(
    engine: impl SqlLayout,
    shard_id: &str,
    expected_offset: NonZeroU64,
    next_offset: NonZeroU64,
) -> StorageResult<SqlStatement> {
    let (sql, binds) = sequence_advance_parts(engine, shard_id, expected_offset, next_offset)?;
    Ok(SqlStatement::new(sql, binds))
}

pub(crate) fn counted_sequence_advance(
    engine: impl SqlLayout,
    shard_id: &str,
    expected_offset: NonZeroU64,
    next_offset: NonZeroU64,
) -> StorageResult<crate::SqlCountedWrite> {
    let (body, binds) = sequence_advance_parts(engine, shard_id, expected_offset, next_offset)?;
    let mut sql = crate::statement::SqlText::start_counted_write()?;
    sql.append(format_args!("{body}"))?;
    sql.finish_counted_write(
        binds,
        crate::catalog::SqlDynamoCount::ExactlyOne,
        crate::catalog::SqlDynamoWriteKind::SystemSequence.error_factory(),
    )
}

fn sequence_advance_parts(
    engine: impl SqlLayout,
    shard_id: &str,
    expected_offset: NonZeroU64,
    next_offset: NonZeroU64,
) -> StorageResult<(std::borrow::Cow<'static, str>, Vec<SqlValue>)> {
    let expected = i64::try_from(expected_offset.get()).map_err(|_| unavailable())?;
    let next = i64::try_from(next_offset.get()).map_err(|_| unavailable())?;
    let sql = crate::sql_template::sql_template!(engine,
        "UPDATE " ["sys_system_stream_sequences"], " SET next_offset = " {1},
        " WHERE scope_key = 'system' AND shard_id = " {2},
        " AND next_offset = " {3},
    );
    Ok((
        sql,
        vec![
            SqlValue::Integer(next),
            SqlValue::Text(shard_id.to_owned()),
            SqlValue::Integer(expected),
        ],
    ))
}

pub(crate) async fn read_history<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<SystemStreamHistory> {
    let page = attempt
        .query(
            history_statement(engine, HistoryRead::Reader),
            ReadProtection::Snapshot,
            SYSTEM_STREAM_SNAPSHOT_BYTES,
        )
        .await?;
    decode_history_page(page, engine)
}

pub(crate) enum HistoryRead<'a> {
    Writer,
    Reader,
    Split(&'a storage_types::SystemStreamLineageId),
}

pub(crate) fn history_statement(engine: impl SqlLayout, read: HistoryRead<'_>) -> SqlStatement {
    let is_postgres = engine.engine() == SqlEngine::Postgres;
    let frontier =
        "((pg_snapshot_xmin(pg_current_snapshot())::text::bigint + s.transaction_offset) * 65536)";
    let mut params = vec![];
    let (next_offset, native_join) = if is_postgres {
        if let HistoryRead::Split(parent) = read {
            params.push(SqlValue::Text(parent.as_str().to_owned()));
            (
                format!(
                    "CASE WHEN l.sealed THEN l.sealed_after_offset WHEN l.lineage_id = $1 THEN \
                     GREATEST({frontier}, l.retired_offset + 1, COALESCE(tail.offset, 0) + 1, \
                     l.initial_sequence) ELSE GREATEST({frontier}, l.initial_sequence) END"
                ),
                crate::sql_template::sql_template!(
                    engine,
                    " LEFT JOIN LATERAL (SELECT split_part(r.offset_text, '-', 1)::bigint AS \
                     offset FROM "["sys_stream_entries"],
                    " AS r WHERE l.lineage_id = $1 AND r.shard_id = l.shard_id AND r.lineage_id = \
                     l.lineage_id ORDER BY r.offset_order DESC, r.offset_text DESC LIMIT 1) AS \
                     tail ON TRUE"
                )
                .into_owned(),
            )
        } else {
            (
                format!(
                    "CASE WHEN l.sealed THEN l.sealed_after_offset ELSE GREATEST({frontier}, \
                     l.initial_sequence) END"
                ),
                String::new(),
            )
        }
    } else {
        (
            "q.next_offset".to_owned(),
            format!(
                " LEFT JOIN {} AS q ON q.scope_key = l.scope_key AND q.shard_id = l.shard_id",
                engine.relation_name("sys_system_stream_sequences"),
            ),
        )
    };
    let extra = if is_postgres {
        "FALSE".to_owned()
    } else {
        format!(
            "EXISTS (SELECT 1 FROM {} AS extra WHERE extra.scope_key = 'system' AND NOT EXISTS \
             (SELECT 1 FROM {} AS known WHERE known.scope_key = extra.scope_key AND \
             known.shard_id = extra.shard_id))",
            engine.relation_name("sys_system_stream_sequences"),
            engine.relation_name("sys_system_stream_lineage"),
        )
    };
    let native_columns = if is_postgres {
        ", pg_snapshot_xmin(pg_current_snapshot())::text::bigint, \
         (pg_snapshot_xmax(pg_current_snapshot())::text::bigint - \
         pg_snapshot_xmin(pg_current_snapshot())::text::bigint), s.transaction_offset"
    } else {
        ""
    };
    SqlStatement::new(
        format!(
            "SELECT s.version, s.scope_id, s.generation, {extra}, l.shard_id, l.lineage_id, \
             l.parent_lineage_id, l.sealed, l.initial_sequence, l.sealed_after_offset, \
             {next_offset}{native_columns} FROM {} AS s LEFT JOIN {} AS l ON l.scope_key = \
             s.scope_key{native_join} WHERE s.scope_key = 'system' ORDER BY l.shard_id, \
             l.lineage_id LIMIT {}",
            engine.relation_name("sys_system_stream_scope"),
            engine.relation_name("sys_system_stream_lineage"),
            SYSTEM_STREAM_SNAPSHOT_ROWS.get(),
        ),
        params,
    )
}

pub(crate) fn decode_history_page(
    page: crate::SqlQueryPage,
    engine: impl SqlLayout,
) -> StorageResult<SystemStreamHistory> {
    if page.more || page.rows.len() > SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
        return Err(unavailable());
    }
    if engine.engine() != SqlEngine::Postgres {
        return decode_history(page.rows);
    }
    let mut frontiers = Vec::with_capacity(page.rows.len());
    let mut minimum_new_position = None;
    let lag = integer(page.rows.first().ok_or_else(unavailable)?, 12)?;
    let lag = u64::try_from(lag).map_err(|_| unavailable())?;
    for row in &page.rows {
        let upper = crate::stream_allocation::mapped_offset(
            crate::stream_allocation::integer(row, 11)?
                .checked_add(crate::stream_allocation::integer(row, 13)?)
                .ok_or_else(unavailable)?,
        )?;
        let (_, bound) = decode_lineage(row)?;
        let minimum = NonZeroU64::new(upper).ok_or_else(unavailable)?;
        minimum_new_position = Some(minimum);
        let high = stream_provider::lineage_frontier_high(
            bound.initial_sequence,
            bound.next_sequence,
            bound.sealed_after.map(NonZeroU64::get),
            upper,
        )?;
        frontiers.push(
            SystemStreamPosition::try_new(text(row, 4)?, text(row, 5)?, format!("{high:020}"))
                .map_err(|_| unavailable())?,
        );
    }
    let history = decode_history(page.rows)?
        .with_sparse_numeric_ordering(frontiers, minimum_new_position.ok_or_else(unavailable)?);
    metrics::gauge!("storage.postgres.stream.frontier.lag.xids").set(lag as f64);
    Ok(history)
}

fn decode_history(rows: Vec<SqlRow>) -> StorageResult<SystemStreamHistory> {
    let mut scope = None;
    let mut lineages = Vec::with_capacity(rows.len());
    let mut bounds = Vec::with_capacity(rows.len());
    for row in rows {
        let current = decode_scope(&row)?;
        if scope.as_ref().is_some_and(|expected| expected != &current) || integer(&row, 3)? != 0 {
            return Err(unavailable());
        }
        scope = Some(current);
        let (lineage, bound) = decode_lineage(&row)?;
        lineages.push(lineage);
        bounds.push(bound);
    }
    let catalog = SystemStreamLineageCatalog::try_new(scope.ok_or_else(unavailable)?, lineages)
        .map_err(|_| unavailable())?;
    SystemStreamHistory::try_new(catalog, bounds).map_err(|_| unavailable())
}

fn decode_scope(row: &SqlRow) -> StorageResult<SystemStreamScope> {
    let version = u8::try_from(integer(row, 0)?).map_err(|_| unavailable())?;
    let generation = u64::try_from(integer(row, 2)?).map_err(|_| unavailable())?;
    SystemStreamScope::try_new_versioned(version, text(row, 1)?, generation)
        .map_err(|_| unavailable())
}

fn decode_lineage(row: &SqlRow) -> StorageResult<(SystemStreamLineage, SystemStreamHistoryBounds)> {
    let sealed = match integer(row, 7)? {
        0 => false,
        1 => true,
        _ => return Err(unavailable()),
    };
    let lineage =
        SystemStreamLineage::try_new(text(row, 4)?, text(row, 5)?, optional_text(row, 6)?, sealed)
            .map_err(|_| unavailable())?;
    let bounds = SystemStreamHistoryBounds {
        lineage_id: lineage.lineage_id().clone(),
        initial_sequence: sequence(integer(row, 8)?)?,
        next_sequence: sequence(integer(row, 10)?)?,
        sealed_after: optional_integer(row, 9)?.map(sequence).transpose()?,
    };
    Ok((lineage, bounds))
}

fn integer(row: &SqlRow, column: usize) -> StorageResult<i64> {
    match row
        .value(ColumnIndex::new(column))
        .map_err(|_| unavailable())?
    {
        SqlValue::Integer(value) => Ok(*value),
        _ => Err(unavailable()),
    }
}

fn optional_integer(row: &SqlRow, column: usize) -> StorageResult<Option<i64>> {
    match row
        .value(ColumnIndex::new(column))
        .map_err(|_| unavailable())?
    {
        SqlValue::Null => Ok(None),
        SqlValue::Integer(value) => Ok(Some(*value)),
        _ => Err(unavailable()),
    }
}

fn text(row: &SqlRow, column: usize) -> StorageResult<String> {
    optional_text(row, column)?.ok_or_else(unavailable)
}

fn optional_text(row: &SqlRow, column: usize) -> StorageResult<Option<String>> {
    match row
        .value(ColumnIndex::new(column))
        .map_err(|_| unavailable())?
    {
        SqlValue::Null => Ok(None),
        SqlValue::Text(value) => Ok(Some(value.clone())),
        _ => Err(unavailable()),
    }
}

fn sequence(value: i64) -> StorageResult<NonZeroU64> {
    u64::try_from(value)
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or_else(unavailable)
}

fn unavailable() -> storage_types::StorageError {
    StorageEnum::HistoryUnavailable.into()
}

/// The routing query supplies this setting before item conditions run. The
/// compatible relation lock keeps an opener from changing it until commit.
pub(crate) fn transaction_offset(page: &crate::SqlQueryPage) -> StorageResult<i64> {
    let row = page.rows.first().ok_or_else(unavailable)?;
    let offset = integer(row, 13)?;
    if offset < 0
        || page
            .rows
            .iter()
            .any(|row| !matches!(integer(row, 13), Ok(value) if value == offset))
    {
        return Err(unavailable());
    }
    Ok(offset)
}
