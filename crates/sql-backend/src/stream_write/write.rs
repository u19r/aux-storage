//! Native-attempt staging for shared stream decisions on all three SQL engines.

use storage_types::{StorageError, StorageResult, StreamItemId};
use stream_provider::{DynamoStreamWritePlan, StreamItem, StreamRowValue};

use super::{counter, sql_statements};
use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlValue,
    constants::STREAM_READ_BYTES,
};

pub(crate) async fn initialize_stream_rows<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    for statement in sql_statements::schema(engine) {
        attempt.execute(statement).await?;
    }
    let page = attempt
        .query(
            crate::SqlStatement::new(
                format!(
                    "SELECT read_work_format FROM {} LIMIT 1",
                    engine.relation_name("sys_dynamo_stream_records")
                ),
                vec![],
            ),
            ReadProtection::Conflict,
            STREAM_READ_BYTES,
        )
        .await?;
    for row in page.rows {
        if !matches!(row.value(ColumnIndex::new(0))?, SqlValue::Integer(version)
            if *version == i64::from(stream_provider::retained_image::system_stream::RETAINED_SYSTEM_STREAM_RECORD_FORMAT))
        {
            return Err(StorageError::internal(
                "Obsolete SystemStream record format",
            ));
        }
    }
    Ok(())
}

pub(crate) fn dynamo_stream_statements(
    engine: impl SqlLayout,
    plan: DynamoStreamWritePlan,
) -> StorageResult<Vec<crate::SqlStatement>> {
    let DynamoStreamWritePlan {
        rows,
        pointer_references,
        record,
        receipt,
    } = plan;
    let mut statements = rows
        .into_iter()
        .map(|row| sql_statements::item(engine, row))
        .collect::<StorageResult<Vec<_>>>()?;
    statements.extend(
        pointer_references
            .into_iter()
            .map(|reference| sql_statements::pointer_reference(engine, reference)),
    );
    statements.push(sql_statements::record(engine, record, &receipt)?);
    statements.push(sql_statements::effect(engine, &receipt)?);
    Ok(statements)
}

pub(crate) async fn append_attempt_allocated_user_item<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    row: StreamRowValue,
) -> StorageResult<StreamItemId> {
    if engine.engine() == SqlEngine::Postgres {
        let page = attempt
            .query(
                sql_statements::postgres_append_user_item(engine, &row)?,
                ReadProtection::Snapshot,
                STREAM_READ_BYTES,
            )
            .await?;
        if page.more || page.rows.len() != 1 {
            return Err(StorageError::internal(
                "Postgres stream append returned no ID",
            ));
        }
        return match page.rows[0].value(ColumnIndex::new(0))? {
            SqlValue::Blob(id) => StreamItemId::try_from(id.as_slice())
                .map_err(|_| StorageError::internal("Postgres stream append returned invalid ID")),
            _ => Err(StorageError::internal(
                "Postgres stream append returned invalid ID",
            )),
        };
    }
    append_sequenced_user_item(attempt, engine, row).await
}

pub(crate) async fn initialize_user_sequence<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    stream: &storage_types::StreamName,
) -> StorageResult<()> {
    let name = sql_statements::user_sequence_name(stream);
    attempt
        .execute(sql_statements::initialize_sequence(engine, &name))
        .await?;
    Ok(())
}

pub(crate) async fn delete_user_sequence<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    stream: &storage_types::StreamName,
) -> StorageResult<()> {
    let name = sql_statements::user_sequence_name(stream);
    attempt
        .execute(sql_statements::delete_sequence(engine, &name))
        .await?;
    Ok(())
}

async fn append_sequenced_user_item<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    row: StreamRowValue,
) -> StorageResult<StreamItemId> {
    let stream = row
        .stream_name()
        .ok_or_else(|| StorageError::internal("sequenced user stream row has no stream name"))?;
    let id = counter::allocate_stream_item_id(attempt, engine, stream).await?;
    store_user_item(attempt, engine, row.into_item(id)).await?;
    Ok(id)
}

pub(crate) async fn store_user_item<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    row: StreamItem,
) -> StorageResult<()> {
    let changed = attempt
        .execute(sql_statements::item(
            engine,
            stream_provider::StreamWriteKind::User(row),
        )?)
        .await?;
    if changed != 1 {
        return Err(StorageError::internal("SQL stream append affected no row"));
    }
    Ok(())
}
