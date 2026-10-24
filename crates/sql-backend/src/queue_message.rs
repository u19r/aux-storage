//! One SQL queue message layout for SQLite, Turso, and Postgres.

use queue_provider::{
    MessageId, QueueCleanupPage, QueueCleanupRequest, QueueDueRequest, QueueIncarnation,
    QueueMessageCounts, QueueMessageRecord, ReceiptHandle,
};
use storage_types::{ReadProtection, StorageError, StorageResult, TimestampMillis, storage_serde};

use crate::{
    SqlAttempt, SqlLayout, SqlRow, SqlStatement, SqlValue,
    constants::{
        QUEUE_CLEANUP_READ_BYTES as CLEANUP_READ_BYTES,
        QUEUE_MESSAGE_READ_BYTES as MESSAGE_READ_BYTES,
    },
    queue_catalog::expect_one,
};

pub(crate) async fn initialize_queue_messages<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_queue_messages} (incarnation {} NOT NULL, \
         message_id {} NOT NULL, due_at {} NOT NULL, expires_at {} NOT NULL, receipt_handle {}, \
         row_blob {} NOT NULL, PRIMARY KEY (incarnation, message_id)){}",
        engine.engine().blob_type(),
        engine.engine().blob_type(),
        engine.engine().integer_type(),
        engine.engine().integer_type(),
        engine.engine().text_type(),
        engine.engine().blob_type(),
        engine.engine().table_suffix(),
        relation_sys_queue_messages = engine.relation_name("sys_queue_messages"),
    );
    attempt.execute(SqlStatement::new(sql, vec![])).await?;
    attempt
        .execute(SqlStatement::new(
            crate::sql_template::sql_template!(
                engine,
                "CREATE INDEX IF NOT EXISTS "["sys_queue_due"],
                " ON "["sys_queue_messages"],
                " (incarnation, due_at, message_id)"
            )
            .to_owned(),
            vec![],
        ))
        .await?;
    attempt
        .execute(SqlStatement::new(
            crate::sql_template::sql_template!(
                engine,
                "CREATE UNIQUE INDEX IF NOT EXISTS "["sys_queue_receipt"],
                " ON "["sys_queue_messages"],
                " (incarnation, receipt_handle)"
            )
            .to_owned(),
            vec![],
        ))
        .await?;
    Ok(())
}

pub(crate) async fn read_due_messages<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    request: &QueueDueRequest,
) -> StorageResult<Vec<QueueMessageRecord>> {
    let sql = format!(
        "SELECT incarnation, message_id, due_at, expires_at, receipt_handle, row_blob FROM \
         {relation_sys_queue_messages} WHERE incarnation = {} AND due_at <= {} ORDER BY due_at, \
         message_id LIMIT {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        relation_sys_queue_messages = engine.relation_name("sys_queue_messages"),
    );
    let page = native
        .query(
            SqlStatement::new(
                sql,
                vec![
                    SqlValue::Blob(request.incarnation.as_bytes().to_vec()),
                    SqlValue::Integer(request.captured_at.timestamp_millis()),
                    SqlValue::Integer(i64::from(request.limit.get())),
                ],
            ),
            ReadProtection::Conflict,
            MESSAGE_READ_BYTES,
        )
        .await?;
    page.rows.into_iter().map(decode_row).collect()
}

pub(crate) async fn read_messages_by_receipt<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    incarnation: QueueIncarnation,
    receipts: Vec<ReceiptHandle>,
) -> StorageResult<Vec<Option<QueueMessageRecord>>> {
    let sql = format!(
        "SELECT incarnation, message_id, due_at, expires_at, receipt_handle, row_blob FROM \
         {relation_sys_queue_messages} WHERE incarnation = {} AND receipt_handle = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_queue_messages = engine.relation_name("sys_queue_messages"),
    );
    let statements = receipts
        .iter()
        .map(|receipt| {
            SqlStatement::new(
                sql.clone(),
                vec![
                    SqlValue::Blob(incarnation.as_bytes().to_vec()),
                    SqlValue::Text(receipt.0.clone()),
                ],
            )
        })
        .collect();
    let pages = native
        .query_batch(statements, ReadProtection::Conflict, MESSAGE_READ_BYTES)
        .await?;
    if pages.len() != receipts.len() {
        return Err(StorageError::internal(
            "SQL queue receipt batch count differs",
        ));
    }
    pages
        .into_iter()
        .zip(receipts)
        .map(|(page, receipt)| {
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "SQL queue receipt lookup is not unique",
                ));
            }
            let record = page.rows.into_iter().next().map(decode_row).transpose()?;
            if record.as_ref().is_some_and(|record| {
                record
                    .message
                    .receipt_handle
                    .as_ref()
                    .is_none_or(|handle| handle.0 != receipt.0)
            }) {
                return Err(StorageError::internal(
                    "SQL queue receipt differs from message",
                ));
            }
            Ok(record)
        })
        .collect()
}

pub(crate) async fn read_messages_by_id<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    incarnation: QueueIncarnation,
    ids: Vec<MessageId>,
) -> StorageResult<Vec<Option<QueueMessageRecord>>> {
    let sql = format!(
        "SELECT incarnation, message_id, due_at, expires_at, receipt_handle, row_blob FROM \
         {relation_sys_queue_messages} WHERE incarnation = {} AND message_id = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_queue_messages = engine.relation_name("sys_queue_messages"),
    );
    let statements = ids
        .iter()
        .map(|id| {
            SqlStatement::new(
                sql.clone(),
                vec![
                    SqlValue::Blob(incarnation.as_bytes().to_vec()),
                    SqlValue::Blob(id.as_bytes().to_vec()),
                ],
            )
        })
        .collect();
    let pages = native
        .query_batch(statements, ReadProtection::Conflict, MESSAGE_READ_BYTES)
        .await?;
    if pages.len() != ids.len() {
        return Err(StorageError::internal(
            "SQL queue message batch count differs",
        ));
    }
    pages
        .into_iter()
        .zip(ids)
        .map(|(page, id)| {
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "SQL queue message lookup is not unique",
                ));
            }
            let record = page.rows.into_iter().next().map(decode_row).transpose()?;
            if record
                .as_ref()
                .is_some_and(|row| row.incarnation != incarnation || row.message.message_id != id)
            {
                return Err(StorageError::internal(
                    "SQL queue message lookup identity differs",
                ));
            }
            Ok(record)
        })
        .collect()
}

pub(crate) async fn write_message_records<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    records: Vec<(QueueMessageRecord, Option<QueueMessageRecord>)>,
) -> StorageResult<()> {
    if records.is_empty() {
        return Ok(());
    }
    let expected = records.len();
    let statements = records
        .into_iter()
        .map(|(record, previous)| write_message_statement(engine, record, previous))
        .collect::<StorageResult<Vec<_>>>()?;
    let changed = native.execute_batch(statements).await?;
    if changed.len() != expected {
        return Err(StorageError::internal(
            "SQL queue write batch count differs",
        ));
    }
    for count in changed {
        expect_one(count, "SQL queue message changed during write")?;
    }
    Ok(())
}

fn write_message_statement(
    engine: impl SqlLayout,
    record: QueueMessageRecord,
    previous: Option<QueueMessageRecord>,
) -> StorageResult<SqlStatement> {
    let blob = storage_serde::to_bytes(&record)?;
    let receipt = receipt_value(&record);
    let due = record.visible_at().timestamp_millis();
    let expires = record.expires_at.timestamp_millis();
    let incarnation = SqlValue::Blob(record.incarnation.as_bytes().to_vec());
    let id = SqlValue::Blob(record.message.message_id.as_bytes().to_vec());
    let statement = if let Some(previous) = previous {
        let mut sql = format!(
            "UPDATE {relation_sys_queue_messages} SET due_at = {}, expires_at = {}, \
             receipt_handle = {}, row_blob = {} WHERE incarnation = {} AND message_id = {} AND \
             due_at = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            engine.engine().placeholder(5),
            engine.engine().placeholder(6),
            engine.engine().placeholder(7),
            relation_sys_queue_messages = engine.relation_name("sys_queue_messages"),
        );
        let mut binds = vec![
            SqlValue::Integer(due),
            SqlValue::Integer(expires),
            receipt,
            SqlValue::Blob(blob),
            incarnation,
            id,
            SqlValue::Integer(previous.visible_at().timestamp_millis()),
        ];
        if let Some(handle) = previous.message.receipt_handle {
            sql.push_str(&format!(
                " AND receipt_handle = {}",
                engine.engine().placeholder(8)
            ));
            binds.push(SqlValue::Text(handle.0));
        } else {
            sql.push_str(" AND receipt_handle IS NULL");
        }
        SqlStatement::new(sql, binds)
    } else {
        let sql = format!(
            "INSERT INTO {relation_sys_queue_messages} (incarnation, message_id, due_at, \
             expires_at, receipt_handle, row_blob) VALUES ({}) ON CONFLICT DO NOTHING",
            engine.engine().placeholders(6),
            relation_sys_queue_messages = engine.relation_name("sys_queue_messages"),
        );
        SqlStatement::new(
            sql,
            vec![
                incarnation,
                id,
                SqlValue::Integer(due),
                SqlValue::Integer(expires),
                receipt,
                SqlValue::Blob(blob),
            ],
        )
    };
    Ok(statement)
}

pub(crate) async fn delete_message_records<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    records: Vec<QueueMessageRecord>,
) -> StorageResult<()> {
    if records.is_empty() {
        return Ok(());
    }
    let expected = records.len();
    let statements = records
        .into_iter()
        .map(|record| delete_message_statement(engine, record))
        .collect();
    let changed = native.execute_batch(statements).await?;
    if changed.len() != expected {
        return Err(StorageError::internal(
            "SQL queue delete batch count differs",
        ));
    }
    for count in changed {
        expect_one(count, "SQL queue message changed during delete")?;
    }
    Ok(())
}

fn delete_message_statement(engine: impl SqlLayout, record: QueueMessageRecord) -> SqlStatement {
    let mut sql = format!(
        "DELETE FROM {relation_sys_queue_messages} WHERE incarnation = {} AND message_id = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_queue_messages = engine.relation_name("sys_queue_messages"),
    );
    let mut binds = vec![
        SqlValue::Blob(record.incarnation.as_bytes().to_vec()),
        SqlValue::Blob(record.message.message_id.as_bytes().to_vec()),
    ];
    if let Some(receipt) = record.message.receipt_handle {
        sql.push_str(&format!(
            " AND receipt_handle = {}",
            engine.engine().placeholder(3)
        ));
        binds.push(SqlValue::Text(receipt.0));
    } else {
        sql.push_str(" AND receipt_handle IS NULL");
    }
    SqlStatement::new(sql, binds)
}

pub(crate) async fn count_queue_messages<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    incarnation: QueueIncarnation,
    captured_at: TimestampMillis,
) -> StorageResult<QueueMessageCounts> {
    let now = engine.engine().placeholder(2);
    let sql = format!(
        "SELECT COALESCE(SUM(CASE WHEN due_at <= {now} THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE \
         WHEN due_at > {now} AND receipt_handle IS NOT NULL THEN 1 ELSE 0 END), 0), \
         COALESCE(SUM(CASE WHEN due_at > {now} AND receipt_handle IS NULL THEN 1 ELSE 0 END), 0) \
         FROM {relation_sys_queue_messages} WHERE incarnation = {} AND expires_at > {now}",
        engine.engine().placeholder(1),
        relation_sys_queue_messages = engine.relation_name("sys_queue_messages"),
    );
    let page = native
        .query(
            SqlStatement::new(
                sql,
                vec![
                    SqlValue::Blob(incarnation.as_bytes().to_vec()),
                    SqlValue::Integer(captured_at.timestamp_millis()),
                ],
            ),
            ReadProtection::Snapshot,
            MESSAGE_READ_BYTES,
        )
        .await?;
    let [row] = page
        .rows
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue counts row is missing"))?;
    let [visible, not_visible, delayed] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue counts row has invalid width"))?;
    Ok(QueueMessageCounts {
        visible: count_value(visible)?,
        not_visible: count_value(not_visible)?,
        delayed: count_value(delayed)?,
    })
}

pub(crate) async fn read_message_cleanup_page<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    request: &QueueCleanupRequest,
) -> StorageResult<QueueCleanupPage> {
    let mut binds = Vec::new();
    let predicate = if let Some(cursor) = request.exclusive_start {
        let incarnation = engine.engine().placeholder(1);
        binds.push(SqlValue::Blob(cursor.incarnation.as_bytes().to_vec()));
        binds.push(SqlValue::Blob(cursor.message_id.as_bytes().to_vec()));
        format!(
            "WHERE incarnation > {incarnation} OR (incarnation = {incarnation} AND message_id > \
             {})",
            engine.engine().placeholder(2)
        )
    } else {
        String::new()
    };
    binds.push(SqlValue::Integer(i64::from(request.limit.get()) + 1));
    let sql = format!(
        "SELECT incarnation, message_id, due_at, expires_at, receipt_handle, row_blob FROM \
         {relation_sys_queue_messages} {predicate} ORDER BY incarnation, message_id LIMIT {}",
        engine.engine().placeholder(binds.len()),
        relation_sys_queue_messages = engine.relation_name("sys_queue_messages"),
    );
    let page = native
        .query(
            SqlStatement::new(sql, binds),
            ReadProtection::Conflict,
            CLEANUP_READ_BYTES,
        )
        .await?;
    Ok(QueueCleanupPage {
        rows: page
            .rows
            .into_iter()
            .map(decode_row)
            .collect::<StorageResult<Vec<_>>>()?,
        more: page.more,
    })
}

fn decode_row(row: SqlRow) -> StorageResult<QueueMessageRecord> {
    let [incarnation, id, due, expires, receipt, bytes] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue message row has invalid width"))?;
    let (
        SqlValue::Blob(incarnation),
        SqlValue::Blob(id),
        SqlValue::Integer(due),
        SqlValue::Integer(expires),
        SqlValue::Blob(bytes),
    ) = (incarnation, id, due, expires, bytes)
    else {
        return Err(StorageError::internal(
            "SQL queue message row has invalid types",
        ));
    };
    let id = id
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue message ID has invalid length"))?;
    let record: QueueMessageRecord = storage_serde::from_bytes(&bytes)?;
    let receipt = match receipt {
        SqlValue::Null => None,
        SqlValue::Text(value) => Some(value),
        _ => {
            return Err(StorageError::internal(
                "SQL queue message receipt has invalid type",
            ));
        }
    };
    if record.incarnation != QueueIncarnation::try_from(incarnation.as_slice())?
        || record.message.message_id != MessageId::from_bytes(id)
        || record.visible_at().timestamp_millis() != due
        || record.expires_at.timestamp_millis() != expires
        || record
            .message
            .receipt_handle
            .as_ref()
            .map(|handle| handle.0.as_str())
            != receipt.as_deref()
    {
        return Err(StorageError::internal("SQL queue message identity differs"));
    }
    Ok(record)
}

fn receipt_value(record: &QueueMessageRecord) -> SqlValue {
    record
        .message
        .receipt_handle
        .as_ref()
        .map_or(SqlValue::Null, |handle| SqlValue::Text(handle.0.clone()))
}

fn count_value(value: SqlValue) -> StorageResult<u64> {
    let SqlValue::Integer(value) = value else {
        return Err(StorageError::internal("SQL queue count has invalid type"));
    };
    u64::try_from(value).map_err(|_| StorageError::internal("SQL queue count is negative"))
}
