//! One SQL queue message layout for SQLite, Turso, and Postgres.

use std::{borrow::Cow, num::NonZeroU32};

use queue_provider::{
    MessageId, QueueCleanupPage, QueueCleanupRequest, QueueCountFacts, QueueDueBatch, QueueDueMark,
    QueueDueRequest, QueueIncarnation, QueueMessageBody, QueueMessageCounts, QueueMessageRecord,
    QueueMessageState, ReceiptHandle,
};
use storage_types::{
    Exact, ReadProtection, StorageError, StorageResult, TimestampMillis, storage_serde,
};

use crate::{
    SqlAttempt, SqlLayout, SqlRow, SqlStatement, SqlValue,
    constants::{
        QUEUE_CLEANUP_READ_BYTES as CLEANUP_READ_BYTES,
        QUEUE_MESSAGE_READ_BYTES as MESSAGE_READ_BYTES,
    },
    queue_catalog::{self, expect_one},
};

pub(crate) async fn initialize_queue_messages<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_queue_messages} (incarnation {} NOT NULL, \
         message_id {} NOT NULL, due_at {} NOT NULL, expires_at {} NOT NULL, receipt_handle {}, \
         state_blob {} NOT NULL, body_blob {} NOT NULL, PRIMARY KEY (incarnation, message_id)){}",
        engine.engine().blob_type(),
        engine.engine().blob_type(),
        engine.engine().integer_type(),
        engine.engine().integer_type(),
        engine.engine().text_type(),
        engine.engine().blob_type(),
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

/// One wave of three statements (four with a dead-letter target, whose
/// catalog row is read by name with `Snapshot`): the claim of the head with
/// `Claim` protection (`FOR UPDATE SKIP LOCKED` on Postgres, which re-checks
/// the `WHERE` clause on the latest row version), an unlocked count of at most
/// `limit + 1` due rows, and the unlocked marks of the rows that are not due
/// (after `captured_at`, or leased inside the clock-skew margin). The
/// unlocked statements do not lock, so concurrent claims do not skip the rows
/// that they observe.
pub(crate) async fn read_due_messages<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    request: &QueueDueRequest,
) -> StorageResult<QueueDueBatch> {
    let claim = crate::sql_template::sql_template!(engine,
        "SELECT incarnation, message_id, due_at, expires_at, receipt_handle, state_blob, body_blob FROM "
            ["sys_queue_messages"],
        " WHERE incarnation = " {1},
        " AND due_at <= " {2},
        " AND (receipt_handle IS NULL OR due_at <= " {3},
        ") ORDER BY due_at, message_id LIMIT " {4},
    );
    let count = crate::sql_template::sql_template!(engine,
        "SELECT COUNT(*) FROM (SELECT 1 FROM " ["sys_queue_messages"],
        " WHERE incarnation = " {1},
        " AND due_at <= " {2},
        " AND (receipt_handle IS NULL OR due_at <= " {3},
        ") LIMIT " {4},
        ") AS due_rows",
    );
    // Every row that is not due for this claim: later rows, and leased rows
    // inside the clock-skew margin (`leased_due_by < due_at <= captured_at`).
    let after = crate::sql_template::sql_template!(engine,
        "SELECT due_at, receipt_handle FROM " ["sys_queue_messages"],
        " WHERE incarnation = " {1},
        " AND due_at > " {3},
        " AND (due_at > " {2},
        " OR receipt_handle IS NOT NULL) ORDER BY due_at, message_id LIMIT " {4},
    );
    let incarnation = || SqlValue::Blob(request.incarnation.as_bytes().to_vec());
    let captured_at = || SqlValue::Integer(request.captured_at.timestamp_millis());
    let binds = |limit: u32| {
        vec![
            incarnation(),
            captured_at(),
            SqlValue::Integer(request.leased_due_by().timestamp_millis()),
            SqlValue::Integer(i64::from(limit)),
        ]
    };
    let mut statements = vec![
        (
            SqlStatement::new(claim, binds(request.limit.get())),
            ReadProtection::Claim,
        ),
        (
            SqlStatement::new(count, binds(request.limit.get().saturating_add(1))),
            ReadProtection::Snapshot,
        ),
        (
            SqlStatement::new(after, binds(request.head.get())),
            ReadProtection::Snapshot,
        ),
    ];
    if let Some(name) = &request.dead_letter_target {
        statements.push((
            queue_catalog::queue_by_name_statement(engine, name),
            ReadProtection::Snapshot,
        ));
    }
    let pages = native
        .query_batch_protected_exact(statements, MESSAGE_READ_BYTES)
        .await?;
    let ([claimed, counted, after], dead_letter_target) = match &request.dead_letter_target {
        Some(name) => {
            let [claimed, counted, after, catalog] = pages.into_array::<4>()?;
            (
                [claimed, counted, after],
                queue_catalog::decode_named_page(catalog, name)?,
            )
        }
        None => (pages.into_array::<3>()?, None),
    };
    let records = claimed
        .rows
        .into_iter()
        .map(decode_record_row)
        .collect::<StorageResult<Vec<_>>>()?;
    let [row] = counted
        .rows
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue due count row is missing"))?;
    let [due_count] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue due count row has invalid width"))?;
    let after = after
        .rows
        .into_iter()
        .map(decode_mark)
        .collect::<StorageResult<Vec<_>>>()?;
    let (drained, next_due) = request.head_outcome(records.len(), count_value(due_count)?, &after);
    Ok(QueueDueBatch {
        records,
        drained,
        next_due,
        dead_letter_target,
    })
}

pub(crate) async fn read_due_head<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    incarnation: QueueIncarnation,
    limit: NonZeroU32,
) -> StorageResult<Vec<QueueDueMark>> {
    let sql = format!(
        "SELECT due_at, receipt_handle FROM {relation_sys_queue_messages} WHERE incarnation = {} \
         ORDER BY due_at, message_id LIMIT {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_queue_messages = engine.relation_name("sys_queue_messages"),
    );
    let page = native
        .query(
            SqlStatement::new(
                sql,
                vec![
                    SqlValue::Blob(incarnation.as_bytes().to_vec()),
                    SqlValue::Integer(i64::from(limit.get())),
                ],
            ),
            ReadProtection::Snapshot,
            MESSAGE_READ_BYTES,
        )
        .await?;
    page.rows.into_iter().map(decode_mark).collect()
}

fn decode_mark(row: SqlRow) -> StorageResult<QueueDueMark> {
    let [due, receipt] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue due mark has invalid width"))?;
    let SqlValue::Integer(due) = due else {
        return Err(StorageError::internal(
            "SQL queue due time has invalid type",
        ));
    };
    let leased = match receipt {
        SqlValue::Null => false,
        SqlValue::Text(_) => true,
        _ => {
            return Err(StorageError::internal(
                "SQL queue message receipt has invalid type",
            ));
        }
    };
    Ok(QueueDueMark {
        due_at: TimestampMillis::from_timestamp(due),
        leased,
    })
}

fn state_by_receipt_sql(engine: impl SqlLayout) -> Cow<'static, str> {
    crate::sql_template::sql_template!(engine,
        "SELECT incarnation, message_id, due_at, expires_at, receipt_handle, state_blob FROM "
            ["sys_queue_messages"],
        " WHERE incarnation = " {1},
        " AND receipt_handle = " {2},
    )
}

fn state_by_id_sql(engine: impl SqlLayout) -> Cow<'static, str> {
    crate::sql_template::sql_template!(engine,
        "SELECT incarnation, message_id, due_at, expires_at, receipt_handle, state_blob FROM "
            ["sys_queue_messages"],
        " WHERE incarnation = " {1},
        " AND message_id = " {2},
    )
}

fn record_by_id_sql(engine: impl SqlLayout) -> Cow<'static, str> {
    crate::sql_template::sql_template!(engine,
        "SELECT incarnation, message_id, due_at, expires_at, receipt_handle, state_blob, body_blob FROM "
            ["sys_queue_messages"],
        " WHERE incarnation = " {1},
        " AND message_id = " {2},
    )
}

pub(crate) async fn read_messages_by_receipt<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    incarnation: QueueIncarnation,
    receipts: Vec<ReceiptHandle>,
) -> StorageResult<storage_types::Exact<Option<QueueMessageState>>> {
    if receipts.is_empty() {
        return Ok(Exact::empty());
    }
    let sql = state_by_receipt_sql(engine);
    // Lock in receipt order so that two batches with the same receipts in
    // another order cannot deadlock on Postgres.
    let mut order = (0..receipts.len()).collect::<Vec<_>>();
    order.sort_by(|left, right| receipts[*left].0.cmp(&receipts[*right].0));
    let statements = order
        .iter()
        .map(|index| {
            SqlStatement::new(
                sql.clone(),
                vec![
                    SqlValue::Blob(incarnation.as_bytes().to_vec()),
                    SqlValue::Text(receipts[*index].0.clone()),
                ],
            )
        })
        .collect();
    let sorted = native
        .query_batch_exact(statements, ReadProtection::Conflict, MESSAGE_READ_BYTES)
        .await?;
    sorted
        .restore_order(order)
        .try_zip_map(receipts, |receipt, page| {
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "SQL queue receipt lookup is not unique",
                ));
            }
            let state = page
                .rows
                .into_iter()
                .next()
                .map(decode_state_row)
                .transpose()?;
            if state.as_ref().is_some_and(|state| {
                state
                    .receipt_handle
                    .as_ref()
                    .is_none_or(|handle| handle.0 != receipt.0)
            }) {
                return Err(StorageError::internal(
                    "SQL queue receipt differs from message",
                ));
            }
            Ok(state)
        })
}

pub(crate) async fn read_messages_by_id<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    incarnation: QueueIncarnation,
    ids: Vec<MessageId>,
) -> StorageResult<storage_types::Exact<Option<QueueMessageRecord>>> {
    read_rows_by_id(
        native,
        record_by_id_sql(engine),
        incarnation,
        ids,
        decode_record_row,
        |record: &QueueMessageRecord| (record.incarnation, record.message.message_id),
    )
    .await
}

pub(crate) async fn read_message_states_by_id<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    incarnation: QueueIncarnation,
    ids: Vec<MessageId>,
) -> StorageResult<storage_types::Exact<Option<QueueMessageState>>> {
    read_rows_by_id(
        native,
        state_by_id_sql(engine),
        incarnation,
        ids,
        decode_state_row,
        |state: &QueueMessageState| (state.incarnation, state.message_id),
    )
    .await
}

/// One protected point statement per id, one batch, input order.
async fn read_rows_by_id<A: SqlAttempt, T>(
    native: &mut A,
    sql: Cow<'static, str>,
    incarnation: QueueIncarnation,
    ids: Vec<MessageId>,
    decode: fn(SqlRow) -> StorageResult<T>,
    identity: fn(&T) -> (QueueIncarnation, MessageId),
) -> StorageResult<storage_types::Exact<Option<T>>> {
    if ids.is_empty() {
        return Ok(Exact::empty());
    }
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
        .query_batch_exact(statements, ReadProtection::Conflict, MESSAGE_READ_BYTES)
        .await?;
    pages.try_zip_map(ids, |id, page| {
        if page.more || page.rows.len() > 1 {
            return Err(StorageError::internal(
                "SQL queue message lookup is not unique",
            ));
        }
        let row = page.rows.into_iter().next().map(decode).transpose()?;
        if row
            .as_ref()
            .is_some_and(|row| identity(row) != (incarnation, id))
        {
            return Err(StorageError::internal(
                "SQL queue message lookup identity differs",
            ));
        }
        Ok(row)
    })
}

/// One `INSERT` per record, each expected to change exactly one row.
pub(crate) fn insert_statements(
    engine: impl SqlLayout,
    records: Vec<QueueMessageRecord>,
) -> StorageResult<Vec<SqlStatement>> {
    let sql = crate::sql_template::sql_template!(engine,
        "INSERT INTO " ["sys_queue_messages"],
        " (incarnation, message_id, due_at, expires_at, receipt_handle, state_blob, body_blob) VALUES ("
            {1},
        ", " {2},
        ", " {3},
        ", " {4},
        ", " {5},
        ", " {6},
        ", " {7},
        ") ON CONFLICT DO NOTHING",
    );
    records
        .into_iter()
        .map(|record| {
            let (state, body) = record.into_parts();
            Ok(SqlStatement::new(
                sql.clone(),
                vec![
                    SqlValue::Blob(state.incarnation.as_bytes().to_vec()),
                    SqlValue::Blob(state.message_id.as_bytes().to_vec()),
                    SqlValue::Integer(state.visible_at().timestamp_millis()),
                    SqlValue::Integer(state.expires_at.timestamp_millis()),
                    receipt_value(&state),
                    SqlValue::Blob(storage_serde::to_bytes(&state)?),
                    SqlValue::Blob(storage_serde::to_bytes(&body)?),
                ],
            ))
        })
        .collect()
}

pub(crate) async fn insert_messages<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    records: Vec<QueueMessageRecord>,
) -> StorageResult<()> {
    if records.is_empty() {
        return Ok(());
    }
    let statements = insert_statements(engine, records)?;
    expect_each(
        native.execute_batch_exact(statements).await?,
        "SQL queue message already exists",
    )
}

/// `(new, previous)`: the update matches the previous due time and receipt
/// (the state read in this attempt) and never writes the body.
pub(crate) fn state_write_statements(
    engine: impl SqlLayout,
    changes: Vec<(QueueMessageState, QueueMessageState)>,
) -> StorageResult<Vec<SqlStatement>> {
    changes
        .into_iter()
        .map(|(state, previous)| {
            let mut binds = vec![
                SqlValue::Integer(state.visible_at().timestamp_millis()),
                receipt_value(&state),
                SqlValue::Blob(storage_serde::to_bytes(&state)?),
                SqlValue::Blob(state.incarnation.as_bytes().to_vec()),
                SqlValue::Blob(state.message_id.as_bytes().to_vec()),
                SqlValue::Integer(previous.visible_at().timestamp_millis()),
            ];
            let sql = match previous.receipt_handle {
                Some(handle) => {
                    binds.push(SqlValue::Text(handle.0));
                    crate::sql_template::sql_template!(engine,
                        "UPDATE " ["sys_queue_messages"],
                        " SET due_at = " {1},
                        ", receipt_handle = " {2},
                        ", state_blob = " {3},
                        " WHERE incarnation = " {4},
                        " AND message_id = " {5},
                        " AND due_at = " {6},
                        " AND receipt_handle = " {7},
                    )
                }
                None => crate::sql_template::sql_template!(engine,
                    "UPDATE " ["sys_queue_messages"],
                    " SET due_at = " {1},
                    ", receipt_handle = " {2},
                    ", state_blob = " {3},
                    " WHERE incarnation = " {4},
                    " AND message_id = " {5},
                    " AND due_at = " {6},
                    " AND receipt_handle IS NULL",
                ),
            };
            Ok(SqlStatement::new(sql, binds))
        })
        .collect()
}

pub(crate) async fn write_message_states<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    changes: Vec<(QueueMessageState, QueueMessageState)>,
) -> StorageResult<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let statements = state_write_statements(engine, changes)?;
    expect_each(
        native.execute_batch_exact(statements).await?,
        "SQL queue message changed during write",
    )
}

/// One `DELETE` per state, each expected to change exactly one row.
pub(crate) fn delete_statements(
    engine: impl SqlLayout,
    states: Vec<QueueMessageState>,
) -> Vec<SqlStatement> {
    states
        .into_iter()
        .map(|state| {
            let mut binds = vec![
                SqlValue::Blob(state.incarnation.as_bytes().to_vec()),
                SqlValue::Blob(state.message_id.as_bytes().to_vec()),
            ];
            let sql = match state.receipt_handle {
                Some(handle) => {
                    binds.push(SqlValue::Text(handle.0));
                    crate::sql_template::sql_template!(engine,
                        "DELETE FROM " ["sys_queue_messages"],
                        " WHERE incarnation = " {1},
                        " AND message_id = " {2},
                        " AND receipt_handle = " {3},
                    )
                }
                None => crate::sql_template::sql_template!(engine,
                    "DELETE FROM " ["sys_queue_messages"],
                    " WHERE incarnation = " {1},
                    " AND message_id = " {2},
                    " AND receipt_handle IS NULL",
                ),
            };
            SqlStatement::new(sql, binds)
        })
        .collect()
}

pub(crate) async fn delete_messages<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    states: Vec<QueueMessageState>,
) -> StorageResult<()> {
    if states.is_empty() {
        return Ok(());
    }
    let statements = delete_statements(engine, states);
    expect_each(
        native.execute_batch_exact(statements).await?,
        "SQL queue message changed during delete",
    )
}

fn expect_each(changed: Exact<u64>, message: &'static str) -> StorageResult<()> {
    for count in changed {
        expect_one(count, message)?;
    }
    Ok(())
}

/// Two bounded regions in one statement (decision D6): each stops after
/// `scan_limit` index entries. The statement groups the rows by their
/// physical facts; the provider's bucket rule gives the facts their meaning.
pub(crate) async fn count_queue_messages<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    incarnation: QueueIncarnation,
    captured_at: TimestampMillis,
    scan_limit: NonZeroU32,
) -> StorageResult<QueueMessageCounts> {
    let relation = engine.relation_name("sys_queue_messages");
    let incarnation_value = engine.engine().placeholder(1);
    let now = engine.engine().placeholder(2);
    let limit = engine.engine().placeholder(3);
    let facts = format!(
        "CASE WHEN expires_at > {now} THEN 1 ELSE 0 END AS live, CASE WHEN receipt_handle IS NULL \
         THEN 0 ELSE 1 END AS leased"
    );
    let region = |due: u8, bound: &str| {
        format!(
            "SELECT {due} AS due, {facts} FROM (SELECT expires_at, receipt_handle FROM {relation} \
             WHERE incarnation = {incarnation_value} AND due_at {bound} {now} ORDER BY due_at, \
             message_id LIMIT {limit}) AS region_{due}"
        )
    };
    let sql = format!(
        "SELECT due, leased, live, COUNT(*) FROM ({} UNION ALL {}) AS facts GROUP BY due, leased, \
         live",
        region(1, "<="),
        region(0, ">"),
    );
    let page = native
        .query(
            SqlStatement::new(
                sql,
                vec![
                    SqlValue::Blob(incarnation.as_bytes().to_vec()),
                    SqlValue::Integer(captured_at.timestamp_millis()),
                    SqlValue::Integer(i64::from(scan_limit.get())),
                ],
            ),
            ReadProtection::Snapshot,
            MESSAGE_READ_BYTES,
        )
        .await?;
    let mut counts = QueueMessageCounts::default();
    for row in page.rows {
        let [due, leased, live, rows] = row
            .into_values()
            .try_into()
            .map_err(|_| StorageError::internal("SQL queue counts row has invalid width"))?;
        let facts = QueueCountFacts {
            due: count_value(due)? == 1,
            leased: count_value(leased)? == 1,
            live: count_value(live)? == 1,
        };
        counts.add(facts, count_value(rows)?);
    }
    Ok(counts)
}

pub(crate) async fn read_message_cleanup_page<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    request: &QueueCleanupRequest,
) -> StorageResult<QueueCleanupPage> {
    let limit = SqlValue::Integer(i64::from(request.limit.get()) + 1);
    let (sql, binds) = match request.exclusive_start {
        Some(cursor) => (
            crate::sql_template::sql_template!(engine,
                "SELECT incarnation, message_id, due_at, expires_at, receipt_handle, state_blob FROM "
                    ["sys_queue_messages"],
                " WHERE incarnation > " {1},
                " OR (incarnation = " {1},
                " AND message_id > " {2},
                ") ORDER BY incarnation, message_id LIMIT " {3},
            ),
            vec![
                SqlValue::Blob(cursor.incarnation.as_bytes().to_vec()),
                SqlValue::Blob(cursor.message_id.as_bytes().to_vec()),
                limit,
            ],
        ),
        None => (
            crate::sql_template::sql_template!(engine,
                "SELECT incarnation, message_id, due_at, expires_at, receipt_handle, state_blob FROM "
                    ["sys_queue_messages"],
                " ORDER BY incarnation, message_id LIMIT " {1},
            ),
            vec![limit],
        ),
    };
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
            .map(decode_state_row)
            .collect::<StorageResult<Vec<_>>>()?,
        more: page.more,
    })
}

fn decode_record_row(row: SqlRow) -> StorageResult<QueueMessageRecord> {
    let [incarnation, id, due, expires, receipt, state, body] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue message row has invalid width"))?;
    let state = decode_state(incarnation, id, due, expires, receipt, state)?;
    let SqlValue::Blob(body) = body else {
        return Err(StorageError::internal(
            "SQL queue message body has invalid type",
        ));
    };
    let body: QueueMessageBody = storage_serde::from_bytes(&body)?;
    Ok(QueueMessageRecord::from_parts(state, body))
}

fn decode_state_row(row: SqlRow) -> StorageResult<QueueMessageState> {
    let [incarnation, id, due, expires, receipt, state] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue state row has invalid width"))?;
    decode_state(incarnation, id, due, expires, receipt, state)
}

fn decode_state(
    incarnation: SqlValue,
    id: SqlValue,
    due: SqlValue,
    expires: SqlValue,
    receipt: SqlValue,
    bytes: SqlValue,
) -> StorageResult<QueueMessageState> {
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
    let state: QueueMessageState = storage_serde::from_bytes(&bytes)?;
    let receipt = match receipt {
        SqlValue::Null => None,
        SqlValue::Text(value) => Some(value),
        _ => {
            return Err(StorageError::internal(
                "SQL queue message receipt has invalid type",
            ));
        }
    };
    if state.incarnation != QueueIncarnation::try_from(incarnation.as_slice())?
        || state.message_id != MessageId::from_bytes(id)
        || state.visible_at().timestamp_millis() != due
        || state.expires_at.timestamp_millis() != expires
        || state
            .receipt_handle
            .as_ref()
            .map(|handle| handle.0.as_str())
            != receipt.as_deref()
    {
        return Err(StorageError::internal("SQL queue message identity differs"));
    }
    Ok(state)
}

fn receipt_value(state: &QueueMessageState) -> SqlValue {
    state
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
