//! One publication intent and immutable snapshot layout for every SQL driver.

use pubsub_provider::{
    AcceptedPublication, DueRow, PublishCandidate, PublishDueRequest, PublishIntent,
    PublishIntentClaim, UndecodableRow,
};
use storage_types::{
    MissExpectation, ReadProtection, StorageError, StorageResult, TimestampMillis, storage_serde,
};

use crate::{SqlAttempt, SqlLayout, SqlStatement, SqlValue, constants::PUBSUB_CATALOG_READ_BYTES};

pub(crate) async fn initialize_pubsub_publications<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let intent = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_pubsub_publish_intents} (message_id {} PRIMARY \
         KEY, generation {} NOT NULL, due_at {} NOT NULL, next_chunk {} NOT NULL, row_blob {} NOT \
         NULL){}",
        engine.engine().text_type(),
        engine.engine().text_type(),
        engine.engine().integer_type(),
        engine.engine().integer_type(),
        engine.engine().blob_type(),
        engine.engine().table_suffix(),
        relation_sys_pubsub_publish_intents = engine.relation_name("sys_pubsub_publish_intents"),
    );
    let chunk = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_pubsub_publish_chunks} (generation {} NOT NULL, \
         chunk_index {} NOT NULL, row_blob {} NOT NULL, PRIMARY KEY(generation, chunk_index)){}",
        engine.engine().text_type(),
        engine.engine().integer_type(),
        engine.engine().blob_type(),
        engine.engine().table_suffix(),
        relation_sys_pubsub_publish_chunks = engine.relation_name("sys_pubsub_publish_chunks"),
    );
    native.execute(SqlStatement::new(intent, vec![])).await?;
    native.execute(SqlStatement::new(chunk, vec![])).await?;
    native
        .execute(SqlStatement::new(
            crate::sql_template::sql_template!(
                engine,
                "CREATE INDEX IF NOT EXISTS "["sys_pubsub_publish_due"],
                " ON "["sys_pubsub_publish_intents"],
                "(due_at, message_id)"
            )
            .into_owned(),
            vec![],
        ))
        .await?;
    Ok(())
}

/// The inserts of one publication: one chunk row per chunk, then the
/// intent. Each must insert exactly one row (`ON CONFLICT DO NOTHING`
/// makes a duplicate a zero count: a conflict, as `expect_insert` is).
pub(crate) fn publication_statements(
    engine: impl SqlLayout,
    publication: AcceptedPublication,
) -> StorageResult<Vec<SqlStatement>> {
    let generation = publication.snapshot.root.generation.to_string();
    let chunk_sql = format!(
        "INSERT INTO {relation_sys_pubsub_publish_chunks} (generation, chunk_index, row_blob) \
         VALUES ({}) ON CONFLICT DO NOTHING",
        engine.engine().placeholders(3),
        relation_sys_pubsub_publish_chunks = engine.relation_name("sys_pubsub_publish_chunks"),
    );
    let mut statements = Vec::with_capacity(publication.snapshot.chunks.len() + 1);
    for (index, chunk) in publication.snapshot.chunks.iter().enumerate() {
        let index = i64::try_from(index)
            .map_err(|_| StorageError::internal("publish chunk index exceeds SQL range"))?;
        statements.push(SqlStatement::new(
            chunk_sql.clone(),
            vec![
                SqlValue::Text(generation.clone()),
                SqlValue::Integer(index),
                SqlValue::Blob(storage_serde::to_bytes(chunk)?),
            ],
        ));
    }
    let intent_sql = format!(
        "INSERT INTO {relation_sys_pubsub_publish_intents} (message_id, generation, due_at, \
         next_chunk, row_blob) VALUES ({}) ON CONFLICT DO NOTHING",
        engine.engine().placeholders(5),
        relation_sys_pubsub_publish_intents = engine.relation_name("sys_pubsub_publish_intents"),
    );
    statements.push(SqlStatement::new(
        intent_sql,
        vec![
            SqlValue::Text(publication.intent.acceptance.message_id.to_string()),
            SqlValue::Text(generation),
            SqlValue::Integer(publication.intent.due_at().timestamp_millis()),
            SqlValue::Integer(0),
            SqlValue::Blob(storage_serde::to_bytes(&publication.intent)?),
        ],
    ));
    Ok(statements)
}

pub(crate) async fn read_due_publish_chunks<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    request: &PublishDueRequest,
) -> StorageResult<Vec<DueRow<PublishCandidate>>> {
    // One bounded range of the due index, then the current chunk of each
    // intent. The window and slot are for ordered-key stores.
    let sql = format!(
        "WITH due AS (SELECT message_id, generation, due_at, next_chunk, row_blob FROM \
         {relation_sys_pubsub_publish_intents} WHERE due_at <= {now} ORDER BY due_at, message_id \
         LIMIT {limit}) SELECT i.message_id, i.generation, i.due_at, i.next_chunk, i.row_blob, \
         c.row_blob FROM due i LEFT JOIN {relation_sys_pubsub_publish_chunks} c ON c.generation = \
         i.generation AND c.chunk_index = i.next_chunk ORDER BY i.due_at, i.message_id",
        now = engine.engine().placeholder(1),
        limit = engine.engine().placeholder(2),
        relation_sys_pubsub_publish_intents = engine.relation_name("sys_pubsub_publish_intents"),
        relation_sys_pubsub_publish_chunks = engine.relation_name("sys_pubsub_publish_chunks"),
    );
    let page = native
        .query(
            SqlStatement::new(
                sql,
                vec![
                    SqlValue::Integer(request.now.timestamp_millis()),
                    SqlValue::Integer(i64::from(request.limit.get())),
                ],
            ),
            // The conditional intent update below owns claim conflicts.
            // Postgres cannot apply FOR UPDATE to the nullable chunk side of
            // this join.
            ReadProtection::Snapshot,
            PUBSUB_CATALOG_READ_BYTES,
        )
        .await?;
    page.rows
        .into_iter()
        .map(|row| {
            let [id, generation, due, next_chunk, intent_blob, chunk_blob] = row
                .into_values()
                .try_into()
                .map_err(|_| StorageError::internal("SQL publish claim row has invalid width"))?;
            let (SqlValue::Text(key), SqlValue::Integer(due_at)) = (&id, &due) else {
                return Err(StorageError::internal(
                    "SQL publish claim key has invalid type",
                ));
            };
            let (key, due_at) = (key.clone(), *due_at);
            // A row that does not decode is a typed observation (review D4).
            let undecodable = |reason| {
                DueRow::Undecodable(UndecodableRow {
                    key: key.clone(),
                    due_at: TimestampMillis::from_timestamp(due_at),
                    reason,
                })
            };
            let Ok(intent) = decode_intent([id, generation, due, next_chunk, intent_blob]) else {
                return Ok(undecodable("publish intent does not decode"));
            };
            let subscriptions = match chunk_blob {
                SqlValue::Blob(_) if intent.snapshot.chunk_count == 0 => {
                    return Ok(undecodable("empty publish has a chunk"));
                }
                SqlValue::Blob(bytes) => match storage_serde::from_bytes(&bytes) {
                    Ok(subscriptions) => subscriptions,
                    Err(_) => return Ok(undecodable("publish chunk does not decode")),
                },
                SqlValue::Null if intent.snapshot.chunk_count == 0 => Vec::new(),
                SqlValue::Null => return Ok(undecodable("publish chunk is missing")),
                _ => return Err(StorageError::internal("SQL publish chunk has invalid type")),
            };
            Ok(DueRow::Decoded(PublishCandidate {
                intent,
                subscriptions,
                due_at: TimestampMillis::from_timestamp(due_at),
            }))
        })
        .collect()
}

/// Move the due time of one undecodable intent (review D4).
pub(crate) async fn move_publish_due<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    row: &UndecodableRow,
    until: TimestampMillis,
) -> StorageResult<()> {
    let sql = format!(
        "UPDATE {relation_sys_pubsub_publish_intents} SET due_at = {} WHERE message_id = {} AND \
         due_at = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        relation_sys_pubsub_publish_intents = engine.relation_name("sys_pubsub_publish_intents"),
    );
    expect_insert(
        native
            .execute(SqlStatement::new(
                sql,
                vec![
                    SqlValue::Integer(until.timestamp_millis()),
                    SqlValue::Text(row.key.clone()),
                    SqlValue::Integer(row.due_at.timestamp_millis()),
                ],
            ))
            .await?,
    )
}

/// The least due time of an intent, without a lock: the head of the due
/// index (an unleased intent is due at its acceptance, review D2).
pub(crate) async fn read_next_publish_due<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<Option<TimestampMillis>> {
    let relation = engine.relation_name("sys_pubsub_publish_intents");
    let page = native
        .query(
            SqlStatement::new(
                format!("SELECT due_at FROM {relation} ORDER BY due_at, message_id LIMIT 1"),
                vec![],
            ),
            ReadProtection::Snapshot,
            PUBSUB_CATALOG_READ_BYTES,
        )
        .await?;
    page.rows
        .into_iter()
        .next()
        .map(|row| match row.into_values().as_slice() {
            [SqlValue::Integer(due)] => Ok(TimestampMillis::from_timestamp(*due)),
            _ => Err(StorageError::internal("SQL publish due has invalid type")),
        })
        .transpose()
}

pub(crate) async fn replace_publish_intent<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    expected: PublishIntent,
    next: Option<PublishIntent>,
) -> StorageResult<()> {
    if let Some(next) = next {
        return expect_insert(
            native
                .execute(update_intent_statement(engine, expected, next)?)
                .await?,
        );
    }
    let id = expected.acceptance.message_id.to_string();
    let previous = storage_serde::to_bytes(&expected)?;
    let sql = format!(
        "DELETE FROM {relation_sys_pubsub_publish_intents} WHERE message_id = {} AND row_blob = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_pubsub_publish_intents = engine.relation_name("sys_pubsub_publish_intents"),
    );
    expect_insert(
        native
            .execute(SqlStatement::new(
                sql,
                vec![SqlValue::Text(id), SqlValue::Blob(previous)],
            ))
            .await?,
    )?;
    let sql = format!(
        "DELETE FROM {relation_sys_pubsub_publish_chunks} WHERE generation = {}",
        engine.engine().placeholder(1),
        relation_sys_pubsub_publish_chunks = engine.relation_name("sys_pubsub_publish_chunks"),
    );
    native
        .execute(SqlStatement::new(
            sql,
            vec![SqlValue::Text(expected.snapshot.generation.to_string())],
        ))
        .await?;
    Ok(())
}

pub(crate) async fn replace_publish_intents<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    replacements: Vec<PublishIntentClaim>,
) -> StorageResult<()> {
    // The `due_at` column is set from the claimed intent; the blob compare
    // owns the condition, so the scanned due time is not needed here.
    let statements = replacements
        .into_iter()
        .map(|claim| update_intent_statement(engine, claim.previous, claim.claimed))
        .collect::<StorageResult<Vec<_>>>()?;
    let expected = statements.len();
    let changed = native.execute_batch(statements).await?;
    if changed.len() != expected {
        return Err(StorageError::internal(
            "SQL publish claim batch returned the wrong count",
        ));
    }
    for count in changed {
        expect_insert(count)?;
    }
    Ok(())
}

fn update_intent_statement(
    engine: impl SqlLayout,
    expected: PublishIntent,
    next: PublishIntent,
) -> StorageResult<SqlStatement> {
    let sql = format!(
        "UPDATE {relation_sys_pubsub_publish_intents} SET due_at = {}, next_chunk = {}, row_blob \
         = {} WHERE message_id = {} AND row_blob = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        engine.engine().placeholder(4),
        engine.engine().placeholder(5),
        relation_sys_pubsub_publish_intents = engine.relation_name("sys_pubsub_publish_intents"),
    );
    Ok(SqlStatement::new(
        sql,
        vec![
            SqlValue::Integer(next.due_at().timestamp_millis()),
            SqlValue::Integer(i64::from(next.next_chunk)),
            SqlValue::Blob(storage_serde::to_bytes(&next)?),
            SqlValue::Text(expected.acceptance.message_id.to_string()),
            SqlValue::Blob(storage_serde::to_bytes(&expected)?),
        ],
    ))
}

fn decode_intent(values: [SqlValue; 5]) -> StorageResult<PublishIntent> {
    let [id, generation, due, next_chunk, blob] = values;
    let (
        SqlValue::Text(id),
        SqlValue::Text(generation),
        // The `due_at` column is the only due source (finding 2); a lease
        // in the blob that disagrees is rewritten by the next claim.
        SqlValue::Integer(_),
        SqlValue::Integer(next_chunk),
        SqlValue::Blob(blob),
    ) = (id, generation, due, next_chunk, blob)
    else {
        return Err(StorageError::internal(
            "SQL publish intent has invalid types",
        ));
    };
    let intent: PublishIntent = storage_serde::from_bytes(&blob)?;
    if intent.acceptance.message_id.as_str() != id
        || intent.snapshot.generation.to_string() != generation
        || i64::from(intent.next_chunk) != next_chunk
    {
        return Err(StorageError::internal("SQL publish intent columns differ"));
    }
    Ok(intent)
}

pub(crate) fn expect_insert(changed: u64) -> StorageResult<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(StorageError::write_miss(
            "pubsub_publish_identity",
            MissExpectation::Absent,
        ))
    }
}
