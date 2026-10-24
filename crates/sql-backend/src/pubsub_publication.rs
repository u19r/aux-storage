//! One publication intent and immutable snapshot layout for every SQL driver.

use std::num::NonZeroU32;

use pubsub_provider::{AcceptedPublication, PublishCandidate, PublishIntent, PubsubMessageId};
use storage_types::{
    ReadProtection, StorageEnum, StorageError, StorageResult, TimestampMillis, storage_serde,
};

use crate::{SqlAttempt, SqlLayout, SqlStatement, SqlValue, constants::PUBSUB_CATALOG_READ_BYTES};

pub(crate) async fn initialize_pubsub_publications<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let intent = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_pubsub_publish_intents} (message_id {} PRIMARY \
         KEY, generation {} NOT NULL, lease_until {}, next_chunk {} NOT NULL, row_blob {} NOT \
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
                "(lease_until, message_id)"
            )
            .into_owned(),
            vec![],
        ))
        .await?;
    Ok(())
}

pub(crate) async fn read_publish_intent<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    id: &PubsubMessageId,
) -> StorageResult<Option<PublishIntent>> {
    let sql = format!(
        "SELECT message_id, generation, lease_until, next_chunk, row_blob FROM \
         {relation_sys_pubsub_publish_intents} WHERE message_id = {}",
        engine.engine().placeholder(1),
        relation_sys_pubsub_publish_intents = engine.relation_name("sys_pubsub_publish_intents"),
    );
    let page = native
        .query(
            SqlStatement::new(sql, vec![SqlValue::Text(id.to_string())]),
            protection,
            PUBSUB_CATALOG_READ_BYTES,
        )
        .await?;
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL publish intent lookup exceeded one row",
        ));
    }
    page.rows
        .into_iter()
        .next()
        .map(|row| {
            let values = row
                .into_values()
                .try_into()
                .map_err(|_| StorageError::internal("SQL publish intent has invalid width"))?;
            let intent = decode_intent(values)?;
            if intent.acceptance.message_id != *id {
                return Err(StorageError::internal("SQL publish intent key differs"));
            }
            Ok(intent)
        })
        .transpose()
}

pub(crate) async fn read_publish_candidate<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    id: &PubsubMessageId,
) -> StorageResult<Option<PublishCandidate>> {
    let Some(intent) = read_publish_intent(native, engine, protection, id).await? else {
        return Ok(None);
    };
    let subscriptions = if intent.snapshot.chunk_count == 0 {
        Vec::new()
    } else {
        let sql = format!(
            "SELECT row_blob FROM {relation_sys_pubsub_publish_chunks} WHERE generation = {} AND \
             chunk_index = {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            relation_sys_pubsub_publish_chunks = engine.relation_name("sys_pubsub_publish_chunks"),
        );
        let page = native
            .query(
                SqlStatement::new(
                    sql,
                    vec![
                        SqlValue::Text(intent.snapshot.generation.to_string()),
                        SqlValue::Integer(i64::from(intent.next_chunk)),
                    ],
                ),
                protection,
                PUBSUB_CATALOG_READ_BYTES,
            )
            .await?;
        let [row] = page
            .rows
            .try_into()
            .map_err(|_| StorageError::internal("SQL publish chunk is missing or repeated"))?;
        if page.more {
            return Err(StorageError::internal(
                "SQL publish chunk lookup is not unique",
            ));
        }
        let [value]: [SqlValue; 1] = row
            .into_values()
            .try_into()
            .map_err(|_| StorageError::internal("SQL publish chunk width differs"))?;
        let SqlValue::Blob(bytes) = value else {
            return Err(StorageError::internal("SQL publish chunk type differs"));
        };
        storage_serde::from_bytes(&bytes)?
    };
    Ok(Some(PublishCandidate {
        intent,
        subscriptions,
    }))
}

pub(crate) async fn write_publication<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    publication: AcceptedPublication,
) -> StorageResult<()> {
    let generation = publication.snapshot.root.generation.to_string();
    let statements = publication
        .snapshot
        .chunks
        .iter()
        .enumerate()
        .map(|(index, chunk)| {
            let index = i64::try_from(index)
                .map_err(|_| StorageError::internal("publish chunk index exceeds SQL range"))?;
            let sql = format!(
                "INSERT INTO {relation_sys_pubsub_publish_chunks} (generation, chunk_index, \
                 row_blob) VALUES ({}) ON CONFLICT DO NOTHING",
                engine.engine().placeholders(3),
                relation_sys_pubsub_publish_chunks =
                    engine.relation_name("sys_pubsub_publish_chunks"),
            );
            Ok(SqlStatement::new(
                sql,
                vec![
                    SqlValue::Text(generation.clone()),
                    SqlValue::Integer(index),
                    SqlValue::Blob(storage_serde::to_bytes(chunk)?),
                ],
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    for changed in native.execute_batch(statements).await? {
        expect_insert(changed)?;
    }
    let sql = format!(
        "INSERT INTO {relation_sys_pubsub_publish_intents} (message_id, generation, lease_until, \
         next_chunk, row_blob) VALUES ({}) ON CONFLICT DO NOTHING",
        engine.engine().placeholders(5),
        relation_sys_pubsub_publish_intents = engine.relation_name("sys_pubsub_publish_intents"),
    );
    expect_insert(
        native
            .execute(SqlStatement::new(
                sql,
                vec![
                    SqlValue::Text(publication.intent.acceptance.message_id.to_string()),
                    SqlValue::Text(generation),
                    SqlValue::Null,
                    SqlValue::Integer(0),
                    SqlValue::Blob(storage_serde::to_bytes(&publication.intent)?),
                ],
            ))
            .await?,
    )
}

pub(crate) async fn read_due_publish_chunks<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    now: TimestampMillis,
    limit: NonZeroU32,
) -> StorageResult<Vec<PublishCandidate>> {
    // Bound both ordered index ranges before joining chunks. The final sort
    // sees at most twice the page limit, regardless of the stored backlog.
    let sql = format!(
        "WITH unleased AS (SELECT message_id, generation, lease_until, next_chunk, row_blob, 0 AS \
         priority FROM {relation_sys_pubsub_publish_intents} WHERE lease_until IS NULL ORDER BY \
         message_id LIMIT {limit}), expired AS (SELECT message_id, generation, lease_until, \
         next_chunk, row_blob, 1 AS priority FROM {relation_sys_pubsub_publish_intents} WHERE \
         lease_until <= {now} ORDER BY lease_until, message_id LIMIT {limit}), candidates AS \
         (SELECT * FROM unleased UNION ALL SELECT * FROM expired) SELECT i.message_id, \
         i.generation, i.lease_until, i.next_chunk, i.row_blob, c.row_blob FROM candidates i LEFT \
         JOIN {relation_sys_pubsub_publish_chunks} c ON c.generation = i.generation AND \
         c.chunk_index = i.next_chunk ORDER BY i.priority, i.lease_until, i.message_id LIMIT \
         {limit}",
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
                    SqlValue::Integer(now.timestamp_millis()),
                    SqlValue::Integer(i64::from(limit.get())),
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
            let [id, generation, lease, next_chunk, intent_blob, chunk_blob] = row
                .into_values()
                .try_into()
                .map_err(|_| StorageError::internal("SQL publish claim row has invalid width"))?;
            let intent = decode_intent([id, generation, lease, next_chunk, intent_blob])?;
            let subscriptions = match chunk_blob {
                SqlValue::Blob(_) if intent.snapshot.chunk_count == 0 => {
                    return Err(StorageError::internal("SQL empty publish has a chunk"));
                }
                SqlValue::Blob(bytes) => storage_serde::from_bytes(&bytes)?,
                SqlValue::Null if intent.snapshot.chunk_count == 0 => Vec::new(),
                SqlValue::Null => {
                    return Err(StorageError::internal("SQL publish chunk is missing"));
                }
                _ => return Err(StorageError::internal("SQL publish chunk has invalid type")),
            };
            Ok(PublishCandidate {
                intent,
                subscriptions,
            })
        })
        .collect()
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
    replacements: Vec<(PublishIntent, PublishIntent)>,
) -> StorageResult<()> {
    let statements = replacements
        .into_iter()
        .map(|(expected, next)| update_intent_statement(engine, expected, next))
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
        "UPDATE {relation_sys_pubsub_publish_intents} SET lease_until = {}, next_chunk = {}, \
         row_blob = {} WHERE message_id = {} AND row_blob = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        engine.engine().placeholder(4),
        engine.engine().placeholder(5),
        relation_sys_pubsub_publish_intents = engine.relation_name("sys_pubsub_publish_intents"),
    );
    let lease = next
        .lease_expires_at
        .map(|time| SqlValue::Integer(time.timestamp_millis()))
        .unwrap_or(SqlValue::Null);
    Ok(SqlStatement::new(
        sql,
        vec![
            lease,
            SqlValue::Integer(i64::from(next.next_chunk)),
            SqlValue::Blob(storage_serde::to_bytes(&next)?),
            SqlValue::Text(expected.acceptance.message_id.to_string()),
            SqlValue::Blob(storage_serde::to_bytes(&expected)?),
        ],
    ))
}

fn decode_intent(values: [SqlValue; 5]) -> StorageResult<PublishIntent> {
    let [id, generation, lease, next_chunk, blob] = values;
    let (
        SqlValue::Text(id),
        SqlValue::Text(generation),
        SqlValue::Integer(next_chunk),
        SqlValue::Blob(blob),
    ) = (id, generation, next_chunk, blob)
    else {
        return Err(StorageError::internal(
            "SQL publish intent has invalid types",
        ));
    };
    let lease = match lease {
        SqlValue::Null => None,
        SqlValue::Integer(value) => Some(value),
        _ => return Err(StorageError::internal("SQL publish lease has invalid type")),
    };
    let intent: PublishIntent = storage_serde::from_bytes(&blob)?;
    if intent.acceptance.message_id.as_str() != id
        || intent.snapshot.generation.to_string() != generation
        || intent.lease_expires_at.map(|time| time.timestamp_millis()) != lease
        || i64::from(intent.next_chunk) != next_chunk
    {
        return Err(StorageError::internal("SQL publish intent columns differ"));
    }
    Ok(intent)
}

fn expect_insert(changed: u64) -> StorageResult<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(StorageEnum::TransactionConflict {
            message: "publish identity already exists".to_owned(),
        }
        .into())
    }
}
