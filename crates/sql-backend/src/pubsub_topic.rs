//! One topic row layout and statement set for SQLite, Turso, and Postgres.

use std::num::NonZeroU32;

use pubsub_provider::{TopicArn, TopicIncarnation, TopicPage, TopicRecord};
use storage_types::{ReadProtection, StorageEnum, StorageError, StorageResult, storage_serde};

use crate::{
    SqlAttempt, SqlLayout, SqlRow, SqlStatement, SqlValue, constants::PUBSUB_CATALOG_READ_BYTES,
};

pub(crate) async fn initialize_pubsub_topics<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_pubsub_topics} (topic_arn {} PRIMARY KEY, \
         incarnation {} NOT NULL, row_blob {} NOT NULL){}",
        engine.engine().text_type(),
        engine.engine().blob_type(),
        engine.engine().blob_type(),
        engine.engine().table_suffix(),
        relation_sys_pubsub_topics = engine.relation_name("sys_pubsub_topics"),
    );
    native.execute(SqlStatement::new(sql, vec![])).await?;
    Ok(())
}

pub(crate) async fn read_topics<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    arns: Vec<TopicArn>,
) -> StorageResult<Vec<Option<TopicRecord>>> {
    let statements = arns
        .iter()
        .map(|arn| {
            SqlStatement::new(
                format!(
                    "SELECT topic_arn, incarnation, row_blob FROM {relation_sys_pubsub_topics} \
                     WHERE topic_arn = {}",
                    engine.engine().placeholder(1),
                    relation_sys_pubsub_topics = engine.relation_name("sys_pubsub_topics"),
                ),
                vec![SqlValue::Text(arn.to_string())],
            )
        })
        .collect();
    let pages = native
        .query_batch(statements, protection, PUBSUB_CATALOG_READ_BYTES)
        .await?;
    if pages.len() != arns.len() {
        return Err(StorageError::internal("SQL topic batch count differs"));
    }
    pages
        .into_iter()
        .zip(arns)
        .map(|(page, arn)| {
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "SQL topic lookup returned multiple rows",
                ));
            }
            let record = page.rows.into_iter().next().map(decode_row).transpose()?;
            if record
                .as_ref()
                .is_some_and(|record| record.topic.topic_arn != arn)
            {
                return Err(StorageError::internal("SQL topic batch identity differs"));
            }
            Ok(record)
        })
        .collect()
}

pub(crate) async fn write_topic<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    record: TopicRecord,
    expected: Option<TopicIncarnation>,
) -> StorageResult<()> {
    let arn = SqlValue::Text(record.topic.topic_arn.to_string());
    let incarnation = SqlValue::Blob(record.incarnation.as_bytes().to_vec());
    let blob = SqlValue::Blob(storage_serde::to_bytes(&record)?);
    let (sql, binds) = if let Some(expected) = expected {
        (
            format!(
                "UPDATE {relation_sys_pubsub_topics} SET row_blob = {} WHERE topic_arn = {} AND \
                 incarnation = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                engine.engine().placeholder(3),
                relation_sys_pubsub_topics = engine.relation_name("sys_pubsub_topics"),
            ),
            vec![blob, arn, SqlValue::Blob(expected.as_bytes().to_vec())],
        )
    } else {
        (
            format!(
                "INSERT INTO {relation_sys_pubsub_topics} (row_blob, topic_arn, incarnation) \
                 VALUES ({}) ON CONFLICT DO NOTHING",
                engine.engine().placeholders(3),
                relation_sys_pubsub_topics = engine.relation_name("sys_pubsub_topics"),
            ),
            vec![blob, arn, incarnation],
        )
    };
    expect_one(native.execute(SqlStatement::new(sql, binds)).await?)
}

pub(crate) async fn delete_topic<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    record: TopicRecord,
) -> StorageResult<()> {
    let sql = format!(
        "DELETE FROM {relation_sys_pubsub_topics} WHERE topic_arn = {} AND incarnation = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_pubsub_topics = engine.relation_name("sys_pubsub_topics"),
    );
    let changed = native
        .execute(SqlStatement::new(
            sql,
            vec![
                SqlValue::Text(record.topic.topic_arn.to_string()),
                SqlValue::Blob(record.incarnation.as_bytes().to_vec()),
            ],
        ))
        .await?;
    expect_one(changed)
}

pub(crate) async fn read_topic_page<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    after: Option<&TopicArn>,
    limit: NonZeroU32,
) -> StorageResult<TopicPage> {
    let mut binds = Vec::new();
    let predicate = if let Some(after) = after {
        binds.push(SqlValue::Text(after.to_string()));
        format!(" WHERE topic_arn > {}", engine.engine().placeholder(1))
    } else {
        String::new()
    };
    binds.push(SqlValue::Integer(i64::from(limit.get()) + 1));
    let sql = format!(
        "SELECT topic_arn, incarnation, row_blob FROM {relation_sys_pubsub_topics}{predicate} \
         ORDER BY topic_arn LIMIT {}",
        engine.engine().placeholder(binds.len()),
        relation_sys_pubsub_topics = engine.relation_name("sys_pubsub_topics"),
    );
    let page = native
        .query(
            SqlStatement::new(sql, binds),
            ReadProtection::Snapshot,
            PUBSUB_CATALOG_READ_BYTES,
        )
        .await?;
    Ok(TopicPage {
        rows: page
            .rows
            .into_iter()
            .map(decode_row)
            .collect::<StorageResult<Vec<_>>>()?,
        more: page.more,
    })
}

fn decode_row(row: SqlRow) -> StorageResult<TopicRecord> {
    let [arn, incarnation, blob] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL topic row has invalid width"))?;
    let (SqlValue::Text(arn), SqlValue::Blob(incarnation), SqlValue::Blob(blob)) =
        (arn, incarnation, blob)
    else {
        return Err(StorageError::internal("SQL topic row has invalid types"));
    };
    let incarnation = TopicIncarnation::try_from(incarnation.as_slice())?;
    let record: TopicRecord = storage_serde::from_bytes(&blob)?;
    if record.topic.topic_arn.as_str() != arn || record.incarnation != incarnation {
        return Err(StorageError::internal("SQL topic row identity differs"));
    }
    Ok(record)
}

fn expect_one(changed: u64) -> StorageResult<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(StorageEnum::TransactionConflict {
            message: "SQL topic changed during write".to_owned(),
        }
        .into())
    }
}
