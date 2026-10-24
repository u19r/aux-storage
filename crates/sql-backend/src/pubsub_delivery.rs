//! One delivery record layout for SQLite, Turso, and Postgres.

use std::num::NonZeroU32;

use pubsub_provider::{DeliveryRecord, DeliveryRecordId};
use storage_types::{
    ReadProtection, StorageEnum, StorageError, StorageResult, TimestampMillis, storage_serde,
};

use crate::{
    SqlAttempt, SqlLayout, SqlRow, SqlStatement, SqlValue, constants::PUBSUB_CATALOG_READ_BYTES,
};

pub(crate) async fn initialize_pubsub_deliveries<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_pubsub_deliveries} (record_id {} PRIMARY KEY, \
         due_at {}, row_blob {} NOT NULL){}",
        engine.engine().text_type(),
        engine.engine().integer_type(),
        engine.engine().blob_type(),
        engine.engine().table_suffix(),
        relation_sys_pubsub_deliveries = engine.relation_name("sys_pubsub_deliveries"),
    );
    native.execute(SqlStatement::new(sql, vec![])).await?;
    native
        .execute(SqlStatement::new(
            crate::sql_template::sql_template!(
                engine,
                "CREATE INDEX IF NOT EXISTS "["sys_pubsub_delivery_due"],
                " ON "["sys_pubsub_deliveries"],
                "(due_at, record_id)"
            )
            .to_owned(),
            vec![],
        ))
        .await?;
    Ok(())
}

pub(crate) async fn write_delivery_records<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    records: Vec<DeliveryRecord>,
) -> StorageResult<()> {
    let sql = format!(
        "INSERT INTO {relation_sys_pubsub_deliveries} (record_id, due_at, row_blob) VALUES ({}) \
         ON CONFLICT DO NOTHING",
        engine.engine().placeholders(3),
        relation_sys_pubsub_deliveries = engine.relation_name("sys_pubsub_deliveries"),
    );
    let statements = records
        .into_iter()
        .map(|record| {
            let due = record
                .due_at()
                .map(|time| SqlValue::Integer(time.timestamp_millis()))
                .unwrap_or(SqlValue::Null);
            Ok(SqlStatement::new(
                sql.clone(),
                vec![
                    SqlValue::Text(record.id.0.clone()),
                    due,
                    SqlValue::Blob(storage_serde::to_bytes(&record)?),
                ],
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    for changed in native.execute_batch(statements).await? {
        if changed != 1 {
            return Err(StorageEnum::TransactionConflict {
                message: "publish delivery already exists".to_owned(),
            }
            .into());
        }
    }
    Ok(())
}

pub(crate) async fn read_deliveries<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    ids: Vec<DeliveryRecordId>,
) -> StorageResult<Vec<Option<DeliveryRecord>>> {
    let statements = ids
        .iter()
        .map(|id| {
            SqlStatement::new(
                format!(
                    "SELECT record_id, due_at, row_blob FROM {relation_sys_pubsub_deliveries} \
                     WHERE record_id = {}",
                    engine.engine().placeholder(1),
                    relation_sys_pubsub_deliveries = engine.relation_name("sys_pubsub_deliveries"),
                ),
                vec![SqlValue::Text(id.0.clone())],
            )
        })
        .collect();
    let pages = native
        .query_batch(statements, protection, PUBSUB_CATALOG_READ_BYTES)
        .await?;
    if pages.len() != ids.len() {
        return Err(StorageError::internal("SQL delivery lookup count differs"));
    }
    pages
        .into_iter()
        .zip(ids)
        .map(|(page, id)| {
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "SQL delivery lookup exceeded one row",
                ));
            }
            let row = page.rows.into_iter().next().map(decode_row).transpose()?;
            if row.as_ref().is_some_and(|row| row.id != id) {
                return Err(StorageError::internal(
                    "SQL delivery lookup identity differs",
                ));
            }
            Ok(row)
        })
        .collect()
}

pub(crate) async fn read_due_deliveries<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    now: TimestampMillis,
    limit: NonZeroU32,
) -> StorageResult<Vec<DeliveryRecord>> {
    let sql = format!(
        "SELECT record_id, due_at, row_blob FROM {relation_sys_pubsub_deliveries} WHERE due_at <= \
         {} ORDER BY due_at, record_id LIMIT {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_pubsub_deliveries = engine.relation_name("sys_pubsub_deliveries"),
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
            ReadProtection::Conflict,
            PUBSUB_CATALOG_READ_BYTES,
        )
        .await?;
    page.rows.into_iter().map(decode_row).collect()
}

pub(crate) async fn replace_deliveries<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    replacements: Vec<(DeliveryRecord, DeliveryRecord)>,
) -> StorageResult<()> {
    if replacements.is_empty() {
        return Ok(());
    }
    let sql = format!(
        "UPDATE {relation_sys_pubsub_deliveries} SET due_at = {}, row_blob = {} WHERE record_id = \
         {} AND row_blob = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        engine.engine().placeholder(4),
        relation_sys_pubsub_deliveries = engine.relation_name("sys_pubsub_deliveries"),
    );
    let statements = replacements
        .into_iter()
        .map(|(expected, next)| {
            if expected.id != next.id {
                return Err(StorageError::internal("delivery identity changed"));
            }
            let due = next
                .due_at()
                .map(|time| SqlValue::Integer(time.timestamp_millis()))
                .unwrap_or(SqlValue::Null);
            Ok(SqlStatement::new(
                sql.clone(),
                vec![
                    due,
                    SqlValue::Blob(storage_serde::to_bytes(&next)?),
                    SqlValue::Text(expected.id.0.clone()),
                    SqlValue::Blob(storage_serde::to_bytes(&expected)?),
                ],
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let expected = statements.len();
    let changed = native.execute_batch(statements).await?;
    if changed.len() == expected && changed.into_iter().all(|count| count == 1) {
        Ok(())
    } else {
        Err(StorageEnum::TransactionConflict {
            message: "delivery changed during update".to_owned(),
        }
        .into())
    }
}

fn decode_row(row: SqlRow) -> StorageResult<DeliveryRecord> {
    let [id, due, blob] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL delivery row has invalid width"))?;
    let (SqlValue::Text(id), SqlValue::Blob(blob)) = (id, blob) else {
        return Err(StorageError::internal("SQL delivery row has invalid types"));
    };
    let due = match due {
        SqlValue::Null => None,
        SqlValue::Integer(value) => Some(value),
        _ => return Err(StorageError::internal("SQL delivery due has invalid type")),
    };
    let record: DeliveryRecord = storage_serde::from_bytes(&blob)?;
    if record.id.0 != id || record.due_at().map(|time| time.timestamp_millis()) != due {
        return Err(StorageError::internal("SQL delivery row identity differs"));
    }
    Ok(record)
}
