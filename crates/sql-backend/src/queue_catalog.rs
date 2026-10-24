//! One SQL queue catalog layout for SQLite, Turso, and Postgres.

use queue_provider::{QueueCatalogPage, QueueCatalogRecord, QueueCatalogRequest, QueueIncarnation};
use storage_types::{StorageEnum, StorageError, StorageResult, storage_serde};

use crate::{
    ReadProtection, SqlAttempt, SqlLayout, SqlRow, SqlStatement, SqlValue,
    constants::QUEUE_CATALOG_READ_BYTES as CATALOG_BYTES,
};

pub(crate) async fn initialize_queue_catalog<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_queue_catalog} (queue_name {} PRIMARY KEY, \
         queue_url {} NOT NULL UNIQUE, incarnation {} NOT NULL UNIQUE, row_blob {} NOT NULL){}",
        engine.engine().text_type(),
        engine.engine().text_type(),
        engine.engine().blob_type(),
        engine.engine().blob_type(),
        engine.engine().table_suffix(),
        relation_sys_queue_catalog = engine.relation_name("sys_queue_catalog"),
    );
    attempt.execute(SqlStatement::new(sql, vec![])).await?;
    Ok(())
}

pub(crate) async fn read_queue_by_name<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    name: &str,
) -> StorageResult<Option<QueueCatalogRecord>> {
    let sql = crate::sql_template::sql_template!(engine,
        "SELECT queue_name, queue_url, incarnation, row_blob FROM " ["sys_queue_catalog"], " WHERE queue_name = " {1},
    );
    read_one(
        native,
        protection,
        SqlStatement::new(sql, vec![SqlValue::Text(name.to_owned())]),
    )
    .await
}

pub(crate) async fn read_queues_by_url<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    urls: Vec<String>,
) -> StorageResult<Vec<Option<QueueCatalogRecord>>> {
    let statements = urls.iter().map(|url| SqlStatement::new(
        crate::sql_template::sql_template!(engine,
            "SELECT queue_name, queue_url, incarnation, row_blob FROM " ["sys_queue_catalog"], " WHERE queue_url = " {1},
        ),
        vec![SqlValue::Text(url.clone())],
    )).collect();
    let pages = native
        .query_batch(statements, protection, CATALOG_BYTES)
        .await?;
    if pages.len() != urls.len() {
        return Err(StorageError::internal(
            "SQL queue catalog batch count differs",
        ));
    }
    pages
        .into_iter()
        .zip(urls)
        .map(|(page, url)| {
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "SQL queue catalog read is not unique",
                ));
            }
            let record = page.rows.into_iter().next().map(decode_row).transpose()?;
            if record
                .as_ref()
                .is_some_and(|record| record.queue.queue_url != url)
            {
                return Err(StorageError::internal("SQL queue catalog URL differs"));
            }
            Ok(record)
        })
        .collect()
}

pub(crate) async fn read_queue_page<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    request: &QueueCatalogRequest,
) -> StorageResult<QueueCatalogPage> {
    let mut binds = Vec::new();
    let mut predicates = Vec::new();
    if let Some(prefix) = &request.prefix {
        binds.push(SqlValue::Text(prefix.clone()));
        let placeholder = engine.engine().placeholder(binds.len());
        predicates.push(format!(
            "substr(queue_name, 1, length({placeholder})) = {placeholder}"
        ));
    }
    if let Some(start) = &request.exclusive_start {
        binds.push(SqlValue::Text(start.clone()));
        predicates.push(format!(
            "queue_name > {}",
            engine.engine().placeholder(binds.len())
        ));
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    binds.push(SqlValue::Integer(i64::from(request.limit.get()) + 1));
    let sql = format!(
        "SELECT queue_name, queue_url, incarnation, row_blob FROM {relation_sys_queue_catalog}{} \
         ORDER BY queue_name ASC LIMIT {}",
        where_clause,
        engine.engine().placeholder(binds.len()),
        relation_sys_queue_catalog = engine.relation_name("sys_queue_catalog"),
    );
    let page = native
        .query(
            SqlStatement::new(sql, binds),
            ReadProtection::Snapshot,
            CATALOG_BYTES,
        )
        .await?;
    let rows = page
        .rows
        .into_iter()
        .map(decode_row)
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(QueueCatalogPage {
        rows,
        more: page.more,
    })
}

pub(crate) async fn write_queue_record<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    record: QueueCatalogRecord,
    expected: Option<QueueIncarnation>,
) -> StorageResult<()> {
    let blob = storage_serde::to_bytes(&record)?;
    let (sql, binds) = if let Some(expected) = expected {
        (
            format!(
                "UPDATE {relation_sys_queue_catalog} SET row_blob = {}, incarnation = {} WHERE \
                 queue_name = {} AND queue_url = {} AND incarnation = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                engine.engine().placeholder(3),
                engine.engine().placeholder(4),
                engine.engine().placeholder(5),
                relation_sys_queue_catalog = engine.relation_name("sys_queue_catalog"),
            ),
            vec![
                SqlValue::Blob(blob),
                SqlValue::Blob(record.incarnation.as_bytes().to_vec()),
                SqlValue::Text(record.queue.queue_name),
                SqlValue::Text(record.queue.queue_url),
                SqlValue::Blob(expected.as_bytes().to_vec()),
            ],
        )
    } else {
        (
            format!(
                "INSERT INTO {relation_sys_queue_catalog} (queue_name, queue_url, incarnation, \
                 row_blob) VALUES ({}) ON CONFLICT DO NOTHING",
                engine.engine().placeholders(4),
                relation_sys_queue_catalog = engine.relation_name("sys_queue_catalog"),
            ),
            vec![
                SqlValue::Text(record.queue.queue_name),
                SqlValue::Text(record.queue.queue_url),
                SqlValue::Blob(record.incarnation.as_bytes().to_vec()),
                SqlValue::Blob(blob),
            ],
        )
    };
    let changed = native.execute(SqlStatement::new(sql, binds)).await?;
    expect_one(changed, "SQL queue catalog changed during write")
}

pub(crate) async fn delete_queue_record<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    record: QueueCatalogRecord,
) -> StorageResult<()> {
    let sql = format!(
        "DELETE FROM {relation_sys_queue_catalog} WHERE queue_name = {} AND incarnation = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_queue_catalog = engine.relation_name("sys_queue_catalog"),
    );
    let changed = native
        .execute(SqlStatement::new(
            sql,
            vec![
                SqlValue::Text(record.queue.queue_name),
                SqlValue::Blob(record.incarnation.as_bytes().to_vec()),
            ],
        ))
        .await?;
    expect_one(changed, "SQL queue catalog changed during delete")
}

async fn read_one<A: SqlAttempt>(
    native: &mut A,
    protection: ReadProtection,
    statement: SqlStatement,
) -> StorageResult<Option<QueueCatalogRecord>> {
    let page = native.query(statement, protection, CATALOG_BYTES).await?;
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL queue catalog read is not unique",
        ));
    }
    page.rows.into_iter().next().map(decode_row).transpose()
}

fn decode_row(row: SqlRow) -> StorageResult<QueueCatalogRecord> {
    let [name, url, incarnation, bytes] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue catalog row has invalid width"))?;
    let (
        SqlValue::Text(name),
        SqlValue::Text(url),
        SqlValue::Blob(incarnation),
        SqlValue::Blob(bytes),
    ) = (name, url, incarnation, bytes)
    else {
        return Err(StorageError::internal(
            "SQL queue catalog row has invalid types",
        ));
    };
    let record: QueueCatalogRecord = storage_serde::from_bytes(&bytes)?;
    if record.queue.queue_name != name
        || record.queue.queue_url != url
        || record.incarnation != QueueIncarnation::try_from(incarnation.as_slice())?
    {
        return Err(StorageError::internal("SQL queue catalog identity differs"));
    }
    Ok(record)
}

pub(crate) fn expect_one(changed: u64, message: &'static str) -> StorageResult<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(StorageEnum::TransactionConflict {
            message: message.to_owned(),
        }
        .into())
    }
}
