//! One SQL queue catalog layout for SQLite, Turso, and Postgres.

use queue_provider::{QueueCatalogPage, QueueCatalogRecord, QueueCatalogRequest, QueueIncarnation};
use storage_types::{MissExpectation, StorageError, StorageResult, storage_serde};

use crate::{
    ReadProtection, SqlAttempt, SqlLayout, SqlQueryPage, SqlRow, SqlStatement, SqlValue,
    constants::QUEUE_CATALOG_READ_BYTES as CATALOG_BYTES,
};

pub(crate) async fn initialize_queue_catalog<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_queue_catalog} (queue_name {} PRIMARY KEY, \
         incarnation {} NOT NULL UNIQUE, row_blob {} NOT NULL){}",
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
    let page = native
        .query(
            queue_by_name_statement(engine, name),
            protection,
            CATALOG_BYTES,
        )
        .await?;
    decode_named_page(page, name)
}

/// The catalog row of one queue name; a claim wave also reads it (the
/// dead-letter target).
pub(crate) fn queue_by_name_statement(engine: impl SqlLayout, name: &str) -> SqlStatement {
    let sql = crate::sql_template::sql_template!(engine,
        "SELECT queue_name, incarnation, row_blob FROM " ["sys_queue_catalog"], " WHERE queue_name = " {1},
    );
    SqlStatement::new(sql, vec![SqlValue::Text(name.to_owned())])
}

pub(crate) fn decode_named_page(
    page: SqlQueryPage,
    name: &str,
) -> StorageResult<Option<QueueCatalogRecord>> {
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL queue catalog read is not unique",
        ));
    }
    let record = page.rows.into_iter().next().map(decode_row).transpose()?;
    if record
        .as_ref()
        .is_some_and(|record| record.queue.queue_name != name)
    {
        return Err(StorageError::internal("SQL queue catalog name differs"));
    }
    Ok(record)
}

/// The catalog rows of `names` in input order, in one batch.
pub(crate) async fn read_queues_by_name<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    names: Vec<String>,
) -> StorageResult<storage_types::Exact<Option<QueueCatalogRecord>>> {
    let statements = names
        .iter()
        .map(|name| queue_by_name_statement(engine, name))
        .collect();
    let pages = native
        .query_batch_exact(statements, protection, CATALOG_BYTES)
        .await?;
    pages.try_zip_map(names, |name, page| decode_named_page(page, &name))
}

pub(crate) async fn read_queue_page<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    request: &QueueCatalogRequest,
) -> StorageResult<QueueCatalogPage> {
    let mut binds = Vec::new();
    let mut predicates = Vec::new();
    // A name range, so the primary key index bounds the scan (the column
    // collation is binary on every engine).
    if let Some(prefix) = request.prefix.as_ref().filter(|prefix| !prefix.is_empty()) {
        binds.push(SqlValue::Text(prefix.clone()));
        predicates.push(format!(
            "queue_name >= {}",
            engine.engine().placeholder(binds.len())
        ));
        if let Some(end) = prefix_end(prefix) {
            binds.push(SqlValue::Text(end));
            predicates.push(format!(
                "queue_name < {}",
                engine.engine().placeholder(binds.len())
            ));
        }
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
        "SELECT queue_name, incarnation, row_blob FROM {relation_sys_queue_catalog}{} ORDER BY \
         queue_name ASC LIMIT {}",
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
                 queue_name = {} AND incarnation = {}",
                engine.engine().placeholder(1),
                engine.engine().placeholder(2),
                engine.engine().placeholder(3),
                engine.engine().placeholder(4),
                relation_sys_queue_catalog = engine.relation_name("sys_queue_catalog"),
            ),
            vec![
                SqlValue::Blob(blob),
                SqlValue::Blob(record.incarnation.as_bytes().to_vec()),
                SqlValue::Text(record.queue.queue_name),
                SqlValue::Blob(expected.as_bytes().to_vec()),
            ],
        )
    } else {
        (
            format!(
                "INSERT INTO {relation_sys_queue_catalog} (queue_name, incarnation, row_blob) \
                 VALUES ({}) ON CONFLICT DO NOTHING",
                engine.engine().placeholders(3),
                relation_sys_queue_catalog = engine.relation_name("sys_queue_catalog"),
            ),
            vec![
                SqlValue::Text(record.queue.queue_name),
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

fn decode_row(row: SqlRow) -> StorageResult<QueueCatalogRecord> {
    let [name, incarnation, bytes] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL queue catalog row has invalid width"))?;
    let (SqlValue::Text(name), SqlValue::Blob(incarnation), SqlValue::Blob(bytes)) =
        (name, incarnation, bytes)
    else {
        return Err(StorageError::internal(
            "SQL queue catalog row has invalid types",
        ));
    };
    let record: QueueCatalogRecord = storage_serde::from_bytes(&bytes)?;
    if record.queue.queue_name != name
        || record.incarnation != QueueIncarnation::try_from(incarnation.as_slice())?
    {
        return Err(StorageError::internal("SQL queue catalog identity differs"));
    }
    Ok(record)
}

/// The least string after every string that starts with `prefix`, in code
/// point order (which is byte order for UTF-8); `None` when no such string
/// exists.
fn prefix_end(prefix: &str) -> Option<String> {
    let mut chars = prefix.chars().collect::<Vec<_>>();
    while let Some(last) = chars.pop() {
        let next = match u32::from(last) {
            0xD7FF => Some('\u{E000}'),
            code => char::from_u32(code + 1),
        };
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

pub(crate) fn expect_one(changed: u64, record: &'static str) -> StorageResult<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(StorageError::write_miss(
            record,
            MissExpectation::Rows {
                expected: 1,
                found: changed,
            },
        ))
    }
}
