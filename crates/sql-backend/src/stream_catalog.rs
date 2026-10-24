//! One SQL user-stream catalog layout for SQLite, Turso, and Postgres.

use storage_types::{StorageError, StorageResult, storage_serde};
use stream_provider::{StreamCatalogLookup, StreamCatalogRecord};

use crate::{
    ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlStatement, SqlValue,
    constants::STREAM_CATALOG_READ_BYTES as CATALOG_BYTES,
};

pub(crate) async fn initialize_stream_catalog<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_user_stream_catalog} (name {} PRIMARY KEY, \
         internal_id {} NOT NULL UNIQUE, row_blob {} NOT NULL){}",
        engine.engine().text_type(),
        engine.engine().blob_type(),
        engine.engine().blob_type(),
        engine.engine().table_suffix(),
        relation_sys_user_stream_catalog = engine.relation_name("sys_user_stream_catalog"),
    );
    attempt.execute(SqlStatement::new(sql, vec![])).await?;
    Ok(())
}

pub(crate) async fn read_stream_record<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    lookup: StreamCatalogLookup<'_>,
) -> StorageResult<Option<StreamCatalogRecord>> {
    // A Postgres writer only observes catalog state here. Locking this row
    // serializes every append to the same user stream across all API nodes.
    let protection = if engine.engine() == SqlEngine::Postgres {
        ReadProtection::Snapshot
    } else {
        protection
    };
    let (sql, value) = match lookup {
        StreamCatalogLookup::Name(name) => (
            crate::sql_template::sql_template!(engine,
                "SELECT row_blob FROM " ["sys_user_stream_catalog"], " WHERE name = " {1},
            ),
            SqlValue::Text(name.to_owned()),
        ),
        StreamCatalogLookup::Id(id) => (
            crate::sql_template::sql_template!(engine,
                "SELECT row_blob FROM " ["sys_user_stream_catalog"], " WHERE internal_id = " {1},
            ),
            SqlValue::Blob(id.as_ref().to_vec()),
        ),
    };
    let record =
        read_catalog_query(native, protection, SqlStatement::new(sql, vec![value])).await?;
    if record
        .as_ref()
        .is_some_and(|record| !lookup.matches(record))
    {
        return Err(StorageError::internal(
            "SQL stream catalog identity differs",
        ));
    }
    Ok(record)
}

async fn read_catalog_query<A: SqlAttempt>(
    native: &mut A,
    protection: ReadProtection,
    statement: SqlStatement,
) -> StorageResult<Option<StreamCatalogRecord>> {
    let page = native.query(statement, protection, CATALOG_BYTES).await?;
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL stream catalog read is not unique",
        ));
    }
    page.rows
        .into_iter()
        .next()
        .map(|row| {
            let [value] = row
                .into_values()
                .try_into()
                .map_err(|_| StorageError::internal("SQL stream catalog row has invalid width"))?;
            let SqlValue::Blob(bytes) = value else {
                return Err(StorageError::internal(
                    "SQL stream catalog row is not bytes",
                ));
            };
            storage_serde::from_bytes(&bytes)
        })
        .transpose()
}

pub(crate) async fn write_stream_record<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    name: &str,
    record: StreamCatalogRecord,
) -> StorageResult<()> {
    if record.stream.name.as_str() != name {
        return Err(StorageError::internal(
            "SQL stream catalog write has another name",
        ));
    }
    let deleted = record.deleted;
    let sql = format!(
        "INSERT INTO {relation_sys_user_stream_catalog} (name, internal_id, row_blob) VALUES ({}) \
         ON CONFLICT(name) DO UPDATE SET internal_id = EXCLUDED.internal_id, row_blob = \
         EXCLUDED.row_blob",
        engine.engine().placeholders(3),
        relation_sys_user_stream_catalog = engine.relation_name("sys_user_stream_catalog"),
    );
    let changed = native
        .execute(SqlStatement::new(
            sql,
            vec![
                SqlValue::Text(name.to_owned()),
                SqlValue::Blob(record.stream.internal_id.as_ref().to_vec()),
                SqlValue::Blob(storage_serde::to_bytes(&record)?),
            ],
        ))
        .await?;
    if changed != 1 {
        return Err(StorageError::internal(
            "SQL stream catalog write affected no row",
        ));
    }
    if engine.engine() == SqlEngine::Postgres {
        return Ok(());
    }
    let sequence_name =
        super::stream_write::sql_statements::user_sequence_name(&record.stream.internal_id);
    let sequence = if deleted {
        super::stream_write::sql_statements::delete_sequence(engine, &sequence_name)
    } else {
        super::stream_write::sql_statements::initialize_sequence(engine, &sequence_name)
    };
    native.execute(sequence).await?;
    Ok(())
}
