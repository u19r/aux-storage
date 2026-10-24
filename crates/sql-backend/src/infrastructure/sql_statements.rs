//! One physical record relation and CAS grammar for every SQL engine.

use infrastructure_provider::{
    limits::InfrastructureLimits,
    page::RecordPageRequest,
    record::{DurableRecord, DurableRecordKey, RecordChange, RecordExpectation},
};
use storage_types::{InfrastructureResultKind, Revision, StorageResult};

use super::codec;
use crate::{SqlEngine, SqlStatement, SqlValue, row_view::SqlValueRef, sql_template::sql_template};

pub(super) fn render_format_schema(engine: SqlEngine) -> SqlStatement {
    let integer = engine.integer_type();
    let blob = engine.blob_type();
    let length = render_blob_length_function(engine);
    let suffix = engine.table_suffix();
    SqlStatement::new(
        format!(
            "CREATE TABLE IF NOT EXISTS sys_infrastructure_format (singleton {integer} PRIMARY \
             KEY CHECK (singleton = 1), version {integer} NOT NULL, store_identity {blob} NOT \
             NULL CHECK ({length}(store_identity) = 16)){suffix}"
        ),
        vec![],
    )
}

pub(super) fn render_record_schema(engine: SqlEngine) -> SqlStatement {
    let text = render_namespace_type(engine);
    let blob = engine.blob_type();
    let length = render_blob_length_function(engine);
    let suffix = engine.table_suffix();
    SqlStatement::new(
        format!(
            "CREATE TABLE IF NOT EXISTS sys_infrastructure_records (namespace {text} NOT NULL \
             CHECK ({length}(namespace) > 0), resource {blob} NOT NULL CHECK ({length}(resource) \
             > 0), generation {blob} NOT NULL CHECK ({length}(generation) = 8), suffix {blob} NOT \
             NULL, revision {blob} NOT NULL CHECK ({length}(revision) = 8), bytes {blob} NOT \
             NULL, PRIMARY KEY (namespace, resource, generation, suffix)){suffix}"
        ),
        vec![],
    )
}

fn render_namespace_type(engine: SqlEngine) -> &'static str {
    match engine {
        SqlEngine::Postgres => "TEXT COLLATE \"C\"",
        SqlEngine::Sqlite | SqlEngine::Turso => "TEXT COLLATE BINARY",
    }
}

fn render_blob_length_function(engine: SqlEngine) -> &'static str {
    match engine {
        SqlEngine::Postgres => "octet_length",
        SqlEngine::Sqlite | SqlEngine::Turso => "length",
    }
}

pub(super) fn initialize_version(engine: SqlEngine, identity: &[u8; 16]) -> SqlStatement {
    SqlStatement::new(
        sql_template!(engine,
        "INSERT INTO sys_infrastructure_format (singleton, version, store_identity) VALUES \
         (1, " {1}, ", " {2}, ") ON CONFLICT (singleton) DO NOTHING"),
        vec![
            SqlValue::Integer(crate::constants::INFRASTRUCTURE_LAYOUT_VERSION),
            SqlValue::Blob(identity.to_vec()),
        ],
    )
}

pub(super) fn existing_record() -> SqlStatement {
    SqlStatement::new("SELECT 1 FROM sys_infrastructure_records LIMIT 1", vec![])
}

pub(super) fn existing_namespace_record(
    engine: SqlEngine,
    namespace: &str,
) -> StorageResult<SqlStatement> {
    Ok(SqlStatement::new(
        sql_template!(engine, "SELECT 1 FROM sys_infrastructure_records WHERE namespace = " {1}, " LIMIT 1"),
        prepare_namespace_binds(namespace)?,
    ))
}

fn prepare_namespace_binds(namespace: &str) -> StorageResult<Vec<SqlValue>> {
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(1)
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    binds.push(SqlValue::try_from(SqlValueRef::Text(namespace))?);
    Ok(binds)
}

pub(super) fn load_version(engine: SqlEngine) -> SqlStatement {
    let identity_bytes = render_column_bytes(engine, "store_identity");
    let types = match engine {
        SqlEngine::Postgres => {
            "pg_typeof(singleton) = 'bigint'::regtype AND pg_typeof(version) = 'bigint'::regtype \
             AND pg_typeof(store_identity) = 'bytea'::regtype"
        }
        SqlEngine::Sqlite | SqlEngine::Turso => {
            "typeof(singleton) = 'integer' AND typeof(version) = 'integer' AND \
             typeof(store_identity) = 'blob'"
        }
    };
    let condition = format!("{types} AND {identity_bytes} = 16");
    let columns = ["singleton", "version", "store_identity"]
        .map(|column| format!("CASE WHEN {condition} THEN {column} ELSE NULL END AS {column}"))
        .join(", ");
    SqlStatement::new(
        format!("SELECT {columns} FROM sys_infrastructure_format LIMIT 2"),
        vec![],
    )
}

pub(super) fn load_batch(
    engine: SqlEngine,
    keys: &[DurableRecordKey],
    limits: InfrastructureLimits,
) -> StorageResult<SqlStatement> {
    let columns = render_record_columns(engine, limits);
    let mut sql = format!(
        "SELECT {columns} FROM sys_infrastructure_records WHERE (namespace, resource, generation, \
         suffix) IN ("
    );
    let count = keys
        .len()
        .checked_mul(4)
        .ok_or(InfrastructureResultKind::NativeCapacityRejected)?;
    let mut binds = Vec::new();
    binds
        .try_reserve_exact(count)
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    for (index, key) in keys.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        let first = index * 4 + 1;
        sql.push_str(&format!(
            "({}, {}, {}, {})",
            engine.placeholder(first),
            engine.placeholder(first + 1),
            engine.placeholder(first + 2),
            engine.placeholder(first + 3)
        ));
        binds.extend(codec::encode_key(key)?);
    }
    sql.push(')');
    Ok(SqlStatement::new(sql, binds))
}

pub(super) fn load_revision(
    engine: SqlEngine,
    key: &DurableRecordKey,
) -> StorageResult<SqlStatement> {
    let revision_bytes = render_column_bytes(engine, "revision");
    let columns = format!("CASE WHEN {revision_bytes} = 8 THEN revision ELSE NULL END AS revision");
    Ok(SqlStatement::new(
        format!(
            "SELECT {columns} FROM sys_infrastructure_records WHERE namespace = {} AND resource = \
             {} AND generation = {} AND suffix = {}",
            engine.placeholder(1),
            engine.placeholder(2),
            engine.placeholder(3),
            engine.placeholder(4)
        ),
        codec::prepare_key_binds(key, 4)?,
    ))
}

pub(super) fn scan(
    engine: SqlEngine,
    request: &RecordPageRequest,
    limits: InfrastructureLimits,
) -> StorageResult<SqlStatement> {
    let (first, last, count) = request.range.bounds();
    let mut binds = codec::prepare_key_binds(first, 6)?;
    binds.push(SqlValue::try_from(SqlValueRef::Blob(
        &last.as_ref().suffix,
    ))?);
    let columns = render_record_columns(engine, limits);
    let mut sql = format!(
        "SELECT {columns} FROM sys_infrastructure_records WHERE namespace = {} AND resource = {} \
         AND generation = {} AND suffix >= {} AND suffix < {}",
        engine.placeholder(1),
        engine.placeholder(2),
        engine.placeholder(3),
        engine.placeholder(4),
        engine.placeholder(5)
    );
    if let Some(after) = &request.after {
        sql.push_str(&format!(" AND suffix > {}", engine.placeholder(6)));
        binds.push(SqlValue::try_from(SqlValueRef::Blob(
            &after.as_ref().suffix,
        ))?);
    }
    sql.push_str(&format!(
        " ORDER BY suffix LIMIT {}",
        count.get().saturating_add(1)
    ));
    Ok(SqlStatement::new(sql, binds))
}

/// An invalid row remains selected, but its payload never reaches the row
/// decoder.
fn render_record_columns(engine: SqlEngine, limits: InfrastructureLimits) -> String {
    let namespace = render_column_bytes(engine, "namespace");
    let resource = render_column_bytes(engine, "resource");
    let suffix = render_column_bytes(engine, "suffix");
    let generation = render_column_bytes(engine, "generation");
    let revision = render_column_bytes(engine, "revision");
    let bytes = render_column_bytes(engine, "bytes");
    let types = render_record_types(engine);
    let condition = format!(
        "{types} AND {namespace} + {resource} + {suffix} + 16 <= {} AND {generation} = 8 AND \
         {revision} = 8 AND {bytes} <= {}",
        limits.max_key_bytes(),
        limits.max_record_bytes()
    );
    crate::constants::INFRASTRUCTURE_RECORD_COLUMNS
        .map(|column| format!("CASE WHEN {condition} THEN {column} ELSE NULL END AS {column}"))
        .join(", ")
}

fn render_record_types(engine: SqlEngine) -> String {
    match engine {
        SqlEngine::Postgres => "TRUE".into(),
        SqlEngine::Sqlite | SqlEngine::Turso => crate::constants::INFRASTRUCTURE_RECORD_COLUMNS
            .map(|column| {
                let storage_class = if column == "namespace" {
                    "text"
                } else {
                    "blob"
                };
                format!("typeof({column}) = '{storage_class}'")
            })
            .join(" AND "),
    }
}

fn render_column_bytes(engine: SqlEngine, column: &str) -> String {
    match engine {
        SqlEngine::Postgres if column == "namespace" => {
            "octet_length(convert_to(namespace, 'UTF8'))".into()
        }
        SqlEngine::Postgres => format!("octet_length({column})"),
        SqlEngine::Sqlite | SqlEngine::Turso => format!("length(CAST({column} AS BLOB))"),
    }
}

pub(super) fn change(engine: SqlEngine, change: &RecordChange) -> StorageResult<SqlStatement> {
    match change {
        RecordChange::Store {
            expected: RecordExpectation::Absent,
            record,
        } => store_absent(engine, record),
        RecordChange::Store {
            expected: RecordExpectation::Revision(expected),
            record,
        } => store_revision(engine, record, *expected),
        RecordChange::Delete { expected, key } => delete(engine, key, *expected),
    }
}

fn store_absent(engine: SqlEngine, record: &DurableRecord) -> StorageResult<SqlStatement> {
    let mut binds = codec::prepare_key_binds(&record.key, 6)?;
    binds.push(codec::encode_revision(record.revision)?);
    binds.push(SqlValue::try_from(SqlValueRef::Blob(&record.bytes))?);
    Ok(SqlStatement::new(
        format!(
            "INSERT INTO sys_infrastructure_records (namespace, resource, generation, suffix, \
             revision, bytes) VALUES ({}) ON CONFLICT (namespace, resource, generation, suffix) \
             DO NOTHING",
            engine.placeholders(6)
        ),
        binds,
    ))
}

fn store_revision(
    engine: SqlEngine,
    record: &DurableRecord,
    expected: Revision,
) -> StorageResult<SqlStatement> {
    let mut binds = codec::prepare_key_binds(&record.key, 7)?;
    binds.push(codec::encode_revision(expected)?);
    binds.push(codec::encode_revision(record.revision)?);
    binds.push(SqlValue::try_from(SqlValueRef::Blob(&record.bytes))?);
    Ok(SqlStatement::new(
        sql_template!(engine,
        "UPDATE sys_infrastructure_records SET revision = " {6}, ", bytes = " {7},
        " WHERE namespace = " {1}, " AND resource = " {2}, " AND generation = " {3},
        " AND suffix = " {4}, " AND revision = " {5}),
        binds,
    ))
}

pub(super) fn delete(
    engine: SqlEngine,
    key: &DurableRecordKey,
    revision: Revision,
) -> StorageResult<SqlStatement> {
    let mut binds = codec::prepare_key_binds(key, 5)?;
    binds.push(codec::encode_revision(revision)?);
    Ok(SqlStatement::new(
        sql_template!(engine,
        "DELETE FROM sys_infrastructure_records WHERE namespace = " {1},
        " AND resource = " {2}, " AND generation = " {3}, " AND suffix = " {4},
        " AND revision = " {5}),
        binds,
    ))
}

pub(super) fn savepoint() -> SqlStatement {
    SqlStatement::new("SAVEPOINT infrastructure_records", vec![])
}

pub(super) fn rollback() -> SqlStatement {
    SqlStatement::new("ROLLBACK TO SAVEPOINT infrastructure_records", vec![])
}

pub(super) fn release() -> SqlStatement {
    SqlStatement::new("RELEASE SAVEPOINT infrastructure_records", vec![])
}
