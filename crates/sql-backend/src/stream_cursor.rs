//! One cursor row layout shared by SQLite, Turso, and Postgres.

use std::num::NonZeroUsize;

use storage_types::{
    StorageEnum, StorageError, StorageResult, StreamItemId, StreamName, storage_serde,
};
use stream_provider::{CursorName, MAX_SYSTEM_STREAM_CURSOR_SCAN_ROWS, StreamCursor};

use crate::{ReadProtection, SqlAttempt, SqlLayout, SqlStatement, SqlValue};

const CURSOR_BYTES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1);

pub(crate) async fn initialize_stream_cursors<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {relation_sys_user_stream_cursors} (stream_name {} NOT NULL, \
         cursor_name {} NOT NULL, position {} NOT NULL, row_blob {} NOT NULL, PRIMARY KEY \
         (stream_name, cursor_name)){}",
        engine.engine().blob_type(),
        engine.engine().text_type(),
        engine.engine().blob_type(),
        engine.engine().blob_type(),
        engine.engine().table_suffix(),
        relation_sys_user_stream_cursors = engine.relation_name("sys_user_stream_cursors"),
    );
    attempt.execute(SqlStatement::new(sql, vec![])).await?;
    Ok(())
}

pub(crate) async fn read_cursor<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    stream_name: &StreamName,
    cursor_name: &CursorName,
) -> StorageResult<Option<StreamCursor>> {
    let sql = format!(
        "SELECT position, row_blob FROM {relation_sys_user_stream_cursors} WHERE stream_name = {} \
         AND cursor_name = {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_user_stream_cursors = engine.relation_name("sys_user_stream_cursors"),
    );
    let page = native
        .query(
            SqlStatement::new(
                sql,
                vec![
                    SqlValue::Blob(stream_name.as_ref().to_vec()),
                    SqlValue::Text(cursor_name.as_str().to_owned()),
                ],
            ),
            protection,
            CURSOR_BYTES,
        )
        .await?;
    if page.more || page.rows.len() > 1 {
        return Err(StorageError::internal(
            "SQL stream cursor read is not unique",
        ));
    }
    page.rows
        .into_iter()
        .next()
        .map(|row| {
            let [position, bytes] = row
                .into_values()
                .try_into()
                .map_err(|_| StorageError::internal("SQL stream cursor row has invalid width"))?;
            let (SqlValue::Blob(position), SqlValue::Blob(bytes)) = (position, bytes) else {
                return Err(StorageError::internal(
                    "SQL stream cursor row has invalid types",
                ));
            };
            let position = StreamItemId::try_from(position.as_slice())
                .map_err(|_| StorageError::internal("SQL stream cursor position is invalid"))?;
            let cursor: StreamCursor = storage_serde::from_bytes(&bytes)?;
            if cursor.stream_name != *stream_name
                || cursor.name != *cursor_name
                || cursor.position != position
            {
                return Err(StorageError::internal("SQL stream cursor identity differs"));
            }
            Ok(cursor)
        })
        .transpose()
}

pub(crate) async fn list_system_cursors<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
) -> StorageResult<Vec<StreamCursor>> {
    let stream_name = StreamName::system_table_stream();
    let sql = format!(
        "SELECT cursor_name, position, row_blob FROM {relation_sys_user_stream_cursors} WHERE \
         stream_name = {} ORDER BY cursor_name LIMIT {}",
        engine.engine().placeholder(1),
        MAX_SYSTEM_STREAM_CURSOR_SCAN_ROWS + 1,
        relation_sys_user_stream_cursors = engine.relation_name("sys_user_stream_cursors"),
    );
    let page = native
        .query(
            SqlStatement::new(sql, vec![SqlValue::Blob(stream_name.as_ref().to_vec())]),
            protection,
            CURSOR_BYTES,
        )
        .await?;
    if page.more || page.rows.len() > MAX_SYSTEM_STREAM_CURSOR_SCAN_ROWS as usize {
        return Err(StorageError::unsupported(
            "System cursor scan exceeds its bound",
        ));
    }
    page.rows
        .into_iter()
        .map(|row| {
            let [name, position, bytes]: [SqlValue; 3] = row
                .into_values()
                .try_into()
                .map_err(|_| StorageError::internal("SQL System cursor row has invalid width"))?;
            let (SqlValue::Text(name), SqlValue::Blob(position), SqlValue::Blob(bytes)) =
                (name, position, bytes)
            else {
                return Err(StorageError::internal(
                    "SQL System cursor row has invalid types",
                ));
            };
            let position = StreamItemId::try_from(position.as_slice())
                .map_err(|_| StorageError::internal("SQL System cursor position is invalid"))?;
            let cursor: StreamCursor = storage_serde::from_bytes(&bytes)?;
            if cursor.stream_name != stream_name
                || cursor.name.as_str() != name
                || cursor.position != position
            {
                return Err(StorageError::internal("SQL System cursor identity differs"));
            }
            Ok(cursor)
        })
        .collect()
}

pub(crate) async fn write_cursor<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    cursor: StreamCursor,
    expected_position: Option<StreamItemId>,
) -> StorageResult<()> {
    let mut binds = vec![
        SqlValue::Blob(cursor.stream_name.as_ref().to_vec()),
        SqlValue::Text(cursor.name.as_str().to_owned()),
        SqlValue::Blob(cursor.position.as_bytes().to_vec()),
        SqlValue::Blob(storage_serde::to_bytes(&cursor)?),
    ];
    let sql = if let Some(expected) = expected_position {
        binds.push(SqlValue::Blob(expected.as_bytes().to_vec()));
        format!(
            "UPDATE {relation_sys_user_stream_cursors} SET position = {}, row_blob = {} WHERE \
             stream_name = {} AND cursor_name = {} AND position = {}",
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(5),
            relation_sys_user_stream_cursors = engine.relation_name("sys_user_stream_cursors"),
        )
    } else {
        format!(
            "INSERT INTO {relation_sys_user_stream_cursors} (stream_name, cursor_name, position, \
             row_blob) VALUES ({}) ON CONFLICT DO NOTHING",
            engine.engine().placeholders(4),
            relation_sys_user_stream_cursors = engine.relation_name("sys_user_stream_cursors"),
        )
    };
    let changed = native.execute(SqlStatement::new(sql, binds)).await?;
    if changed != 1 {
        return Err(StorageEnum::TransactionConflict {
            message: "SQL stream cursor position changed".to_owned(),
        }
        .into());
    }
    Ok(())
}

pub(crate) async fn delete_cursor<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    stream_name: &StreamName,
    cursor_name: &CursorName,
) -> StorageResult<()> {
    let sql = format!(
        "DELETE FROM {relation_sys_user_stream_cursors} WHERE stream_name = {} AND cursor_name = \
         {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        relation_sys_user_stream_cursors = engine.relation_name("sys_user_stream_cursors"),
    );
    let changed = native
        .execute(SqlStatement::new(
            sql,
            vec![
                SqlValue::Blob(stream_name.as_ref().to_vec()),
                SqlValue::Text(cursor_name.as_str().to_owned()),
            ],
        ))
        .await?;
    if changed != 1 {
        return Err(StorageEnum::TransactionConflict {
            message: "SQL stream cursor disappeared".to_owned(),
        }
        .into());
    }
    Ok(())
}
