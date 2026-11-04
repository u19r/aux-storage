//! SQL text and bindings for ordered stream pages and point reads.

use storage_types::{StorageEnum, StorageResult, StreamItemId, StreamName};
use stream_provider::{ReadDirection, StreamReadRequest, StreamRowKind, SystemStreamHistoryRead};

use crate::{SqlEngine, SqlLayout, SqlStatement, SqlValue};

// A cursor may pass only transactions known complete in its read snapshot.
const POSTGRES_VISIBLE_USER_ROW: &str =
    " AND item_id < (decode('58494431', 'hex') || \
     int8send(pg_snapshot_xmin(pg_current_snapshot())::text::bigint))";

pub(super) fn read_history_table_states(
    engine: impl SqlLayout,
    histories: &[storage_types::TableHistoryId],
) -> [SqlStatement; 2] {
    let placeholders = (1..=histories.len())
        .map(|index| engine.engine().placeholder(index))
        .collect::<Vec<_>>()
        .join(", ");
    let binds = || {
        histories
            .iter()
            .map(|history| SqlValue::Text(uuid::Uuid::from_bytes(*history.as_ref()).to_string()))
            .collect::<Vec<_>>()
    };
    // Separate selects preserve Postgres row protection; FOR UPDATE cannot
    // protect a UNION result. The driver pipelines both in this snapshot.
    [
        SqlStatement::new(
            format!(
                "SELECT id, 0, table_name, key_schema FROM {} WHERE id IN ({placeholders}) AND \
                 table_status <> 'DELETING'",
                engine.relation_name("tables"),
            ),
            binds(),
        ),
        SqlStatement::new(
            format!(
                "SELECT history_id, 1 FROM {} WHERE history_id IN ({placeholders})",
                engine.relation_name("sys_table_reclaim"),
            ),
            binds(),
        ),
    ]
}

pub(super) fn read_rows(
    engine: impl SqlLayout,
    stream: &StreamRowKind,
    request: &StreamReadRequest,
) -> SqlStatement {
    let (mut predicates, binds) = row_predicates(engine, stream.value(), request);
    if engine.engine() == SqlEngine::Postgres && matches!(stream, StreamRowKind::User(_)) {
        predicates.push_str(POSTGRES_VISIBLE_USER_ROW);
    }
    if engine.engine() == SqlEngine::Postgres && !matches!(stream, StreamRowKind::User(_)) {
        predicates.push_str(
            " AND item_id < (decode('ffffffff', 'hex') || \
             int8send(pg_snapshot_xmin(pg_current_snapshot())::text::bigint * 65536))",
        );
    }
    row_statement(engine, request, predicates, binds)
}

pub(super) fn read_table_history_index(
    engine: impl SqlLayout,
    read: &stream_provider::TableHistoryIndexRead,
) -> StorageResult<SqlStatement> {
    let request = read.row_read();
    let (mut predicates, mut binds) =
        row_predicates(engine, &read.lane.physical_stream_name()?, &request);
    predicates.push_str(&format!(
        " AND item_id <= {}",
        engine.engine().placeholder(binds.len() + 1)
    ));
    binds.push(SqlValue::Blob(
        read.through.native_key().as_bytes().to_vec(),
    ));
    // The caller captured this lineage's safe committed high. Applying the
    // old merged-pointer xmin encoding here would compare unrelated layouts.
    Ok(row_statement(engine, &request, predicates, binds))
}

fn row_predicates(
    engine: impl SqlLayout,
    stream: &StreamName,
    request: &StreamReadRequest,
) -> (String, Vec<SqlValue>) {
    let mut predicates = format!("stream_name = {}", engine.engine().placeholder(1));
    let mut binds = vec![SqlValue::Blob(stream.as_ref().to_vec())];
    if let Some(start) = request.exclusive_start {
        let comparison = match request.direction {
            ReadDirection::Forward => ">",
            ReadDirection::Backward => "<",
        };
        predicates.push_str(&format!(
            " AND item_id {comparison} {}",
            engine.engine().placeholder(2)
        ));
        binds.push(SqlValue::Blob(start.as_bytes().to_vec()));
    }
    (predicates, binds)
}

fn row_statement(
    engine: impl SqlLayout,
    request: &StreamReadRequest,
    predicates: String,
    mut binds: Vec<SqlValue>,
) -> SqlStatement {
    let order = match request.direction {
        ReadDirection::Forward => "ASC",
        ReadDirection::Backward => "DESC",
    };
    let sql = format!(
        "SELECT item_id, row_blob FROM {relation_sys_dynamo_stream_items} WHERE {predicates} \
         ORDER BY item_id {order} LIMIT {}",
        engine.engine().placeholder(binds.len() + 1),
        relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
    );
    binds.push(SqlValue::Integer(i64::from(request.limit.get()) + 1));
    SqlStatement::new(sql, binds)
}

pub(super) fn read_item(
    engine: impl SqlLayout,
    stream: &StreamRowKind,
    item_id: StreamItemId,
) -> SqlStatement {
    let sql = crate::sql_template::sql_template!(engine,
        "SELECT item_id, row_blob FROM " ["sys_dynamo_stream_items"], " WHERE stream_name = " {1},
        " AND item_id = " {2};
        postgres_suffix(matches!(stream, StreamRowKind::User(_)),
            " AND item_id < (decode('58494431', 'hex') || int8send(pg_snapshot_xmin(pg_current_snapshot())::text::bigint))")
    );
    SqlStatement::new(
        sql,
        vec![
            SqlValue::Blob(stream.value().as_ref().to_vec()),
            SqlValue::Blob(item_id.as_bytes().to_vec()),
        ],
    )
}

pub(super) fn read_rows_through(
    engine: impl SqlLayout,
    stream: &StreamName,
    request: &StreamReadRequest,
    through: StreamItemId,
) -> SqlStatement {
    let (mut predicates, mut binds) = row_predicates(engine, stream, request);
    predicates.push_str(&format!(
        " AND item_id <= {}",
        engine.engine().placeholder(binds.len() + 1)
    ));
    binds.push(SqlValue::Blob(through.as_bytes().to_vec()));
    row_statement(engine, request, predicates, binds)
}

pub(super) fn read_item_through(
    engine: impl SqlLayout,
    stream: &StreamName,
    id: StreamItemId,
    through: StreamItemId,
) -> SqlStatement {
    let sql = crate::sql_template::sql_template!(engine,
        "SELECT item_id, row_blob FROM " ["sys_dynamo_stream_items"], " WHERE stream_name = " {1},
        " AND item_id = " {2}, " AND item_id <= " {3}
    );
    SqlStatement::new(
        sql,
        vec![
            SqlValue::Blob(stream.as_ref().to_vec()),
            SqlValue::Blob(id.as_bytes().to_vec()),
            SqlValue::Blob(through.as_bytes().to_vec()),
        ],
    )
}

pub(super) fn read_history(
    engine: impl SqlLayout,
    read: &SystemStreamHistoryRead,
) -> StorageResult<SqlStatement> {
    let after = read
        .after
        .offset()
        .order()
        .map_err(|_| StorageEnum::HistoryUnavailable)?;
    Ok(SqlStatement::new(
        format!(
            "SELECT shard_id, lineage_id, offset_order, offset_text, table_id, record_blob, \
             created_at FROM {} WHERE shard_id = {} AND lineage_id = {} AND offset_order > {} AND \
             offset_order <= {} ORDER BY offset_order, offset_text LIMIT {}",
            engine.relation_name("sys_dynamo_stream_records"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            engine.engine().placeholder(5)
        ),
        vec![
            SqlValue::Text(read.after.shard_id().as_str().to_owned()),
            SqlValue::Text(read.after.lineage_id().as_str().to_owned()),
            SqlValue::Blob(after.to_be_bytes().to_vec()),
            SqlValue::Blob(read.through.to_be_bytes().to_vec()),
            SqlValue::Integer(i64::from(read.limit) + 1),
        ],
    ))
}

pub(crate) fn scan_trim_records(
    engine: impl SqlLayout,
    after: Option<&storage_types::SystemStreamPosition>,
) -> StorageResult<SqlStatement> {
    let (predicate, binds) = trim_cursor_predicate(engine, after)?;
    Ok(SqlStatement::new(
        format!(
            "SELECT r.shard_id, r.lineage_id, r.offset_order, r.offset_text, r.table_id, \
             r.created_at FROM {} AS r {predicate} ORDER BY r.shard_id, r.lineage_id, \
             r.offset_order, r.offset_text LIMIT {}",
            engine.relation_name("sys_dynamo_stream_records"),
            stream_provider::MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS + 1
        ),
        binds,
    ))
}

fn trim_cursor_predicate(
    engine: impl SqlLayout,
    after: Option<&storage_types::SystemStreamPosition>,
) -> StorageResult<(String, Vec<SqlValue>)> {
    let mut binds = Vec::new();
    let predicate = if let Some(position) = after {
        binds.extend([
            SqlValue::Text(position.shard_id().as_str().to_owned()),
            SqlValue::Text(position.lineage_id().as_str().to_owned()),
            SqlValue::Blob(
                position
                    .offset()
                    .order()
                    .map_err(|_| {
                        storage_types::StorageError::internal(
                            "SystemStream trim cursor offset is invalid",
                        )
                    })?
                    .to_be_bytes()
                    .to_vec(),
            ),
            SqlValue::Text(position.offset().as_str().to_owned()),
        ]);
        format!(
            "WHERE (r.shard_id, r.lineage_id, r.offset_order, r.offset_text) > ({})",
            engine.engine().placeholders(4),
        )
    } else {
        String::new()
    };
    Ok((predicate, binds))
}

pub(super) fn read_history_metadata(
    engine: impl SqlLayout,
    history: &storage_types::ItemHistoryAddress,
) -> StorageResult<SqlStatement> {
    let (sql, binds) = history_header_statement(engine, history)?.into_parts();
    Ok(SqlStatement::new(
        format!(
            "{sql} ORDER BY item_id ASC LIMIT {}",
            storage_provider::maintenance::ItemHistoryMetadataPage::MAX_ENTRIES + 1
        ),
        binds,
    ))
}

pub(super) fn read_history_header(
    engine: impl SqlLayout,
    reference: &storage_types::ItemHistoryReference,
) -> StorageResult<SqlStatement> {
    let (sql, mut binds) = history_header_statement(engine, reference.address())?.into_parts();
    binds.push(SqlValue::Blob(
        StreamItemId::from(reference.version()).as_bytes().to_vec(),
    ));
    Ok(SqlStatement::new(
        format!("{sql} AND item_id = {}", engine.engine().placeholder(2)),
        binds,
    ))
}

fn history_header_statement(
    engine: impl SqlLayout,
    history: &storage_types::ItemHistoryAddress,
) -> StorageResult<SqlStatement> {
    let stream = history.physical_stream_name()?;
    // This codec-derived numeric bound is SQL syntax: PostgreSQL substr takes
    // int4, while the shared SQL integer bind is int8.
    let prefix = stream_provider::StreamRowValue::history_metadata_prefix_bytes(history)?.get();
    let length = if engine.engine() == SqlEngine::Postgres {
        "octet_length(row_blob)"
    } else {
        "length(row_blob)"
    };
    Ok(SqlStatement::new(
        format!(
            "SELECT item_id, {length}, substr(row_blob, 1, {prefix}) FROM {} WHERE stream_name = \
             {}",
            engine.relation_name("sys_dynamo_stream_items"),
            engine.engine().placeholder(1),
        ),
        vec![SqlValue::Blob(stream.as_ref().to_vec())],
    ))
}

pub(super) fn read_history_namespace(
    engine: impl SqlLayout,
    history: storage_types::TableHistoryId,
    after: Option<&[u8]>,
) -> StorageResult<SqlStatement> {
    let prefix = history.stream_prefix();
    if after.is_some_and(|cursor| !cursor.starts_with(&prefix)) {
        return Err(storage_types::StorageError::internal(
            "History cursor belongs to another namespace",
        ));
    }
    let mut end = prefix.clone();
    let _ = end.pop();
    end.push(b'0');
    let mut sql = format!(
        "SELECT stream_name FROM {} WHERE stream_name >= {} AND stream_name < {}",
        engine.relation_name("sys_dynamo_stream_items"),
        engine.engine().placeholder(1),
        engine.engine().placeholder(2)
    );
    let mut values = vec![SqlValue::Blob(prefix), SqlValue::Blob(end)];
    if let Some(after) = after {
        sql.push_str(&format!(
            " AND stream_name > {}",
            engine.engine().placeholder(3)
        ));
        values.push(SqlValue::Blob(after.to_vec()));
    }
    sql.push_str(" ORDER BY stream_name LIMIT 1");
    Ok(SqlStatement::new(sql, values))
}
