//! SQL text and bindings for ordered stream pages and point reads.

use storage_types::{StorageEnum, StorageResult, StreamItemId, StreamName};
use stream_provider::{ReadDirection, StreamReadRequest, StreamRowKind, SystemStreamHistoryRead};

use crate::{SqlEngine, SqlLayout, SqlStatement, SqlValue};

// A cursor may pass only transactions known complete in its read snapshot.
const POSTGRES_VISIBLE_USER_ROW: &str =
    " AND item_id < (decode('58494431', 'hex') || \
     int8send(pg_snapshot_xmin(pg_current_snapshot())::text::bigint))";

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

pub(super) fn read_reference(
    engine: impl SqlLayout,
    stream: &StreamName,
    item_id: StreamItemId,
) -> SqlStatement {
    SqlStatement::new(
        crate::sql_template::sql_template!(engine,
            "SELECT item_stream, item_item_id, owner_kind FROM " ["sys_dynamo_stream_pointer_owners"],
            " WHERE owner_stream = " {1}, " AND owner_item_id = " {2}
        ),
        vec![
            SqlValue::Blob(stream.as_ref().to_vec()),
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
            "SELECT r.shard_id, r.lineage_id, r.offset_order, r.offset_text, r.pointer_item_id, \
             r.record_blob, i.row_blob FROM {relation_sys_dynamo_stream_records} AS r LEFT JOIN \
             {relation_sys_dynamo_stream_items} AS i ON i.stream_name = {} AND i.item_id = \
             r.pointer_item_id WHERE r.shard_id = {} AND r.lineage_id = {} AND r.offset_order > \
             {} AND r.offset_order <= {} ORDER BY r.offset_order, r.offset_text LIMIT {}",
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
            engine.engine().placeholder(5),
            engine.engine().placeholder(6),
            relation_sys_dynamo_stream_records = engine.relation_name("sys_dynamo_stream_records"),
            relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
        ),
        vec![
            SqlValue::Blob(StreamName::system_table_stream().as_ref().to_vec()),
            SqlValue::Text(read.after.shard_id().as_str().to_owned()),
            SqlValue::Text(read.after.lineage_id().as_str().to_owned()),
            SqlValue::Blob(after.to_be_bytes().to_vec()),
            SqlValue::Blob(read.through.to_be_bytes().to_vec()),
            SqlValue::Integer(i64::from(read.limit) + 1),
        ],
    ))
}

pub(super) fn read_effect(
    engine: impl SqlLayout,
    effect: &storage_types::SystemStreamEffectDigest,
) -> SqlStatement {
    SqlStatement::new(
        crate::sql_template::sql_template!(engine,
            "SELECT receipt_blob FROM " ["sys_dynamo_stream_effects"], " WHERE effect_id = " {1}
        ),
        vec![SqlValue::Text(effect.as_str().to_owned())],
    )
}

pub(super) fn read_record(
    engine: impl SqlLayout,
    position: &storage_types::SystemStreamPosition,
) -> StorageResult<SqlStatement> {
    let order = position
        .offset()
        .order()
        .map_err(|_| StorageEnum::HistoryUnavailable)?;
    Ok(SqlStatement::new(
        crate::sql_template::sql_template!(engine,
            "SELECT record_blob FROM " ["sys_dynamo_stream_records"],
            " WHERE shard_id = " {1}, " AND lineage_id = " {2},
            " AND offset_order = " {3}, " AND offset_text = " {4}
        ),
        vec![
            SqlValue::Text(position.shard_id().as_str().to_owned()),
            SqlValue::Text(position.lineage_id().as_str().to_owned()),
            SqlValue::Blob(order.to_be_bytes().to_vec()),
            SqlValue::Text(position.offset().as_str().to_owned()),
        ],
    ))
}

pub(crate) fn scan_trim_records(
    engine: impl SqlLayout,
    after: Option<&storage_types::SystemStreamPosition>,
) -> StorageResult<SqlStatement> {
    let (predicate, mut binds) = trim_cursor_predicate(engine, after)?;
    Ok(SqlStatement::new(
        format!(
            "SELECT r.shard_id, r.lineage_id, r.offset_order, r.offset_text, r.effect_id, \
             r.pointer_item_id, r.created_at, r.record_blob, i.row_blob FROM \
             {relation_sys_dynamo_stream_records} AS r LEFT JOIN \
             {relation_sys_dynamo_stream_items} AS i ON i.stream_name = {} AND i.item_id = \
             r.pointer_item_id {predicate} ORDER BY r.shard_id, r.lineage_id, r.offset_order, \
             r.offset_text LIMIT {}",
            engine.engine().placeholder(binds.len() + 1),
            stream_provider::MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS + 1,
            relation_sys_dynamo_stream_records = engine.relation_name("sys_dynamo_stream_records"),
            relation_sys_dynamo_stream_items = engine.relation_name("sys_dynamo_stream_items"),
        ),
        {
            binds.push(SqlValue::Blob(
                StreamName::system_table_stream().as_ref().to_vec(),
            ));
            binds
        },
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
