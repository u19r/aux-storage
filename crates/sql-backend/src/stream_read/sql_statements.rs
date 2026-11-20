//! SQL text and bindings for ordered stream pages and point reads.

use storage_types::{StorageEnum, StorageResult, StreamItemId, StreamName};
use stream_provider::{ReadDirection, StreamReadRequest, SystemStreamHistoryRead};

use crate::{SqlEngine, SqlLayout, SqlStatement, SqlValue};

// A cursor may pass only transactions known complete in its read snapshot.
const POSTGRES_VISIBLE_USER_ROW: &str =
    " AND item_id < (decode('58494431', 'hex') || \
     int8send(pg_snapshot_xmin(pg_current_snapshot())::text::bigint + (SELECT transaction_offset \
     FROM sys_system_stream_scope WHERE scope_key = 'system')))";

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
    stream: &storage_types::StreamName,
    request: &StreamReadRequest,
) -> SqlStatement {
    let (mut predicates, binds) = row_predicates(engine, stream, request);
    if engine.engine() == SqlEngine::Postgres {
        predicates.push_str(POSTGRES_VISIBLE_USER_ROW);
    }
    row_statement(engine, request, predicates, binds)
}

pub(super) fn read_table_history_index(
    engine: impl SqlLayout,
    read: &stream_provider::TableHistoryIndexRead,
) -> StorageResult<SqlStatement> {
    table_history_statement(engine, read, "entry_blob")
}

pub(super) fn read_table_history_observations(
    engine: impl SqlLayout,
    read: &stream_provider::TableHistoryIndexRead,
) -> StorageResult<SqlStatement> {
    table_history_statement(engine, read, "created_at")
}

fn table_history_statement(
    engine: impl SqlLayout,
    read: &stream_provider::TableHistoryIndexRead,
    projection: &str,
) -> StorageResult<SqlStatement> {
    let (mut predicates, mut binds) = table_history_predicates(engine, &read.lane, read.through);
    if let Some(start) = read.exclusive_start {
        let comparison = match read.direction {
            ReadDirection::Forward => ">",
            ReadDirection::Backward => "<",
        };
        predicates.push_str(&format!(
            " AND offset_order {comparison} {}",
            engine.engine().placeholder(5)
        ));
        binds.push(SqlValue::Blob(sequence_order(start).to_be_bytes().to_vec()));
    }
    let direction = match read.direction {
        ReadDirection::Forward => "ASC",
        ReadDirection::Backward => "DESC",
    };
    let sql = format!(
        "SELECT table_id, shard_id, lineage_id, offset_order, offset_text, {projection} FROM {} \
         WHERE {predicates} ORDER BY offset_order {direction}, offset_text {direction} LIMIT {}",
        engine.relation_name("sys_stream_entries"),
        engine.engine().placeholder(binds.len() + 1)
    );
    binds.push(SqlValue::Integer(i64::from(read.limit.get()) + 1));
    Ok(SqlStatement::new(sql, binds))
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
    stream: &storage_types::StreamName,
    item_id: StreamItemId,
) -> SqlStatement {
    let sql = crate::sql_template::sql_template!(engine,
        "SELECT item_id, row_blob FROM " ["sys_dynamo_stream_items"], " WHERE stream_name = " {1},
        " AND item_id = " {2};
        postgres_suffix(true,
            " AND item_id < (decode('58494431', 'hex') || int8send(pg_snapshot_xmin(pg_current_snapshot())::text::bigint + (SELECT transaction_offset FROM sys_system_stream_scope WHERE scope_key = 'system')))")
    );
    SqlStatement::new(
        sql,
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
            "SELECT table_id, shard_id, lineage_id, offset_order, offset_text, entry_blob FROM {} \
             WHERE shard_id = {} AND lineage_id = {} AND offset_order > {} AND offset_order <= {} \
             ORDER BY offset_order, offset_text LIMIT {}",
            engine.relation_name("sys_stream_entries"),
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
    start: &storage_types::SystemStreamPosition,
    limit: usize,
) -> StorageResult<SqlStatement> {
    Ok(SqlStatement::new(
        format!(
            "SELECT shard_id, lineage_id, offset_order, offset_text, created_at FROM {} WHERE \
             shard_id = {} AND lineage_id = {} AND (offset_order, offset_text) > ({}, {}) ORDER \
             BY offset_order, offset_text LIMIT {limit}",
            engine.relation_name("sys_stream_entries"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2),
            engine.engine().placeholder(3),
            engine.engine().placeholder(4),
        ),
        vec![
            SqlValue::Text(start.shard_id().as_str().to_owned()),
            SqlValue::Text(start.lineage_id().as_str().to_owned()),
            SqlValue::Blob(
                start
                    .offset()
                    .order()
                    .map_err(|_| storage_types::StorageError::internal("Invalid trim offset"))?
                    .to_be_bytes()
                    .to_vec(),
            ),
            SqlValue::Text(start.offset().as_str().to_owned()),
        ],
    ))
}

fn sequence_order(sequence: storage_types::TableStreamSequence) -> u64 {
    let bytes = sequence.native_key();
    let mut order = [0; 8];
    order.copy_from_slice(&bytes.as_bytes()[4..]);
    u64::from_be_bytes(order)
}

fn table_history_predicates(
    engine: impl SqlLayout,
    lane: &storage_types::TableHistoryLane,
    through: storage_types::TableStreamSequence,
) -> (String, Vec<SqlValue>) {
    let predicates = format!(
        "table_id = {} AND shard_id = {} AND lineage_id = {} AND offset_order <= {}",
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
        engine.engine().placeholder(4)
    );
    let binds = vec![
        SqlValue::Blob(lane.history_id().as_ref().to_vec()),
        SqlValue::Text(lane.shard_id().as_str().to_owned()),
        SqlValue::Text(lane.lineage_id().as_str().to_owned()),
        SqlValue::Blob(sequence_order(through).to_be_bytes().to_vec()),
    ];
    (predicates, binds)
}

pub(super) fn read_table_history_boundary(
    engine: impl SqlLayout,
    read: &stream_provider::TableHistoryBoundaryRead,
) -> SqlStatement {
    let (predicates, binds) = table_history_predicates(engine, &read.lane, read.through);
    let direction = match read.direction {
        ReadDirection::Forward => "ASC",
        ReadDirection::Backward => "DESC",
    };
    SqlStatement::new(
        format!(
            "SELECT offset_order, offset_text FROM {} WHERE {predicates} ORDER BY offset_order \
             {direction}, offset_text {direction} LIMIT 1",
            engine.relation_name("sys_stream_entries")
        ),
        binds,
    )
}
