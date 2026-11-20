use storage_types::{StreamItemId, StreamName};

use super::sql_statements;
use crate::SqlEngine;

#[test]
fn given_stream_point_kinds_when_sql_rendered_then_preserve_full_sql_and_bind_order() {
    let item_id = StreamItemId::random();
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        for stream in [StreamName::new(b"stream"), StreamName::new(b"table")] {
            let suffix = if engine == SqlEngine::Postgres {
                " AND item_id < (decode('58494431', 'hex') || \
                 int8send(pg_snapshot_xmin(pg_current_snapshot())::text::bigint + (SELECT \
                 transaction_offset FROM sys_system_stream_scope WHERE scope_key = 'system')))"
            } else {
                ""
            };
            let (sql, binds) = sql_statements::read_item(engine, &stream, item_id).into_parts();
            assert!(matches!(sql, std::borrow::Cow::Borrowed(_)));
            assert_eq!(
                sql,
                format!(
                    "SELECT item_id, row_blob FROM sys_dynamo_stream_items WHERE stream_name = \
                     {prefix}1 AND item_id = {prefix}2{suffix}"
                )
            );
            assert!(
                matches!(binds.as_slice(), [crate::SqlValue::Blob(name), crate::SqlValue::Blob(id)] if name.as_slice() == stream.as_ref() && id.as_slice() == item_id.as_bytes())
            );
        }
    }
}
