use crate::{
    dialect::{PostgresDialect, SqliteDialect, TursoDialect},
    provider_core::statements::stream,
};

#[test]
fn sqlite_and_turso_stream_sql_matches_for_shared_operations() {
    let sqlite = SqliteDialect;
    let turso = TursoDialect;
    let sqlite_batch = stream::batch_insert_stream_entries("(?1, ?2, ?3, ?4, ?5)");
    let turso_batch = stream::batch_insert_stream_entries("(?1, ?2, ?3, ?4, ?5)");

    let sqlite_statements = [
        stream::insert_stream_entry(&sqlite),
        sqlite_batch.as_str(),
        stream::get_stream_internal_id(&sqlite),
        stream::delete_stream_cursors(&sqlite),
        stream::delete_stream_items(&sqlite),
        stream::delete_user_stream(&sqlite),
        stream::get_stream(&sqlite),
        stream::read_stream_forward(&sqlite),
        stream::read_stream_backward(&sqlite),
        stream::get_latest_stream_item(&sqlite),
        stream::insert_cursor(&sqlite),
        stream::delete_cursor(&sqlite),
        stream::get_cursor_position(&sqlite),
        stream::advance_cursor_position(&sqlite),
    ];
    let turso_statements = [
        stream::insert_stream_entry(&turso),
        turso_batch.as_str(),
        stream::get_stream_internal_id(&turso),
        stream::delete_stream_cursors(&turso),
        stream::delete_stream_items(&turso),
        stream::delete_user_stream(&turso),
        stream::get_stream(&turso),
        stream::read_stream_forward(&turso),
        stream::read_stream_backward(&turso),
        stream::get_latest_stream_item(&turso),
        stream::insert_cursor(&turso),
        stream::delete_cursor(&turso),
        stream::get_cursor_position(&turso),
        stream::advance_cursor_position(&turso),
    ];

    assert_eq!(sqlite_statements, turso_statements);
}

#[test]
fn stream_ddl_documents_existing_backend_overrides() {
    assert!(stream::create_stream_items_table(&SqliteDialect).contains("DEFAULT 2"));
    assert!(!stream::create_stream_items_table(&TursoDialect).contains("DEFAULT 2"));
    assert!(stream::create_stream_items_table(&PostgresDialect).contains("BYTEA"));
}
