use crate::{
    dialect::{PostgresDialect, SqliteDialect, TursoDialect},
    provider_core::statements::queue,
};

#[test]
fn sqlite_and_turso_queue_sql_matches_when_dialect_has_no_override() {
    let sqlite = SqliteDialect;
    let turso = TursoDialect;

    let sqlite_statements = [
        queue::create_queues_table(&sqlite),
        queue::create_messages_table(&sqlite),
        queue::create_messages_queue_visibility_index(&sqlite),
        queue::create_messages_queue_receipt_index(&sqlite),
        queue::upsert_queue(&sqlite),
        queue::get_queue(&sqlite),
        queue::get_queue_by_name(&sqlite),
        queue::list_queues_with_prefix(&sqlite),
        queue::list_all_queues(&sqlite),
        queue::delete_messages_for_queue(&sqlite),
        queue::delete_queue(&sqlite),
        queue::set_queue_attributes(&sqlite),
        queue::send_message(&sqlite),
        queue::receive_messages(&sqlite),
        queue::claim_message(&sqlite),
        queue::delete_message(&sqlite),
        queue::change_message_visibility(&sqlite),
        queue::update_message_checkpoint(&sqlite),
    ];
    let turso_statements = [
        queue::create_queues_table(&turso),
        queue::create_messages_table(&turso),
        queue::create_messages_queue_visibility_index(&turso),
        queue::create_messages_queue_receipt_index(&turso),
        queue::upsert_queue(&turso),
        queue::get_queue(&turso),
        queue::get_queue_by_name(&turso),
        queue::list_queues_with_prefix(&turso),
        queue::list_all_queues(&turso),
        queue::delete_messages_for_queue(&turso),
        queue::delete_queue(&turso),
        queue::set_queue_attributes(&turso),
        queue::send_message(&turso),
        queue::receive_messages(&turso),
        queue::claim_message(&turso),
        queue::delete_message(&turso),
        queue::change_message_visibility(&turso),
        queue::update_message_checkpoint(&turso),
    ];

    assert_eq!(sqlite_statements, turso_statements);
}

#[test]
fn postgres_queue_sql_uses_postgres_placeholders_and_upsert() {
    assert!(queue::receive_messages(&PostgresDialect).contains("LIMIT $3"));
    assert!(queue::upsert_queue(&PostgresDialect).contains("ON CONFLICT"));
}
