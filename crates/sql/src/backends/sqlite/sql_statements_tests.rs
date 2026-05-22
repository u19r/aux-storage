use queue_provider::ReceiptHandle;
use storage_types::{StreamItemId, StreamName};
use stream_provider::ReadDirection;
use uuid::Uuid;

use super::sql_statements::{
    list_all_tables, list_tables_after, read_stream_from_position, update_message_checkpoint,
};

#[test]
fn update_message_checkpoint_uses_queue_name_column() {
    let receipt_handle = ReceiptHandle::from("receipt-handle");
    let (sql, _) =
        update_message_checkpoint(r#"{"progress":"50"}"#, "checkpoint-queue", &receipt_handle);

    assert!(sql.contains("queue_name = ?2"));
    assert!(!sql.contains("queue_url = ?2"));
}

#[test]
fn read_stream_from_position_uses_item_id_range_predicate() {
    let stream_name = StreamName::system_table_stream();
    let item_id = StreamItemId::from(Uuid::nil());

    let (forward_sql, _) =
        read_stream_from_position(&stream_name, &item_id, 100, ReadDirection::Forward);
    assert!(forward_sql.contains("stream_name = ?2 AND item_id > ?3"));
    assert!(!forward_sql.contains("IS NULL"));

    let (backward_sql, _) =
        read_stream_from_position(&stream_name, &item_id, 100, ReadDirection::Backward);
    assert!(backward_sql.contains("stream_name = ?2 AND item_id < ?3"));
    assert!(!backward_sql.contains("IS NULL"));
}

#[test]
fn list_tables_queries_use_distinct_sql_for_optional_start() {
    let (all_tables_sql, _) = list_all_tables(10);
    assert!(!all_tables_sql.contains("IS NULL"));
    assert!(!all_tables_sql.contains("WHERE table_name >"));

    let (tables_after_sql, _) = list_tables_after(10, "table_b".to_string());
    assert!(tables_after_sql.contains("WHERE table_name > ?1"));
    assert!(!tables_after_sql.contains("IS NULL"));
}
