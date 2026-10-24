pub(super) const fn expiration_progress_revision() -> &'static str {
    "SELECT revision FROM sys_maintenance_task WHERE kind = ?1"
}

// The fixture has one source table and one unsplit logical lineage.
// Keep the logical record and GSI hold so the consumer must detect missing
// history.
pub(super) fn inject_required_gsi_history_gap(
    engine: sql_backend::SqlEngine,
) -> sql_backend::SqlStatement {
    sql_backend::SqlStatement::new(
        format!(
            "DELETE FROM sys_dynamo_stream_items WHERE stream_name = {} AND item_id = (
         SELECT pointer_item_id FROM sys_dynamo_stream_records
         ORDER BY offset_order DESC LIMIT 1
     )",
            engine.placeholder(1)
        ),
        vec![sql_backend::SqlValue::Blob(
            storage_types::StreamName::system_table_stream()
                .as_ref()
                .to_vec(),
        )],
    )
}
