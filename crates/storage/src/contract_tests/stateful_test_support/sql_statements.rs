pub(super) const fn expiration_progress_revision() -> &'static str {
    "SELECT revision FROM sys_maintenance_task WHERE kind = ?1"
}

// Keep the source reference and GSI checkpoint while deleting its exact item
// history body.
pub(super) fn latest_system_record() -> sql_backend::SqlStatement {
    sql_backend::SqlStatement::new(
        "SELECT record_blob FROM sys_dynamo_stream_records ORDER BY offset_order DESC LIMIT 1",
        vec![],
    )
}

pub(super) fn delete_history_body(
    engine: sql_backend::SqlEngine,
    pointer: &storage_types::ItemHistoryPointer,
) -> storage_types::StorageResult<sql_backend::SqlStatement> {
    Ok(sql_backend::SqlStatement::new(
        format!(
            "DELETE FROM sys_dynamo_stream_items WHERE stream_name = {} AND item_id = {}",
            engine.placeholder(1),
            engine.placeholder(2)
        ),
        vec![
            sql_backend::SqlValue::Blob(
                pointer
                    .metadata()
                    .address()
                    .physical_stream_name()?
                    .as_ref()
                    .to_vec(),
            ),
            sql_backend::SqlValue::Blob(
                storage_types::StreamItemId::from(pointer.metadata().version())
                    .as_bytes()
                    .to_vec(),
            ),
        ],
    ))
}
