#[tokio::test]
async fn given_future_and_due_markers_in_one_hour_when_cleanup_reads_one_marker_then_due_work_progresses()
 {
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(":memory:")
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    storage_provider::retention_test_support::qualify_due_marker_progress(&store).await;
}

#[tokio::test]
async fn given_pending_and_idle_tables_with_equal_deadlines_when_cleanup_reads_one_marker_then_both_make_progress()
 {
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(":memory:")
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    storage_provider::retention_test_support::qualify_pending_marker_progress(&store).await;
}
