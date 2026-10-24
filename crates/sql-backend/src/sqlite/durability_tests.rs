use super::durability::initialize_batch;

#[tokio::test]
async fn given_memory_database_when_initialized_then_has_no_durability_batch() {
    let connection = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
    assert!(initialize_batch(connection).await.unwrap().is_none());
}

#[tokio::test]
async fn given_pinned_reader_when_writes_commit_then_batch_acknowledges_without_barrier_table() {
    let path = std::env::temp_dir().join(format!("sqlite-durability-{}.db", uuid::Uuid::now_v7()));
    let connection = tokio_rusqlite::Connection::open(&path).await.unwrap();
    connection
        .call(|native| {
            native.pragma_update(None, "journal_mode", "WAL")?;
            native.pragma_update(None, "synchronous", "NORMAL")?;
            native.execute_batch("CREATE TABLE proof (id INTEGER PRIMARY KEY)")?;
            Ok(())
        })
        .await
        .unwrap();
    let batch = initialize_batch(connection.clone()).await.unwrap().unwrap();
    let reader = rusqlite::Connection::open(&path).unwrap();
    reader.execute_batch("BEGIN; SELECT * FROM proof;").unwrap();
    let writes = (0..32).map(|id| {
        let connection = connection.clone();
        let batch = batch.clone();
        async move {
            connection
                .call(move |native| {
                    native.execute("INSERT INTO proof VALUES (?)", [id])?;
                    Ok(())
                })
                .await
                .unwrap();
            batch.wait().await
        }
    });
    for result in futures::future::join_all(writes).await {
        result.unwrap();
    }
    reader.execute_batch("ROLLBACK").unwrap();
    drop(reader);
    connection
        .call(|native| {
            native
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); INSERT INTO proof VALUES (32)")?;
            Ok(())
        })
        .await
        .unwrap();
    batch.wait().await.unwrap();
    drop(batch);
    connection.close().await.unwrap();
    let reopened = rusqlite::Connection::open(&path).unwrap();
    let count: i64 = reopened
        .query_row("SELECT count(*) FROM proof", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 33);
    let tables: i64 = reopened
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tables, 1);
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn given_missing_wal_when_batch_waits_then_outcome_is_unknown() {
    let path = std::env::temp_dir().join(format!("sqlite-durability-{}.db", uuid::Uuid::now_v7()));
    let connection = tokio_rusqlite::Connection::open(&path).await.unwrap();
    connection
        .call(|native| {
            native.pragma_update(None, "journal_mode", "WAL")?;
            native.execute_batch("CREATE TABLE proof (id INTEGER PRIMARY KEY)")?;
            Ok(())
        })
        .await
        .unwrap();
    let batch = initialize_batch(connection.clone()).await.unwrap().unwrap();
    let mut wal_path = path.as_os_str().to_os_string();
    wal_path.push("-wal");
    std::fs::remove_file(wal_path).unwrap();
    assert!(matches!(
        (batch.wait().await.unwrap_err()).base_error(),
        Some(storage_types::StorageEnum::SyncSubmissionAmbiguous { .. })
    ));
    drop(batch);
    connection.close().await.unwrap();
    std::fs::remove_file(path).unwrap();
}
