use std::time::Duration;

use storage_provider::SqliteSettings;

use crate::{
    backends::sqlite::{SQLiteStorageProvider, transaction_manager::with_transaction},
    error_handler::map_sqlite_error,
};

async fn create_file_backed_providers() -> (
    tempfile::TempDir,
    SQLiteStorageProvider,
    SQLiteStorageProvider,
) {
    let temp_dir = crate::sql_test_support::temp_dir("database");
    let database_path = temp_dir.path().join("transaction-contention.sqlite");
    let database_path = database_path
        .to_str()
        .expect("temporary database path should be UTF-8");
    let settings = SqliteSettings {
        force_file_backed_database: true,
        ..SqliteSettings::default()
    };
    let first = SQLiteStorageProvider::new_with_settings(database_path, settings.clone())
        .await
        .expect("create first SQLite provider");
    let second = SQLiteStorageProvider::new_with_settings(database_path, settings)
        .await
        .expect("create second SQLite provider");
    first
        .connection
        .call(|connection| {
            connection.execute_batch(
                "CREATE TABLE transaction_contention (
                    id INTEGER PRIMARY KEY,
                    value INTEGER NOT NULL
                );
                INSERT INTO transaction_contention (id, value) VALUES (1, 0);",
            )?;
            Ok(())
        })
        .await
        .expect("create contention table");
    (temp_dir, first, second)
}

#[tokio::test]
async fn given_concurrent_writers_when_read_precedes_write_then_transactions_are_serialized() {
    let (_temp_dir, first, second) = create_file_backed_providers().await;
    let (read_started_tx, read_started_rx) = tokio::sync::oneshot::channel();
    let first_connection = first.connection.clone();
    let first_write = tokio::spawn(async move {
        with_transaction(&first_connection, move |sqlite| {
            sqlite
                .query_row(
                    "SELECT value FROM transaction_contention WHERE id = 1",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .map_err(map_sqlite_error)?;
            read_started_tx
                .send(())
                .map_err(|_| storage_types::StorageError::internal("signal initial read"))?;
            std::thread::sleep(Duration::from_millis(100));
            sqlite
                .execute(
                    "UPDATE transaction_contention SET value = 2 WHERE id = 1",
                    (),
                )
                .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    });

    read_started_rx.await.expect("wait for initial read");
    with_transaction(&second.connection, |sqlite| {
        sqlite
            .execute(
                "UPDATE transaction_contention SET value = 1 WHERE id = 1",
                (),
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    })
    .await
    .expect("second writer transaction");

    first_write
        .await
        .expect("join first writer")
        .expect("first writer transaction");
}
