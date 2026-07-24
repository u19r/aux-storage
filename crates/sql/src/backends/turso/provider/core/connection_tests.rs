use std::{sync::Arc, time::Duration};

use tokio::sync::Notify;

use crate::backends::turso::provider::TursoStorageProvider;

#[tokio::test]
async fn given_sustained_write_contention_when_transaction_retries_then_operation_succeeds() {
    let temp_dir = tempfile::tempdir().expect("temporary database directory");
    let database_path = temp_dir.path().join("contention.sqlite");
    let provider = TursoStorageProvider::new(
        database_path
            .to_str()
            .expect("temporary database path should be UTF-8"),
    )
    .await
    .expect("create Turso provider");
    let connection = provider.primary_connection().await.expect("connection");
    connection
        .execute(
            "CREATE TABLE transaction_contention (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
            (),
        )
        .await
        .expect("create contention table");
    connection
        .execute(
            "INSERT INTO transaction_contention (id, value) VALUES (1, 0)",
            (),
        )
        .await
        .expect("seed contention row");
    drop(connection);

    let holder_provider = provider.clone();
    let holder_started = Arc::new(Notify::new());
    let holder_started_task = Arc::clone(&holder_started);
    let holder = tokio::spawn(async move {
        holder_provider
            .with_transaction(false, |connection| {
                let holder_provider = holder_provider.clone();
                let holder_started = Arc::clone(&holder_started_task);
                Box::pin(async move {
                    holder_provider
                        .execute(
                            connection,
                            "UPDATE transaction_contention SET value = 1 WHERE id = 1",
                            Vec::new(),
                        )
                        .await?;
                    holder_started.notify_one();
                    tokio::time::sleep(Duration::from_millis(2_500)).await;
                    Ok(())
                })
            })
            .await
    });

    holder_started.notified().await;
    let retry_provider = provider.clone();
    provider
        .with_transaction(true, move |connection| {
            let retry_provider = retry_provider.clone();
            Box::pin(async move {
                retry_provider
                    .execute(
                        connection,
                        "UPDATE transaction_contention SET value = 2 WHERE id = 1",
                        Vec::new(),
                    )
                    .await?;
                Ok(())
            })
        })
        .await
        .expect("retry transaction after sustained contention");
    holder
        .await
        .expect("join contention holder")
        .expect("contention holder transaction");
}
