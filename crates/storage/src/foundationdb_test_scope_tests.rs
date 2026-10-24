use std::{collections::HashMap, panic::AssertUnwindSafe};

use futures::FutureExt as _;
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, TableName,
};

use crate::{DatabaseManager, FoundationDbTestScope, PutItemInput};

async fn write_chunked_row(db: &DatabaseManager, table: &TableName) {
    db.create_table(&CreateTableRequest::new(
        table.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("real table creation");
    db.put_item(
        PutItemInput::builder()
            .table_name(table.clone())
            .item(HashMap::from([
                ("pk".to_owned(), AttributeValue::S("row".to_owned())),
                ("payload".to_owned(), AttributeValue::S("x".repeat(250_000))),
            ]))
            .build(),
    )
    .await
    .expect("real chunked item write");
}

async fn assert_neighbor_row(db: &DatabaseManager, table: &TableName) {
    let stored = db
        .get_item_decode::<_, storage_types::AttributeMap>(
            table.clone(),
            HashMap::from([("pk".to_owned(), AttributeValue::S("row".to_owned()))]),
        )
        .await
        .expect("neighbor read")
        .expect("neighbor row survives cleanup");
    assert_eq!(
        stored.get("payload"),
        Some(&AttributeValue::S("x".repeat(250_000)))
    );
}

#[tokio::test]
#[ignore = "requires a reachable FoundationDB cluster compatible with the pinned storage client"]
async fn given_two_owned_native_scopes_when_success_panic_or_cancellation_closes_one_then_all_its_keys_are_removed_and_neighbor_data_survives()
 {
    let cluster_file = std::env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE");
    let first = FoundationDbTestScope::try_new(cluster_file.clone())
        .await
        .expect("first owned scope");
    let second = FoundationDbTestScope::try_new(cluster_file)
        .await
        .expect("neighbor owned scope");
    assert_ne!(
        first.settings().subspace_prefix,
        second.settings().subspace_prefix
    );
    second
        .run(async {
            let neighbor = FoundationDbTestScope::open_database(second.settings().clone())
                .await
                .expect("neighbor database");
            let table = TableName::new("owned_scope_data");
            write_chunked_row(&neighbor, &table).await;
            first
                .run(async {
                    let db = FoundationDbTestScope::open_database(first.settings().clone())
                        .await
                        .expect("first database");
                    write_chunked_row(&db, &table).await;
                })
                .await
                .expect("success closes table metadata, item chunks, and control keys");
            first.cleanup().await.expect("cleanup is idempotent");
            assert_neighbor_row(&neighbor, &table).await;

            let panic = AssertUnwindSafe(first.run(async {
                let db = FoundationDbTestScope::open_database(first.settings().clone())
                    .await
                    .expect("reopened first database");
                write_chunked_row(&db, &table).await;
                panic!("native scope assertion failure");
            }))
            .catch_unwind()
            .await
            .expect_err("the original panic remains visible");
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&"native scope assertion failure")
            );
            first.cleanup().await.expect("panic cleanup left no keys");
            assert_neighbor_row(&neighbor, &table).await;

            // The surviving parent owns deletion after the cancelled writer
            // stops.
            let settings = first.settings().clone();
            let task_table = table.clone();
            let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
            let writer = tokio::spawn(async move {
                let db = FoundationDbTestScope::open_database(settings)
                    .await
                    .expect("cancelled writer database");
                write_chunked_row(&db, &task_table).await;
                ready_sender
                    .send(())
                    .expect("parent receives write completion");
                std::future::pending::<()>().await;
            });
            ready_receiver.await.expect("writer committed its data");
            writer.abort();
            assert!(
                writer
                    .await
                    .expect_err("writer cancellation")
                    .is_cancelled()
            );
            first
                .cleanup()
                .await
                .expect("parent removes cancelled writer data");
            first
                .cleanup()
                .await
                .expect("cancellation cleanup is idempotent");
            assert_neighbor_row(&neighbor, &table).await;
        })
        .await
        .expect("neighbor scope also closes completely");
}

struct NativeWriterProcess(std::process::Child);

impl NativeWriterProcess {
    fn terminate_and_reap(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if self.0.try_wait()?.is_none() {
            self.0.kill()?;
        }
        self.0.wait()
    }
}

impl Drop for NativeWriterProcess {
    fn drop(&mut self) {
        if let Err(error) = self.terminate_and_reap() {
            eprintln!("native writer process could not be reaped: {error}");
        }
    }
}

#[tokio::test]
#[ignore = "requires a reachable FoundationDB cluster compatible with the pinned storage client"]
async fn given_owned_native_writer_process_when_killed_and_reaped_then_parent_removes_all_its_keys_and_preserves_neighbor_data()
 {
    let cluster_file = std::env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE");
    let table = TableName::new("owned_scope_process_data");
    if let Ok(prefix) = std::env::var("FDB_OWNED_CHILD_NAMESPACE_PREFIX") {
        let db = FoundationDbTestScope::open_database(storage_provider::FoundationDbSettings {
            cluster_file: Some(cluster_file),
            subspace_prefix: Some(prefix),
            ..Default::default()
        })
        .await
        .expect("inherited child database");
        write_chunked_row(&db, &table).await;
        let ready_path =
            std::env::var_os("FDB_OWNED_CHILD_READY_PATH").expect("parent readiness path");
        std::fs::write(ready_path, "committed").expect("publish child readiness");
        std::future::pending::<()>().await;
        unreachable!("the parent terminates the writer after its committed write");
    }
    let first = FoundationDbTestScope::try_new(cluster_file.clone())
        .await
        .expect("process scope");
    let neighbor = FoundationDbTestScope::try_new(cluster_file.clone())
        .await
        .expect("neighbor scope");
    assert_ne!(
        first.settings().subspace_prefix,
        neighbor.settings().subspace_prefix
    );
    neighbor.run(async {
        let neighbor_db = FoundationDbTestScope::open_database(neighbor.settings().clone()).await.expect("neighbor database");
        write_chunked_row(&neighbor_db, &table).await;
        first.run(async {
            let artifacts = tempfile::tempdir().expect("owned process handshake workspace");
            let ready_path = artifacts.path().join("ready");
            let stderr_path = artifacts.path().join("child-stderr");
            let stderr = std::fs::File::create(&stderr_path).expect("owned child diagnostics");
            let child = std::process::Command::new(std::env::current_exe().expect("test executable"))
                .args(["--exact", "foundationdb_test_scope_tests::given_owned_native_writer_process_when_killed_and_reaped_then_parent_removes_all_its_keys_and_preserves_neighbor_data", "--ignored", "--nocapture"])
                .env("FDB_CLUSTER_FILE", &cluster_file)
                .env("FDB_OWNED_CHILD_NAMESPACE_PREFIX", first.settings().subspace_prefix.as_ref().expect("owned namespace"))
                .env("FDB_OWNED_CHILD_READY_PATH", &ready_path)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(stderr)
                .spawn().expect("native writer child");
            let mut writer = NativeWriterProcess(child);
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            while !ready_path.is_file() {
                assert!(writer.0.try_wait().expect("child status").is_none(), "writer exited before committing: {}", std::fs::read_to_string(&stderr_path).unwrap_or_default());
                assert!(tokio::time::Instant::now() < deadline, "writer readiness deadline: {}", std::fs::read_to_string(&stderr_path).unwrap_or_default());
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert_eq!(std::fs::read_to_string(&ready_path).expect("read readiness"), "committed");
            assert!(writer.0.try_wait().expect("ready child status").is_none(), "committed writer remains live until forced termination");
            let status = writer.terminate_and_reap().expect("force terminate and reap native writer");
            assert!(!status.success(), "the writer was forcibly terminated");
            #[cfg(unix)] {
                use std::os::unix::process::ExitStatusExt as _;
                assert_eq!(status.signal(), Some(9), "writer termination uses SIGKILL");
            }
        }).await.expect("parent removes process-owned metadata, chunks, and control keys after reaping");
        first.cleanup().await.expect("process cleanup is idempotent");
        assert_neighbor_row(&neighbor_db, &table).await;
    }).await.expect("neighbor closes completely");
}
