use uuid::Uuid;

use super::read_fdb_keys_concurrently;
use crate::{
    backends::fdb::{
        fdb_support_tests::connect_fdb_store, foundationdb_operation_metrics_reset,
        foundationdb_operation_metrics_snapshot,
    },
    sorted_kv_store::{SortedKvStore, TransactionPriority},
};

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster"]
async fn concurrent_point_reads_preserve_empty_order_duplicates_missing_and_snapshot_modes() {
    let Some(store) = connect_fdb_store("fdb-concurrent-point-reads").await else {
        eprintln!("Skipping concurrent point-read test: unable to connect to local cluster");
        return;
    };

    let namespace = format!("tests/concurrent-point-reads/{}/", Uuid::now_v7());
    let keys = (0..50)
        .map(|index| format!("{namespace}key-{index:02}").into_bytes())
        .collect::<Vec<_>>();
    let write_trx = store
        .create_transaction()
        .expect("create point-read fixture transaction");
    for (index, key) in keys.iter().enumerate() {
        write_trx.set(key, format!("value-{index:02}").as_bytes());
    }
    write_trx
        .commit()
        .await
        .expect("commit point-read fixtures");

    let mut requested_keys = Vec::with_capacity(100);
    let mut expected = Vec::with_capacity(100);
    for index in 0..100 {
        if index % 10 == 3 {
            requested_keys.push(format!("{namespace}missing-{index}").into_bytes());
            expected.push(None);
        } else {
            let key_index = (index * 7) % keys.len();
            requested_keys.push(keys[key_index].clone());
            expected.push(Some(format!("value-{key_index:02}").into_bytes()));
        }
    }

    let empty_trx = store
        .create_transaction()
        .expect("create empty point-read transaction");
    assert!(
        read_fdb_keys_concurrently(&empty_trx, Vec::new(), false)
            .await
            .expect("empty point-read batch")
            .is_empty()
    );

    for snapshot in [false, true] {
        let trx = store
            .create_transaction()
            .expect("create point-read transaction");
        let actual = read_fdb_keys_concurrently(&trx, requested_keys.clone(), snapshot)
            .await
            .expect("point-read batch");
        assert_eq!(actual, expected, "snapshot={snapshot}");
    }

    let single_trx = store
        .create_transaction()
        .expect("create single point-read transaction");
    assert_eq!(
        read_fdb_keys_concurrently(&single_trx, vec![keys[0].clone()], false)
            .await
            .expect("single point-read batch"),
        vec![Some(b"value-00".to_vec())]
    );
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster"]
async fn batch_priority_is_clone_local_and_configured_for_each_attempt() {
    let Some(store) = connect_fdb_store("fdb-batch-priority").await else {
        eprintln!("Skipping batch-priority test: unable to connect to local cluster");
        return;
    };
    let _metrics_guard = crate::backends::fdb::foundationdb_operation_metrics_test_guard();

    let batch_store = store.with_transaction_priority(TransactionPriority::Batch);
    assert_eq!(store.transaction_priority, TransactionPriority::Default);
    assert_eq!(batch_store.transaction_priority, TransactionPriority::Batch);
    assert!(std::sync::Arc::ptr_eq(
        &store.database,
        &batch_store.database
    ));
    assert!(std::sync::Arc::ptr_eq(&store.config, &batch_store.config));

    foundationdb_operation_metrics_reset();
    let trx = batch_store
        .create_transaction()
        .expect("create batch-priority transaction");
    batch_store
        .configure_transaction(&trx, Some("batch.priority.first"), true)
        .expect("configure first batch-priority transaction");
    trx.get(b"__batch_priority_first", false)
        .await
        .expect("execute first batch-priority read");

    let next_attempt = trx
        .on_error(foundationdb::FdbError::from_code(1020))
        .await
        .expect("recreate retry transaction");
    batch_store
        .configure_transaction(&next_attempt, Some("batch.priority.retry"), true)
        .expect("configure retried batch-priority transaction");

    let metrics = foundationdb_operation_metrics_snapshot();
    assert!(
        metrics.contains("operation=\"priority_batch\"")
            && metrics
                .lines()
                .filter(|line| line.contains("operation=\"priority_batch\""))
                .any(|line| line
                    .split_whitespace()
                    .last()
                    .is_some_and(|value| value != "0")),
        "batch-priority metric was not recorded: {metrics}"
    );
}
