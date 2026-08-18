use std::{path::PathBuf, time::Duration};

use storage_provider::ReadSequenceMappedRangeRequest;
use storage_types::{
    AttributeValue, GlobalSecondaryIndex, IndexKey, IndexName, ItemKey, KeySchemaElement, KeyType,
    Projection, TableKey, TableName,
};

use super::{
    mapped_range::MappedRangeAttemptError, read_context::read_sequence_mapped_range_attempt,
};
use crate::{
    FoundationDbConfig, FoundationDbKvStore,
    keyspace::{compact::TableStorageId, table_identity::TableIdentity, tuple_keys},
};

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster"]
async fn mapped_range_transaction_too_old_rebuilds_the_whole_attempt() {
    let cluster_file = std::env::var_os("FDB_CLUSTER_FILE")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
        .or_else(|| {
            [
                "/usr/local/etc/foundationdb/fdb.cluster",
                "/opt/homebrew/etc/foundationdb/fdb.cluster",
                "/etc/foundationdb/fdb.cluster",
            ]
            .into_iter()
            .map(PathBuf::from)
            .find(|path| path.is_file())
        });
    let Some(cluster_file) = cluster_file else {
        return;
    };
    let store = FoundationDbKvStore::connect(FoundationDbConfig {
        cluster_file_path: Some(cluster_file.to_string_lossy().into_owned()),
        tenant_name: Some(format!("mapped-retry-{}", uuid::Uuid::now_v7()).into_bytes()),
        subspace_prefix: None,
        cache_read_version_ms: 0,
        immediate_gsi_consistency: false,
        report_conflicting_keys: false,
    })
    .expect("connect FoundationDB store");
    store
        .check_reachable(Duration::from_secs(3))
        .await
        .expect("FoundationDB is reachable");

    let table_name = TableName::new("mapped-retry");
    let index_name = IndexName::new("status");
    let tenant_keyspace = store.config().tenant_name.clone().unwrap_or_default();
    let table = TableIdentity::user_indexes_for_table_with_tenant(
        TableStorageId::new(7),
        &table_name,
        Some(&[GlobalSecondaryIndex {
            index_name: index_name.clone(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "status".to_string(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: None,
                non_key_attributes: None,
            },
        }]),
        tenant_keyspace,
    );
    let primary_key = tuple_keys::item_key(
        &table,
        &ItemKey::Table(TableKey::new(
            table_name.clone(),
            AttributeValue::S("pk".to_string()),
            None,
        )),
    )
    .expect("build primary fixture key");
    let gsi_key = tuple_keys::item_key(
        &table,
        &ItemKey::Index(IndexKey::new(
            table_name,
            index_name.clone(),
            AttributeValue::S("open".to_string()),
            None,
            TableKey::new(
                TableName::new("mapped-retry"),
                AttributeValue::S("pk".to_string()),
                None,
            ),
        )),
    )
    .expect("build GSI fixture key");
    let gsi_range = tuple_keys::gsi_prefix(&table, &index_name).expect("build GSI fixture range");
    let fixture = store
        .create_transaction()
        .expect("create mapped fixture transaction");
    fixture.set(&store.prefix_slice(&primary_key), b"retry-value");
    fixture.set(&store.prefix_slice(&gsi_key), b"gsi-value");
    fixture.commit().await.expect("commit mapped fixture");

    let request = ReadSequenceMappedRangeRequest {
        begin: gsi_range.start,
        end: gsi_range.end,
        mapper: None,
        exclusive_start: None,
        reverse: false,
        target_bytes: 1024,
    };
    let trx = store
        .create_transaction()
        .expect("create stale transaction");
    trx.set_option(foundationdb::options::TransactionOption::Timeout(10_000))
        .expect("set transaction timeout");
    trx.get_read_version()
        .await
        .expect("capture transaction read version");
    let writer_store = store.clone();
    let writer_prefix = format!("mapped-retry-advance-{}", uuid::Uuid::now_v7());
    let writer = tokio::spawn(async move {
        for index in 0..240 {
            let writer_trx = writer_store
                .create_transaction()
                .expect("create version-advance transaction");
            writer_trx.set(
                format!("{writer_prefix}-{index}").as_bytes(),
                b"version-advance",
            );
            writer_trx
                .commit()
                .await
                .expect("commit version-advance transaction");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    });
    tokio::time::sleep(Duration::from_secs(6)).await;
    writer.await.expect("version-advance task");

    let request = super::read_context::prefix_mapped_range_request(&store, request);
    let error = read_sequence_mapped_range_attempt(&trx, &request, store.physical_prefix())
        .await
        .expect_err("expired transaction must fail the mapped attempt");
    let MappedRangeAttemptError::Fdb(error) = error else {
        panic!("expected FoundationDB retry error");
    };
    assert_eq!(error.code(), 1007, "expected transaction_too_old");
    assert!(error.is_retryable(), "timeout error: {error:?}");

    let retry = trx.on_error(error).await.expect("rebuild transaction");
    let page = read_sequence_mapped_range_attempt(&retry, &request, store.physical_prefix())
        .await
        .expect("retry attempt succeeds");
    page.validate_complete(false)
        .expect("retry page is complete");
}
