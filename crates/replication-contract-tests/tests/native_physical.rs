//! Partial N17 qualification: real domain effects join opaque physical records.
//! Publication receipt and unknown-outcome proof remain separate.

#![cfg(feature = "native")]

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use infrastructure_provider::{
    batch::RecordBatch,
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordAddress, RecordChange,
        RecordExpectation,
    },
};
use replication_contract_tests::{HarnessResult, native::fixture};
use storage_provider::{
    DescribeTimeToLiveAttempt as _, DynamoOperations as _, DynamoPort,
    maintenance::{ExpirationPort, ExpirationScanRead},
    resolve_table_in_attempt,
};
use storage_types::{
    AttemptMode, CreateTableRequest, GetItemRequest, GetStreamRecordsRequest,
    InfrastructureContext, InfrastructureResultKind, PutItemRequest, QueryRequest, StorageEnum,
    StorageError, StorageResult, TableName, UpdateTimeToLiveRequest,
    maintenance::TargetIncarnation,
};
use stream_provider::{StreamOperations as _, StreamPort, SystemStreamPageRequest};

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_memory_sqlite_when_put_and_sidecar_share_an_attempt_then_all_effects_commit_or_rollback()
-> HarnessResult<()> {
    let stamp = verify_domain_join(&fixture::memory_sqlite().await?).await?;
    assert!(stamp.is_none());
    Ok(())
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_file_sqlite_when_put_and_sidecar_share_an_attempt_then_all_effects_commit_or_rollback()
-> HarnessResult<()> {
    let fixture = fixture::FileSqlite::try_new().await?;
    let stamp = verify_domain_join(&fixture.store).await?;
    assert!(stamp.is_none());
    Ok(())
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_when_put_and_sidecar_share_an_attempt_then_all_effects_commit_or_rollback()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_domain_join(&fixture.store).await;
    fixture.finish().await?;
    assert!(result?.is_none());
    Ok(())
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_when_put_and_sidecar_share_an_attempt_then_all_effects_commit_or_rollback()
-> HarnessResult<()> {
    let fixture = replication_contract_tests::native::turso::FileTurso::try_new().await?;
    let stamp = verify_domain_join(&fixture.store).await?;
    assert!(stamp.is_none());
    Ok(())
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_when_put_and_sidecar_share_an_attempt_then_all_effects_commit_or_rollback()
-> HarnessResult<()> {
    let fixture = replication_contract_tests::native::rocksdb::RocksDb::try_new().await?;
    let stamp = verify_domain_join(&fixture.store).await?;
    assert!(stamp.is_none());
    Ok(())
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_when_put_and_sidecar_share_an_attempt_then_all_effects_commit_or_rollback()
-> HarnessResult<()> {
    let fixture = replication_contract_tests::native::foundationdb::FoundationDb::try_new().await?;
    let result = verify_domain_join(&fixture.store).await;
    fixture.finish().await?;
    assert!(result?.is_some());
    Ok(())
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_sqlite_attempt_without_writes_when_commit_completes_then_result_has_no_native_stamp()
-> HarnessResult<()> {
    verify_commit_without_writes(&fixture::FileSqlite::try_new().await?.store).await
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_attempt_without_writes_when_commit_completes_then_result_has_no_native_stamp()
-> HarnessResult<()> {
    let fixture = fixture::Postgres::try_new().await?;
    let result = verify_commit_without_writes(&fixture.store).await;
    fixture.finish().await?;
    result
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_attempt_without_writes_when_commit_completes_then_result_has_no_native_stamp()
-> HarnessResult<()> {
    let fixture = replication_contract_tests::native::turso::FileTurso::try_new().await?;
    verify_commit_without_writes(&fixture.store).await
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_attempt_without_writes_when_commit_completes_then_result_has_no_native_stamp()
-> HarnessResult<()> {
    let fixture = replication_contract_tests::native::rocksdb::RocksDb::try_new().await?;
    verify_commit_without_writes(&fixture.store).await
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_attempt_without_writes_when_commit_completes_then_result_has_no_native_stamp()
-> HarnessResult<()> {
    let fixture = replication_contract_tests::native::foundationdb::FoundationDb::try_new().await?;
    let result = verify_commit_without_writes(&fixture.store).await;
    fixture.finish().await?;
    result
}

async fn verify_commit_without_writes<P: DynamoPort>(store: &P) -> HarnessResult<()> {
    let (response, stamp) = store
        .with_committed_dynamo_attempt(|_| Box::pin(async { Ok(7_u8) }))
        .await?;
    assert_eq!(7, response);
    assert!(
        stamp.is_none(),
        "a read-only commit minted a new native stamp"
    );
    Ok(())
}

async fn verify_domain_join<P>(store: &P) -> HarnessResult<Option<[u8; 10]>>
where
    P: DynamoPort + StreamPort + 'static,
    for<'native> <P as DynamoPort>::Attempt<'native>: PhysicalRecordAttempt + ExpirationPort,
{
    let name = TableName::new("native_join");
    let create: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
            { "AttributeName": "pk", "AttributeType": "S" },
            { "AttributeName": "gpk", "AttributeType": "S" }
        ],
        "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
        "GlobalSecondaryIndexes": [{
            "IndexName": "joined_gsi",
            "KeySchema": [{ "AttributeName": "gpk", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" }
        }],
        "StreamSpecification": { "StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES" }
    }))?;
    store
        .create_table(create)
        .await
        .map_err(|error| format!("create table: {error:?}"))?;
    let ttl: UpdateTimeToLiveRequest = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "TimeToLiveSpecification": { "AttributeName": "ttl", "Enabled": true }
    }))?;
    store
        .update_time_to_live(ttl)
        .await
        .map_err(|error| format!("install TTL: {error:?}"))?;
    let mut settled_streams = verify_domain_effects(store, &name, 0)
        .await
        .map_err(|error| format!("empty effects: {error:?}"))?;
    let mut committed_stamp = None;
    for (id, commit) in [
        ("rolled_back", false),
        ("committed", true),
        ("rolled_back_after", false),
    ] {
        let put: PutItemRequest = serde_json::from_value(serde_json::json!({
            "TableName": name,
            "Item": { "pk": { "S": id }, "gpk": { "S": "joined" }, "ttl": { "N": "61" } }
        }))?;
        let expected = put.item.clone().into_attribute_map()?;
        let sidecar = sidecar_key(id)?;
        let cache = store.metadata_cache();
        let captured_at = store.now();
        let write = vec![RecordChange::Store {
            expected: RecordExpectation::Absent,
            record: DurableRecord {
                key: sidecar.clone(),
                revision: 0.into(),
                bytes: Arc::from(&b"joined"[..]),
            },
        }];
        let result = store
            .with_committed_dynamo_attempt(move |attempt| {
                Box::pin(async move {
                    fixture::put_item_in_catalog(attempt, &cache, put, captured_at).await?;
                    let changes = RecordBatch::try_new(&write, fixture::limits()?)?;
                    assert_eq!(
                        ConditionalWriteResult::Applied,
                        attempt.store_records(&changes).await?
                    );
                    if commit {
                        Ok(())
                    } else {
                        Err(rollback_witness())
                    }
                })
            })
            .await;
        if commit {
            let ((), stamp) = result?;
            committed_stamp = stamp;
        } else {
            assert!(
                matches!((result).as_ref(), Err(error) if matches!(error.as_ref(), StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding { context: InfrastructureContext::AttemptSettlement }))),
                "rollback witness replaced: {:?}",
                result
                    .as_ref()
                    .err()
                    .map(storage_common::diagnostics::storage_error_diagnostic)
            );
        }
        let get: GetItemRequest = serde_json::from_value(serde_json::json!({
            "TableName": name, "Key": { "pk": { "S": id } }, "ConsistentRead": true
        }))?;
        let item = store
            .get_item(get)
            .await?
            .item
            .map(|item| item.into_attribute_map())
            .transpose()?;
        assert_eq!(commit.then_some(expected), item);
        let records = fixture::with_records(store, AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                attempt
                    .load_records(
                        &[sidecar],
                        storage_types::ReadProtection::Snapshot,
                        fixture::limits()?,
                    )
                    .await
            })
        })
        .await?;
        assert_eq!(commit, records[0].is_some());
        if let Some(record) = &records[0] {
            assert_eq!(b"joined", record.bytes.as_ref());
        }
        let count = usize::from(id != "rolled_back");
        let observed_streams = verify_domain_effects(store, &name, count)
            .await
            .map_err(|error| format!("effects after {id}: {error:?}"))?;
        if commit {
            settled_streams = observed_streams;
        } else {
            assert_eq!(
                settled_streams, observed_streams,
                "native rollback altered the committed stream record set"
            );
        }
    }
    Ok(committed_stamp)
}

async fn verify_domain_effects<P>(
    store: &P,
    name: &TableName,
    expected_count: usize,
) -> HarnessResult<[serde_json::Value; 2]>
where
    P: DynamoPort + StreamPort + 'static,
    for<'native> <P as DynamoPort>::Attempt<'native>: PhysicalRecordAttempt + ExpirationPort,
{
    let query: QueryRequest = serde_json::from_value(serde_json::json!({
        "TableName": name, "IndexName": "joined_gsi",
        "KeyConditionExpression": "gpk = :value",
        "ExpressionAttributeValues": { ":value": { "S": "joined" } }
    }))?;
    let items = store
        .query(query)
        .await
        .map_err(|error| format!("query GSI: {:?}", error.as_ref()))?
        .items
        .unwrap_or_default();
    assert_eq!(
        expected_count,
        items.len(),
        "GSI effects did not settle with primary"
    );
    let committed: GetItemRequest = serde_json::from_value(serde_json::json!({
        "TableName": name, "Key": { "pk": { "S": "committed" } }, "ConsistentRead": true
    }))?;
    let primary = store
        .get_item(committed)
        .await?
        .item
        .map(|item| item.into_attribute_map())
        .transpose()?;
    assert_eq!(expected_count != 0, primary.is_some());
    for item in items {
        assert_eq!(
            primary.as_ref(),
            Some(&item.into_attribute_map()?),
            "GSI image differs from committed primary"
        );
    }
    // Postgres stream serving may wait on pg_snapshot_xmin while unrelated
    // schemas in the same database hold older native transactions. Poll real
    // public pages after commit; an empty rollback observation is immediate.
    let (table_stream, regional) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let request = GetStreamRecordsRequest::table(name.clone(), None, Some(10));
            let table_stream = store
                .get_stream_records(request)
                .await
                .map_err(|error| format!("table stream: {:?}", error.as_ref()))?;
            let scope = store.system_stream_scope().await?;
            let regional = store
                .get_system_stream_page(SystemStreamPageRequest::try_new(scope, None, 10)?)
                .await
                .map_err(|error| format!("system stream: {:?}", error.as_ref()))?;
            if expected_count == 0
                || (table_stream.records().len() >= expected_count
                    && regional.records().len() >= expected_count)
            {
                return HarnessResult::Ok((table_stream, regional));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert_eq!(
        expected_count,
        table_stream.records().len(),
        "table stream escaped native rollback"
    );
    let frontier = table_stream
        .table_frontier()
        .ok_or("table stream omitted its observed frontier")?;
    assert_eq!(frontier.head.is_some(), expected_count != 0);
    assert_eq!(frontier.oldest.is_some(), expected_count != 0);
    if let Some(head) = frontier.head {
        assert!(frontier.can_resume_from(frontier.table_history_id, head));
        assert!(!frontier.can_resume_from(
            frontier.table_history_id,
            head.predecessor().ok_or("head has no predecessor")?,
        ));
        let idle = store
            .get_stream_records(GetStreamRecordsRequest::table(
                name.clone(),
                Some(head.to_string()),
                Some(10),
            ))
            .await?;
        assert!(idle.records().is_empty());
        assert_eq!(idle.table_frontier(), Some(frontier));
    }
    assert_eq!(
        expected_count,
        regional.records().len(),
        "regional stream escaped native rollback"
    );
    assert!(regional.terminal());
    for record in table_stream.records().iter().chain(regional.records()) {
        assert!(record.old_image.is_none());
        let image = record
            .new_image
            .as_ref()
            .ok_or("committed stream lost image")?;
        assert_eq!(
            Some(&storage_types::AttributeValue::S("committed".into())),
            image.get("pk")
        );
    }
    let cache = store.metadata_cache();
    let name = name.clone();
    let rows = fixture::with_records(store, AttemptMode::ReadWrite, move |attempt| {
        Box::pin(async move {
            let catalog = attempt;
            let (table, _) = resolve_table_in_attempt(catalog, &cache, &name).await?;
            let config = catalog
                .read_ttl_config(&table)
                .await?
                .ok_or::<StorageError>(
                    InfrastructureResultKind::Encoding {
                        context: InfrastructureContext::RecordEncoding,
                    }
                    .into(),
                )?;
            let read = ExpirationScanRead {
                table_name: name,
                table_incarnation: TargetIncarnation::from(*table.history_id.as_ref()),
                ttl_attribute: config.attribute_name,
                policy_generation: config.policy_generation,
                cutoff_seconds: 61,
                cursor: None,
                limit: NonZeroUsize::new(10).expect("TTL page bound"),
            };
            let (rows, more) =
                catalog
                    .scan_expiration_index(&table, &read)
                    .await
                    .map_err(|error| {
                        storage_types::StorageError::internal(&format!(
                            "expiration scan: {:?}",
                            error.as_ref()
                        ))
                    })?;
            assert!(!more);
            rows.into_iter()
                .map(|row| {
                    let key = storage_common::ttl::TtlIndexKeyToken::from(row.ttl_item_key)
                        .parse_key_map(&table.info)?;
                    Ok((row.indexed_expiration_seconds, key))
                })
                .collect::<StorageResult<Vec<_>>>()
        })
    })
    .await
    .map_err(|error| format!("expiration rows: {:?}", error.as_ref()))?;
    assert_eq!(
        expected_count,
        rows.len(),
        "TTL index escaped native rollback"
    );
    for (seconds, key) in rows {
        assert_eq!(61, seconds);
        assert_eq!(
            Some(&storage_types::AttributeValue::S("committed".into())),
            key.get("pk")
        );
    }
    Ok([
        serde_json::to_value(table_stream.records())?,
        serde_json::to_value(regional.records())?,
    ])
}

fn sidecar_key(id: &str) -> StorageResult<DurableRecordKey> {
    RecordAddress {
        namespace: "native-domain-join".into(),
        resource: b"PutItem".to_vec(),
        generation: 1,
        suffix: id.as_bytes().to_vec(),
    }
    .try_into()
}

fn rollback_witness() -> StorageError {
    InfrastructureResultKind::Encoding {
        context: InfrastructureContext::AttemptSettlement,
    }
    .into()
}
