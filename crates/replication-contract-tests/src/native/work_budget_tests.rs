//! Real native attempts share cumulative budgets across customer and sidecar
//! effects.
use std::{num::NonZeroUsize, sync::Arc};

use infrastructure_provider::{
    batch::RecordBatch,
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, RecordAddress, RecordChange, RecordExpectation,
    },
};
use storage_provider::{DynamoOperations, DynamoPort, native_admission::NativeAttemptSource};
use storage_types::{
    AttemptMode, CreateTableRequest, GetItemRequest, NativeAttemptRejection, NativeWorkLimits,
    PutItemRequest, ReadProtection, StorageError,
};

use super::fixture;
use crate::HarnessResult;

fn budgets() -> [NativeWorkLimits; 2] {
    [
        NativeWorkLimits {
            data_bytes: NonZeroUsize::new(131072).unwrap(),
            call_guards: NonZeroUsize::new(65536).unwrap(),
        },
        NativeWorkLimits {
            data_bytes: NonZeroUsize::new(67108864).unwrap(),
            call_guards: NonZeroUsize::new(256).unwrap(),
        },
    ]
}

#[cfg(feature = "native-sqlite")]
#[tokio::test]
async fn given_sqlite_bounded_attempts_when_cumulative_reads_exhaust_and_callback_swallows_failure_then_all_joined_effects_abort_and_fresh_work_commits()
-> HarnessResult<()> {
    for budget in budgets() {
        verify(&fixture::memory_sqlite().await?, budget).await?;
    }
    Ok(())
}

#[cfg(feature = "native-postgres")]
#[tokio::test]
async fn given_postgres_bounded_attempts_when_cumulative_reads_exhaust_and_callback_swallows_failure_then_all_joined_effects_abort_and_fresh_work_commits()
-> HarnessResult<()> {
    for budget in budgets() {
        let fixture = fixture::Postgres::try_new().await?;
        let result = verify(&fixture.store, budget).await;
        fixture.finish().await?;
        result?;
    }
    Ok(())
}

#[cfg(feature = "native-turso")]
#[tokio::test]
async fn given_turso_bounded_attempts_when_cumulative_reads_exhaust_and_callback_swallows_failure_then_all_joined_effects_abort_and_fresh_work_commits()
-> HarnessResult<()> {
    for budget in budgets() {
        verify(&super::turso::FileTurso::try_new().await?.store, budget).await?;
    }
    Ok(())
}

#[cfg(feature = "native-rocksdb")]
#[tokio::test]
async fn given_rocksdb_bounded_attempts_when_cumulative_reads_exhaust_and_callback_swallows_failure_then_all_joined_effects_abort_and_fresh_work_commits()
-> HarnessResult<()> {
    for budget in budgets() {
        verify(&super::rocksdb::RocksDb::try_new().await?.store, budget).await?;
    }
    Ok(())
}

#[cfg(feature = "native-foundationdb")]
#[tokio::test]
async fn given_foundationdb_bounded_attempts_when_cumulative_reads_exhaust_and_callback_swallows_failure_then_all_joined_effects_abort_and_fresh_work_commits()
-> HarnessResult<()> {
    for budget in budgets() {
        let fixture = super::foundationdb::FoundationDb::try_new().await?;
        let result = verify(&fixture.store, budget).await;
        fixture.finish().await?;
        result?;
    }
    Ok(())
}

async fn verify<P>(store: &P, budget: NativeWorkLimits) -> HarnessResult<()>
where
    P: DynamoPort + NativeAttemptSource + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let create: CreateTableRequest = serde_json::from_value(
        serde_json::json!({"TableName": "bounded_join",
        "BillingMode": "PAY_PER_REQUEST", "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]}),
    )?;
    store.create_table(create).await?;
    store.bind_work_limits(budget)?;
    let put: PutItemRequest =
        serde_json::from_value(serde_json::json!({"TableName": "bounded_join",
        "Item": {"pk": {"S": "customer"}, "value": {"S": "joined"}}}))?;
    let expected_item = put.item.clone().into_attribute_map()?;
    let get: GetItemRequest =
        serde_json::from_value(serde_json::json!({"TableName": "bounded_join",
        "Key": {"pk": {"S": "customer"}}, "ConsistentRead": true}))?;
    let record = DurableRecord {
        key: RecordAddress {
            namespace: "native-work-budget".into(),
            resource: b"joined".to_vec(),
            generation: 0,
            suffix: b"customer".to_vec(),
        }
        .try_into()?,
        revision: 0.into(),
        bytes: Arc::from(vec![7; 1024]),
    };
    let key = record.key.clone();
    for exhaust in [true, false] {
        let cache = store.metadata_cache();
        let captured_at = store.now();
        let put = put.clone();
        let record = record.clone();
        let result = store
            .with_dynamo_attempt(AttemptMode::ReadWriteItems, move |attempt| {
                Box::pin(async move {
                    fixture::put_item_in_catalog(attempt, &cache, put, captured_at).await?;
                    let changes = [RecordChange::Store {
                        expected: RecordExpectation::Absent,
                        record: record.clone(),
                    }];
                    assert_eq!(
                        attempt
                            .store_records(&RecordBatch::try_new(&changes, fixture::limits()?)?)
                            .await?,
                        ConditionalWriteResult::Applied
                    );
                    if exhaust {
                        for _ in 0..4096 {
                            match attempt
                                .load_records(
                                    std::slice::from_ref(&record.key),
                                    ReadProtection::Snapshot,
                                    fixture::limits()?,
                                )
                                .await
                            {
                                Ok(rows) => {
                                    assert_eq!(rows[0].as_ref().unwrap().bytes, record.bytes)
                                }
                                Err(error) => {
                                    assert_eq!(
                                        error.native_attempt_rejection(),
                                        Some(NativeAttemptRejection::Capacity)
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        return Err(StorageError::internal(
                            "native cumulative admission was not enforced",
                        ));
                    }
                    Ok(())
                })
            })
            .await;
        if exhaust {
            assert_eq!(
                result
                    .expect_err("swallowed capacity failure committed")
                    .native_attempt_rejection(),
                Some(NativeAttemptRejection::Capacity)
            );
        } else {
            result?;
        }
        let item = store.get_item(get.clone()).await?.item;
        assert_eq!(
            item.is_some(),
            !exhaust,
            "native admission split customer effects from sidecar settlement"
        );
        if let Some(item) = item {
            assert_eq!(item.into_attribute_map()?, expected_item);
        }
        let key = key.clone();
        let loaded = fixture::with_records(store, AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                attempt
                    .load_records(&[key], ReadProtection::Snapshot, fixture::limits()?)
                    .await
            })
        })
        .await?;
        assert_eq!(loaded[0].is_some(), !exhaust);
        if let Some(record) = &loaded[0] {
            assert_eq!(record.bytes.as_ref(), &[7; 1024]);
        }
    }
    Ok(())
}
