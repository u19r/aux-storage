//! Durable effects and ownership through the initialized replacement store.

use std::num::{NonZeroU32, NonZeroUsize};

use anyhow::Context as _;
use scheduled_bg_jobs::store::{
    CommitOperationId, CommitOutcome, Mutation, StoreKey, StoreTransaction, StoreTransactionInput,
    TransactionLimits,
};
use storage_types::{
    MaxIndexers, StreamRetentionDuration, TimestampMillis, context::WrappedError as _,
};

use super::test_support::TestStore;

#[tokio::test]
async fn given_isolated_stores_when_one_reopens_and_closes_then_the_other_keeps_its_own_value()
-> anyhow::Result<()> {
    let first = TestStore::new_durable().await;
    let second = TestStore::new_durable().await;
    let staged = async {
        for (fixture, value) in [(&first, "first"), (&second, "second")] {
            fixture
                .operations
                .create_table(super::test_support::hash_table_request(
                    storage_types::TableName::new("isolation"),
                ))
                .await?;
            fixture.operations.put_item(storage_types::PutItemRequest::try_from(serde_json::json!({
                "TableName": "isolation", "Item": {"pk": {"S": "same-key"}, "value": {"S": value}}
            })).map_err(anyhow::Error::msg)?).await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = staged {
        first.finish().await;
        second.finish().await;
        return Err(error);
    }
    let first = first.reopen().await;
    let outcome = async {
        for (fixture, value) in [(&first, "first"), (&second, "second")] {
            let item = fixture.operations.get_item(storage_types::GetItemRequest::try_from(serde_json::json!({
                "TableName": "isolation", "Key": {"pk": {"S": "same-key"}}, "ConsistentRead": true
            })).map_err(anyhow::Error::msg)?).await?.item.context("isolated item is absent")?;
            anyhow::ensure!(item.to_attribute_map()? == serde_json::from_value::<storage_types::AttributeMap>(serde_json::json!({
                "pk": {"S": "same-key"}, "value": {"S": value}
            }))?, "store isolation or exact durable value changed");
        }
        Ok::<_, anyhow::Error>(())
    }.await;
    first.finish().await;
    if let Err(error) = outcome {
        second.finish().await;
        return Err(error);
    }
    let second = second.reopen().await;
    let outcome = async {
        let item = second.operations.get_item(storage_types::GetItemRequest::try_from(serde_json::json!({
            "TableName": "isolation", "Key": {"pk": {"S": "same-key"}}, "ConsistentRead": true
        })).map_err(anyhow::Error::msg)?).await?.item.context("cleanup removed another store's item")?;
        anyhow::ensure!(item.attribute_value("value")? == Some(storage_types::AttributeValue::S("second".into())));
        Ok(())
    }.await;
    second.finish().await;
    outcome
}

fn job_transaction(
    operation_id: CommitOperationId,
    key: StoreKey,
    value: &[u8],
) -> anyhow::Result<StoreTransaction> {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id,
        checks: Vec::new(),
        mutations: vec![Mutation::Put(key, value.to_vec().into())],
        limits: TransactionLimits {
            max_checks: NonZeroU32::new(100).expect("checks"),
            max_mutations: NonZeroU32::new(100).expect("mutations"),
            max_bytes: NonZeroUsize::new(1024 * 1024).expect("bytes"),
        },
    })
    .map_err(|err| anyhow::anyhow!("transaction: {err:?}"))
}

fn job_key(suffix: &str) -> anyhow::Result<StoreKey> {
    StoreKey::try_from(format!("p-jobs-same-op/{suffix}").into_bytes())
        .map_err(|err| anyhow::anyhow!("store key: {err:?}"))
}

#[tokio::test]
async fn given_committed_job_when_store_reopens_then_receipt_and_effect_persist()
-> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let operation_id = CommitOperationId::from([11; 16]);
    let staged: anyhow::Result<StoreKey> = async {
        let key = job_key("reopen-k1")?;
        let transaction = job_transaction(operation_id, key.clone(), b"v-reopen")?;
        let outcome = fixture
            .scheduled_job_store
            .commit(&transaction)
            .await
            .map_err(|err| anyhow::anyhow!("pre-reopen commit: {err:?}"))?;
        anyhow::ensure!(
            outcome == CommitOutcome::Committed,
            "pre-reopen commit not applied: {outcome:?}"
        );
        Ok(key)
    }
    .await;
    let (fixture, key) = match staged {
        Ok(key) => (fixture, key),
        Err(case) => {
            fixture.finish().await;
            return Err(case);
        }
    };
    let reopened = fixture.reopen().await;
    let outcome = async {
        let transaction = job_transaction(operation_id, key.clone(), b"v-reopen")?;
        let replayed = reopened
            .scheduled_job_store
            .commit(&transaction)
            .await
            .map_err(|err| anyhow::anyhow!("post-reopen replay: {err:?}"))?;
        anyhow::ensure!(
            replayed == CommitOutcome::Committed,
            "receipt did not survive reopen: {replayed:?}"
        );
        let read = reopened
            .scheduled_job_store
            .get(key)
            .await
            .map_err(|err| anyhow::anyhow!("post-reopen read: {err:?}"))?
            .ok_or_else(|| anyhow::anyhow!("value missing after reopen"))?;
        anyhow::ensure!(
            read.as_ref() == b"v-reopen",
            "persisted effect changed across reopen"
        );
        Ok(())
    }
    .await;
    reopened.finish().await;
    outcome
}

#[tokio::test]
async fn given_indexer_declarations_when_store_reopens_then_raw_updates_enforce_them()
-> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let table = storage_types::TableName::new("durable-indexer-declaration");
    let mut create = super::test_support::hash_table_request(table.clone());
    create.max_indexers = storage_types::MaxIndexers::try_new(2)?;
    fixture.operations.create_table(create).await?;
    let mut put = storage_types::PutItemRequest::try_from(serde_json::json!({
        "TableName": table.as_ref(),
        "Item": {"pk": {"S": "k1"}, "value": {"S": "same"}, "other": {"S": "same"}}
    }))
    .map_err(anyhow::Error::msg)?;
    put.indexers = Some(vec!["value".into(), "other".into()]);
    fixture.operations.put_item(put).await?;
    let fixture = fixture.reopen().await;
    let outcome = async {
        for (value, accepted) in [
            (serde_json::json!({"S": "changed"}), true),
            (serde_json::json!({"N": "7"}), false),
        ] {
            let update = storage_types::UpdateItemRequest::try_from(serde_json::json!({
                "TableName": table.as_ref(), "Key": {"pk": {"S": "k1"}},
                "UpdateExpression": "SET #v = :v",
                "ExpressionAttributeNames": {"#v": "value"},
                "ExpressionAttributeValues": {":v": value}
            }))
            .map_err(anyhow::Error::msg)?;
            let result = fixture.operations.update_item(update).await;
            if accepted {
                result?;
            } else {
                let error = result.expect_err("stored declaration rejects numeric value");
                anyhow::ensure!(
                    error
                        .to_string()
                        .contains("Indexers:attribute_must_be_string"),
                    "wrong error: {error}"
                );
            }
        }
        let stored = fixture
            .operations
            .get_item(
                storage_types::GetItemRequest::try_from(serde_json::json!({
                    "TableName": table.as_ref(), "Key": {"pk": {"S": "k1"}}
                }))
                .map_err(anyhow::Error::msg)?,
            )
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("item missing"))?
            .to_attribute_map()?;
        anyhow::ensure!(
            stored.get("value") == Some(&storage_types::AttributeValue::S("changed".into())),
            "failed update changed item"
        );
        let mut update = storage_types::UpdateItemRequest::try_from(serde_json::json!({
            "TableName": table.as_ref(), "Key": {"pk": {"S": "k1"}},
            "UpdateExpression": "SET #v = :v",
            "ExpressionAttributeNames": {"#v": "value"},
            "ExpressionAttributeValues": {":v": {"N": "7"}}
        }))
        .map_err(anyhow::Error::msg)?;
        update.indexers = Some(vec![]);
        fixture.operations.update_item(update).await?;
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

async fn verify_complete_update(fixture: &TestStore) -> anyhow::Result<()> {
    let mut create = super::test_support::hash_table_request(storage_types::TableName::new(
        "complete-table-update",
    ));
    create.max_indexers = MaxIndexers::try_new(1)?;
    create.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(1));
    fixture.operations.create_table(create.clone()).await?;
    let response = fixture
        .operations
        .update_table(serde_json::from_value(serde_json::json!({
            "TableName": create.table_name,
            "MaxIndexers": 3,
            "DeletionProtectionEnabled": true,
            "AuxStreamDurationHours": 7,
            "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "new-index",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))?)
        .await
        .map_err(|error| anyhow::anyhow!("complete update: {:?}", error.to_enum()))?;
    anyhow::ensure!(response.table_description.max_indexers.get() == 3);
    anyhow::ensure!(response.table_description.deletion_protection_enabled);
    anyhow::ensure!(
        response
            .table_description
            .stream_specification
            .as_ref()
            .is_some_and(|spec| spec.stream_enabled)
    );
    let indexes = response
        .table_description
        .global_secondary_indexes
        .as_ref()
        .context("response omitted new GSI")?;
    anyhow::ensure!(
        indexes.len() == 1 && indexes[0].index_status.is_some() && indexes[0].backfilling.is_some(),
        "response omitted readiness"
    );
    verify_committed_update(fixture).await
}

async fn verify_committed_update(fixture: &TestStore) -> anyhow::Result<()> {
    let table = storage_types::TableName::new("complete-table-update");
    let metadata = fixture.operations.get_table_info(table.clone()).await?;
    anyhow::ensure!(metadata.max_indexers.get() == 3 && metadata.deletion_protection_enabled);
    anyhow::ensure!(metadata.table_stream_duration == StreamRetentionDuration::FiniteHours(7));
    anyhow::ensure!(
        metadata
            .stream_specification
            .as_ref()
            .is_some_and(|spec| spec.stream_enabled)
    );
    anyhow::ensure!(
        metadata
            .global_secondary_indexes
            .as_ref()
            .is_some_and(|indexes| indexes.len() == 1)
    );
    let markers = fixture
        .stream_retention_repository
        .list_due_stream_trim_markers(TimestampMillis::now() + 8 * 60 * 60 * 1000, 100)
        .await?;
    let marker = markers
        .iter()
        .filter(|marker| marker.scope.table_name == table)
        .max_by_key(|marker| marker.policy_version)
        .context("updated table has no marker")?;
    let state = fixture
        .stream_retention_repository
        .load_stream_trim_states(std::slice::from_ref(&marker.scope))
        .await?
        .pop()
        .flatten()
        .context("updated table has no state")?;
    anyhow::ensure!(state.policy_version == marker.policy_version && state.policy_version > 1);
    Ok(())
}

async fn verify_deleted_index(fixture: &TestStore) -> anyhow::Result<()> {
    let table = storage_types::TableName::new("complete-table-update");
    let metadata = fixture.operations.get_table_info(table.clone()).await?;
    anyhow::ensure!(metadata.max_indexers.get() == 4 && !metadata.deletion_protection_enabled);
    anyhow::ensure!(metadata.table_stream_duration == StreamRetentionDuration::FiniteHours(9));
    anyhow::ensure!(
        metadata
            .stream_specification
            .as_ref()
            .is_some_and(|spec| !spec.stream_enabled)
    );
    anyhow::ensure!(
        metadata
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .is_empty()
    );
    let markers = fixture
        .stream_retention_repository
        .list_due_stream_trim_markers(TimestampMillis::now() + 10 * 60 * 60 * 1000, 100)
        .await?;
    let marker = markers
        .iter()
        .filter(|marker| marker.scope.table_name == table)
        .max_by_key(|marker| marker.policy_version)
        .context("deleted index update has no marker")?;
    let state = fixture
        .stream_retention_repository
        .load_stream_trim_states(std::slice::from_ref(&marker.scope))
        .await?
        .pop()
        .flatten()
        .context("deleted index update has no state")?;
    anyhow::ensure!(state.policy_version == marker.policy_version && state.policy_version > 2);
    Ok(())
}

#[tokio::test]
async fn given_combined_table_update_when_committed_then_all_settings_and_gsi_survive_reopen()
-> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    if let Err(error) = verify_complete_update(&fixture).await {
        fixture.finish().await;
        return Err(error);
    }
    let fixture = fixture.reopen().await;
    let result = verify_committed_update(&fixture).await;
    fixture.finish().await;
    result
}

#[tokio::test]
async fn given_existing_gsi_when_combined_delete_commits_then_settings_and_deletion_survive_reopen()
-> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let result = async {
        verify_complete_update(&fixture).await?;
        let response = fixture
            .operations
            .update_table(serde_json::from_value(serde_json::json!({
                "TableName": "complete-table-update",
                "MaxIndexers": 4,
                "DeletionProtectionEnabled": false,
                "AuxStreamDurationHours": 9,
                "StreamSpecification": {"StreamEnabled": false},
                "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "new-index"}}]
            }))?)
            .await
            .map_err(|error| anyhow::anyhow!("combined delete: {:?}", error.to_enum()))?;
        anyhow::ensure!(
            response
                .table_description
                .global_secondary_indexes
                .as_deref()
                .unwrap_or_default()
                .is_empty()
        );
        verify_deleted_index(&fixture).await
    }
    .await;
    if result.is_err() {
        fixture.finish().await;
        return result;
    }
    let fixture = fixture.reopen().await;
    let result = verify_deleted_index(&fixture).await;
    fixture.finish().await;
    result
}
