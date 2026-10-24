//! GSI worker authority and restart behavior through shared operation owners.

use std::num::NonZeroU64;

use serde_json::json;
use storage_provider::maintenance::GsiBuildRequest;
use storage_types::{
    CreateTableRequest, PutItemRequest, QueryRequest, TableName, TimestampMillis,
    UpdateTableRequest, maintenance::StepResult,
};

use super::test_support::TestStore;

pub(super) fn table_request(table: &str) -> CreateTableRequest {
    serde_json::from_value(json!({
        "TableName": table,
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
            {"AttributeName": "gsi_sk", "AttributeType": "S"}
        ],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [{
            "IndexName": "by_group",
            "KeySchema": [
                {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                {"AttributeName": "gsi_sk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .expect("valid GSI table")
}

pub(super) fn build_request(fence: u64) -> GsiBuildRequest {
    GsiBuildRequest {
        destination: b"aux-storage:conformance:gsi:v1"
            .to_vec()
            .try_into()
            .unwrap(),
        job: [7; 16].into(),
        fence: NonZeroU64::new(fence).unwrap(),
        page_limit: storage_backfill::GsiPageLimit::try_from(1usize).unwrap(),
        captured_at: TimestampMillis::from_timestamp(1_800_000_000_000),
    }
}

pub(super) async fn put_group(
    fixture: &TestStore,
    table: &str,
    key: &str,
    group: &str,
) -> anyhow::Result<()> {
    fixture
        .operations
        .put_item(
            PutItemRequest::try_from(json!({
                "TableName": table,
                "Item": {"pk": {"S": key}, "gsi_pk": {"S": group}, "gsi_sk": {"S": key}}
            }))
            .map_err(anyhow::Error::msg)?,
        )
        .await?;
    Ok(())
}

pub(super) async fn group_keys(
    fixture: &TestStore,
    table: &str,
    group: &str,
) -> anyhow::Result<Vec<String>> {
    let page = fixture
        .operations
        .query(
            QueryRequest::try_from(json!({
                "TableName": table, "IndexName": "by_group",
                "KeyConditionExpression": "gsi_pk = :g",
                "ExpressionAttributeValues": {":g": {"S": group}}, "Limit": 100
            }))
            .map_err(anyhow::Error::msg)?,
        )
        .await?;
    let mut keys = page
        .items
        .unwrap_or_default()
        .iter()
        .map(|item| match item.attribute_value("pk")? {
            Some(storage_types::AttributeValue::S(key)) => Ok(key),
            _ => anyhow::bail!("index row has no string primary key"),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    keys.sort_unstable();
    Ok(keys)
}

#[tokio::test]
async fn given_higher_job_fence_when_old_worker_advances_then_superseded() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        fixture
            .operations
            .create_table(table_request("gsi_fence"))
            .await?;
        let repository = &fixture.gsi_build_repository;
        repository.advance_gsi_build(build_request(1)).await?;
        repository.advance_gsi_build(build_request(2)).await?;
        let stale = repository.advance_gsi_build(build_request(1)).await?;
        anyhow::ensure!(
            matches!(stale, StepResult::Superseded),
            "stale GSI worker was not rejected"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_insert_behind_scan_cursor_when_reopened_between_pages_then_replay_retains_item()
-> anyhow::Result<()> {
    let mut fixture = TestStore::new_durable().await;
    let setup = async {
        let mut request = table_request("gsi_scan_replay");
        let indexes = request.global_secondary_indexes.take().unwrap();
        fixture.operations.create_table(request).await?;
        for key in ["A", "B", "C"] {
            put_group(&fixture, "gsi_scan_replay", key, "x").await?;
        }
        fixture
            .operations
            .update_table(UpdateTableRequest {
                table_name: TableName::new("gsi_scan_replay"),
                global_secondary_index_updates: Some(
                    indexes
                        .into_iter()
                        .map(|create| storage_types::GlobalSecondaryIndexUpdate {
                            create: Some(create),
                            update: None,
                            delete: None,
                        })
                        .collect(),
                ),
                ..serde_json::from_value(json!({"TableName": "gsi_scan_replay"}))?
            })
            .await?;
        let first = fixture
            .gsi_build_repository
            .advance_gsi_build(build_request(1))
            .await?;
        anyhow::ensure!(
            matches!(first, StepResult::Advanced(())),
            "scan did not start"
        );
        put_group(&fixture, "gsi_scan_replay", "0", "x").await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = setup {
        fixture.finish().await;
        return Err(error);
    }
    let mut complete = false;
    for _ in 0..32 {
        fixture = fixture.reopen().await;
        let outcome = async {
            let step = fixture
                .gsi_build_repository
                .advance_gsi_build(build_request(1))
                .await?;
            anyhow::ensure!(
                !matches!(step, StepResult::Superseded),
                "worker authority changed"
            );
            Ok::<_, anyhow::Error>(matches!(step, StepResult::Complete))
        }
        .await;
        match outcome {
            Ok(true) => {
                complete = true;
                break;
            }
            Ok(false) => continue,
            Err(error) => {
                fixture.finish().await;
                return Err(error);
            }
        }
    }
    let outcome = async {
        anyhow::ensure!(complete, "GSI scan and replay did not complete");
        anyhow::ensure!(
            group_keys(&fixture, "gsi_scan_replay", "x").await? == ["0", "A", "B", "C"],
            "scan/replay lost a committed item"
        );
        put_group(&fixture, "gsi_scan_replay", "A", "y").await?;
        put_group(&fixture, "gsi_scan_replay", "D", "x").await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = outcome {
        fixture.finish().await;
        return Err(error);
    }
    for _ in 0..32 {
        fixture = fixture.reopen().await;
        let outcome = async {
            fixture
                .gsi_build_repository
                .advance_gsi_build(build_request(1))
                .await?;
            Ok::<_, anyhow::Error>(
                group_keys(&fixture, "gsi_scan_replay", "x").await? == ["0", "B", "C", "D"]
                    && group_keys(&fixture, "gsi_scan_replay", "y").await? == ["A"],
            )
        }
        .await;
        match outcome {
            Ok(true) => {
                fixture.finish().await;
                return Ok(());
            }
            Ok(false) => {}
            Err(error) => {
                fixture.finish().await;
                return Err(error);
            }
        }
    }
    fixture.finish().await;
    anyhow::bail!("active continuation lost an insert or retained the old index key after restart")
}

#[tokio::test]
async fn given_sanitized_index_key_when_index_is_recreated_then_physical_column_is_reused()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        fixture.create_hash_table(TableName::new("gsi_column_reuse")).await;
        let create: UpdateTableRequest = serde_json::from_value(json!({
            "TableName": "gsi_column_reuse",
            "AttributeDefinitions": [{"AttributeName": "group-name", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "by_group", "KeySchema": [{"AttributeName": "group-name", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))?;
        fixture.operations.update_table(create.clone()).await?;
        fixture.operations.update_table(serde_json::from_value(json!({
            "TableName": "gsi_column_reuse", "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "by_group"}}]
        }))?).await?;
        // The public route reserves a deleted index name for 30 seconds.
        fixture.advance_time(std::time::Duration::from_secs(30));
        fixture.operations.update_table(create).await
            .map_err(|error| anyhow::anyhow!("recreate: {error:?}"))?;
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
}
