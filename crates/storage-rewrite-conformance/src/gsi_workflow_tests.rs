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

#[tokio::test]
#[cfg_attr(any(feature = "postgres", feature = "foundationdb"), ignore)]
async fn given_writes_during_build_then_index_equals_projection_of_table() -> anyhow::Result<()> {
    use std::{collections::BTreeMap, sync::Arc};
    let fixture = TestStore::new().await;
    let outcome = async {
        let mut request = table_request("gsi_live_fill");
        request.global_secondary_indexes = None;
        request.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true, stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        fixture.operations.create_table(request).await?;
        for key in 0..200 {
            if key % 2 == 0 { put_group(&fixture, "gsi_live_fill", &format!("k{key:03}"), "g0").await?; }
            else { fixture.operations.put_item(PutItemRequest::try_from(json!({"TableName":"gsi_live_fill", "Item":{"pk":{"S":format!("k{key:03}")}}})).map_err(anyhow::Error::msg)?).await?; }
        }
        super::gsi_lifecycle_tests::create_index(&fixture, "gsi_live_fill").await?;
        let first = fixture.maintenance.advance_gsi_build(build_request(1)).await?;
        anyhow::ensure!(matches!(first, StepResult::Advanced(())), "early page must leave the build unfinished");
        // k000 is the first source key. Commit its move after that page and
        // before any further build work: overlap is witnessed without a timer.
        put_group(&fixture, "gsi_live_fill", "k000", "witness").await?;
        let start = Arc::new(tokio::sync::Barrier::new(5));
        let mut writers = Vec::new();
        for writer in 0..4 {
            let operations = Arc::clone(&fixture.operations);
            let start = Arc::clone(&start);
            writers.push(tokio::spawn(async move {
                start.wait().await;
                for ordinal in 0..250 {
                    let key = format!("k{:03}", writer * 75 + ordinal % 75);
                    match ordinal % 3 {
                        0 => {
                            let mut item = json!({"pk":{"S":key}, "value":{"N":ordinal.to_string()}});
                            if ordinal % 6 != 0 {
                                item["gsi_pk"] = json!({"S":format!("g{}", ordinal % 5)});
                                item["gsi_sk"] = json!({"S":key});
                            }
                            operations.put_item(PutItemRequest::try_from(json!({"TableName":"gsi_live_fill", "Item":item})).map_err(anyhow::Error::msg)?).await?;
                        }
                        1 => { operations.update_item(storage_types::UpdateItemRequest::try_from(json!({"TableName":"gsi_live_fill", "Key":{"pk":{"S":key}}, "UpdateExpression":"SET gsi_pk = :g, gsi_sk = :k", "ExpressionAttributeValues":{":g":{"S":format!("g{}", ordinal % 5)}, ":k":{"S":key}}})).map_err(anyhow::Error::msg)?).await?; }
                        _ => { operations.delete_item(storage_types::DeleteItemRequest::try_from(json!({"TableName":"gsi_live_fill", "Key":{"pk":{"S":key}}})).map_err(anyhow::Error::msg)?).await?; }
                    }
                }
                Ok::<_, anyhow::Error>(())
            }));
        }
        start.wait().await;
        let build = async {
            super::gsi_lifecycle_tests::finish_backfill(&fixture, 64).await?;
            Ok::<_, anyhow::Error>(())
        }.await;
        let writes = futures::future::join_all(writers).await;
        build?;
        for writer in writes { writer??; }
        let mut expected = BTreeMap::new();
        let mut base_keys = std::collections::BTreeSet::new();
        for item in scan_all(&fixture, None).await? {
            let Some(storage_types::AttributeValue::S(pk)) = item.attribute_value("pk")? else { anyhow::bail!("missing primary key"); };
            anyhow::ensure!(base_keys.insert(pk.clone()), "base scan duplicated a key");
            if item.attribute_value("gsi_pk")?.is_some() && item.attribute_value("gsi_sk")?.is_some() {
                expected.insert(pk, serde_json::to_value(item)?);
            }
        }
        anyhow::ensure!(base_keys.contains("k003") && !expected.contains_key("k003"), "sparse item must remain absent from the index");
        anyhow::ensure!(!base_keys.contains("k002"), "deleted item survived");
        let mut actual = BTreeMap::new();
        for item in scan_all(&fixture, Some("by_group")).await? {
                let Some(storage_types::AttributeValue::S(pk)) = item.attribute_value("pk")? else { anyhow::bail!("missing index primary key"); };
                anyhow::ensure!(actual.insert(pk, serde_json::to_value(item)?).is_none());
        }
        anyhow::ensure!(actual == expected, "index differs from current table projection after 1000 writes");
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
}

async fn scan_all(
    fixture: &TestStore,
    index: Option<&str>,
) -> anyhow::Result<Vec<storage_types::WireItem>> {
    let mut request: storage_types::ScanRequest = serde_json::from_value(json!({
        "TableName": "gsi_live_fill", "IndexName": index, "Limit": 17
    }))?;
    let mut items = Vec::new();
    loop {
        let page = fixture.operations.scan(request.clone()).await?;
        items.extend(page.items.unwrap_or_default());
        request.exclusive_start_key = page
            .last_evaluated_key
            .map(storage_types::ExclusiveStartKey::Key);
        if request.exclusive_start_key.is_none() {
            return Ok(items);
        }
    }
}

#[tokio::test]
#[cfg_attr(any(feature = "postgres", feature = "foundationdb"), ignore)]
async fn given_gsi_filling_when_second_create_or_delete_requested_then_change_is_rejected()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let mut request = table_request("gsi_change_guard");
        let indexes = request.global_secondary_indexes.take().unwrap();
        fixture.operations.create_table(request).await?;
        let create = |name: &str| {
            let mut index = serde_json::to_value(&indexes[0]).unwrap();
            index["IndexName"] = json!(name);
            serde_json::from_value::<UpdateTableRequest>(json!({
                "TableName": "gsi_change_guard", "GlobalSecondaryIndexUpdates": [{"Create": index}]
            })).unwrap()
        };
        fixture.operations.update_table(create("by_group")).await?;
        let delete = serde_json::from_value(json!({
            "TableName": "gsi_change_guard", "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "by_group"}}]
        }))?;
        for request in [create("second"), delete] {
            let error = fixture.operations.update_table(request).await.unwrap_err();
            anyhow::ensure!(matches!(error.as_ref(), storage_types::StorageEnum::LimitExceeded { .. }));
        }
        super::gsi_lifecycle_tests::finish_backfill(&fixture, 8).await?;
        let info = fixture.operations.get_table_info(TableName::new("gsi_change_guard")).await?;
        anyhow::ensure!(info.global_secondary_indexes.as_ref().unwrap().len() == 1);
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
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
        let repository = &fixture.maintenance;
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
        request.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
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
            .maintenance
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
                .maintenance
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
                .maintenance
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
async fn given_sanitized_index_key_when_index_is_recreated_then_new_generation_serves()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        fixture.create_hash_table(TableName::new("gsi_column_reuse")).await;
        let name = TableName::new("gsi_column_reuse");
        super::table_stream_eligibility_tests::set_stream(&fixture, &name, true).await?;
        let create: UpdateTableRequest = serde_json::from_value(json!({
            "TableName": "gsi_column_reuse",
            "AttributeDefinitions": [{"AttributeName": "group-name", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "by_group", "KeySchema": [{"AttributeName": "group-name", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))?;
        fixture.operations.update_table(create.clone()).await?;
        super::gsi_lifecycle_tests::finish_backfill(&fixture, 64).await?;
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

#[tokio::test]
#[cfg_attr(any(feature = "postgres", feature = "foundationdb"), ignore)]
async fn given_ttl_attribute_when_user_gsi_is_recreated_then_current_projection_serves()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = TableName::new("ttl_gsi_column_ownership");
        fixture.create_hash_table(table.clone()).await;
        super::table_stream_eligibility_tests::set_stream(&fixture, &table, true).await?;
        fixture.operations.update_time_to_live(serde_json::from_value(json!({
            "TableName": table, "TimeToLiveSpecification": {"AttributeName": "expires", "Enabled": true}
        }))?).await?;
        super::gsi_lifecycle_tests::finish_backfill(&fixture, 64).await?;
        let create: UpdateTableRequest = serde_json::from_value(json!({
            "TableName": table,
            "AttributeDefinitions": [{"AttributeName": "expires", "AttributeType": "N"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "by_expiry", "KeySchema": [{"AttributeName": "expires", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))?;
        fixture.operations.put_item(PutItemRequest::try_from(json!({
            "TableName": table, "Item": {"pk": {"S": "A"}, "expires": {"N": "2000000000"}}
        })).map_err(anyhow::Error::msg)?).await?;
        fixture.operations.update_table(create.clone()).await?;
        super::gsi_lifecycle_tests::finish_backfill(&fixture, 64).await?;
        fixture.operations.update_table(serde_json::from_value(json!({
            "TableName": table, "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "by_expiry"}}]
        }))?).await?;
        for _ in 0..8 { fixture.maintenance.reclaim_table_page().await?; }
        fixture.operations.put_item(PutItemRequest::try_from(json!({
            "TableName": table, "Item": {"pk": {"S": "A"}, "expires": {"N": "2100000000"}}
        })).map_err(anyhow::Error::msg)?).await?;
        fixture.advance_time(std::time::Duration::from_secs(30));
        fixture.operations.update_table(create.clone()).await
            .map_err(|error| anyhow::anyhow!("recreate TTL-attribute user GSI: {error:?}"))?;
        super::gsi_lifecycle_tests::finish_backfill(&fixture, 64).await?;
        let scan = fixture.operations.scan(storage_types::ScanRequest::try_from(json!({
            "TableName": table
        })).map_err(anyhow::Error::msg)?).await?;
        for (value, expected) in [("2000000000", Vec::new()), ("2100000000", scan.items.unwrap_or_default())] {
            let query = fixture.operations.query(QueryRequest::try_from(json!({
                "TableName": table, "IndexName": "by_expiry", "KeyConditionExpression": "expires = :e",
                "ExpressionAttributeValues": {":e": {"N": value}}
            })).map_err(anyhow::Error::msg)?).await?;
            anyhow::ensure!(serde_json::to_value(query.items.unwrap_or_default())? == serde_json::to_value(expected)?, "GSI differs from current table projection");
        }
        let mut peer = serde_json::to_value(create)?;
        peer["GlobalSecondaryIndexUpdates"][0]["Create"]["IndexName"] = json!("by_expiry_peer");
        fixture.operations.update_table(serde_json::from_value(peer)?).await?;
        super::gsi_lifecycle_tests::finish_backfill(&fixture, 64).await?;
        fixture.operations.update_table(serde_json::from_value(json!({
            "TableName": table, "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "by_expiry"}}]
        }))?).await?;
        for _ in 0..8 { fixture.maintenance.reclaim_table_page().await?; }
        let peer = fixture.operations.query(QueryRequest::try_from(json!({
            "TableName": table, "IndexName": "by_expiry_peer", "KeyConditionExpression": "expires = :e",
            "ExpressionAttributeValues": {":e": {"N": "2100000000"}}
        })).map_err(anyhow::Error::msg)?).await?;
        anyhow::ensure!(peer.count == 1, "retiring one user GSI removed its peer's shared column");
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
}
