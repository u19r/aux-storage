//! Backfill lifecycle through shared operations and the initialized GSI
//! repository.

use serde_json::json;
use storage_types::{
    DeleteItemRequest, QueryRequest, TableName, UpdateTableRequest, maintenance::StepResult,
};

use super::{
    gsi_workflow_tests::{build_request, group_keys, put_group, table_request},
    test_support::TestStore,
};

async fn seed_table(fixture: &TestStore, table: &str, rows: &[(&str, &str)]) -> anyhow::Result<()> {
    let mut request = table_request(table);
    request.global_secondary_indexes = None;
    fixture.operations.create_table(request).await?;
    for (key, group) in rows {
        put_group(fixture, table, key, group).await?;
    }
    Ok(())
}

pub(super) async fn create_index(fixture: &TestStore, table: &str) -> anyhow::Result<()> {
    let indexes = table_request(table).global_secondary_indexes.unwrap();
    let request: UpdateTableRequest = serde_json::from_value(json!({
        "TableName": table,
        "GlobalSecondaryIndexUpdates": indexes.into_iter().map(|index| json!({"Create": index})).collect::<Vec<_>>()
    }))?;
    fixture.operations.update_table(request).await?;
    Ok(())
}

pub(super) async fn finish_backfill(
    fixture: &TestStore,
    page_limit: usize,
) -> anyhow::Result<usize> {
    let mut request = build_request(1);
    request.page_limit = storage_backfill::GsiPageLimit::try_from(page_limit)?;
    let mut advances = 0;
    for _ in 0..48 {
        match fixture
            .gsi_build_repository
            .advance_gsi_build(request.clone())
            .await?
        {
            StepResult::Complete => return Ok(advances),
            StepResult::Advanced(()) => advances += 1,
            StepResult::Superseded => anyhow::bail!("backfill worker lost authority"),
        }
    }
    anyhow::bail!("backfill did not complete within bounded fixture steps")
}

#[tokio::test]
async fn given_items_before_index_when_backfilled_then_query_serves() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        seed_table(&fixture, "gsi_init", &[("A", "x"), ("B", "x")]).await?;
        create_index(&fixture, "gsi_init").await?;
        finish_backfill(&fixture, 8).await?;
        anyhow::ensure!(group_keys(&fixture, "gsi_init", "x").await? == ["A", "B"]);
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_concurrent_move_and_delete_when_backfilled_then_index_converges()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        seed_table(&fixture, "gsi_move", &[("A", "x"), ("B", "x"), ("C", "x")]).await?;
        create_index(&fixture, "gsi_move").await?;
        let first = fixture
            .gsi_build_repository
            .advance_gsi_build(build_request(1))
            .await?;
        anyhow::ensure!(
            matches!(first, StepResult::Advanced(())),
            "backfill must be unfinished before foreground changes"
        );
        put_group(&fixture, "gsi_move", "A", "y").await?;
        fixture
            .operations
            .delete_item(DeleteItemRequest::new(
                TableName::new("gsi_move"),
                [(
                    "pk".to_owned(),
                    storage_types::AttributeValue::S("B".to_owned()),
                )],
            ))
            .await?;
        finish_backfill(&fixture, 1).await?;
        anyhow::ensure!(group_keys(&fixture, "gsi_move", "y").await? == ["A"]);
        anyhow::ensure!(
            group_keys(&fixture, "gsi_move", "x").await? == ["C"],
            "old group retained a moved or deleted item"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_many_items_when_backfilled_with_small_pages_then_converges() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        seed_table(&fixture, "gsi_pages", &[]).await?;
        let expected = (0..8)
            .map(|ordinal| format!("k-{ordinal:02}"))
            .collect::<Vec<_>>();
        for key in &expected {
            put_group(&fixture, "gsi_pages", key, "x").await?;
        }
        create_index(&fixture, "gsi_pages").await?;
        finish_backfill(&fixture, 1).await?;
        anyhow::ensure!(
            group_keys(&fixture, "gsi_pages", "x").await? == expected,
            "small-page backfill lost or duplicated keys"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_restart_when_backfill_redriven_then_no_duplication() -> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let outcome = async {
        seed_table(&fixture, "gsi_restart", &[("A", "x"), ("B", "x")]).await?;
        create_index(&fixture, "gsi_restart").await?;
        let step = fixture
            .gsi_build_repository
            .advance_gsi_build(build_request(1))
            .await?;
        anyhow::ensure!(matches!(step, StepResult::Advanced(())));
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = outcome {
        fixture.finish().await;
        return Err(error);
    }
    let fixture = fixture.reopen().await;
    let outcome = async {
        finish_backfill(&fixture, 1).await?;
        fixture.gsi_build_repository.advance_gsi_build(build_request(1)).await?;
        anyhow::ensure!(group_keys(&fixture, "gsi_restart", "x").await? == ["A", "B"]);
        let page = fixture.operations.query(QueryRequest::try_from(json!({
            "TableName": "gsi_restart", "IndexName": "by_group", "KeyConditionExpression": "gsi_pk = :g",
            "ExpressionAttributeValues": {":g": {"S": "x"}}, "ScanIndexForward": false
        })).map_err(anyhow::Error::msg)?).await?;
        let keys = page.items.unwrap_or_default().iter().map(|item| item.attribute_value("pk")).collect::<Result<Vec<_>, _>>()?;
        anyhow::ensure!(keys == [Some(storage_types::AttributeValue::S("B".to_owned())), Some(storage_types::AttributeValue::S("A".to_owned()))],
            "reverse read lost ordering after restart");
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_claim_loss_when_fence_changes_then_no_corruption() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        seed_table(&fixture, "gsi_fence_identity", &[("A", "x")]).await?;
        create_index(&fixture, "gsi_fence_identity").await?;
        fixture
            .gsi_build_repository
            .advance_gsi_build(build_request(1))
            .await?;
        let mut foreign = build_request(2);
        foreign.job = [9; 16].into();
        let error = fixture
            .gsi_build_repository
            .advance_gsi_build(foreign)
            .await
            .expect_err("foreign job cannot take catalog authority");
        anyhow::ensure!(
            matches!(error.as_ref(), storage_types::StorageEnum::Validation { message, .. }
            if message == "GSI build task is bound to another catalog job")
        );
        finish_backfill(&fixture, 8).await?;
        anyhow::ensure!(group_keys(&fixture, "gsi_fence_identity", "x").await? == ["A"]);
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_index_deleted_and_recreated_when_backfilled_then_new_index_serves()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        seed_table(&fixture, "gsi_recreate", &[("A", "x")]).await?;
        create_index(&fixture, "gsi_recreate").await?;
        finish_backfill(&fixture, 8).await?;
        anyhow::ensure!(group_keys(&fixture, "gsi_recreate", "x").await? == ["A"]);
        fixture.operations.update_table(serde_json::from_value(json!({
            "TableName": "gsi_recreate", "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "by_group"}}]
        }))?).await?;
        fixture.advance_time(std::time::Duration::from_secs(30));
        create_index(&fixture, "gsi_recreate").await?;
        finish_backfill(&fixture, 1).await?;
        anyhow::ensure!(group_keys(&fixture, "gsi_recreate", "x").await? == ["A"]);
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
}

pub(super) async fn put_replay_item(
    fixture: &TestStore,
    key: &str,
    group: &str,
    large: bool,
) -> anyhow::Result<()> {
    let mut item = json!({"pk": {"S": key}, "gsi_pk": {"S": group}, "gsi_sk": {"S": key}});
    if large {
        let payload = (0..128)
            .map(|part| {
                uuid::Uuid::new_v5(
                    &uuid::Uuid::NAMESPACE_OID,
                    format!("{key}/{group}/{part}").as_bytes(),
                )
                .to_string()
            })
            .collect::<String>();
        item["payload"] = json!({"S": payload});
    }
    fixture
        .operations
        .put_item(
            storage_types::PutItemRequest::try_from(json!({
                "TableName": "revision_replay", "Item": item, "AuxItemStreamTtlHours": 1
            }))
            .map_err(anyhow::Error::msg)?,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn given_replayed_gsi_history_when_foreground_moves_and_deletes_then_index_stays_current()
-> anyhow::Result<()> {
    for large in [false, true] {
        let fixture = TestStore::new().await;
        let outcome = async {
            let mut request = table_request("revision_replay");
            request.stream_specification = Some(storage_types::StreamSpecification {
                stream_enabled: true,
                stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
            });
            fixture.operations.create_table(request).await?;
            finish_backfill(&fixture, 8).await?;
            for key in ["move", "delete"] {
                put_replay_item(&fixture, key, "old", large).await?;
            }
            anyhow::ensure!(
                group_keys(&fixture, "revision_replay", "old").await? == ["delete", "move"]
            );
            anyhow::ensure!(
                finish_backfill(&fixture, 8).await? > 0,
                "worker did not consume put history"
            );
            put_replay_item(&fixture, "move", "new", false).await?;
            fixture
                .operations
                .delete_item(DeleteItemRequest::new(
                    TableName::new("revision_replay"),
                    [(
                        "pk".to_owned(),
                        storage_types::AttributeValue::S("delete".to_owned()),
                    )],
                ))
                .await?;
            // Check before the worker can repair a foreground projection.
            anyhow::ensure!(
                group_keys(&fixture, "revision_replay", "old")
                    .await?
                    .is_empty()
            );
            anyhow::ensure!(group_keys(&fixture, "revision_replay", "new").await? == ["move"]);
            anyhow::ensure!(
                finish_backfill(&fixture, 8).await? > 0,
                "worker did not consume mutation history"
            );
            anyhow::ensure!(
                group_keys(&fixture, "revision_replay", "old")
                    .await?
                    .is_empty()
            );
            anyhow::ensure!(group_keys(&fixture, "revision_replay", "new").await? == ["move"]);
            Ok::<_, anyhow::Error>(())
        }
        .await;
        fixture.finish().await;
        outcome?;
    }
    Ok(())
}

#[tokio::test]
async fn given_retained_gsi_history_when_table_is_recreated_then_index_ignores_old_incarnation()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let mut request = table_request("revision_replay");
        request.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        fixture.operations.create_table(request.clone()).await?;
        put_replay_item(&fixture, "embedded", "old", false).await?;
        put_replay_item(&fixture, "referenced", "old", true).await?;
        anyhow::ensure!(
            finish_backfill(&fixture, 8).await? > 0,
            "old incarnation history was not consumed"
        );
        anyhow::ensure!(
            group_keys(&fixture, "revision_replay", "old").await? == ["embedded", "referenced"]
        );
        fixture
            .operations
            .delete_table(storage_types::DeleteTableRequest {
                table_name: TableName::new("revision_replay"),
            })
            .await?;
        fixture.advance_time(std::time::Duration::from_secs(30));
        fixture.operations.create_table(request).await?;
        anyhow::ensure!(
            group_keys(&fixture, "revision_replay", "old")
                .await?
                .is_empty()
        );
        finish_backfill(&fixture, 8).await?;
        anyhow::ensure!(
            group_keys(&fixture, "revision_replay", "old")
                .await?
                .is_empty()
        );
        put_replay_item(&fixture, "new", "new", false).await?;
        anyhow::ensure!(
            finish_backfill(&fixture, 8).await? > 0,
            "new incarnation history was not consumed"
        );
        anyhow::ensure!(
            group_keys(&fixture, "revision_replay", "old")
                .await?
                .is_empty()
        );
        anyhow::ensure!(group_keys(&fixture, "revision_replay", "new").await? == ["new"]);
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
#[tokio::test]
async fn given_batch_gsi_members_when_replaced_and_deleted_then_only_current_projection_remains()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = "batch_gsi_visibility";
        fixture
            .operations
            .create_table(table_request(table))
            .await?;
        let puts = (0..25)
            .map(|index| {
                json!({"PutRequest": {"Item": {
                    "pk": {"S": format!("key-{index:02}")},
                    "gsi_pk": {"S": "before"}, "gsi_sk": {"S": format!("key-{index:02}")}
                }}})
            })
            .collect::<Vec<_>>();
        let request =
            storage_types::BatchWriteItemRequest::try_from(json!({"RequestItems": {table: puts}}))
                .map_err(anyhow::Error::msg)?;
        let result = fixture.operations.batch_write_item(request).await?;
        anyhow::ensure!(
            result
                .unprocessed_items
                .as_ref()
                .is_none_or(|items| items.values().all(Vec::is_empty))
        );
        let expected = (0..25)
            .map(|index| format!("key-{index:02}"))
            .collect::<Vec<_>>();
        anyhow::ensure!(group_keys(&fixture, table, "before").await? == expected);
        let writes = (0..25)
            .map(|index| {
                if index % 2 == 0 {
                    json!({"DeleteRequest": {"Key": {"pk": {"S": format!("key-{index:02}")}}}})
                } else {
                    json!({"PutRequest": {"Item": {
                        "pk": {"S": format!("key-{index:02}")},
                        "gsi_pk": {"S": "after"}, "gsi_sk": {"S": format!("key-{index:02}")}
                    }}})
                }
            })
            .collect::<Vec<_>>();
        let request = storage_types::BatchWriteItemRequest::try_from(
            json!({"RequestItems": {table: writes}}),
        )
        .map_err(anyhow::Error::msg)?;
        let result = fixture.operations.batch_write_item(request).await?;
        anyhow::ensure!(
            result
                .unprocessed_items
                .as_ref()
                .is_none_or(|items| items.values().all(Vec::is_empty))
        );
        anyhow::ensure!(group_keys(&fixture, table, "before").await?.is_empty());
        let expected = (0..25)
            .filter(|index| index % 2 == 1)
            .map(|index| format!("key-{index:02}"))
            .collect::<Vec<_>>();
        anyhow::ensure!(group_keys(&fixture, table, "after").await? == expected);
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
