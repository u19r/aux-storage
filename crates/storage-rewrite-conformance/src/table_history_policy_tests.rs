//! One table policy covers every ordinary write verb without item schedules.

use serde_json::json;
use storage_types::{
    BatchWriteItemRequest, DeleteItemRequest, GetStreamRecordsRequest, PutItemRequest,
    TimestampMillis, TransactWriteItemsRequest, UpdateItemRequest,
};

use super::test_support::{TestStore, hash_table_request, unique_table_name};

#[tokio::test]
async fn given_one_table_duration_when_all_write_forms_commit_then_only_one_table_schedule_exists()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let name = unique_table_name();
        let mut request = hash_table_request(name.clone());
        request.aux_stream_duration_hours = Some(storage_types::StreamRetentionDuration::from_hours(168).unwrap());
        request.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        fixture.operations.create_table(request).await?;
        let initial_markers = fixture.stream_retention_repository.list_due_stream_trim_markers(
            TimestampMillis::from_timestamp(i64::MAX), 100,
        ).await?;
        let initial_scope = initial_markers.iter().find(|marker| marker.scope.table_name == name)
            .expect("new table schedule").scope.clone();
        let initial_states = fixture.stream_retention_repository.load_stream_trim_states(
            std::slice::from_ref(&initial_scope),
        ).await?;
        let initial = initial_states[0].as_ref().expect("new table state");
        initial.require_valid_frontier()?;
        anyhow::ensure!(!initial.frontier.lineages.is_empty(), "source lanes were not captured");
        anyhow::ensure!(initial.frontier.lineages.iter().all(|lane|
            lane.retired_through.is_none() && lane.cleaned_through.is_none()
        ));
        fixture.operations.put_item(serde_json::from_value::<PutItemRequest>(json!({
            "TableName": name, "Item": {"pk": {"S": "a"}, "value": {"N": "1"}}
        }))?).await?;
        fixture.operations.update_item(serde_json::from_value::<UpdateItemRequest>(json!({
            "TableName": name, "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET #v = :v",
            "ExpressionAttributeNames": {"#v": "value"}, "ExpressionAttributeValues": {":v": {"N": "2"}}
        }))?).await?;
        fixture.operations.batch_write_item(serde_json::from_value::<BatchWriteItemRequest>(json!({
            "RequestItems": {name.as_ref(): [
                {"PutRequest": {"Item": {"pk": {"S": "b"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "c"}}}}
            ]}
        }))?).await?;
        fixture.operations.transact_write_items(serde_json::from_value::<TransactWriteItemsRequest>(json!({
            "TransactItems": [
                {"Update": {"TableName": name, "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET #v = :v",
                    "ExpressionAttributeNames": {"#v": "value"}, "ExpressionAttributeValues": {":v": {"N": "3"}}}},
                {"Delete": {"TableName": name, "Key": {"pk": {"S": "b"}}}}
            ]
        }))?).await?;
        fixture.operations.delete_item(DeleteItemRequest::new(name.clone(), storage_types::KeyAttributes::from([
            ("pk".to_owned(), storage_types::AttributeValue::S("c".to_owned()))
        ]))).await?;
        let markers = fixture.stream_retention_repository.list_due_stream_trim_markers(
            TimestampMillis::from_timestamp(i64::MAX), 100,
        ).await?;
        let markers = markers.iter().filter(|marker| marker.scope.table_name == name).collect::<Vec<_>>();
        anyhow::ensure!(markers.len() == 1, "writes created item-specific retention schedules");
        let states = fixture.stream_retention_repository.load_stream_trim_states(
            std::slice::from_ref(&initial_scope),
        ).await?;
        anyhow::ensure!(states[0].as_ref() == Some(initial), "ordinary writes changed table policy or frontier");
        let page = fixture.operations.get_stream_records(GetStreamRecordsRequest::table(name, None, Some(100))).await?;
        anyhow::ensure!(page.records().len() == 7, "a committed write lost its table history");
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_empty_trim_when_later_write_arrives_then_table_schedule_reclaims_it()
-> anyhow::Result<()> {
    use std::time::Duration;

    use storage_provider::{StreamDurationTrimConfig, StreamDurationTrimWorker};
    let fixture = TestStore::new().await;
    let outcome = async {
        let name = unique_table_name();
        let mut request = hash_table_request(name.clone());
        request.aux_stream_duration_hours =
            Some(storage_types::StreamRetentionDuration::from_hours(24).unwrap());
        request.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        fixture.operations.create_table(request).await?;
        let worker = StreamDurationTrimWorker::new(
            fixture.stream_retention_repository.as_ref(),
            StreamDurationTrimConfig::default(),
        );
        fixture.advance_time(Duration::from_secs(24 * 3600));
        let idle = worker.run_due_page(fixture.now(), fixture.now()).await?;
        anyhow::ensure!(idle.rows_deleted == 0 && idle.state_writes == 1);
        fixture.advance_time(Duration::from_secs(60));
        fixture
            .operations
            .put_item(serde_json::from_value::<PutItemRequest>(json!({
                "TableName": name, "Item": {"pk": {"S": "later"}}
            }))?)
            .await?;
        fixture.advance_time(Duration::from_secs(24 * 3600 - 60));
        let before_expiry = worker.run_due_page(fixture.now(), fixture.now()).await?;
        anyhow::ensure!(
            before_expiry.rows_deleted == 0 && before_expiry.state_writes == 1,
            "idle table lost its schedule or removed an unexpired event"
        );
        fixture.advance_time(Duration::from_secs(3600));
        let expired = worker.run_due_page(fixture.now(), fixture.now()).await?;
        anyhow::ensure!(
            expired.rows_deleted == 1,
            "later event was never rediscovered after idle trim"
        );
        let markers = fixture
            .stream_retention_repository
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 100)
            .await?;
        anyhow::ensure!(markers.len() == 1 && markers[0].due_at > fixture.now());
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
