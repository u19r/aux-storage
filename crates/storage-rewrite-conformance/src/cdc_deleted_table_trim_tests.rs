//! Deleted IDs stay hidden while fresh aliases and source checkpoints survive
//! cleanup.

use std::collections::BTreeSet;

use storage_provider::TableReclaimStep;
use storage_types::{
    DeleteTableRequest, DescribeStreamRequest, StreamRecord, SystemStreamCheckpointToken, TableName,
};
use stream_provider::SystemStreamPageRequest;

use super::{
    dynamodb_stream_history_tests::{create_stream_table, put_value},
    test_support::{TestStore, hash_table_request},
};

fn snapshot_values(records: &[StreamRecord]) -> BTreeSet<String> {
    records
        .iter()
        .map(|record| {
            let storage_types::AttributeValue::S(value) =
                record.item.as_ref().unwrap().get("value").unwrap()
            else {
                panic!("snapshot value");
            };
            value.clone()
        })
        .collect()
}

async fn assert_current_snapshots(
    store: &TestStore,
    token: Option<SystemStreamCheckpointToken>,
) -> anyhow::Result<()> {
    let scope = store.stream_operations.system_stream_scope().await?;
    let page = store
        .stream_operations
        .get_system_stream_page(SystemStreamPageRequest::try_new(scope, token, 100)?)
        .await?;
    anyhow::ensure!(
        snapshot_values(page.records()) == BTreeSet::from(["peer".to_owned(), "fresh".to_owned()])
    );
    Ok(())
}

async fn verify_before_restart(
    store: &TestStore,
) -> anyhow::Result<(TableName, SystemStreamCheckpointToken)> {
    let (deleted, old_arn) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
    let (peer, _) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
    put_value(store, &deleted, "item", "old").await;
    put_value(store, &peer, "item", "peer").await;
    let scope = store.stream_operations.system_stream_scope().await?;
    let first = store
        .stream_operations
        .get_system_stream_page(SystemStreamPageRequest::try_new(scope, None, 1)?)
        .await?;
    anyhow::ensure!(snapshot_values(first.records()) == BTreeSet::from(["old".to_owned()]));
    let checkpoint = first.issued_checkpoint().token().clone();
    store
        .operations
        .delete_table(DeleteTableRequest {
            table_name: deleted.clone(),
        })
        .await?;
    let mut create = hash_table_request(deleted.clone());
    create.stream_specification = Some(storage_types::StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
    });
    store.operations.create_table(create).await?;
    put_value(store, &deleted, "item", "fresh").await;
    anyhow::ensure!(
        store
            .operations
            .describe_stream(DescribeStreamRequest {
                stream_arn: old_arn,
                exclusive_start_shard_id: None,
                limit: None,
                shard_filter: None,
            })
            .await
            .is_err(),
        "old stream ARN resolved to the replacement"
    );
    assert_current_snapshots(store, None).await?;
    assert_current_snapshots(store, Some(checkpoint.clone())).await?;
    for _ in 0..2 {
        anyhow::ensure!(
            store.table_reclaim_repository.reclaim_table_page().await?
                == TableReclaimStep::Advanced
        );
    }
    Ok((deleted, checkpoint))
}

async fn verify_after_restart(
    store: &TestStore,
    table: TableName,
    checkpoint: SystemStreamCheckpointToken,
) -> anyhow::Result<()> {
    assert_current_snapshots(store, Some(checkpoint.clone())).await?;
    for _ in 0..100 {
        if store.table_reclaim_repository.reclaim_table_page().await? == TableReclaimStep::Complete
        {
            break;
        }
    }
    anyhow::ensure!(
        store.table_reclaim_repository.reclaim_table_page().await? == TableReclaimStep::Complete
    );
    assert_current_snapshots(store, None).await?;
    assert_current_snapshots(store, Some(checkpoint)).await?;
    let page = store
        .operations
        .get_stream_records(storage_types::GetStreamRecordsRequest::table(
            table,
            None,
            Some(100),
        ))
        .await?;
    anyhow::ensure!(snapshot_values(page.records()) == BTreeSet::from(["fresh".to_owned()]));
    Ok(())
}

#[tokio::test]
async fn given_deleted_table_and_fresh_same_name_when_cleanup_restarts_then_old_snapshots_stay_hidden_and_source_checkpoints_resume()
-> anyhow::Result<()> {
    let store = TestStore::new_durable().await;
    let (table, checkpoint) = match verify_before_restart(&store).await {
        Ok(case) => case,
        Err(error) => {
            store.finish().await;
            return Err(error);
        }
    };
    let store = store.reopen().await;
    let outcome = verify_after_restart(&store, table, checkpoint).await;
    store.finish().await;
    outcome
}
