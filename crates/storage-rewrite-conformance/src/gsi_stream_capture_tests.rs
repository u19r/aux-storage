//! Temporary GSI consumers own capture independently of a public stream.

use serde_json::json;
use storage_types::{IndexName, TableName};

use super::{
    gsi_workflow_tests::{build_request, put_group, table_request},
    table_stream_eligibility_tests::{finish_initialization, set_stream},
    test_support::{TestStore, unique_table_name},
};

async fn create_table(store: &TestStore) -> anyhow::Result<TableName> {
    let table = unique_table_name();
    let mut request = table_request(table.as_ref());
    request.global_secondary_indexes = None;
    store.operations.create_table(request).await?;
    Ok(table)
}

async fn add_index(store: &TestStore, table: &TableName, name: &str) -> anyhow::Result<()> {
    let mut index = table_request(table.as_ref())
        .global_secondary_indexes
        .unwrap()
        .remove(0);
    index.index_name = IndexName::new(name);
    store
        .operations
        .update_table(serde_json::from_value(json!({
            "TableName": table, "GlobalSecondaryIndexUpdates": [{"Create": index}]
        }))?)
        .await?;
    Ok(())
}

async fn delete_index(
    store: &TestStore,
    table: &TableName,
    name: &IndexName,
) -> anyhow::Result<()> {
    store
        .operations
        .update_table(serde_json::from_value(json!({
            "TableName": table, "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": name}}]
        }))?)
        .await?;
    Ok(())
}

#[tokio::test]
async fn given_two_internal_builds_when_one_finishes_and_the_other_is_cancelled_then_only_the_last_owner_stops_capture()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let table = create_table(&store).await?;
        put_group(&store, table.as_ref(), "base", "old").await?;
        add_index(&store, &table, "first").await?;
        add_index(&store, &table, "second").await?;
        for _ in 0..64 {
            let info = store.operations.get_table_info(table.clone()).await?;
            if info
                .global_secondary_indexes
                .as_ref()
                .unwrap()
                .iter()
                .filter(|index| index.capture_for_build)
                .count()
                == 1
            {
                break;
            }
            store
                .gsi_build_repository
                .advance_gsi_build(build_request(1))
                .await?;
        }
        let info = store.operations.get_table_info(table.clone()).await?;
        let indexes = info.global_secondary_indexes.as_ref().unwrap();
        let pending = indexes
            .iter()
            .filter(|index| index.capture_for_build)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            pending.len() == 1,
            "completion released another build's capture"
        );
        anyhow::ensure!(info.requires_stream_entry() && !info.has_enabled_stream());
        put_group(&store, table.as_ref(), "base", "during").await?;
        let before = super::cdc_stream_tests::system_records(&store, 100)
            .await?
            .len();
        anyhow::ensure!(before == 1);
        delete_index(&store, &table, &pending[0].index_name).await?;
        let info = store.operations.get_table_info(table.clone()).await?;
        anyhow::ensure!(
            !info.requires_stream_entry(),
            "cancelled last build retained capture"
        );
        put_group(&store, table.as_ref(), "base", "after").await?;
        anyhow::ensure!(
            super::cdc_stream_tests::system_records(&store, 100)
                .await?
                .len()
                == before
        );
        let active = &info.global_secondary_indexes.as_ref().unwrap()[0].index_name;
        let response = store.operations.query(serde_json::from_value(json!({
            "TableName": table, "IndexName": active, "KeyConditionExpression": "gsi_pk = :group",
            "ExpressionAttributeValues": {":group": {"S": "after"}}
        }))?).await?;
        anyhow::ensure!(response.items.unwrap_or_default().len() == 1);
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_internal_capture_when_public_stream_is_enabled_then_full_initialization_precedes_publication_and_build_completion_keeps_it_enabled()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let table = create_table(&store).await?;
        for index in 0..40 {
            put_group(&store, table.as_ref(), &format!("key-{index:02}"), "base").await?;
        }
        add_index(&store, &table, "by_group").await?;
        let response = set_stream(&store, &table, true).await?;
        anyhow::ensure!(
            !response
                .table_description
                .stream_specification
                .unwrap()
                .stream_enabled
        );
        store
            .gsi_build_repository
            .advance_gsi_build(build_request(1))
            .await?;
        anyhow::ensure!(
            !store
                .operations
                .get_table_info(table.clone())
                .await?
                .has_enabled_stream()
        );
        store
            .stream_initialization_repository
            .advance_table_stream_initialization()
            .await?;
        anyhow::ensure!(
            !store
                .operations
                .get_table_info(table.clone())
                .await?
                .has_enabled_stream(),
            "public stream was published before all current images were captured"
        );
        finish_initialization(&store, &table).await?;
        let info = store.operations.get_table_info(table.clone()).await?;
        anyhow::ensure!(info.has_enabled_stream() && !info.has_internal_stream_capture());
        anyhow::ensure!(
            set_stream(&store, &table, false).await.is_err(),
            "public-dependent build allowed its required public stream to be disabled"
        );
        anyhow::ensure!(
            store
                .operations
                .get_table_info(table.clone())
                .await?
                .has_enabled_stream(),
            "rejected disable changed public stream intent"
        );
        super::gsi_lifecycle_tests::finish_backfill(&store, 4).await?;
        anyhow::ensure!(
            super::gsi_workflow_tests::group_keys(&store, table.as_ref(), "base")
                .await?
                .len()
                == 40,
            "public opt-in disrupted the in-progress index build"
        );
        anyhow::ensure!(
            store
                .operations
                .get_table_info(table.clone())
                .await?
                .has_enabled_stream()
        );
        put_group(&store, table.as_ref(), "key-00", "changed").await?;
        anyhow::ensure!(
            super::cdc_stream_tests::system_records(&store, 100)
                .await?
                .len()
                == 1,
            "GSI completion disabled the explicitly requested stream"
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_public_initialization_during_internal_build_when_public_enable_is_cancelled_then_internal_capture_remains_until_build_completion()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let table = create_table(&store).await?;
        put_group(&store, table.as_ref(), "base", "before").await?;
        add_index(&store, &table, "by_group").await?;
        set_stream(&store, &table, true).await?;
        set_stream(&store, &table, false).await?;
        let info = store.operations.get_table_info(table.clone()).await?;
        anyhow::ensure!(info.has_internal_stream_capture() && !info.has_public_stream_request());
        put_group(&store, table.as_ref(), "base", "during").await?;
        super::gsi_lifecycle_tests::finish_backfill(&store, 1).await?;
        anyhow::ensure!(
            !store
                .operations
                .get_table_info(table.clone())
                .await?
                .requires_stream_entry()
        );
        anyhow::ensure!(
            super::gsi_workflow_tests::group_keys(&store, table.as_ref(), "during").await?
                == ["base"]
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_internal_build_when_required_history_is_retired_then_terminal_failure_releases_capture()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let table = create_table(&store).await?;
        put_group(&store, table.as_ref(), "base", "before").await?;
        add_index(&store, &table, "by_group").await?;
        put_group(&store, table.as_ref(), "base", "during").await?;
        let scope = store.stream_operations.system_stream_scope().await?;
        let trimmed = storage_provider::SystemStreamTrimExecutor::new(
            store.system_stream_trim_repository.as_ref(),
            100,
        )
        .run_page(scope, store.now() + 60_000, None)
        .await?;
        anyhow::ensure!(trimmed.reclaimed_records() == 1);
        super::gsi_lifecycle_tests::finish_backfill(&store, 1).await?;
        anyhow::ensure!(
            super::gsi_workflow_tests::group_keys(&store, table.as_ref(), "during")
                .await
                .is_err(),
            "retired history incorrectly activated the incomplete index"
        );
        anyhow::ensure!(
            !store
                .operations
                .get_table_info(table.clone())
                .await?
                .requires_stream_entry(),
            "terminally blocked internal build retained capture"
        );
        put_group(&store, table.as_ref(), "base", "after").await?;
        anyhow::ensure!(
            super::cdc_stream_tests::system_records(&store, 100)
                .await?
                .is_empty(),
            "write after terminal build failure created history"
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}
