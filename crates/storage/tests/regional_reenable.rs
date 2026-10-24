#![cfg(all(feature = "regional", feature = "test-support"))]

use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;
use storage::{
    DatabaseManager, DeleteItemInput,
    admission::AdmissionClass,
    regional_test_support::{RegionalFixture, TABLE, await_item, item, key, put},
};
use storage_types::{DescribeTableRequest, StorageEnum, TableName};

#[tokio::test]
async fn given_disabled_populated_table_when_ordinary_writes_and_reenable_complete_then_fresh_lineage_adopts_exact_prior_history_across_reopen()
-> Result<()> {
    let mut fixture = RegionalFixture::start_with_options(
        (65536, 128, None),
        storage::DatabaseManagerRuntimeOptions::default(),
        true,
    )
    .await?;
    let result = async {
        fixture.create_global_table().await?;
        put(&fixture.managers[0], "history", "first-source").await?;
        await_item(&fixture.managers[1], "history", "first-source").await?;
        put(&fixture.managers[1], "history", "second-source").await?;
        await_item(&fixture.managers[0], "history", "second-source").await?;
        put(&fixture.managers[1], "deleted", "before-disable").await?;
        await_item(&fixture.managers[0], "deleted", "before-disable").await?;
        let lineage =
            storage::regional_test_support::history::current_lineage(&fixture.managers[0], 1)
                .await?;
        let source = storage::regional_test_support::history::read_originals(
            &fixture.managers[0],
            1,
            1,
            "history",
        )
        .await?;
        let remote = storage::regional_test_support::history::read_originals(
            &fixture.managers[0],
            1,
            2,
            "history",
        )
        .await?;
        anyhow::ensure!(
            source.len() == 1 && remote.len() == 1,
            "both regional origins must be represented before disable"
        );
        let namespace = storage::regional_test_support::archive::archive_namespace(
            &fixture.directory.path().join("archive-1"),
        )?;
        for (origin, original) in [(1, &source[0]), (2, &remote[0])] {
            let object = storage::regional_test_support::archive::await_archived_original(
                &namespace,
                original.as_bytes(),
            )
            .await?;
            let unavailable = object.with_extension("unavailable");
            storage::regional_test_support::archive::await_archive_handoff(
                &fixture,
                (origin, "history", "second-source"),
                (&object, &unavailable),
            )
            .await?;
        }
        let removal = serde_json::from_value(
            json!({"TableName": TABLE, "ReplicaUpdates": [{"Delete": {"RegionName": "region-2"}}]}),
        )?;
        accept_update(&fixture.managers[0], removal).await?;
        await_mode(&fixture.managers[0], false).await?;
        require_removed(&fixture.managers[1]).await?;
        put(&fixture.managers[0], "history", "ordinary-overwrite").await?;
        put(&fixture.managers[0], "ordinary", "between-lineages").await?;
        fixture.managers[0]
            .delete_item(
                DeleteItemInput::builder()
                    .table_name(TableName::new(TABLE))
                    .key(key("deleted"))
                    .build(),
            )
            .await?;
        fixture.restart().await?;
        await_item(&fixture.managers[0], "history", "ordinary-overwrite").await?;
        require_removed(&fixture.managers[1]).await?;
        let enable = serde_json::from_value(
            json!({"TableName": TABLE, "ReplicaUpdates": [{"Create": {"RegionName": "region-2"}}]}),
        )?;
        accept_update(&fixture.managers[0], enable)
            .await
            .context("re-enable retired destination")?;
        await_mode(&fixture.managers[0], true).await?;
        await_item(&fixture.managers[1], "history", "ordinary-overwrite").await?;
        await_item(&fixture.managers[1], "ordinary", "between-lineages").await?;
        let renewed =
            storage::regional_test_support::history::current_lineage(&fixture.managers[0], 1)
                .await?;
        anyhow::ensure!(renewed != lineage, "re-enable reused the retired lineage");
        for (serving, manager) in fixture.managers.iter().enumerate() {
            anyhow::ensure!(
                storage::regional_test_support::history::current_lineage(manager, serving + 1)
                    .await?
                    == renewed,
                "re-enabled regions disagree about the current lineage"
            );
            anyhow::ensure!(
                manager
                    .get_item(TableName::new(TABLE), key("deleted"))
                    .await?
                    .is_none(),
                "re-enable resurrected an ordinary deletion"
            );
            anyhow::ensure!(
                storage::regional_test_support::history::read_historical_originals(
                    manager,
                    (serving + 1, 1),
                    "history",
                    lineage
                )
                .await?
                    == source,
                "re-enable changed source originals"
            );
            anyhow::ensure!(
                storage::regional_test_support::history::read_historical_originals(
                    manager,
                    (serving + 1, 2),
                    "history",
                    lineage
                )
                .await?
                    == remote,
                "re-enable discarded remote ancestry"
            );
            anyhow::ensure!(
                storage::regional_test_support::history::read_originals(
                    manager,
                    serving + 1,
                    1,
                    "ordinary"
                )
                .await?
                .is_empty(),
                "baseline invented an ordinary-write origin event"
            );
        }
        put(&fixture.managers[1], "history", "after-reenable").await?;
        let accepted = storage::regional_test_support::history::read_originals(
            &fixture.managers[1],
            2,
            2,
            "history",
        )
        .await?;
        anyhow::ensure!(
            accepted.len() == 1,
            "re-enabled destination did not author exactly one current-lineage event"
        );
        let delivery = storage::regional_test_support::await_item_for(
            &fixture.managers[0],
            "history",
            "after-reenable",
            Duration::from_secs(180),
        )
        .await;
        let received = storage::regional_test_support::history::read_originals(
            &fixture.managers[0],
            1,
            2,
            "history",
        )
        .await;
        delivery.context("deliver the first new-lineage event after re-enable")?;
        let received = received?;
        anyhow::ensure!(
            received == accepted,
            "re-enabled delivery changed exact new originals"
        );
        fixture.restart().await?;
        for (serving, manager) in fixture.managers.iter().enumerate() {
            await_item(manager, "history", "after-reenable").await?;
            anyhow::ensure!(
                storage::regional_test_support::history::current_lineage(manager, serving + 1)
                    .await?
                    == renewed,
                "reopen changed the new lineage"
            );
            anyhow::ensure!(
                storage::regional_test_support::history::read_historical_originals(
                    manager,
                    (serving + 1, 1),
                    "history",
                    lineage
                )
                .await?
                    == source,
                "reopen changed inherited source history"
            );
            anyhow::ensure!(
                storage::regional_test_support::history::read_historical_originals(
                    manager,
                    (serving + 1, 2),
                    "history",
                    lineage
                )
                .await?
                    == remote,
                "reopen lost inherited remote history"
            );
            let observed = manager
                .get_item(TableName::new(TABLE), key("ordinary"))
                .await?
                .context("reopen lost ordinary baseline data")?
                .into_attribute_map()?;
            anyhow::ensure!(
                observed == item("ordinary", "between-lineages").into_attribute_map()?,
                "reopen changed ordinary baseline data"
            );
        }
        Ok(())
    }
    .await;
    result.and(fixture.stop().await)
}

async fn accept_update(
    manager: &DatabaseManager,
    request: storage_types::UpdateTableRequest,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        match manager.update_table(request.clone()).await {
            Ok(_) => return Ok(()),
            Err(error)
                if matches!(error.as_ref(), StorageEnum::Validation { message, .. } if message == "table configuration control is in progress")
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn await_mode(manager: &DatabaseManager, global: bool) -> Result<()> {
    // Re-enable captures and transfers both 64-partition prior origins before
    // activation. This observation allowance does not change native or
    // transport operation deadlines.
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(if global { 600 } else { 120 });
    while tokio::time::Instant::now() < deadline {
        let request = DescribeTableRequest {
            table_name: TableName::new(TABLE),
        };
        let description = manager
            .run_dynamo_admitted("default", AdmissionClass::PointRead, |dynamo| async move {
                dynamo.describe_table(request).await
            })
            .await?
            .table;
        if description.aux_configuration_primary_region.is_some() == global
            && description.table_status == storage_types::TableStatus::Active
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    storage::regional_test_support::dump_jobs(manager).await?;
    anyhow::bail!("requested regional mode did not become active")
}

async fn require_removed(manager: &DatabaseManager) -> Result<()> {
    let error = manager
        .get_item(TableName::new(TABLE), key("history"))
        .await
        .err()
        .context("removed region exposed its retired table")?;
    anyhow::ensure!(
        matches!(
            error.as_ref(),
            StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
        ),
        "removed region returned a different failure: {error}"
    );
    Ok(())
}
