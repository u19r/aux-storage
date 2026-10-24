#![cfg(all(feature = "regional", feature = "test-support"))]

use std::time::Duration;

use anyhow::{Context, Result};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{
    event::content::{CanonicalEventCodec, CanonicalPayload, MutationCause},
    protocol::PayloadBounds,
};
use serde_json::json;
use storage::{
    DatabaseManager, PutItemInput,
    regional_test_support::{RegionalFixture, TABLE, accept_control, history, key},
};
use storage_types::{
    AttributeMap, AttributeValue, TableName, TimeToLiveStatus, TimestampMillis, WireItem,
};

#[tokio::test]
async fn given_one_paused_native_index_builder_when_peer_fails_and_recovers_then_shared_readiness_preserves_public_writes_and_exact_projections()
-> Result<()> {
    let intervals = storage_common::DatabaseJobIntervals {
        gsi_backfill_interval_ms: storage_common::JobIntervalMillis(1000),
        ..Default::default()
    };
    let options = storage::DatabaseManagerRuntimeOptions::builder()
        .database_job_intervals(intervals)
        .build();
    let mut fixture =
        RegionalFixture::start_with_options((65536, 4, Some(75)), options, false).await?;
    let result = async {
        fixture.create_global_table().await?;
        let expiry = TimestampMillis::now().timestamp_millis() / 1000 + 3600;
        put(&fixture.managers[0], expiry, "before-build").await?;
        await_exact(&fixture.managers[1], expiry, "before-build").await?;
        reopen_target(&mut fixture, false).await?;
        let create: storage_types::UpdateTableRequest = serde_json::from_value(json!({"TableName": TABLE,
            "AttributeDefinitions": [{"AttributeName": "value", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {"IndexName": "value-index", "KeySchema": [{"AttributeName": "value", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}}]
        }))?;
        accept_control(|| fixture.managers[0].update_table(create.clone())).await?;
        for manager in &fixture.managers {
            await_index(manager, storage_types::IndexStatus::Creating).await?;
            require_unready(manager).await?;
        }
        put(&fixture.managers[0], expiry, "while-build-paused").await?;
        await_exact(&fixture.managers[1], expiry, "while-build-paused").await?;
        let proxy = fixture.proxy.as_ref().context("index recovery requires its owned WAN proxy")?;
        let connections = proxy.connections()?;
        proxy.toxic("peer-outage", "timeout", "upstream", json!({"timeout": 0})).await?;
        put(&fixture.managers[0], expiry, "while-peer-unavailable").await?;
        proxy.await_connection(connections).await?;
        let stopped = fixture.managers.pop().unwrap();
        stopped.shutdown_background_services().await?;
        drop(stopped);
        await_index(&fixture.managers[0], storage_types::IndexStatus::Creating).await?;
        require_unready(&fixture.managers[0]).await?;
        await_exact(&fixture.managers[0], expiry, "while-peer-unavailable").await?;
        let paused = storage::startup::database_manager_from_features(&fixture.features[1], fixture.runtime_options.clone()).await?;
        paused.stop_database_jobs_for_test().await?;
        fixture.managers.push(paused);
        await_index(&fixture.managers[1], storage_types::IndexStatus::Creating).await?;
        require_unready(&fixture.managers[1]).await?;
        fixture.proxy.as_ref().unwrap().remove("peer-outage").await?;
        reopen_target(&mut fixture, true).await?;
        for manager in &fixture.managers {
            await_index(manager, storage_types::IndexStatus::Active).await?;
            await_exact(manager, expiry, "while-peer-unavailable").await?;
            require_projection(manager, expiry, "while-peer-unavailable").await?;
        }
        fixture.restart().await?;
        for manager in &fixture.managers {
            await_index(manager, storage_types::IndexStatus::Active).await?;
            require_projection(manager, expiry, "while-peer-unavailable").await?;
        }
        Ok(())
    }.await;
    result.and(fixture.stop().await)
}

async fn reopen_target(fixture: &mut RegionalFixture, jobs: bool) -> Result<()> {
    let target = fixture.managers.pop().unwrap();
    target.shutdown_background_services().await?;
    drop(target);
    let target = storage::startup::database_manager_from_features(
        &fixture.features[1],
        fixture.runtime_options.clone(),
    )
    .await?;
    if !jobs {
        target.stop_database_jobs_for_test().await?;
    }
    fixture.managers.push(target);
    Ok(())
}

async fn await_index(manager: &DatabaseManager, status: storage_types::IndexStatus) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let request = storage_types::DescribeTableRequest {
            table_name: TableName::new(TABLE),
        };
        let description = manager
            .run_dynamo_admitted(
                "default",
                storage::admission::AdmissionClass::PointRead,
                |dynamo| async move { dynamo.describe_table(request).await },
            )
            .await?
            .table;
        if description
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|index| {
                index.index_name.as_ref() == "value-index"
                    && matches!(
                        (index.index_status.as_ref(), &status),
                        (
                            Some(storage_types::IndexStatus::Creating),
                            storage_types::IndexStatus::Creating
                        ) | (
                            Some(storage_types::IndexStatus::Active),
                            storage_types::IndexStatus::Active
                        )
                    )
            })
        {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "shared index did not reach {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn query(
    manager: &DatabaseManager,
    value: &str,
) -> storage_types::StorageResult<storage_types::QueryResponse> {
    let request = serde_json::from_value(json!({"TableName": TABLE, "IndexName": "value-index", "KeyConditionExpression": "#v = :v",
        "ExpressionAttributeNames": {"#v": "value"}, "ExpressionAttributeValues": {":v": {"S": value}}})).unwrap();
    manager
        .run_dynamo_admitted(
            "default",
            storage::admission::AdmissionClass::RangeRead,
            |dynamo| async move { dynamo.query(request).await },
        )
        .await
}

async fn require_unready(manager: &DatabaseManager) -> Result<()> {
    let error = query(manager, "before-build")
        .await
        .err()
        .context("incomplete shared index accepted queries")?;
    anyhow::ensure!(
        matches!(error.as_ref(), storage_types::StorageEnum::Validation { message, .. } if message == "Global secondary index value-index is not ready for queries"),
        "unready shared index returned the wrong rejection: {error}"
    );
    Ok(())
}

async fn require_projection(manager: &DatabaseManager, expiry: i64, value: &str) -> Result<()> {
    let rows = query(manager, value).await?.items.unwrap_or_default();
    anyhow::ensure!(
        rows.len() == 1
            && rows[0].to_attribute_map()? == item(expiry, value)?.into_attribute_map()?,
        "ready shared index lost its exact current projection"
    );
    for stale in ["before-build", "while-build-paused"] {
        anyhow::ensure!(
            query(manager, stale)
                .await?
                .items
                .unwrap_or_default()
                .is_empty(),
            "index retained a superseded receiver projection"
        );
    }
    Ok(())
}

#[tokio::test]
async fn given_independent_native_ttl_workers_when_same_victim_expires_and_is_refreshed_then_identical_children_cannot_delete_the_new_winner()
-> Result<()> {
    let intervals = storage_common::DatabaseJobIntervals {
        gsi_backfill_interval_ms: storage_common::JobIntervalMillis(1000),
        ..Default::default()
    };
    let options = storage::DatabaseManagerRuntimeOptions::builder()
        .database_job_intervals(intervals)
        .build();
    let mut fixture =
        RegionalFixture::start_with_options((65536, 4, Some(75)), options, false).await?;
    let result = async {
        fixture.create_global_table().await?;
        let request: storage_types::UpdateTimeToLiveRequest = serde_json::from_value(json!({"TableName": TABLE, "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expires"}}))?;
        accept_control(|| fixture.managers[0].update_time_to_live(request.clone())).await.context("accept shared TTL policy after enable settles")?;
        for manager in &fixture.managers {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
            loop {
                let policy = manager.describe_time_to_live(&TableName::new(TABLE)).await?.time_to_live_description;
                if policy.is_some_and(|policy| policy.time_to_live_status == TimeToLiveStatus::Enabled) { break; }
                anyhow::ensure!(tokio::time::Instant::now() < deadline, "shared TTL policy did not activate");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        let expiry = TimestampMillis::now().timestamp_millis() / 1000 + 3600;
        put(&fixture.managers[0], expiry, "victim").await?;
        await_exact(&fixture.managers[1], expiry, "victim").await?;
        let parent = history::read_originals(&fixture.managers[0], 1, 1, "ttl-victim").await?;
        anyhow::ensure!(parent.len() == 1, "victim needs one exact originating version");
        // Native page calls remain available; no delivery worker can make the second region
        // observe the first deletion before independently revalidating its own victim.
        for manager in &fixture.managers { manager.stop_database_jobs_for_test().await?; }
        let request = storage_common::TtlSweepRequest {
            table_name: TableName::new(TABLE),
            input: storage_common::TtlSweepPageInput::try_new("expires", TimestampMillis::from_timestamp((expiry + 1) * 1000), 25, None)?,
        };
        let commands = [fixture.managers[0].prepare_ttl_sweep_command(request.clone()).await?, fixture.managers[1].prepare_ttl_sweep_command(request).await?];
        let (source, remote) = tokio::join!(fixture.managers[0].run_ttl_sweep_command(commands[0].clone()), fixture.managers[1].run_ttl_sweep_command(commands[1].clone()));
        anyhow::ensure!(source?.deleted_items == 1 && remote?.deleted_items == 1, "both native owners must independently delete the same victim");
        for manager in &fixture.managers {
            anyhow::ensure!(manager.get_item(TableName::new(TABLE), key("ttl-victim")).await?.is_none(), "native expiration did not delete its victim");
        }
        let source = history::read_originals(&fixture.managers[0], 1, 1, "ttl-victim").await?;
        let remote = history::read_originals(&fixture.managers[1], 2, 2, "ttl-victim").await?;
        let child = expiration(&source)?;
        anyhow::ensure!(expiration(&remote)? == child, "regional discovery changed the deterministic expiration ID or bytes");
        anyhow::ensure!(source.iter().filter(|original| *original == &parent[0]).count() == 1, "expiration changed or duplicated its immutable parent");
        let refreshed_expiry = expiry + 7200;
        put(&fixture.managers[0], refreshed_expiry, "refreshed").await?;
        anyhow::ensure!(fixture.managers[0].run_ttl_sweep_command(commands[0].clone()).await?.deleted_items == 0, "stale cutoff deleted the refreshed winner");
        await_exact(&fixture.managers[0], refreshed_expiry, "refreshed").await?;
        fixture.restart().await?;
        if let Err(error) = await_exact(&fixture.managers[1], refreshed_expiry, "refreshed").await {
            for manager in &fixture.managers { storage::regional_test_support::dump_jobs(manager).await?; }
            return Err(error.context("refresh delivery after independent expiration and restart"));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            let received = history::read_originals(&fixture.managers[0], 1, 2, "ttl-victim").await?;
            if received.iter().any(|original| original == &child) { break; }
            anyhow::ensure!(tokio::time::Instant::now() < deadline, "independently accepted expiration did not recover through registered delivery");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for (index, manager) in fixture.managers.iter().enumerate() {
            anyhow::ensure!(manager.run_ttl_sweep_command(commands[index].clone()).await?.deleted_items == 0, "replayed sweep deleted the refreshed winner");
            await_exact(manager, refreshed_expiry, "refreshed").await?;
            anyhow::ensure!(expiration(&history::read_originals(manager, index + 1, 1, "ttl-victim").await?)? == child, "delivery changed the accepted expiration child");
        }
        fixture.restart().await?;
        for (index, manager) in fixture.managers.iter().enumerate() {
            await_exact(manager, refreshed_expiry, "refreshed").await?;
            let originals = history::read_originals(manager, index + 1, 1, "ttl-victim").await?;
            anyhow::ensure!(originals.len() == 3 && expiration(&originals)? == child && originals.contains(&parent[0]), "reopen lost or duplicated victim, expiration or refresh history");
        }
        Ok(())
    }.await;
    result.and(fixture.stop().await)
}

fn item(expiry: i64, value: &str) -> Result<WireItem> {
    let attributes = [
        ("pk".into(), AttributeValue::S("ttl-victim".into())),
        ("value".into(), AttributeValue::S(value.into())),
        ("expires".into(), AttributeValue::N(expiry.to_string())),
    ]
    .into_iter()
    .collect::<AttributeMap>();
    Ok(WireItem::try_from(&attributes)?)
}

async fn put(manager: &DatabaseManager, expiry: i64, value: &str) -> Result<()> {
    manager
        .put_item(
            PutItemInput::builder()
                .table_name(TableName::new(TABLE))
                .item(item(expiry, value)?)
                .build(),
        )
        .await?;
    Ok(())
}

async fn await_exact(manager: &DatabaseManager, expiry: i64, value: &str) -> Result<()> {
    let expected = item(expiry, value)?.into_attribute_map()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        if manager
            .get_item(TableName::new(TABLE), key("ttl-victim"))
            .await?
            .map(|item| item.into_attribute_map())
            .transpose()?
            == Some(expected.clone())
        {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "exact TTL-bearing public item did not converge for {value}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn expiration(originals: &[String]) -> Result<String> {
    let codec = CanonicalJsonCodec::new(PayloadBounds {
        event_bytes: 65536.try_into()?,
        manifest_bytes: 1048576.try_into()?,
    });
    let mut selected = None;
    for original in originals {
        let (_, _, payload) = codec.decode_event_content(original.as_bytes())?;
        if matches!(payload, CanonicalPayload::Item(item) if item.cause == MutationCause::Expiration)
        {
            anyhow::ensure!(
                selected.is_none(),
                "one victim produced duplicate canonical expiration originals"
            );
            selected = Some(original.clone());
        }
    }
    selected.context("native expiration child was not retained")
}
