#![cfg(all(feature = "regional", feature = "test-support"))]

use std::{net::TcpListener, time::Duration};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use storage::{
    DatabaseManager,
    regional_test_support::{
        RegionalFixture, TABLE, await_item, key, open, operator::features, put,
    },
};
use storage_types::TableName;

#[tokio::test]
async fn given_operator_startup_when_public_table_enable_and_writes_complete_then_tcp_replication_survives_restart()
-> Result<()> {
    let mut fixture = RegionalFixture::start(65536).await?;
    let result = async {
        let empty = TableName::new("empty_regional_slice");
        fixture.managers[0]
            .create_table(&serde_json::from_value(json!({
                "TableName": empty, "BillingMode": "PAY_PER_REQUEST",
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
            }))?)
            .await?;
        fixture.managers[0]
            .update_table(serde_json::from_value(json!({
                "TableName": empty, "ReplicaUpdates": [{"Create": {"RegionName": "region-2"}}]
            }))?)
            .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            if fixture.managers[1]
                .get_item(empty.clone(), key("first"))
                .await
                .is_ok_and(|item| item.is_none())
            {
                break;
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "empty enable did not publish its complete target"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let first = storage::regional_test_support::item("first", "after-empty-enable");
        let expected = first.clone().into_attribute_map()?;
        fixture.managers[1]
            .put_item(
                storage::PutItemInput::builder()
                    .table_name(empty.clone())
                    .item(first.clone())
                    .build(),
            )
            .await?;
        loop {
            if let Some(item) = fixture.managers[0]
                .get_item(empty.clone(), key("first"))
                .await?
                && item.into_attribute_map()? == expected
            {
                break;
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "empty target did not become an authoring region"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        fixture.create_global_table().await?;
        put(&fixture.managers[0], "live", "first").await?;
        await_item(&fixture.managers[1], "live", "first").await?;
        put(&fixture.managers[1], "live", "remote-winner").await?;
        await_item(&fixture.managers[0], "live", "remote-winner").await?;
        fixture.restart().await?;
        for manager in &fixture.managers {
            let item = manager
                .get_item(empty.clone(), key("first"))
                .await?
                .context("empty enable lost its first accepted item after reopen")?;
            anyhow::ensure!(
                item.into_attribute_map()? == expected,
                "empty enable changed its first accepted item after reopen"
            );
        }
        await_item(&fixture.managers[0], "live", "remote-winner").await?;
        await_item(&fixture.managers[1], "live", "remote-winner").await?;
        put(&fixture.managers[0], "live", "after-restart").await?;
        await_item(&fixture.managers[1], "live", "after-restart").await?;
        Ok(())
    }
    .await;
    result.and(fixture.stop().await)
}

#[tokio::test]
async fn given_bounded_source_declaration_when_complete_transaction_exceeds_it_then_no_member_is_accepted()
-> Result<()> {
    for (declaration_bytes, frame_events) in [(4096, 128), (65536, 32)] {
        let fixture =
            RegionalFixture::start_with_limits(declaration_bytes, frame_events, None).await?;
        let result = async {
            fixture.create_global_table().await?;
            let keys = transaction_partition_keys(&fixture.managers[0]).await?;
            let before_source = storage::regional_test_support::history::read_partition_originals(
                &fixture.managers[0],
                1,
                1,
                &keys[0],
            )
            .await?;
            let before_destination =
                storage::regional_test_support::history::read_partition_originals(
                    &fixture.managers[1],
                    2,
                    1,
                    &keys[0],
                )
                .await?;
            let members = keys
                .iter()
                .map(|key| {
                    json!({"Put": {"TableName": TABLE, "Item": {
                        "pk": {"S": key}, "value": {"S": "must-abort"}
                    }}})
                })
                .collect::<Vec<_>>();
            let request = serde_json::from_value(
                json!({"TransactItems": members, "ClientRequestToken": "bounded-position"}),
            )?;
            let error = fixture.managers[0]
                .transact_write_items(request)
                .await
                .err()
                .context("complete declaration did not fail admission")?;
            anyhow::ensure!(
                error.native_attempt_rejection()
                    == Some(storage_types::NativeAttemptRejection::Capacity),
                "unexpected failure: {error:?}"
            );
            for key_value in &keys {
                anyhow::ensure!(
                    fixture.managers[0]
                        .get_item(TableName::new(TABLE), key(key_value))
                        .await?
                        .is_none(),
                    "aborted member became visible at source"
                );
                anyhow::ensure!(
                    fixture.managers[1]
                        .get_item(TableName::new(TABLE), key(key_value))
                        .await?
                        .is_none(),
                    "aborted member became visible at destination"
                );
            }
            anyhow::ensure!(
                storage::regional_test_support::history::read_partition_originals(
                    &fixture.managers[0],
                    1,
                    1,
                    &keys[0]
                )
                .await?
                    == before_source,
                "aborted transaction authored source originals"
            );
            anyhow::ensure!(
                storage::regional_test_support::history::read_partition_originals(
                    &fixture.managers[1],
                    2,
                    1,
                    &keys[0]
                )
                .await?
                    == before_destination,
                "aborted transaction published destination originals"
            );
            put(&fixture.managers[0], "after-abort", "accepted").await?;
            await_item(&fixture.managers[1], "after-abort", "accepted").await?;
            Ok(())
        }
        .await;
        result.and(fixture.stop().await)?;
    }
    Ok(())
}

async fn transaction_partition_keys(manager: &DatabaseManager) -> Result<Vec<String>> {
    use replication_types::{
        event::content::CanonicalEventCodec, identity::journal_partition::JournalPartitionCount,
    };
    let discovery = manager
        .discover_canonical_history(
            regional_replication::history::discovery::CanonicalHistoryDiscoveryRequest {
                serving_region: "region-1".parse()?,
                serving_group: [1; 16].try_into()?,
                table: TableName::new(TABLE),
                after: None,
            },
        )
        .await?;
    let codec = replication_provider::canonical_event_codec::CanonicalJsonCodec::new(
        replication_types::protocol::PayloadBounds {
            event_bytes: 65536.try_into()?,
            manifest_bytes: 65536.try_into()?,
        },
    );
    let partitions = JournalPartitionCount::try_from(64)?;
    let partition = partitions.select(discovery.lineage, &codec.encode_key(&key("member-0"))?);
    let mut keys = Vec::new();
    for index in 0..100000 {
        let candidate = format!("member-{index}");
        if partitions.select(discovery.lineage, &codec.encode_key(&key(&candidate))?) == partition {
            keys.push(candidate);
        }
        if keys.len() == 100 {
            return Ok(keys);
        }
    }
    anyhow::bail!("could not prepare a complete single-partition transaction")
}

#[tokio::test]
async fn given_invalid_operator_budgets_when_startup_is_requested_then_no_listener_is_published()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let provider =
        storage::regional_test_support::provider::ProviderFixture::start(directory.path()).await?;
    let result = async {
        for (field, value) in [
            ("/budgets/attempt/call_guards", json!(0)),
            ("/budgets/reserved_attempts", json!(100)),
            ("/configuration/limits/import_bytes", json!(2097152)),
            ("/physical_limits/key_bytes", json!(usize::MAX)),
            ("/physical_limits/bytes", json!(1)),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let endpoint = listener.local_addr()?;
            let peer = TcpListener::bind("127.0.0.1:0")?;
            let features = features(
                directory.path(),
                1,
                [endpoint, peer.local_addr()?],
                65536,
                provider.backends[0].clone(),
            )?;
            let path = &features.regional_connections["default"];
            let mut startup: Value = serde_json::from_slice(&std::fs::read(path)?)?;
            *startup.pointer_mut(field).context("known budget field")? = value;
            std::fs::write(path, serde_json::to_vec(&startup)?)?;
            drop(listener);
            anyhow::ensure!(
                open(&features).await.is_err(),
                "invalid startup accepted: {field}"
            );
            let unpublished =
                TcpListener::bind(endpoint).context("invalid startup published listener")?;
            drop(unpublished);
        }
        Ok(())
    }
    .await;
    result.and(provider.finish().await)
}

#[tokio::test]
#[ignore = "requires toxiproxy-server; run the regional network gate"]
async fn given_operator_started_regions_when_latency_and_ack_loss_occur_then_public_reads_converge_and_retry_preserves_acceptance()
-> Result<()> {
    let fixture = RegionalFixture::start_with_limits(65536, 128, Some(0)).await?;
    let result = async {
    fixture.create_global_table_with_schema(serde_json::from_value(json!({
        "TableName": TABLE, "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "value", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [{"IndexName": "value-index", "KeySchema": [{"AttributeName": "value", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}]
    }))?).await?;
    let proxy = fixture.proxy.as_ref().context("network fixture")?;
    for latency in [20, 75, 150] {
        proxy.latency(latency).await?;
        let value = format!("latency-{latency}");
        let started = std::time::Instant::now();
        put(&fixture.managers[0], "network", &value).await?;
        await_item(&fixture.managers[1], "network", &value).await?;
        anyhow::ensure!(started.elapsed() >= Duration::from_millis(latency), "delivery bypassed Toxiproxy");
        for direction in ["upstream", "downstream"] { proxy.remove(&format!("latency-{direction}")).await?; }
    }
    proxy.toxic("lost-ack", "timeout", "downstream", json!({"timeout": 0})).await?;
    put(&fixture.managers[0], "network", "ack-lost").await?;
    await_item(&fixture.managers[1], "network", "ack-lost").await?;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    proxy.remove("lost-ack").await?;
    put(&fixture.managers[0], "network", "after-reconciliation").await?;
    await_item(&fixture.managers[1], "network", "after-reconciliation").await?;
    Ok(())
    }.await;
    result.and(fixture.stop().await)
}
