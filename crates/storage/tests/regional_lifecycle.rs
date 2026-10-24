#![cfg(all(feature = "regional", feature = "test-support"))]

use anyhow::{Context, Result};
use serde_json::json;
use storage::{
    DatabaseManager,
    admission::AdmissionClass,
    regional_test_support::{RegionalFixture, TABLE, accept_control, await_item, item, put},
};
use storage_types::{DescribeTableRequest, TableDescription, TableName, TimeToLiveStatus};

#[tokio::test]
async fn given_global_indexed_table_when_ttl_primary_transfer_and_disable_complete_then_authority_and_synchronous_indexes_survive_reopen()
-> Result<()> {
    let intervals = storage_common::DatabaseJobIntervals {
        ttl_sweep_interval_ms: storage_common::JobIntervalMillis(1000),
        gsi_backfill_interval_ms: storage_common::JobIntervalMillis(1000),
        ..Default::default()
    };
    let options = storage::DatabaseManagerRuntimeOptions::builder()
        .database_job_intervals(intervals)
        .build();
    let mut fixture =
        RegionalFixture::start_with_options((65536, 128, Some(0)), options, false).await?;
    let result = async {
        fixture.create_global_table_with_schema(serde_json::from_value(json!({
            "TableName": TABLE, "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "value", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [{"IndexName": "value-index", "KeySchema": [{"AttributeName": "value", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}]
        }))?).await.context("enable populated indexed table")?;
        for manager in &fixture.managers { require_index(manager, "baseline", "before-enable").await?; }
        put(&fixture.managers[0], "indexed", "old-index-key").await?;
        await_item(&fixture.managers[1], "indexed", "old-index-key").await?;
        for manager in &fixture.managers { require_index(manager, "indexed", "old-index-key").await?; }
        put(&fixture.managers[1], "indexed", "new-index-key").await?;
        await_item(&fixture.managers[0], "indexed", "new-index-key").await?;
        for manager in &fixture.managers {
            require_index(manager, "indexed", "new-index-key").await?;
            anyhow::ensure!(query_index(manager, "value-index", "old-index-key").await?.is_empty(), "overwritten GSI entry remained visible");
        }
        let expiry = storage_types::TimestampMillis::now().timestamp_millis() / 1000;
        put_expiring(&fixture.managers[0], "already-expired", json!({"N": (expiry - 10).to_string()})).await?;
        put_expiring(&fixture.managers[0], "future-expiry", json!({"N": (expiry + 3600).to_string()})).await?;
        put_expiring(&fixture.managers[0], "invalid-expiry", json!({"S": "not-a-number"})).await?;
        for key in ["already-expired", "future-expiry", "invalid-expiry"] {
            await_present(&fixture.managers[1], key).await.with_context(|| format!("checkpoint replicated pre-policy item {key}"))?;
        }
        let ttl: storage_types::UpdateTimeToLiveRequest = serde_json::from_value(json!({
            "TableName": TABLE, "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expires"}
        }))?;
        accept_control(|| fixture.managers[0].update_time_to_live(ttl.clone())).await.context("accept primary TTL control")?;
        for (region, manager) in fixture.managers.iter().enumerate() {
            await_ttl(manager, "expires").await.context("wait for TTL build activation")?;
            await_absent(manager, "already-expired").await.context("wait for pre-policy expiration")?;
            anyhow::ensure!(manager.get_item(TableName::new(TABLE), storage::regional_test_support::key("future-expiry")).await?.is_some(), "future expiry was deleted in region {region}");
            anyhow::ensure!(manager.get_item(TableName::new(TABLE), storage::regional_test_support::key("invalid-expiry")).await?.is_some(), "nonnumeric expiry was deleted in region {region}");
            require_hidden_ttl_rejected(manager, "expires", &["value-index"]).await?;
            anyhow::ensure!(query_index(manager, "value-index", "already-expired").await.context("query index after TTL deletion")?.is_empty(), "TTL deletion retained its GSI projection");
        }
        let transfer: storage_types::UpdateTableRequest = serde_json::from_value(json!({
            "TableName": TABLE, "AuxConfigurationPrimaryRegion": "region-2"
        }))?;
        accept_control(|| fixture.managers[0].update_table(transfer.clone())).await.context("accept configuration primary transfer")?;
        for manager in &fixture.managers { await_primary(manager, Some("region-2")).await.context("wait for transferred primary")?; }
        let rejected = fixture.managers[0].update_time_to_live(serde_json::from_value(json!({
            "TableName": TABLE, "TimeToLiveSpecification": {"Enabled": false, "AttributeName": "expires"}
        }))?).await.err().ok_or_else(|| anyhow::anyhow!("former primary retained configuration authority"))?;
        anyhow::ensure!(matches!(rejected.as_ref(), storage_types::StorageEnum::Validation { message, .. } if message == "TTL control requires the active configuration primary"), "former primary returned the wrong public rejection");
        let disable: storage_types::UpdateTimeToLiveRequest = serde_json::from_value(json!({
            "TableName": TABLE, "TimeToLiveSpecification": {"Enabled": false, "AttributeName": "expires"}
        }))?;
        accept_control(|| fixture.managers[1].update_time_to_live(disable.clone())).await.context("disable TTL at transferred primary")?;
        for manager in &fixture.managers { await_ttl_status(manager, None, TimeToLiveStatus::Disabled).await?; }
        put_expiring_attribute(&fixture.managers[1], "new-policy-expired", "later", json!({"N": (expiry - 10).to_string()})).await?;
        await_present(&fixture.managers[0], "new-policy-expired").await?;
        let enable: storage_types::UpdateTimeToLiveRequest = serde_json::from_value(json!({
            "TableName": TABLE, "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "later"}
        }))?;
        accept_control(|| fixture.managers[1].update_time_to_live(enable.clone())).await.context("re-enable TTL on another attribute")?;
        for manager in &fixture.managers {
            await_ttl(manager, "later").await?;
            await_absent(manager, "new-policy-expired").await?;
            anyhow::ensure!(manager.get_item(TableName::new(TABLE), storage::regional_test_support::key("future-expiry")).await?.is_some(), "new policy deleted an item with no matching expiry");
        }
        let removal: storage_types::UpdateTableRequest = serde_json::from_value(json!({
            "TableName": TABLE, "ReplicaUpdates": [{"Delete": {"RegionName": "region-1"}}]
        }))?;
        let ordinary_table = TableName::new("ordinary_during_removal");
        fixture.managers[0].create_table(&serde_json::from_value(json!({
            "TableName": ordinary_table, "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}], "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
        }))?).await?;
        let proxy = fixture.proxy.as_ref().context("removal fixture needs its owned WAN proxy")?;
        proxy.latency(75).await?;
        proxy.toxic("removal-wait", "timeout", "upstream", json!({"timeout": 0})).await?;
        let connections = proxy.connections()?;
        put(&fixture.managers[0], "sealed-pending", "accepted-before-removal").await?;
        proxy.await_connection(connections).await?;
        anyhow::ensure!(fixture.managers[1].get_item(TableName::new(TABLE), storage::regional_test_support::key("sealed-pending")).await?.is_none(), "held delivery reached the survivor before removal");
        accept_control(|| fixture.managers[1].update_table(removal.clone())).await.context("accept last-secondary removal")?;
        let seal_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
        let mut probe_accepted = false;
        loop {
            match fixture.managers[0].put_item(storage::PutItemInput::builder().table_name(TableName::new(TABLE)).item(item("seal-probe", "before-seal")).build()).await {
                Ok(_) => probe_accepted = true,
                Err(error) if matches!(error.as_ref(), storage_types::StorageEnum::InfrastructureResult(storage_types::InfrastructureResultKind::Unavailable { context: storage_types::InfrastructureContext::RegionalSource })) => break,
                Err(error) => return Err(anyhow::Error::from(error).context("observe source sealing")),
            }
            anyhow::ensure!(tokio::time::Instant::now() < seal_deadline, "removal did not seal its source while accepted delivery was held");
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let sealed = storage::regional_test_support::history::read_originals(&fixture.managers[0], 1, 1, "sealed-pending").await?;
        let probes = storage::regional_test_support::history::read_originals(&fixture.managers[0], 1, 1, "seal-probe").await?;
        anyhow::ensure!(sealed.len() == 1, "sealing lost its accepted original");
        let pending = description(&fixture.managers[1]).await?;
        anyhow::ensure!(pending.table_status == storage_types::TableStatus::Updating
            && pending.replicas.as_ref().is_some_and(|replicas| replicas.iter().any(|replica| replica.region_name == "region-1" && replica.replica_status == storage_types::ReplicaStatus::Deleting)), "removal committed before its sealed history drained");
        await_item(&fixture.managers[0], "sealed-pending", "accepted-before-removal").await?;
        fixture.managers[0].put_item(storage::PutItemInput::builder().table_name(ordinary_table.clone()).item(item("ordinary", "while-removal-waits")).build()).await?;
        anyhow::ensure!(fixture.managers[0].get_item(ordinary_table.clone(), storage::regional_test_support::key("ordinary")).await?.context("ordinary write failed during removal")?.into_attribute_map()? == item("ordinary", "while-removal-waits").into_attribute_map()?, "ordinary item changed while removal waited");
        proxy.remove("removal-wait").await?;
        await_primary(&fixture.managers[1], None).await.context("wait for local-mode removal")?;
        await_item(&fixture.managers[1], "sealed-pending", "accepted-before-removal").await?;
        require_index(&fixture.managers[1], "sealed-pending", "accepted-before-removal").await?;
        if probe_accepted { await_item(&fixture.managers[1], "seal-probe", "before-seal").await?; }
        for (key, originals) in [("sealed-pending", &sealed), ("seal-probe", &probes)] {
            anyhow::ensure!(storage::regional_test_support::history::read_originals(&fixture.managers[1], 2, 1, key).await? == *originals, "removal lost or rewrote a sealed original");
        }
        require_removed(&fixture.managers[0]).await.context("removed region hid its retired table")?;
        put(&fixture.managers[1], "local", "after-disable").await?;
        require_index(&fixture.managers[1], "local", "after-disable").await?;
        fixture.restart().await.context("reopen after TTL and removal")?;
        await_item(&fixture.managers[1], "sealed-pending", "accepted-before-removal").await?;
        anyhow::ensure!(storage::regional_test_support::history::read_originals(&fixture.managers[1], 2, 1, "sealed-pending").await? == sealed, "reopen lost sealed removal history");
        anyhow::ensure!(fixture.managers[0].get_item(ordinary_table, storage::regional_test_support::key("ordinary")).await?.is_some(), "reopen lost unrelated removal-time write");
        await_item(&fixture.managers[1], "indexed", "new-index-key").await?;
        await_item(&fixture.managers[1], "local", "after-disable").await?;
        require_index(&fixture.managers[1], "indexed", "new-index-key").await?;
        require_index(&fixture.managers[1], "local", "after-disable").await?;
        await_ttl(&fixture.managers[1], "later").await?;
        require_removed(&fixture.managers[0]).await.context("reopen preserved replica removal")?;
        Ok::<_, anyhow::Error>(())
    }.await;
    result.and(fixture.stop().await)
}

#[tokio::test]
async fn given_local_ttl_indexed_table_when_enabled_and_bootstrapped_then_internal_schema_and_live_items_survive_reopen()
-> Result<()> {
    let intervals = storage_common::DatabaseJobIntervals {
        ttl_sweep_interval_ms: storage_common::JobIntervalMillis(1000),
        gsi_backfill_interval_ms: storage_common::JobIntervalMillis(1000),
        ..Default::default()
    };
    let options = storage::DatabaseManagerRuntimeOptions::builder()
        .database_job_intervals(intervals)
        .build();
    let mut fixture =
        RegionalFixture::start_with_options((65536, 128, None), options, false).await?;
    let result = async {
        fixture.managers[0].create_table(&serde_json::from_value(json!({
            "TableName": TABLE, "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "value", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [{"IndexName": "value-index", "KeySchema": [{"AttributeName": "value", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}, {"IndexName": "second-value-index", "KeySchema": [{"AttributeName": "value", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}]
        }))?).await?;
        let expiry = storage_types::TimestampMillis::now().timestamp_millis() / 1000;
        for key in ["future-expiry", "second-future-expiry"] {
            put_expiring(&fixture.managers[0], key, json!({"N": (expiry + 3600).to_string()})).await?;
        }
        put_expiring(&fixture.managers[0], "already-expired", json!({"N": (expiry - 10).to_string()})).await?;
        fixture.managers[0].update_time_to_live(serde_json::from_value(json!({
            "TableName": TABLE, "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expires"}
        }))?).await?;
        await_ttl(&fixture.managers[0], "expires").await.context("activate local TTL build before bootstrap")?;
        await_absent(&fixture.managers[0], "already-expired").await?;
        fixture.managers[0].update_table(serde_json::from_value(json!({
            "TableName": TABLE, "ReplicaUpdates": [{"Create": {"RegionName": "region-2"}}]
        }))?).await.context("bootstrap captured internal TTL schema")?;
        for reopen in [false, true] {
            if reopen { fixture.restart().await?; }
            for manager in &fixture.managers {
                for key in ["future-expiry", "second-future-expiry"] { await_present(manager, key).await?; }
                await_ttl(manager, "expires").await?;
                await_absent(manager, "already-expired").await?;
                for key in ["future-expiry", "second-future-expiry"] {
                    for index in ["value-index", "second-value-index"] {
                        let rows = query_index(manager, index, key).await?;
                        anyhow::ensure!(rows.len() == 1 && rows[0].to_attribute_map()?.get("expires") == Some(&storage_types::AttributeValue::N((expiry + 3600).to_string())), "bootstrap exposed primary without exact TTL-bearing GSI image");
                    }
                }
                require_hidden_ttl_rejected(manager, "expires", &["value-index", "second-value-index"]).await?;
            }
        }
        Ok::<_, anyhow::Error>(())
    }.await;
    result.and(fixture.stop().await)
}

async fn require_removed(manager: &DatabaseManager) -> Result<()> {
    let error = manager
        .get_item(
            TableName::new(TABLE),
            storage::regional_test_support::key("indexed"),
        )
        .await
        .expect_err("removed region exposed its retired table");
    anyhow::ensure!(
        matches!(
            error.as_ref(),
            storage_types::StorageEnum::TableNotFound { .. }
                | storage_types::StorageEnum::ResourceNotFound { .. }
        ),
        "removed region returned an error other than not found: {error:?}"
    );
    Ok(())
}

async fn require_hidden_ttl_rejected(
    manager: &DatabaseManager,
    attribute: &str,
    expected_indexes: &[&str],
) -> Result<()> {
    let name = format!("{}{}", storage_types::HIDDEN_TTL_INDEX_PREFIX, TABLE);
    let scan: storage_types::ScanRequest =
        serde_json::from_value(json!({"TableName": TABLE, "IndexName": name}))?;
    let error = manager
        .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
            dynamo.scan(scan).await
        })
        .await
        .expect_err("public scan admitted hidden TTL index");
    anyhow::ensure!(
        matches!(
            error.as_ref(),
            storage_types::StorageEnum::Validation { .. }
        ),
        "hidden TTL scan returned wrong public error"
    );
    let query: storage_types::QueryRequest = serde_json::from_value(
        json!({"TableName": TABLE, "IndexName": name,
        "KeyConditionExpression": "#e = :e", "ExpressionAttributeNames": {"#e": attribute}, "ExpressionAttributeValues": {":e": {"N": "1"}}}),
    )?;
    let error = manager
        .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
            dynamo.query(query).await
        })
        .await
        .expect_err("public query admitted hidden TTL index");
    anyhow::ensure!(
        matches!(
            error.as_ref(),
            storage_types::StorageEnum::Validation { .. }
        ),
        "hidden TTL query returned wrong public error"
    );
    let description = description(manager).await?;
    let indexes = description
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default();
    anyhow::ensure!(
        indexes.len() == expected_indexes.len()
            && expected_indexes.iter().all(|name| indexes
                .iter()
                .any(|index| index.index_name.as_ref() == *name)),
        "hidden TTL schema leaked into public description"
    );
    Ok(())
}

async fn put_expiring(
    manager: &DatabaseManager,
    key: &str,
    expiry: serde_json::Value,
) -> Result<()> {
    put_expiring_attribute(manager, key, "expires", expiry).await
}

async fn put_expiring_attribute(
    manager: &DatabaseManager,
    key: &str,
    attribute: &str,
    expiry: serde_json::Value,
) -> Result<()> {
    let mut attributes = json!({"pk": {"S": key}, "value": {"S": key}});
    attributes[attribute] = expiry;
    let item = storage_types::WireItem::dynamo_json(serde_json::to_vec(&attributes)?);
    manager
        .put_item(
            storage::PutItemInput::builder()
                .table_name(TableName::new(TABLE))
                .item(item)
                .build(),
        )
        .await?;
    Ok(())
}

async fn await_present(manager: &DatabaseManager, key: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    while tokio::time::Instant::now() < deadline {
        match manager
            .get_item(
                TableName::new(TABLE),
                storage::regional_test_support::key(key),
            )
            .await
        {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {}
            Err(error)
                if matches!(
                    error.as_ref(),
                    storage_types::StorageEnum::ResourceNotFound { .. }
                        | storage_types::StorageEnum::TableNotFound { .. }
                ) => {}
            Err(error) => return Err(error.into()),
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    anyhow::bail!("pre-policy item {key} did not replicate")
}

async fn await_absent(manager: &DatabaseManager, key: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    while tokio::time::Instant::now() < deadline {
        if manager
            .get_item(
                TableName::new(TABLE),
                storage::regional_test_support::key(key),
            )
            .await?
            .is_none()
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    anyhow::bail!("pre-policy expired item was not deleted")
}

async fn query_index(
    manager: &DatabaseManager,
    index: &str,
    value: &str,
) -> Result<Vec<storage_types::WireItem>> {
    let request = serde_json::from_value(json!({"TableName": TABLE, "IndexName": index,
        "KeyConditionExpression": "#v = :v", "ExpressionAttributeNames": {"#v": "value"},
        "ExpressionAttributeValues": {":v": {"S": value}}}))?;
    let response = manager
        .run_dynamo_admitted("default", AdmissionClass::RangeRead, |dynamo| async move {
            dynamo.query(request).await
        })
        .await?;
    Ok(response.items.unwrap_or_default())
}

async fn require_index(manager: &DatabaseManager, key: &str, value: &str) -> Result<()> {
    let rows = query_index(manager, "value-index", value).await?;
    anyhow::ensure!(
        rows.len() == 1 && rows[0].to_attribute_map()? == item(key, value).into_attribute_map()?,
        "primary became visible before its exact GSI projection"
    );
    Ok(())
}

async fn description(manager: &DatabaseManager) -> Result<TableDescription> {
    let request = DescribeTableRequest {
        table_name: TableName::new(TABLE),
    };
    Ok(manager
        .run_dynamo_admitted("default", AdmissionClass::PointRead, |dynamo| async move {
            dynamo.describe_table(request).await
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?
        .table)
}

async fn await_primary(manager: &DatabaseManager, primary: Option<&str>) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut observed = None;
    while tokio::time::Instant::now() < deadline {
        let table = description(manager).await?;
        if table.aux_configuration_primary_region.as_deref() == primary
            && table.table_status == storage_types::TableStatus::Active
        {
            return Ok(());
        }
        observed = Some((table.aux_configuration_primary_region, table.table_status));
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    storage::regional_test_support::dump_jobs(manager).await?;
    anyhow::bail!(
        "configuration ownership did not converge through public DescribeTable: \
         expected={primary:?}, observed={observed:?}"
    )
}

async fn await_ttl(manager: &DatabaseManager, attribute: &str) -> Result<()> {
    await_ttl_status(manager, Some(attribute), TimeToLiveStatus::Enabled).await
}

async fn await_ttl_status(
    manager: &DatabaseManager,
    attribute: Option<&str>,
    status: TimeToLiveStatus,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    while tokio::time::Instant::now() < deadline {
        if let Some(ttl) = manager
            .describe_time_to_live(&TableName::new(TABLE))
            .await?
            .time_to_live_description
            && ttl.attribute_name.as_deref() == attribute
            && ttl.time_to_live_status == status
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    storage::regional_test_support::dump_jobs(manager).await?;
    anyhow::bail!("replicated TTL policy did not become enabled")
}
