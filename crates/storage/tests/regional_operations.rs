#![cfg(all(feature = "regional", feature = "test-support"))]

use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;
use storage::{
    DeleteItemInput,
    regional_test_support::{RegionalFixture, TABLE, await_item, item, key},
};
use storage_types::{StorageEnum, TableName, TransactWriteItemsRequest};

const LOCAL_TABLE: &str = "ordinary_alongside_regional";
const ITEM_KEY: &str = "mixed-operation";

#[tokio::test]
async fn given_local_and_global_tables_when_mixed_writes_replay_or_fail_then_atomic_effects_and_exact_regional_history_survive_reopen()
-> Result<()> {
    let mut fixture = RegionalFixture::start_with_limits(65536, 1, None).await?;
    let result = async {
        fixture.create_global_table_with_schema(serde_json::from_value(json!({
            "TableName": TABLE, "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
        }))?).await?;
        fixture.managers[0].create_table(&serde_json::from_value(json!({
            "TableName": LOCAL_TABLE, "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
        }))?).await?;
        let codec = replication_provider::canonical_event_codec::CanonicalJsonCodec::new(replication_types::protocol::PayloadBounds {
            event_bytes: 65536.try_into()?, manifest_bytes: 1048576.try_into()?,
        });
        use replication_types::event::content::CanonicalEventCodec;
        let lineage = storage::regional_test_support::history::current_lineage(&fixture.managers[0], 1).await?;
        let partitions = replication_types::identity::journal_partition::JournalPartitionCount::try_from(64)?;
        let partition = partitions.select(lineage, &codec.encode_key(&key("partial-0"))?);
        let mut colliding = vec!["partial-0".to_owned()];
        let mut separate = None;
        for index in 1..4096 {
            let candidate = format!("partial-{index}");
            if partitions.select(lineage, &codec.encode_key(&key(&candidate))?) == partition {
                if colliding.len() < 3 { colliding.push(candidate); }
            } else if separate.is_none() { separate = Some(candidate); }
            if colliding.len() == 3 && separate.is_some() { break; }
        }
        anyhow::ensure!(colliding.len() == 3, "could not bind distinct fixture keys to one source partition");
        let separate = separate.context("missing separate source partition")?;
        let partial: storage_types::BatchWriteItemRequest = serde_json::from_value(json!({"RequestItems": {
            TABLE: [
                {"PutRequest": {"Item": item(&colliding[0], "selected-first").into_attribute_map()?}},
                {"DeleteRequest": {"Key": key(&colliding[1]), "AuxItemStreamTtlHours": 1}},
                {"PutRequest": {"Item": item(&separate, "selected-third").into_attribute_map()?}},
                {"PutRequest": {"Item": item(&colliding[2], "retry-put").into_attribute_map()?, "Indexers": [], "AuxItemStreamTtlHours": 1}}
            ],
            LOCAL_TABLE: [{"PutRequest": {"Item": item("partial-local", "selected-ordinary").into_attribute_map()?}}]
        }}))?;
        let separate_ordinal = partial.request_items.iter().take_while(|(name, _)| *name != &TableName::new(TABLE)).map(|(_, writes)| writes.len()).sum::<usize>() + 2;
        let response = fixture.managers[0].batch_write_item(partial.clone()).await?;
        let mut expected = storage_types::VecMap::new();
        let originals = partial.request_items.get(&TableName::new(TABLE)).context("missing partial regional requests")?;
        expected.insert(TableName::new(TABLE), vec![originals[1].clone(), originals[3].clone()]);
        anyhow::ensure!(serde_json::to_value(&response.unprocessed_items)? == serde_json::to_value(Some(&expected))?, "partial batch changed its rejected request bodies or order");
        await_item(&fixture.managers[1], &colliding[0], "selected-first").await?;
        await_item(&fixture.managers[1], &separate, "selected-third").await?;
        let separate_originals = storage::regional_test_support::history::read_originals(&fixture.managers[0], 1, 1, &separate).await?;
        let event = codec.decode_event(separate_originals.first().context("selected separate key lost its original")?.as_bytes())?;
        anyhow::ensure!(matches!(&event.as_ref().id, replication_types::event::identity::CanonicalEventId::Origin(origin) if usize::try_from(u32::from(origin.ordinal))? == separate_ordinal), "partial batch renumbered a surviving original ordinal");
        for rejected in [&colliding[1], &colliding[2]] {
            for (region, manager) in fixture.managers.iter().enumerate() {
                anyhow::ensure!(manager.get_item(TableName::new(TABLE), key(rejected)).await?.is_none()
                    && storage::regional_test_support::history::read_originals(manager, region + 1, 1, rejected).await?.is_empty(), "partial batch accepted a rejected member or its history");
            }
        }
        anyhow::ensure!(fixture.managers[0].get_item(TableName::new(LOCAL_TABLE), key("partial-local")).await?.context("partial batch lost its ordinary member")?.into_attribute_map()? == item("partial-local", "selected-ordinary").into_attribute_map()?, "partial batch changed its ordinary member");
        let mut pending = response.unprocessed_items.context("partial batch omitted its unprocessed map")?;
        for _ in 0..2 {
            pending = fixture.managers[0].batch_write_item(storage_types::BatchWriteItemRequest { request_items: pending, ..partial.clone() }).await?.unprocessed_items.context("retry omitted its unprocessed map")?;
        }
        anyhow::ensure!(pending.is_empty(), "exact partial retries failed to make progress");
        await_item(&fixture.managers[1], &colliding[2], "retry-put").await?;
        let mut partial_history = Vec::new();
        for selected in colliding.iter().chain(std::iter::once(&separate)) {
            let history = storage::regional_test_support::history::read_originals(&fixture.managers[0], 1, 1, selected).await?;
            anyhow::ensure!(history.len() == 1, "partial retry lost or reauthored an accepted original");
            require_delivered_history(&fixture, selected, &history).await?;
            partial_history.push((selected.clone(), history));
        }
        let mut bounded_batch: storage_types::BatchWriteItemRequest = serde_json::from_value(json!({"RequestItems": {
            TABLE: [{"PutRequest": {"Item": {"pk": {"S": "bounded-batch"}, "value": {"S": "x".repeat(70000)}}, "Indexers": [], "AuxItemStreamTtlHours": 1}}],
            LOCAL_TABLE: [{"PutRequest": {"Item": {"pk": {"S": "bounded-batch"}, "value": {"S": "ordinary-must-abort"}}}}]
        }}))?;
        let unprocessed = fixture.managers[0].batch_write_item(bounded_batch.clone()).await?;
        anyhow::ensure!(serde_json::to_value(&unprocessed.unprocessed_items)? == serde_json::to_value(&bounded_batch.request_items)?, "known-aborted batch changed its original unprocessed requests");
        for table in [TABLE, LOCAL_TABLE] {
            anyhow::ensure!(fixture.managers[0].get_item(TableName::new(table), key("bounded-batch")).await?.is_none(), "bounded batch accepted a source member before reporting unprocessed");
        }
        anyhow::ensure!(fixture.managers[1].get_item(TableName::new(TABLE), key("bounded-batch")).await?.is_none(), "bounded batch published an aborted member");
        anyhow::ensure!(storage::regional_test_support::history::read_originals(&fixture.managers[0], 1, 1, "bounded-batch").await?.is_empty(), "bounded batch authored history before its known abort");
        bounded_batch.request_items.get_mut(&TableName::new(TABLE)).context("missing bounded batch table")?[0].put_request.as_mut().context("missing bounded batch put")?.item = item("bounded-batch", "within-budget");
        let retried = fixture.managers[0].batch_write_item(bounded_batch).await?;
        anyhow::ensure!(retried.unprocessed_items.as_ref().is_none_or(|items| items.is_empty()), "fresh bounded retry left unprocessed members");
        await_item(&fixture.managers[1], "bounded-batch", "within-budget").await?;
        let retried_history = storage::regional_test_support::history::read_originals(&fixture.managers[0], 1, 1, "bounded-batch").await?;
        anyhow::ensure!(retried_history.len() == 1, "fresh bounded retry did not author exactly one global original");
        require_delivered_history(&fixture, "bounded-batch", &retried_history).await?;
        let batch = fixture.managers[0].batch_write_item(serde_json::from_value(json!({
            "RequestItems": {
                TABLE: [{"PutRequest": {"Item": {"pk": {"S": ITEM_KEY}, "value": {"S": "batch"}}}}],
                LOCAL_TABLE: [{"PutRequest": {"Item": {"pk": {"S": ITEM_KEY}, "value": {"S": "local-batch"}}}}]
            }
        }))?).await?;
        anyhow::ensure!(batch.unprocessed_items.as_ref().is_none_or(|items| items.is_empty()), "mixed batch left unprocessed members");
        await_item(&fixture.managers[1], ITEM_KEY, "batch").await?;
        require_local(&fixture, "local-batch").await?;
        let request = mixed_transaction("transaction", "local-transaction")?;
        for _ in 0..2 { fixture.managers[0].transact_write_items(request.clone()).await?; }
        await_item(&fixture.managers[1], ITEM_KEY, "transaction").await?;
        require_local(&fixture, "local-transaction").await?;
        require_mixed_reads(&fixture).await?;
        let originals = storage::regional_test_support::history::read_originals(&fixture.managers[0], 1, 1, ITEM_KEY).await?;
        anyhow::ensure!(originals.len() == 2, "local members or token replay authored extra regional events");
        require_delivered_history(&fixture, ITEM_KEY, &originals).await?;
        for (region, manager) in fixture.managers.iter().enumerate() { require_checkpoint_view(manager, region + 1, &originals).await?; }
        let mismatch = fixture.managers[0].transact_write_items(mixed_transaction("mismatch", "local-mismatch")?).await
            .err().context("changed request reused an accepted token")?;
        anyhow::ensure!(matches!(mismatch.as_ref(), StorageEnum::IdempotentParameterMismatch), "token mismatch returned the wrong error: {mismatch:?}");
        let rejected = fixture.managers[0].transact_write_items(serde_json::from_value(json!({
            "TransactItems": [
                {"Put": {"TableName": TABLE, "Item": {"pk": {"S": ITEM_KEY}, "value": {"S": "must-abort"}}}},
                {"ConditionCheck": {"TableName": LOCAL_TABLE, "Key": {"pk": {"S": ITEM_KEY}}, "ConditionExpression": "attribute_not_exists(pk)"}}
            ]
        }))?).await.err().context("failed local condition accepted the regional member")?;
        anyhow::ensure!(matches!(rejected.as_ref(), StorageEnum::TransactionCanceled { .. }) && rejected.is_canceled_condition_check_failed(1), "transaction condition returned the wrong error: {rejected:?}");
        await_item(&fixture.managers[0], ITEM_KEY, "transaction").await?;
        await_item(&fixture.managers[1], ITEM_KEY, "transaction").await?;
        require_local(&fixture, "local-transaction").await?;
        anyhow::ensure!(storage::regional_test_support::history::read_originals(&fixture.managers[0], 1, 1, ITEM_KEY).await? == originals, "rejected mixed transaction changed source history");
        for _ in 0..2 { storage::regional_test_support::put(&fixture.managers[0], "same-image", "unchanged").await?; }
        await_item(&fixture.managers[1], "same-image", "unchanged").await?;
        let unchanged = storage::regional_test_support::history::read_originals(&fixture.managers[0], 1, 1, "same-image").await?;
        anyhow::ensure!(unchanged.len() == 2 && unchanged[0] != unchanged[1], "same-image writes lost their distinct canonical originals");
        require_delivered_history(&fixture, "same-image", &unchanged).await?;
        for _ in 0..2 {
            fixture.managers[0].delete_item(DeleteItemInput::builder().table_name(TableName::new(TABLE)).key(key(ITEM_KEY)).build()).await?;
        }
        let deleted = storage::regional_test_support::history::read_originals(&fixture.managers[0], 1, 1, ITEM_KEY).await?;
        anyhow::ensure!(deleted.len() == 4 && deleted[..2] == originals, "deletion or absent deletion lost canonical identity");
        require_delivered_history(&fixture, ITEM_KEY, &deleted).await?;
        for manager in &fixture.managers { require_materialization_stream(manager).await?; }
        fixture.restart().await?;
        for (region, manager) in fixture.managers.iter().enumerate() {
            for (selected, history) in &partial_history {
                anyhow::ensure!(storage::regional_test_support::history::read_originals(manager, region + 1, 1, selected).await? == *history, "reopen changed partial-batch original bytes");
            }
            await_item(manager, &colliding[0], "selected-first").await?;
            await_item(manager, &colliding[2], "retry-put").await?;
            await_item(manager, &separate, "selected-third").await?;
            anyhow::ensure!(manager.get_item(TableName::new(TABLE), key(&colliding[1])).await?.is_none(), "partial retry resurrected an absent deletion");
        }
        require_local(&fixture, "local-transaction").await?;
        for (region, manager) in fixture.managers.iter().enumerate() {
            anyhow::ensure!(manager.get_item(TableName::new(TABLE), key(ITEM_KEY)).await?.is_none(), "reopen resurrected deleted data");
            anyhow::ensure!(storage::regional_test_support::history::read_originals(manager, region + 1, 1, ITEM_KEY).await? == deleted, "reopen changed exact mixed-operation history");
            require_materialization_stream(manager).await?;
            await_item(manager, "same-image", "unchanged").await?;
            anyhow::ensure!(storage::regional_test_support::history::read_originals(manager, region + 1, 1, "same-image").await? == unchanged, "reopen lost exact same-image history");
        }
        Ok::<_, anyhow::Error>(())
    }.await;
    result.and(fixture.stop().await)
}

async fn require_materialization_stream(manager: &storage::DatabaseManager) -> Result<()> {
    let response = manager
        .get_stream_records_for_table_name(&TableName::new(TABLE), None, Some(100))
        .await?;
    anyhow::ensure!(
        response.last_evaluated_key().is_none(),
        "mixed-operation stream exceeded its complete fixture page"
    );
    let records: Vec<_> = response
        .records()
        .iter()
        .filter(|record| record.keys == key(ITEM_KEY))
        .collect();
    anyhow::ensure!(
        records.len() == 3,
        "winning transitions were lost or replayed: {records:?}"
    );
    for (index, record) in records.iter().enumerate() {
        anyhow::ensure!(
            records[..index]
                .iter()
                .all(|previous| previous.sequence_number != record.sequence_number),
            "winning transitions reused a stream position"
        );
        let value = |image: &Option<storage_types::AttributeMap>| {
            image.as_ref().and_then(|item| item.get("value")).cloned()
        };
        let expected = [
            (None, Some("batch")),
            (Some("batch"), Some("transaction")),
            (Some("transaction"), None),
        ][index];
        anyhow::ensure!(
            (value(&record.old_image), value(&record.new_image))
                == (
                    expected
                        .0
                        .map(|value| storage_types::AttributeValue::S(value.into())),
                    expected
                        .1
                        .map(|value| storage_types::AttributeValue::S(value.into()))
                ),
            "materialization stream lost its exact old/new transition: {record:?}"
        );
    }
    Ok(())
}

fn mixed_transaction(global: &str, local: &str) -> Result<TransactWriteItemsRequest> {
    Ok(serde_json::from_value(json!({
        "ClientRequestToken": "public-mixed-operation",
        "TransactItems": [
            {"Put": {"TableName": TABLE, "Item": {"pk": {"S": ITEM_KEY}, "value": {"S": global}}}},
            {"Put": {"TableName": LOCAL_TABLE, "Item": {"pk": {"S": ITEM_KEY}, "value": {"S": local}}}}
        ]
    }))?)
}

async fn require_local(fixture: &RegionalFixture, value: &str) -> Result<()> {
    let local = fixture.managers[0]
        .get_item(TableName::new(LOCAL_TABLE), key(ITEM_KEY))
        .await?
        .context("mixed operation lost its local member")?
        .into_attribute_map()?;
    anyhow::ensure!(
        local == item(ITEM_KEY, value).into_attribute_map()?,
        "mixed operation changed its local member"
    );
    let unavailable = fixture.managers[1]
        .get_item(TableName::new(LOCAL_TABLE), key(ITEM_KEY))
        .await
        .err()
        .context("ordinary table replicated to another region")?;
    anyhow::ensure!(
        matches!(
            unavailable.as_ref(),
            StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
        ),
        "ordinary table returned the wrong remote error: {unavailable:?}"
    );
    Ok(())
}

async fn require_delivered_history(
    fixture: &RegionalFixture,
    key_value: &str,
    originals: &[String],
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let observed = storage::regional_test_support::history::read_originals(
            &fixture.managers[1],
            2,
            1,
            key_value,
        )
        .await?;
        if observed == originals {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "mixed-operation exact history did not converge"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn require_mixed_reads(fixture: &RegionalFixture) -> Result<()> {
    let manager = &fixture.managers[0];
    for tables in [[LOCAL_TABLE, TABLE], [TABLE, LOCAL_TABLE]] {
        let request = serde_json::from_value(
            json!({"TransactItems": tables.map(|table| json!({"Get": {"TableName": table, "Key": key(ITEM_KEY)}}))}),
        )?;
        let response = manager.transact_get_items(request).await?;
        let expected = tables.map(|table| json!({"Item": {"pk": {"S": ITEM_KEY}, "value": {"S": if table == TABLE { "transaction" } else { "local-transaction" }}}}));
        anyhow::ensure!(
            serde_json::to_value(response.responses)? == json!(expected),
            "mixed transactional reads changed member order or values"
        );
    }
    let response = manager
        .transact_get_items(serde_json::from_value(json!({"TransactItems": [
            {"Get": {"TableName": TABLE, "Key": key("absent-read")}},
            {"Get": {"TableName": TABLE, "Key": key(ITEM_KEY)}}
        ]}))?)
        .await?;
    anyhow::ensure!(
        serde_json::to_value(response.responses)?
            == json!([{}, {"Item": item(ITEM_KEY, "transaction").into_attribute_map()?}]),
        "repeated table names changed absent/member slots"
    );
    let missing = manager
        .transact_get_items(serde_json::from_value(json!({"TransactItems": [
            {"Get": {"TableName": "absent_table", "Key": key(ITEM_KEY)}}
        ]}))?)
        .await
        .err()
        .context("missing transaction table became readable")?;
    anyhow::ensure!(
        matches!(missing.as_ref(), StorageEnum::ResourceNotFound { .. }),
        "missing transaction table returned the wrong error: {missing:?}"
    );
    let request: storage_types::BatchGetItemRequest =
        serde_json::from_value(json!({"RequestItems": {
            TABLE: {"Keys": [key(ITEM_KEY), key("absent-read")]},
            LOCAL_TABLE: {"Keys": [key(ITEM_KEY)]}
        }}))?;
    let response = manager.batch_get_item(request).await?;
    anyhow::ensure!(
        serde_json::to_value(&response)?["Responses"]
            == json!({
                TABLE: [item(ITEM_KEY, "transaction").into_attribute_map()?],
                LOCAL_TABLE: [item(ITEM_KEY, "local-transaction").into_attribute_map()?]
            }),
        "mixed batch read changed values or returned an absent item"
    );
    anyhow::ensure!(
        response.unprocessed_keys.is_none_or(|keys| keys.is_empty()),
        "mixed batch read left unprocessed keys"
    );
    let missing = manager
        .batch_get_item(serde_json::from_value(json!({"RequestItems": {
            TABLE: {"Keys": [key(ITEM_KEY)]}, "absent_table": {"Keys": [key(ITEM_KEY)]}
        }}))?)
        .await
        .err()
        .context("missing batch table became readable")?;
    anyhow::ensure!(
        matches!(missing.as_ref(), StorageEnum::TableNotFound { name, .. } if name == "absent_table"),
        "missing batch table returned the wrong error: {missing:?}"
    );
    Ok(())
}

async fn require_checkpoint_view(
    manager: &storage::DatabaseManager,
    serving: usize,
    originals: &[String],
) -> Result<()> {
    use regional_replication::history::{
        checkpoint::{
            CanonicalCheckpointAction as Action, CanonicalCheckpointId, CanonicalCheckpointRequest,
        },
        discovery::CanonicalHistoryDiscoveryRequest,
        retention::CheckpointRetention,
        selection::CanonicalViewSelection,
        view::CanonicalViewRequest,
    };
    use replication_types::event::content::CanonicalEventCodec;
    let region: replication_types::identity::replica::RegionId =
        format!("region-{serving}").parse()?;
    let group = [serving as u8; 16].try_into()?;
    let discovery = manager
        .discover_canonical_history(CanonicalHistoryDiscoveryRequest {
            serving_region: region.clone(),
            serving_group: group,
            table: TableName::new(TABLE),
            after: None,
        })
        .await?;
    let (lineage, membership) = (discovery.lineage, discovery.membership);
    drop(discovery);
    let codec = replication_provider::canonical_event_codec::CanonicalJsonCodec::new(
        replication_types::protocol::PayloadBounds {
            event_bytes: 65536.try_into()?,
            manifest_bytes: 1048576.try_into()?,
        },
    );
    let canonical_key = codec.encode_key(&key(ITEM_KEY))?;
    let partition =
        replication_types::identity::journal_partition::JournalPartitionCount::try_from(64)?
            .select(lineage, &canonical_key);
    let view = || CanonicalViewRequest {
        serving_region: region.clone(),
        serving_group: group,
        table: TableName::new(TABLE),
        lineage,
        membership,
        receiver: replication_types::identity::origin::OriginId {
            region: region.clone(),
            database_group: group,
            journal_partition: partition,
            journal_generation: 1.into(),
        },
        selection: CanonicalViewSelection::Item {
            key: canonical_key.clone(),
        },
        checkpoint: None,
        after: None,
    };
    let operation = CanonicalCheckpointId::try_from(uuid::Uuid::new_v4())?;
    let request = |operation, action| CanonicalCheckpointRequest {
        view: view(),
        operation,
        action,
        retention: CheckpointRetention::ExplicitRelease,
    };
    let created = manager
        .change_canonical_checkpoint(request(operation, Action::Create))
        .await
        .context("create receiver checkpoint")?
        .context("missing created checkpoint")?;
    let expected = serde_json::to_value(&created.through)?;
    drop(created);
    let observed = manager
        .change_canonical_checkpoint(request(operation, Action::Observe))
        .await
        .context("observe receiver checkpoint")?
        .context("missing observed checkpoint")?;
    anyhow::ensure!(
        serde_json::to_value(&observed.through)? == expected,
        "observing changed a checkpoint cursor"
    );
    drop(observed);
    let mut after = None;
    let mut delivered = Vec::new();
    loop {
        let mut selected = view();
        selected.checkpoint = Some(operation);
        selected.after = after;
        let page = manager
            .read_canonical_view(selected)
            .await
            .context("read checkpoint view")?;
        after = Some(page.through.clone());
        let complete = serde_json::to_value(&page.through)?["position"]
            == serde_json::to_value(page.observed_head)?;
        if let Some(record) = &page.record {
            delivered.push(record.original.clone());
        }
        drop(page);
        if complete {
            break;
        }
    }
    let mut mismatch = view();
    mismatch.checkpoint = Some(operation);
    mismatch.after = after.clone();
    mismatch.selection = CanonicalViewSelection::System;
    let error = manager
        .read_canonical_view(mismatch)
        .await
        .err()
        .context("mismatched checkpoint projection accepted its cursor")?;
    anyhow::ensure!(
        matches!(error.as_ref(), StorageEnum::Validation { .. }),
        "cursor mismatch returned the wrong error: {error:?}"
    );
    anyhow::ensure!(
        delivered == originals,
        "checkpoint view changed original bytes or delivery order"
    );
    let replacement = CanonicalCheckpointId::try_from(uuid::Uuid::new_v4())?;
    drop(
        manager
            .change_canonical_checkpoint(request(
                replacement,
                Action::Rollover {
                    previous: operation,
                },
            ))
            .await
            .context("rollover receiver checkpoint")?
            .context("missing rollover checkpoint")?,
    );
    let mut released = view();
    released.checkpoint = Some(operation);
    released.after = after;
    let error = manager
        .read_canonical_view(released)
        .await
        .err()
        .context("released checkpoint accepted its cursor")?;
    anyhow::ensure!(
        matches!(error.as_ref(), StorageEnum::HistoryUnavailable),
        "released checkpoint returned the wrong error: {error:?}"
    );
    drop(
        manager
            .change_canonical_checkpoint(request(replacement, Action::Release))
            .await
            .context("release receiver checkpoint")?
            .context("missing released checkpoint")?,
    );
    Ok(())
}
