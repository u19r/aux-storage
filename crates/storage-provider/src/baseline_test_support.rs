//! Real-provider proof that baseline mutations never publish stream events.

use std::{num::NonZeroUsize, sync::Arc};

use storage_types::{
    AttemptMode, AttributeValue, BoundTableInfo, DeadlineClass, ItemProvenance, KeyAttributes,
    StorageResult, TableName, maintenance::TargetIncarnation,
};
use stream_provider::{PositionedStreamEntry, StreamAttempt, StreamChange};

use crate::{
    AcceptedDeleteItem, CatalogAttempt, DynamoOperations, DynamoPort, DynamoStreamWriteAttempt,
    ItemReadAttempt as _, ItemWriteAttempt, PutItemShape, TtlPolicyAttempt as _,
    maintenance::{ExpirationPort, ExpirationScanRead},
};

pub async fn qualify_baseline_history<P: DynamoPort + DynamoOperations>(store: P)
where for<'a> P::Attempt<'a>: ExpirationPort {
    let name = TableName::new("baseline-history");
    store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": name, "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"}],
        "GlobalSecondaryIndexes": [{"IndexName": "ByGroup",
            "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    })).unwrap()).await.unwrap();
    store
        .update_time_to_live(
            serde_json::from_value(serde_json::json!({
                "TableName": name,
                "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expires"}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let mut groups = Vec::new();
    let mut ttl_item_key = None;
    assert_primary_presence(&store, &name, false).await;
    for value in [Some("a"), Some("b"), None, Some("c")] {
        write_baseline(&store, &name, value).await.unwrap();
        assert_primary_presence(&store, &name, value.is_some()).await;
        assert_derived_state(&store, &name, value, &mut groups, &mut ttl_item_key).await;
        assert!(
            read_history(&store).await.is_empty(),
            "baseline published an event"
        );
    }
    store
        .put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "Item": fixture_item("ordinary")
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_derived_state(
        &store,
        &name,
        Some("ordinary"),
        &mut groups,
        &mut ttl_item_key,
    )
    .await;
    let published = read_history(&store).await;
    assert_eq!(published.len(), 1);
    let entry = &published[0].entry;
    assert_eq!(entry.change(), StreamChange::Modify);
    assert_eq!(
        entry
            .old_image()
            .unwrap()
            .item
            .to_attribute_map()
            .unwrap()
            .get("body"),
        Some(&AttributeValue::S("c".repeat(100_000)))
    );
    assert_eq!(
        entry
            .new_image()
            .unwrap()
            .item
            .to_attribute_map()
            .unwrap()
            .get("body"),
        Some(&AttributeValue::S("ordinary".into()))
    );
    let published_bytes = published[0].encode_value().unwrap();
    for value in [None, Some("d"), Some("e")] {
        write_baseline(&store, &name, value).await.unwrap();
        assert_primary_presence(&store, &name, value.is_some()).await;
        assert_derived_state(&store, &name, value, &mut groups, &mut ttl_item_key).await;
        let retained = read_history(&store).await;
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].history_id, published[0].history_id);
        assert_eq!(retained[0].position, published[0].position);
        assert_eq!(
            retained[0].encode_value().unwrap(),
            published_bytes,
            "baseline changed an already published event"
        );
    }
}

fn fixture_epoch(value: &str) -> i64 {
    4_102_444_800
        + match value {
            "a" => 1,
            "b" => 2,
            "c" => 3,
            "ordinary" => 4,
            "d" => 5,
            "e" => 6,
            _ => panic!("unknown baseline fixture image"),
        }
}

fn fixture_item(value: &str) -> serde_json::Value {
    let body = if value == "ordinary" {
        value.to_owned()
    } else {
        value.repeat(100_000)
    };
    serde_json::json!({
        "pk": {"S": "key"}, "body": {"S": body}, "gsi_pk": {"S": value},
        "expires": {"N": fixture_epoch(value).to_string()}
    })
}

async fn assert_derived_state<P: DynamoPort + DynamoOperations>(
    store: &P,
    name: &TableName,
    current: Option<&str>,
    groups: &mut Vec<String>,
    ttl_item_key: &mut Option<String>,
) where
    for<'a> P::Attempt<'a>: ExpirationPort,
{
    if let Some(current) = current {
        groups.push(current.to_owned());
    }
    for group in groups.iter() {
        let result = store
            .query(
                serde_json::from_value(serde_json::json!({
                    "TableName": name, "IndexName": "ByGroup",
                    "KeyConditionExpression": "gsi_pk = :group",
                    "ExpressionAttributeValues": {":group": {"S": group}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(result.last_evaluated_key.is_none());
        let items = result.items.unwrap_or_default();
        if current == Some(group.as_str()) {
            assert_eq!(result.count, 1);
            assert_eq!(items.len(), 1);
            assert_eq!(
                serde_json::to_value(items[0].to_attribute_map().unwrap()).unwrap(),
                fixture_item(group)
            );
        } else {
            assert_eq!(result.count, 0);
            assert!(items.is_empty(), "stale GSI group {group}");
        }
    }
    let expected_epoch = current.map(fixture_epoch);
    let name = name.clone();
    let current_key = crate::DynamoPort::with_attempt(
        store,
        storage_types::AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                let table = load_table(attempt, &name).await?;
                let config = attempt.read_ttl_config(&table).await?.unwrap();
                assert_eq!(config.attribute_name, "expires");
                let read = ExpirationScanRead {
                    table_name: name,
                    table_incarnation: TargetIncarnation::from(*table.history_id.as_ref()),
                    ttl_attribute: config.attribute_name,
                    policy_generation: config.policy_generation,
                    cutoff_seconds: fixture_epoch("e"),
                    cursor: None,
                    limit: NonZeroUsize::new(16).unwrap(),
                };
                let (rows, more) = attempt.scan_expiration_index(&table, &read).await?;
                assert!(!more);
                let key = KeyAttributes::from([("pk".into(), AttributeValue::S("key".into()))]);
                let (observed, _) = attempt.observe_for_write(&table, &key).await?;
                if let Some(expected_epoch) = expected_epoch {
                    assert_eq!(rows.len(), 1, "stale or missing TTL index address");
                    assert_eq!(rows[0].indexed_expiration_seconds, expected_epoch);
                    let crate::ItemWriteObservation::Live { ttl_epoch, .. } = observed else {
                        panic!("expected live primary image");
                    };
                    assert_eq!(ttl_epoch, Some(expected_epoch));
                    Ok(Some(rows[0].ttl_item_key.clone()))
                } else {
                    assert!(rows.is_empty());
                    assert!(matches!(observed, crate::ItemWriteObservation::Absent));
                    Ok(None)
                }
            })
        },
    )
    .await
    .unwrap();
    if let Some(current_key) = current_key {
        if let Some(original) = ttl_item_key.as_ref() {
            assert_eq!(&current_key, original);
        } else {
            *ttl_item_key = Some(current_key);
        }
    }
}

async fn assert_primary_presence<P: DynamoPort>(store: &P, name: &TableName, expected: bool) {
    let name = name.clone();
    crate::DynamoPort::with_attempt(
        store,
        storage_types::AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                let table = load_table(attempt, &name).await?;
                let key = KeyAttributes::from([("pk".into(), AttributeValue::S("key".into()))]);
                let absent =
                    KeyAttributes::from([("pk".into(), AttributeValue::S("absent".into()))]);
                let items = attempt
                    .read_primary_pairs(&[(&table, &key), (&table, &absent), (&table, &key)])
                    .await?;
                assert_eq!(
                    items
                        .into_iter()
                        .map(|item| item.is_some())
                        .collect::<Vec<_>>(),
                    [expected, false, expected]
                );
                assert!(attempt.read_primary_pairs(&[]).await?.is_empty());
                Ok(())
            })
        },
    )
    .await
    .unwrap();
}

async fn write_baseline<P: DynamoPort>(
    store: &P,
    name: &TableName,
    value: Option<&str>,
) -> StorageResult<()> {
    let name = name.clone();
    let value = value.map(fixture_item);
    crate::DynamoPort::with_attempt(
        store,
        storage_types::AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                let table = load_table(attempt, &name).await?;
                let key = KeyAttributes::from([("pk".into(), AttributeValue::S("key".into()))]);
                let (observed, lifecycle) = attempt.observe_for_write(&table, &key).await?;
                if let Some(value) = value {
                    let request = serde_json::from_value(serde_json::json!({
                        "TableName": name, "Item": value
                    }))
                    .unwrap();
                    let shape = Arc::new(PutItemShape::try_from_request(request)?);
                    let bound = shape.bind_table(table)?;
                    let (accepted, _) = bound.accept_old(observed, lifecycle)?.into_result()?;
                    crate::prepare_baseline_upsert(attempt, accepted, ItemProvenance::default())
                        .await?;
                } else {
                    storage_types::validate_transact_key(&table.info, &key)?;
                    let old = crate::dynamo_put_item::ObservedWrite::try_new(observed, &key)?;
                    let accepted =
                        AcceptedDeleteItem::try_from_observed(table, key, old, lifecycle)?;
                    crate::prepare_baseline_delete(attempt, &accepted).await?;
                }
                attempt.flush_dynamo_write().await?;
                Ok(())
            })
        },
    )
    .await
}

async fn load_table(
    attempt: &mut impl CatalogAttempt,
    name: &TableName,
) -> StorageResult<Arc<BoundTableInfo>> {
    let stamp = attempt.read_catalog_stamp(name).await?.unwrap();
    attempt.read_table_at_stamp(name, stamp).await
}

async fn read_history<P: DynamoPort>(store: &P) -> Vec<PositionedStreamEntry> {
    crate::DynamoPort::with_attempt(
        store,
        storage_types::AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                let source = attempt.read_system_page_start().await?;
                let initial = source.initial_checkpoint().unwrap();
                let high = source.capture_high().unwrap();
                let plan = source.plan_page(&initial, &high, 200).unwrap();
                let pages = attempt.read_system_history_pages(plan.as_ref()).await?;
                let observations = pages
                    .iter()
                    .zip(plan.as_ref())
                    .map(
                        |(page, read)| stream_provider::SystemStreamHistoryObservation {
                            read,
                            retired_through: page.retired_through.clone(),
                            records: page
                                .records
                                .iter()
                                .map(|(entry, _)| stream_provider::SystemStreamHistoryRecord {
                                    position: entry.position.clone(),
                                })
                                .collect(),
                        },
                    )
                    .collect::<Vec<_>>();
                plan.finish(&observations).unwrap();
                Ok(pages
                    .into_iter()
                    .flat_map(|page| page.records)
                    .map(|(entry, _)| entry)
                    .collect())
            })
        },
    )
    .await
    .unwrap()
}
