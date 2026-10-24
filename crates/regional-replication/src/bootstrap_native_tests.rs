use std::sync::Arc;

use replication_types::event::{content::CanonicalEventCodec, identity::VersionIdentity};
use storage_provider::{DynamoOperations, DynamoPort};
use storage_types::{
    AttemptMode, AttributeValue, GetItemRequest, KeyAttributes, PutItemRequest, TableName, WireItem,
};

use crate::{bootstrap, test_support as fixture};

#[tokio::test]
async fn given_frozen_bootstrap_cut_when_live_rows_change_then_exact_baselines_and_tail_survive_hidden_activation_and_reopen()
 {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.sqlite");
    let destination_path = directory.path().join("destination.sqlite");
    let source = fixture::open(&source_path).await;
    let destination = fixture::open(&destination_path).await;
    let pair = fixture::NativeCluster::start(&[&source, &destination]).await;
    fixture::create_table(&source, &TableName::new(fixture::NAME)).await;
    for name in ["customer", "deleted", "unchanged"] {
        pair.operations[0]
            .put_item(PutItemRequest::new(
                TableName::new(fixture::NAME),
                item(name, "before-capture"),
            ))
            .await
            .unwrap();
    }
    pair.operations[0].update_table(serde_json::from_value(serde_json::json!({"TableName": fixture::NAME, "ReplicaUpdates": [{"Create": {"RegionName": "region-2"}}]})).unwrap()).await.unwrap();
    let journal = Arc::clone(&pair.journals[0]);
    let plan = Arc::new(
        Arc::clone(&journal)
            .with_admitted_attempt(
                (
                    &source,
                    storage_provider::native_admission::NativeWork::Recovery,
                ),
                crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                move |attempt, _| {
                    Box::pin(async move { journal.select_pending_enable(attempt).await })
                },
            )
            .await
            .unwrap()
            .unwrap(),
    );
    let reserved = bootstrap::reserve_hidden_target(
        &destination,
        Arc::clone(&pair.journals[1]),
        Arc::new(plan.targets[0].clone()),
    )
    .await
    .unwrap();
    let journal = Arc::clone(&pair.journals[0]);
    let record = Arc::clone(&journal)
        .with_admitted_attempt(
            (
                &source,
                storage_provider::native_admission::NativeWork::Recovery,
            ),
            crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
            move |attempt, _| {
                Box::pin(async move {
                    journal
                        .execute_enable(
                            attempt,
                            &plan,
                            &crate::enable::EnableChange::Reserve(Box::new(reserved.proposed())),
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    let proposal = Arc::new(record.proposal.unwrap());
    bootstrap::begin_source_capture(
        &source,
        Arc::clone(&pair.journals[0]),
        Arc::clone(&proposal),
    )
    .await
    .unwrap();
    pair.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            item("customer", "inside-cut"),
        ))
        .await
        .unwrap();
    pair.operations[0]
        .delete_item(storage_types::DeleteItemRequest::new(
            TableName::new(fixture::NAME),
            key("deleted"),
        ))
        .await
        .unwrap();
    let mut ready = false;
    for _ in 0..512 {
        if bootstrap::advance_source_capture(
            &source,
            Arc::clone(&pair.journals[0]),
            Arc::clone(&proposal),
            (fixture::codec(), fixture::limits()),
        )
        .await
        .unwrap()
            == bootstrap::SourceCaptureStatus::BaselineReady
        {
            ready = true;
            break;
        }
    }
    assert!(
        ready,
        "native capture did not freeze complete scan/tail authority"
    );
    let baselines = bootstrap::read_source_capture_baselines(
        &source,
        Arc::clone(&pair.journals[0]),
        Arc::clone(&proposal),
        (None, fixture::codec()),
    )
    .await
    .unwrap();
    assert!(baselines.continuation.is_none());
    assert_eq!(
        baselines.authorities.len(),
        3,
        "scan lost a deleted key or invented a baseline"
    );
    for baseline in &baselines.authorities {
        let name = match baseline.item().key.get("pk").unwrap() {
            AttributeValue::S(name) => name,
            _ => panic!("unexpected key"),
        };
        assert_eq!(
            baseline
                .item()
                .images
                .new
                .as_ref()
                .unwrap()
                .to_attribute_map()
                .unwrap(),
            item(name, "before-capture").into_attribute_map().unwrap()
        );
    }
    let state = replication_provider::control::source_capture_state(&proposal).unwrap();
    let partition = fixture::partitions().select(
        state.configuration.lineage,
        &fixture::codec().encode_key(&key("customer")).unwrap(),
    );
    let cut = bootstrap::read_source_capture_cut(
        &source,
        Arc::clone(&pair.journals[0]),
        Arc::clone(&proposal),
        u32::from(u16::from(partition)),
    )
    .await
    .unwrap();
    for name in ["customer", "deleted", "new-after-cut"] {
        pair.operations[0]
            .put_item(PutItemRequest::new(
                TableName::new(fixture::NAME),
                item(name, "beyond-cut"),
            ))
            .await
            .unwrap();
    }
    let frozen = bootstrap::read_source_capture_cut(
        &source,
        Arc::clone(&pair.journals[0]),
        Arc::clone(&proposal),
        u32::from(u16::from(partition)),
    )
    .await
    .unwrap();
    assert_eq!(
        frozen.through, cut.through,
        "live writes moved a declared bootstrap cut"
    );
    let repeated = bootstrap::read_source_capture_baselines(
        &source,
        Arc::clone(&pair.journals[0]),
        Arc::clone(&proposal),
        (None, fixture::codec()),
    )
    .await
    .unwrap();
    assert_eq!(
        repeated
            .authorities
            .iter()
            .map(|baseline| baseline.bytes())
            .collect::<Vec<_>>(),
        baselines
            .authorities
            .iter()
            .map(|baseline| baseline.bytes())
            .collect::<Vec<_>>(),
        "current beyond-cut rows replaced retained baseline images"
    );
    let hidden = pair.operations[1]
        .get_item(GetItemRequest::new(
            TableName::new(fixture::NAME),
            key("customer"),
        ))
        .await
        .err()
        .unwrap();
    assert!(matches!(
        hidden.as_ref(),
        storage_types::StorageEnum::TableNotFound { .. }
            | storage_types::StorageEnum::ResourceNotFound { .. }
    ));
    let journal = Arc::clone(&pair.journals[0]);
    let originals = source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let mut originals = Vec::new();
                for index in 0..64u16 {
                    let partition = index.into();
                    let through = journal.load_source_head(attempt, partition).await?;
                    if u64::from(through) == 0 {
                        continue;
                    }
                    let request = crate::source_frame::SourceFrameRequest {
                        origin: replication_types::identity::origin::OriginId {
                            region: fixture::region(1),
                            database_group: [1; 16].try_into().unwrap(),
                            journal_generation: 1.into(),
                            journal_partition: partition,
                        },
                        range: (0.into(), through).try_into().unwrap(),
                        membership: state.membership,
                        tables: vec![(state.configuration.table, state.configuration.lineage)],
                    };
                    originals.extend(
                        crate::source_frame::load_source_frame(
                            &journal,
                            attempt,
                            (&request, &fixture::limits()),
                            &fixture::codec(),
                        )
                        .await
                        .unwrap()
                        .events()
                        .iter()
                        .cloned(),
                    );
                }
                Ok(originals)
            })
        })
        .await
        .unwrap();
    assert_eq!(
        originals.len(),
        5,
        "bootstrap invented pre-enable origin events"
    );
    let mut activated = false;
    for _ in 0..512 {
        pair.drive_once().await;
        activated = pair.operations[1]
            .describe_table(storage_types::DescribeTableRequest {
                table_name: TableName::new(fixture::NAME),
            })
            .await
            .is_ok_and(|response| {
                response.table.table_status == storage_types::TableStatus::Active
                    && response.table.aux_configuration_primary_region.as_deref()
                        == Some("region-1")
            });
        if activated && require_items(&destination).await {
            break;
        }
    }
    assert!(
        activated && require_items(&destination).await,
        "complete frozen image and subsequent delivery did not activate/converge"
    );
    pair.shutdown().await;
    drop(source);
    drop(destination);
    for path in [&source_path, &destination_path] {
        let store = fixture::reopen(path).await;
        assert!(require_items(&store).await);
        let journal = fixture::journal(if path == &source_path { 1 } else { 2 });
        let baselines = baselines
            .authorities
            .iter()
            .map(|baseline| {
                (
                    VersionIdentity::Baseline {
                        lineage: baseline.item().lineage,
                        key: baseline.item().canonical_key.clone(),
                    },
                    baseline.bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>();
        let originals = originals.clone();
        store
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    for (identity, bytes) in baselines {
                        assert_eq!(
                            journal
                                .load_baseline(attempt, &identity, &fixture::codec())
                                .await?
                                .unwrap()
                                .bytes(),
                            bytes
                        );
                    }
                    for original in originals {
                        assert_eq!(
                            journal
                                .load_acceptance(attempt, &original.as_ref().id, &fixture::codec())
                                .await?
                                .unwrap()
                                .event
                                .as_ref()
                                .bytes,
                            original.as_ref().bytes
                        );
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}

fn key(name: &str) -> KeyAttributes {
    KeyAttributes::from([("pk".into(), AttributeValue::S(name.into()))])
}

fn item(name: &str, value: &str) -> WireItem {
    WireItem::try_from(
        &[
            ("pk".into(), AttributeValue::S(name.into())),
            ("value".into(), AttributeValue::S(value.into())),
        ]
        .into_iter()
        .collect::<storage_types::AttributeMap>(),
    )
    .unwrap()
}

async fn require_items(store: &fixture::Store) -> bool {
    for (name, value) in [
        ("customer", "beyond-cut"),
        ("deleted", "beyond-cut"),
        ("new-after-cut", "beyond-cut"),
        ("unchanged", "before-capture"),
    ] {
        let observed = store
            .get_item(GetItemRequest::new(
                TableName::new(fixture::NAME),
                key(name),
            ))
            .await
            .unwrap()
            .item;
        if observed.map(|item| item.into_attribute_map().unwrap())
            != Some(item(name, value).into_attribute_map().unwrap())
        {
            return false;
        }
    }
    true
}

#[tokio::test]
async fn given_held_replica_addition_when_trim_is_requested_then_capture_keeps_exact_history_and_finishes_before_trim_admission()
 {
    use crate::{
        history_capture::{
            HistoryCaptureRequest, HistoryCaptureStatus, advance_history_capture,
            load_history_capture_cuts, observe_history_capture,
        },
        trim::{
            executor::{TrimRequest, execute_trim},
            record::TrimDecision,
        },
    };
    let directory = tempfile::tempdir().unwrap();
    let paths = (1..=3)
        .map(|index| directory.path().join(format!("region-{index}.sqlite")))
        .collect::<Vec<_>>();
    let source = fixture::open(&paths[0]).await;
    let destination = fixture::open(&paths[1]).await;
    let joining = fixture::open(&paths[2]).await;
    let stores = [&source, &destination, &joining];
    let cluster = fixture::NativeCluster::start(&stores).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("before-addition"),
        ))
        .await
        .unwrap();
    for _ in 0..512 {
        cluster.drive_once().await;
        if destination
            .get_item(GetItemRequest::new(
                TableName::new(fixture::NAME),
                fixture::key(),
            ))
            .await
            .unwrap()
            .item
            .is_some_and(|observed| {
                observed.into_attribute_map().unwrap()
                    == fixture::item("before-addition")
                        .into_attribute_map()
                        .unwrap()
            })
        {
            break;
        }
    }
    fixture::assert_item(&destination, Some("before-addition")).await;
    let original = fixture::frame(&source, 1, 0, 1).await;
    let replication_types::event::content::CanonicalPayload::Item(first) =
        &original.events()[0].as_ref().payload
    else {
        panic!("expected item original");
    };
    assert_eq!(first.parents.len(), 1);
    let configuration_id = first.parents[0].clone();
    let journal = Arc::clone(&cluster.journals[0]);
    let configuration_original = source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                Ok(journal
                    .load_acceptance(attempt, &configuration_id, &fixture::codec())
                    .await?
                    .unwrap()
                    .event)
            })
        })
        .await
        .unwrap();
    let replication_types::event::content::CanonicalPayload::System(configuration) =
        &configuration_original.as_ref().payload
    else {
        panic!("first original omitted its configuration ancestor");
    };
    assert!(matches!(
        configuration.proposal.operation().change,
        replication_types::configuration::control::ConfigurationChange::EnableGlobal { .. }
    ));
    let removal = replication_provider::control::bind_removal(
        [211; 16].try_into().unwrap(),
        &configuration.proposal,
        &[fixture::region(2)],
    )
    .unwrap();
    let seal = crate::lifecycle::SealRequest::try_new(removal, fixture::region(2)).unwrap();
    crate::transport::test_support::measure_seal_request(&seal);
    cluster.operations[1]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("before-addition-from-peer"),
        ))
        .await
        .unwrap();
    for _ in 0..512 {
        cluster.drive_once().await;
        if source
            .get_item(GetItemRequest::new(
                TableName::new(fixture::NAME),
                fixture::key(),
            ))
            .await
            .unwrap()
            .item
            .is_some_and(|observed| {
                observed.into_attribute_map().unwrap()
                    == fixture::item("before-addition-from-peer")
                        .into_attribute_map()
                        .unwrap()
            })
        {
            break;
        }
    }
    fixture::assert_item(&source, Some("before-addition-from-peer")).await;
    let peer_original = fixture::frame(&destination, 2, 0, 1).await;
    let trim = crate::trim::native_trim_tests::capture_proposal(&stores[..2]).await;
    trim.validate().unwrap();
    cluster.operations[0].update_table(serde_json::from_value(serde_json::json!({"TableName": fixture::NAME, "ReplicaUpdates": [{"Create": {"RegionName": "region-3"}}]})).unwrap()).await.unwrap();
    let journal = Arc::clone(&cluster.journals[0]);
    let plan = Arc::new(
        Arc::clone(&journal)
            .with_admitted_attempt(
                (
                    &source,
                    storage_provider::native_admission::NativeWork::Recovery,
                ),
                crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                move |attempt, _| {
                    Box::pin(async move { journal.select_pending_addition(attempt).await })
                },
            )
            .await
            .unwrap()
            .unwrap(),
    );
    let reserved = bootstrap::reserve_hidden_target(
        &joining,
        Arc::clone(&cluster.journals[2]),
        Arc::new(plan.target().clone()),
    )
    .await
    .unwrap();
    let journal = Arc::clone(&cluster.journals[0]);
    let record = Arc::clone(&journal)
        .with_admitted_attempt(
            (
                &source,
                storage_provider::native_admission::NativeWork::Recovery,
            ),
            crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
            move |attempt, _| {
                Box::pin(async move {
                    journal
                        .execute_addition(
                            attempt,
                            &plan,
                            &crate::addition::AdditionChange::Reserve(Box::new(
                                reserved.proposed(),
                            )),
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    let proposal = record.proposal.unwrap();
    let mut captures = Vec::new();
    for (offset, store) in stores[..2].iter().enumerate() {
        let request = Arc::new(HistoryCaptureRequest {
            proposal: proposal.clone(),
            region: fixture::region(u8::try_from(offset + 1).unwrap()),
        });
        crate::transport::test_support::measure_history_request(&request);
        let mut ready = false;
        for _ in 0..512 {
            if advance_history_capture(
                *store,
                Arc::clone(&cluster.journals[offset]),
                Arc::clone(&request),
            )
            .await
            .unwrap()
                == HistoryCaptureStatus::Ready
            {
                ready = true;
                break;
            }
        }
        assert!(
            ready,
            "real addition failed to retain its complete source history"
        );
        captures.push(request);
    }
    let before = load_history_capture_cuts(
        &source,
        Arc::clone(&cluster.journals[0]),
        Arc::clone(&captures[0]),
        0,
    )
    .await
    .unwrap();
    assert_eq!(before.cuts.len(), 64);
    let error = execute_trim(
        &source,
        Arc::clone(&cluster.journals[0]),
        Arc::new(TrimRequest::Prepare(Arc::clone(&trim))),
        (fixture::limits(), fixture::codec()),
    )
    .await
    .err()
    .expect("trim stole an active capture slot");
    assert!(
        matches!(error.as_ref(), storage_types::StorageEnum::Unsupported { message } if message == "Table control is in progress")
    );
    assert!(
        execute_trim(
            &source,
            Arc::clone(&cluster.journals[0]),
            Arc::new(TrimRequest::Observe(trim)),
            (fixture::limits(), fixture::codec())
        )
        .await
        .unwrap()
        .observation
        .is_none()
    );
    assert_eq!(
        observe_history_capture(
            &source,
            Arc::clone(&cluster.journals[0]),
            Arc::clone(&captures[0])
        )
        .await
        .unwrap(),
        Some(HistoryCaptureStatus::Ready)
    );
    let after = load_history_capture_cuts(
        &source,
        Arc::clone(&cluster.journals[0]),
        Arc::clone(&captures[0]),
        0,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_vec(&before).unwrap(),
        serde_json::to_vec(&after).unwrap()
    );
    let retained = fixture::frame(&source, 1, 0, 1).await;
    assert_eq!(
        retained.events()[0].as_ref().id,
        original.events()[0].as_ref().id
    );
    assert_eq!(
        retained.events()[0].as_ref().bytes,
        original.events()[0].as_ref().bytes
    );
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("during-held-addition"),
        ))
        .await
        .unwrap();
    let child = fixture::frame(&source, 1, 1, 2).await;
    let root = child.events()[0].clone();
    let replication_types::event::content::CanonicalPayload::Item(item) = &root.as_ref().payload
    else {
        panic!("expected dependent item original");
    };
    assert!(
        matches!(&item.predecessor, Some(VersionIdentity::Mutation(id)) if replication_types::event::identity::CanonicalEventId::Origin(id.clone()) == peer_original.events()[0].as_ref().id)
    );
    assert!(
        cluster.operations[2]
            .get_item(GetItemRequest::new(
                TableName::new(fixture::NAME),
                fixture::key()
            ))
            .await
            .is_err()
    );
    let mut complete = false;
    for _ in 0..512 {
        cluster.drive_once().await;
        complete = observe_history_capture(
            &source,
            Arc::clone(&cluster.journals[0]),
            Arc::clone(&captures[0]),
        )
        .await
        .unwrap()
            == Some(HistoryCaptureStatus::Released)
            && joining
                .get_item(GetItemRequest::new(
                    TableName::new(fixture::NAME),
                    fixture::key(),
                ))
                .await
                .is_ok_and(|response| {
                    response.item.is_some_and(|observed| {
                        observed.into_attribute_map().unwrap()
                            == fixture::item("during-held-addition")
                                .into_attribute_map()
                                .unwrap()
                    })
                });
        if complete {
            break;
        }
    }
    assert!(
        complete,
        "rejected trim damaged capture, activation or subsequent delivery"
    );
    let originals = [
        configuration_original,
        original.events()[0].clone(),
        peer_original.events()[0].clone(),
        root.clone(),
    ];
    for (offset, store) in stores.iter().enumerate() {
        require_originals(store, u8::try_from(offset + 1).unwrap(), &originals).await;
    }
    let checkpoint =
        crate::history::checkpoint::CanonicalCheckpointId::try_from(uuid::Uuid::new_v4()).unwrap();
    for (offset, store) in stores.iter().enumerate() {
        let index = u8::try_from(offset + 1).unwrap();
        let state = fixture::installed_state(store, index, fixture::NAME).await;
        drop(cluster.history[offset].portable_checkpoint(crate::history::portable_checkpoint::PortableCheckpointRequest {
            serving_region: fixture::region(index), serving_group: [index; 16].try_into().unwrap(), table: TableName::new(fixture::NAME), lineage: state.configuration.lineage, membership: state.membership,
            portable_checkpoint: checkpoint, action: crate::history::portable_checkpoint::PortableCheckpointAction::CreateCapture, retention: crate::history::retention::CheckpointRetention::ExplicitRelease,
        }).await.unwrap().unwrap());
    }
    let causal = require_causal_equality(&cluster, &stores, checkpoint, &root, &originals).await;
    let trim = crate::trim::native_trim_tests::capture_proposal(&stores).await;
    let prepared = execute_trim(
        &source,
        Arc::clone(&cluster.journals[0]),
        Arc::new(TrimRequest::Prepare(Arc::clone(&trim))),
        (fixture::limits(), fixture::codec()),
    )
    .await
    .unwrap();
    assert!(matches!(
        prepared.observation.unwrap().decision,
        TrimDecision::Preparing
    ));
    execute_trim(
        &source,
        Arc::clone(&cluster.journals[0]),
        Arc::new(TrimRequest::Cancel(trim)),
        (fixture::limits(), fixture::codec()),
    )
    .await
    .unwrap();
    cluster.shutdown().await;
    drop(source);
    drop(destination);
    drop(joining);
    let reopened = futures::future::join_all(paths.iter().map(|path| fixture::open(path))).await;
    let stores = reopened.iter().collect::<Vec<_>>();
    let cluster = fixture::NativeCluster::start(&stores).await;
    for (offset, store) in stores.iter().enumerate() {
        fixture::assert_item(store, Some("during-held-addition")).await;
        require_originals(store, u8::try_from(offset + 1).unwrap(), &originals).await;
    }
    assert_eq!(
        require_causal_equality(&cluster, &stores, checkpoint, &root, &originals).await,
        causal,
        "reopen changed persisted causal closure"
    );
    cluster.shutdown().await;
}

async fn require_originals(
    store: &fixture::Store,
    index: u8,
    originals: &[replication_types::event::content::CanonicalEvent],
) {
    let originals = originals.to_vec();
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let journal = fixture::journal(index);
                for original in originals {
                    let accepted = journal
                        .load_acceptance(attempt, &original.as_ref().id, &fixture::codec())
                        .await?
                        .expect("joining/reopened replica lost a captured original");
                    assert_eq!(accepted.event.as_ref().bytes, original.as_ref().bytes);
                }
                Ok(())
            })
        })
        .await
        .unwrap();
}

async fn require_causal_equality(
    cluster: &fixture::NativeCluster,
    stores: &[&fixture::Store],
    checkpoint: crate::history::checkpoint::CanonicalCheckpointId,
    root: &replication_types::event::content::CanonicalEvent,
    expected: &[replication_types::event::content::CanonicalEvent],
) -> Vec<String> {
    let mut canonical = None;
    for (offset, store) in stores.iter().enumerate() {
        let index = u8::try_from(offset + 1).unwrap();
        let state = fixture::installed_state(store, index, fixture::NAME).await;
        let page = cluster.history[offset]
            .causal_read(crate::history::causal_read::CanonicalCausalReadRequest {
                serving_region: fixture::region(index),
                serving_group: [index; 16].try_into().unwrap(),
                table: TableName::new(fixture::NAME),
                lineage: state.configuration.lineage,
                membership: state.membership,
                checkpoint,
                causal_history: root.as_ref().id.clone(),
                ancestry_scopes: Default::default(),
            })
            .await
            .unwrap();
        assert_eq!(page.root, root.as_ref().id);
        let events = page
            .originals
            .iter()
            .map(|bytes| fixture::codec().decode_event(bytes.as_bytes()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            events.len(),
            expected.len(),
            "causal closure duplicated or invented an original"
        );
        for (event, original) in events.iter().zip(expected) {
            assert_eq!(
                event.as_ref().id,
                original.as_ref().id,
                "causal closure changed parent-before-child order"
            );
            assert_eq!(event.as_ref().bytes, original.as_ref().bytes);
        }
        if let Some(previous) = &canonical {
            assert_eq!(
                &page.originals, previous,
                "regions disagreed on exact causal closure"
            );
        }
        canonical = Some(page.originals.clone());
        drop(page);
    }
    canonical.unwrap()
}
