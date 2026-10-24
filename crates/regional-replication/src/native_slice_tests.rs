use std::sync::Arc;

use replication_contract_tests::native::commit_fault::{NativeCommitBoundary, NativeCommitFault};
use replication_types::regional_import::RegionalImport;
use storage_provider::{DynamoOperations, PutItemShape};
use storage_types::{GetItemRequest, PutItemRequest, TableName, context::WrappedError};

use crate::{
    import::{ImportExecution, execute_import},
    import_receipt::{ImportReceiptObservation, reconcile_import_pages},
    origin::{OriginExecution, execute_item_write},
    test_support as fixture,
    test_support::{assert_item, delete, deliver, frame, put},
};

#[tokio::test]
async fn given_captured_enable_when_cancellation_loses_ack_and_names_are_reused_then_jobs_recover_without_deleting_new_incarnations()
 {
    use replication_types::event::content::CanonicalEventCodec;
    use storage_provider::DynamoPort;
    use storage_types::{AttemptMode, StorageEnum};
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("cancel-source.sqlite");
    let destination_path = directory.path().join("cancel-destination.sqlite");
    let lost_ack = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let source = fixture::open_with_probe(&source_path, Some(lost_ack.clone())).await;
    let destination = fixture::open(&destination_path).await;
    let pair = fixture::NativeCluster::start(&[&source, &destination]).await;
    fixture::create_table(&source, &TableName::new(fixture::NAME)).await;
    pair.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("before-capture"),
        ))
        .await
        .unwrap();
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
    let target = Arc::new(plan.targets[0].clone());
    let reservation = crate::bootstrap::reserve_hidden_target(
        &destination,
        Arc::clone(&pair.journals[1]),
        Arc::clone(&target),
    )
    .await
    .unwrap();
    let duplicate = crate::bootstrap::reserve_hidden_target(
        &destination,
        Arc::clone(&pair.journals[1]),
        Arc::clone(&target),
    )
    .await
    .unwrap();
    assert_eq!(duplicate.proposed(), reservation.proposed());
    let mut changed = target.as_ref().clone();
    changed.schema.table.created_at = storage_types::TimestampMillis::from_timestamp(
        changed.schema.table.created_at.timestamp_millis() + 1,
    );
    changed.schema.validate(&changed.target.name).unwrap();
    let mismatch = crate::bootstrap::reserve_hidden_target(
        &destination,
        Arc::clone(&pair.journals[1]),
        Arc::new(changed),
    )
    .await
    .err()
    .unwrap();
    assert!(
        !mismatch.is_commit_unknown()
            && matches!(
                mismatch.as_ref(),
                StorageEnum::InfrastructureResult(
                    storage_types::InfrastructureResultKind::Encoding {
                        context: storage_types::InfrastructureContext::PublicationHistory
                    }
                )
            )
    );
    let reserved = reservation.proposed();
    let record = observe_enable(
        &source,
        Arc::clone(&plan),
        crate::enable::EnableChange::Reserve(Box::new(reserved.clone())),
    )
    .await;
    let proposal = Arc::new(record.proposal.unwrap());
    assert!(
        observe_enable(
            &source,
            Arc::clone(&plan),
            crate::enable::EnableChange::Reserve(Box::new(reserved))
        )
        .await
        .proposal
        .unwrap()
            == *proposal
    );
    crate::bootstrap::begin_source_capture(
        &source,
        Arc::clone(&pair.journals[0]),
        Arc::clone(&proposal),
    )
    .await
    .unwrap();
    pair.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("accepted-during-capture"),
        ))
        .await
        .unwrap();
    let hidden = pair.operations[1]
        .get_item(GetItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::key(),
        ))
        .await
        .err()
        .unwrap();
    assert!(matches!(
        hidden.as_ref(),
        StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
    ));
    let state = replication_provider::control::source_capture_state(&proposal).unwrap();
    let binding = state
        .sources
        .iter()
        .find(|binding| binding.region == fixture::region(1))
        .unwrap()
        .clone();
    let partition = binding.partitions.select(
        state.configuration.lineage,
        &fixture::codec().encode_key(&fixture::key()).unwrap(),
    );
    let selection = crate::history::source_selection::CanonicalSourceSelection {
        serving_region: fixture::region(1),
        serving_group: [1; 16].try_into().unwrap(),
        table: TableName::new(fixture::NAME),
        lineage: state.configuration.lineage,
        membership: state.membership,
        source: binding.clone(),
        origin: replication_types::identity::origin::OriginId {
            region: binding.region,
            database_group: binding.database_group,
            journal_generation: binding.generation,
            journal_partition: partition,
        },
    };
    let accepted = pair.history[0]
        .read(crate::history::read::CanonicalHistoryRequest {
            selection,
            range: (0.into(), 1.into()).try_into().unwrap(),
            cut: None,
        })
        .await
        .unwrap();
    assert_eq!(accepted.originals.len(), 1);
    let event = fixture::codec()
        .decode_event(accepted.originals[0].as_bytes())
        .unwrap();
    drop(accepted);
    lost_ack.arm();
    let unknown = crate::enable::cancel_enable_plan(
        &source,
        Arc::clone(&pair.journals[0]),
        Arc::clone(&plan),
    )
    .await
    .err()
    .unwrap();
    assert!(lost_ack.was_observed() && unknown.is_commit_unknown());
    pair.shutdown().await;
    drop(source);
    drop(destination);
    let source = fixture::open(&source_path).await;
    let destination = fixture::open(&destination_path).await;
    let pair = fixture::NativeCluster::start(&[&source, &destination]).await;
    assert!(matches!(
        observe_enable(
            &source,
            Arc::clone(&plan),
            crate::enable::EnableChange::Observe
        )
        .await
        .phase,
        crate::enable::EnablePhase::Cancelling(_)
    ));
    crate::enable::cancel_enable_plan(&source, Arc::clone(&pair.journals[0]), Arc::clone(&plan))
        .await
        .unwrap();
    let mut cancelled = false;
    for _ in 0..512 {
        pair.drive_once().await;
        let phase = observe_enable(
            &source,
            Arc::clone(&plan),
            crate::enable::EnableChange::Observe,
        )
        .await
        .phase;
        if phase == crate::enable::EnablePhase::Cancelled {
            cancelled = true;
            break;
        }
    }
    assert!(
        cancelled,
        "registered jobs did not finish canceled activation"
    );
    assert!(
        crate::bootstrap::reconcile_hidden_cancellation(
            &destination,
            Arc::clone(&pair.journals[1]),
            Arc::clone(&target)
        )
        .await
        .unwrap()
    );
    let hidden = pair.operations[1]
        .get_item(GetItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::key(),
        ))
        .await
        .err()
        .unwrap();
    assert!(matches!(
        hidden.as_ref(),
        StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }
    ));
    let description = pair.operations[0]
        .describe_table(storage_types::DescribeTableRequest {
            table_name: TableName::new(fixture::NAME),
        })
        .await
        .unwrap();
    assert_eq!(
        description.table.table_status,
        storage_types::TableStatus::Active
    );
    assert!(description.table.aux_configuration_primary_region.is_none());
    fixture::assert_item(&source, Some("accepted-during-capture")).await;
    let journal = Arc::clone(&pair.journals[0]);
    source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let retained = journal
                    .load_acceptance(attempt, &event.as_ref().id, &fixture::codec())
                    .await?
                    .unwrap();
                assert_eq!(retained.event.as_ref().bytes, event.as_ref().bytes);
                Ok(())
            })
        })
        .await
        .unwrap();
    // Ordinary deletion reserves the name briefly; reuse follows that existing
    // contract.
    let reusable = destination.clone().with_clock(Arc::new(fixture::FixedClock(
        storage_provider::name_locked_until(storage_types::Clock::now(&destination)),
    )));
    fixture::create_table(&reusable, &TableName::new(fixture::NAME)).await;
    pair.operations[1]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("new-incarnation"),
        ))
        .await
        .unwrap();
    assert!(
        crate::bootstrap::reserve_hidden_target(
            &destination,
            Arc::clone(&pair.journals[1]),
            Arc::clone(&target)
        )
        .await
        .is_err()
    );
    crate::bootstrap::cancel_hidden_target(
        &destination,
        Arc::clone(&pair.journals[1]),
        Arc::clone(&target),
    )
    .await
    .unwrap();
    crate::enable::cancel_enable_plan(&source, Arc::clone(&pair.journals[0]), Arc::clone(&plan))
        .await
        .unwrap();
    fixture::assert_item(&destination, Some("new-incarnation")).await;
    pair.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("ordinary-after-cancel"),
        ))
        .await
        .unwrap();
    pair.shutdown().await;
    drop(source);
    drop(destination);
    let source = fixture::reopen(&source_path).await;
    let destination = fixture::reopen(&destination_path).await;
    fixture::assert_item(&source, Some("ordinary-after-cancel")).await;
    fixture::assert_item(&destination, Some("new-incarnation")).await;
}

async fn observe_enable(
    store: &fixture::Store,
    plan: Arc<crate::enable::EnablePlan>,
    change: crate::enable::EnableChange,
) -> crate::enable::EnableRecord {
    let journal = fixture::journal(1);
    Arc::clone(&journal)
        .with_admitted_attempt(
            (
                store,
                storage_provider::native_admission::NativeWork::Recovery,
            ),
            crate::journal::NativeAttempt::Items(storage_types::AttemptMode::ReadWrite),
            move |attempt, _| {
                Box::pin(async move { journal.execute_enable(attempt, &plan, &change).await })
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn given_installed_regions_when_import_aborts_or_loses_native_ack_then_atomic_items_and_exact_receipts_survive_restart()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination_path = directory.path().join("destination.sqlite");
    let before_commit = Arc::new(NativeCommitFault::new(NativeCommitBoundary::BeforeCommit));
    let destination =
        fixture::open_with_probe(&destination_path, Some(before_commit.clone())).await;
    fixture::install_pair(&source, &destination).await;
    put(&source, 1, "first").await;
    reject_condition(&source).await;
    put(&source, 1, "second").await;
    put(&source, 1, "second").await;
    let import = frame(&source, 1, 0, 3).await;
    assert_eq!(
        import.events().len(),
        3,
        "same-image writes retain original events"
    );
    for fault in [
        crate::journal::record_fault_test_support::TableFault::MissingSchema,
        crate::journal::record_fault_test_support::TableFault::IncarnationMismatch,
    ] {
        let original = crate::journal::record_fault_test_support::damage_table(
            &destination,
            &fixture::journal(2),
            import.bindings()[0].local_table,
            fault,
        )
        .await;
        let error = execute_import(
            &destination,
            fixture::journal(2),
            ImportExecution {
                import: fixture::bounded_import(Arc::clone(&import)),
                codec: fixture::codec(),
            },
        )
        .await
        .err()
        .expect("unavailable schema or stale native incarnation acknowledged an import");
        use storage_types::{InfrastructureContext as C, InfrastructureResultKind as K};
        assert!(
            matches!(
                (fault, error.as_ref()),
                (
                    crate::journal::record_fault_test_support::TableFault::MissingSchema,
                    storage_types::StorageEnum::InfrastructureResult(K::Unavailable {
                        context: C::RegionalSchema
                    })
                ) | (
                    crate::journal::record_fault_test_support::TableFault::IncarnationMismatch,
                    storage_types::StorageEnum::InfrastructureResult(K::Encoding {
                        context: C::PublicationHistory
                    })
                )
            ),
            "{fault:?}: {error:?}"
        );
        assert!(!error.is_commit_unknown());
        assert_item(&destination, None).await;
        crate::journal::record_fault_test_support::restore(
            &destination,
            &fixture::journal(2),
            original,
        )
        .await;
        assert!(matches!(
            reconcile_import_pages(
                &destination,
                fixture::journal(2),
                (
                    fixture::bounded_import(Arc::clone(&import)),
                    fixture::codec()
                )
            )
            .await
            .unwrap(),
            ImportReceiptObservation::Unresolved
        ));
    }
    before_commit.arm();
    let error = execute_import(
        &destination,
        fixture::journal(2),
        ImportExecution {
            import: fixture::bounded_import(Arc::clone(&import)),
            codec: fixture::codec(),
        },
    )
    .await
    .err()
    .expect("pre-commit interruption must not acknowledge import");
    assert!(before_commit.was_observed());
    assert!(!error.is_commit_unknown(), "proven rollback became unknown");
    assert_item(&destination, None).await;
    assert!(matches!(
        reconcile_import_pages(
            &destination,
            fixture::journal(2),
            (
                fixture::bounded_import(Arc::clone(&import)),
                fixture::codec()
            )
        )
        .await
        .unwrap(),
        ImportReceiptObservation::Unresolved
    ));
    drop(destination);
    let after_commit = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let destination = fixture::open_with_probe(&destination_path, Some(after_commit.clone())).await;
    storage_provider::native_admission::NativeAttemptSource::bind_work_limits(
        &destination,
        fixture::budgets().attempt,
    )
    .unwrap();
    after_commit.arm();
    let error = execute_import(
        &destination,
        fixture::journal(2),
        ImportExecution {
            import: fixture::bounded_import(Arc::clone(&import)),
            codec: fixture::codec(),
        },
    )
    .await
    .err()
    .expect("lost native acknowledgement must preserve an unknown outcome");
    assert!(after_commit.was_observed());
    assert!(
        error.is_commit_unknown(),
        "lost durable acknowledgement became an abort"
    );
    let ImportReceiptObservation::Durable(committed) = reconcile_import_pages(
        &destination,
        fixture::journal(2),
        (
            fixture::bounded_import(Arc::clone(&import)),
            fixture::codec(),
        ),
    )
    .await
    .unwrap() else {
        panic!("native committed import lost its exact receipt");
    };
    assert_eq!(committed.pages()[0].events.len(), 3);
    assert_eq!(
        deliver(&destination, 2, Arc::clone(&import)).await.pages(),
        committed.pages()
    );
    assert_item(&destination, Some("second")).await;
    drop(destination);
    let destination = fixture::reopen(&destination_path).await;
    assert_item(&destination, Some("second")).await;
    match reconcile_import_pages(
        &destination,
        fixture::journal(2),
        (
            fixture::bounded_import(Arc::clone(&import)),
            fixture::codec(),
        ),
    )
    .await
    .unwrap()
    {
        ImportReceiptObservation::Durable(receipt) => {
            assert_eq!(receipt.pages(), committed.pages())
        }
        ImportReceiptObservation::Unresolved => {
            panic!("committed receipt disappeared after restart")
        }
    }
    // A receiving member relays persisted originals through the native
    // retained-source path. Selection reads only the receiver; it does not call
    // the publishing store.
    use storage_provider::DynamoPort;
    let request = crate::source_frame::SourceFrameRequest {
        origin: import.manifest().origin.clone(),
        range: import.manifest().range,
        membership: import.manifest().membership,
        tables: vec![(
            import.bindings()[0].source_table,
            import.bindings()[0].lineage,
        )],
    };
    let relayed = destination
        .with_dynamo_attempt(storage_types::AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                Ok(crate::source_frame::load_source_frame(
                    &fixture::journal(2),
                    attempt,
                    (&request, &fixture::limits()),
                    &fixture::codec(),
                )
                .await
                .unwrap())
            })
        })
        .await
        .unwrap();
    assert_eq!(relayed.events().len(), import.events().len());
    for (retained, original) in relayed.events().iter().zip(import.events()) {
        assert_eq!(retained.as_ref().id, original.as_ref().id);
        assert_eq!(retained.as_ref().bytes, original.as_ref().bytes);
    }
    let relay = Arc::new(
        RegionalImport::try_new(
            (
                import.bindings().to_vec(),
                relayed.manifest().clone(),
                relayed.events().to_vec(),
            ),
            &*fixture::codec(),
        )
        .unwrap(),
    );
    deliver(&destination, 2, relay).await;
    assert_item(&destination, Some("second")).await;
    delete(&source, 1).await;
    delete(&source, 1).await;
    let deletion = frame(&source, 1, 3, 5).await;
    assert_eq!(
        deletion.events().len(),
        2,
        "absent deletes retain original events"
    );
    deliver(&destination, 2, deletion).await;
    assert_item(&destination, None).await;
    deliver(&destination, 2, import).await;
    assert_item(&destination, None).await;
}

#[tokio::test]
async fn given_received_numeric_spellings_when_imported_and_reopened_then_normalized_winners_keep_exact_predecessors()
 {
    use replication_types::{
        event::{content::CanonicalPayload, interval::IntervalCodec},
        protocol::CanonicalDigest,
    };
    use serde_json::json;
    use storage_types::WireItem;

    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("numeric-source.sqlite")).await;
    let destination_path = directory.path().join("numeric-destination.sqlite");
    let destination = fixture::open(&destination_path).await;
    fixture::install_pair(&source, &destination).await;
    put(&source, 1, "seed").await;
    let initial = frame(&source, 1, 0, 1).await;
    let original = initial.events()[0].as_ref();
    // A legal peer can author a different spelling than local write
    // normalization.
    let received_image = json!({
        "pk": {"S": "customer"}, "value": {"S": "seed"},
        "number": {"N": "1e3"},
        "padded": {"N": format!("{}1", "0".repeat(170))},
        "padded_exponent": {"N": format!("1e{}3", "0".repeat(168))},
        "long": {"N": format!("{}1e-130", "0".repeat(200))},
        "nested": {"M": {"list": {"L": [
            {"N": "-0e0"}, {"NS": ["1e3", "2.00"]}
        ]}}}
    });
    let received_image = WireItem::DynamoJson {
        data: serde_json::to_vec(&received_image).unwrap(),
    };
    let received_bytes = received_image.dynamo_json_bytes().unwrap().into_owned();
    let mut envelope: serde_json::Value = serde_json::from_slice(original.bytes.as_ref()).unwrap();
    envelope["payload"]["Item"]["images"]["new"] = serde_json::from_slice(&received_bytes).unwrap();
    let codec = fixture::codec();
    let received = codec
        .decode_event(&serde_json::to_vec(&envelope).unwrap())
        .unwrap();
    let mut manifest = initial.manifest().clone();
    manifest.links[0].digest = received.as_ref().digest;
    manifest.digest = CanonicalDigest::of(codec.encode_interval(&manifest).unwrap().as_ref());
    let import = Arc::new(
        RegionalImport::try_new(
            (
                initial.bindings().to_vec(),
                manifest,
                vec![received.clone()],
            ),
            &*codec,
        )
        .unwrap(),
    );
    let accepted = deliver(&destination, 2, Arc::clone(&import)).await;
    drop(destination);
    let destination = fixture::reopen(&destination_path).await;
    let materialized = destination
        .get_item(GetItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::key(),
        ))
        .await
        .unwrap()
        .item
        .unwrap();
    let expected = json!({
        "pk": {"S": "customer"}, "value": {"S": "seed"},
        "number": {"N": "1000"}, "padded": {"N": format!("{}1", "0".repeat(170))},
        "padded_exponent": {"N": "1000"},
        "long": {"N": format!("0.{}1", "0".repeat(129))},
        "nested": {"M": {"list": {"L": [
            {"N": "0"}, {"NS": ["1000", "2.00"]}
        ]}}}
    });
    assert_eq!(
        materialized.into_attribute_map().unwrap(),
        serde_json::from_value(expected).unwrap()
    );
    put(&destination, 2, "next").await;
    let next = frame(&destination, 2, 0, 1).await;
    let CanonicalPayload::Item(item) = &next.events()[0].as_ref().payload else {
        panic!("missing successor")
    };
    assert_eq!(
        item.predecessor.as_ref(),
        match &received.as_ref().payload {
            CanonicalPayload::Item(item) => Some(&item.version),
            _ => panic!("missing original"),
        }
    );
    // The successor borrows the retained original, not the normalized native
    // row.
    assert_eq!(
        item.images
            .old
            .as_ref()
            .unwrap()
            .dynamo_json_bytes()
            .unwrap()
            .as_ref(),
        received_bytes
    );
    assert_eq!(
        deliver(&destination, 2, import).await.pages(),
        accepted.pages()
    );
    assert_item(&destination, Some("next")).await;
}

async fn reject_condition(store: &fixture::Store) {
    let mut request = PutItemRequest::new(TableName::new(fixture::NAME), fixture::item("rejected"));
    request.condition_expression = Some("attribute_not_exists(pk)".into());
    let result = execute_item_write(
        store,
        OriginExecution {
            journal: fixture::journal(1),
            codec: fixture::codec(),
        },
        PutItemShape::try_from_request(request).unwrap(),
    )
    .await;
    match result {
        Err(failure) => assert!(matches!(
            failure.error.to_enum(),
            storage_types::StorageEnum::ConditionalCheckFailed
        )),
        Ok(_) => panic!("failed condition committed"),
    }
    assert_item(store, Some("first")).await;
}

#[tokio::test]
async fn given_equal_hlc_regions_when_time_moves_backwards_and_stores_reopen_then_conflicts_and_new_writes_preserve_ordered_history()
 {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first.sqlite");
    let first = fixture::open(&first_path).await;
    let second = fixture::open(&directory.path().join("second.sqlite")).await;
    fixture::install_pair(&first, &second).await;
    let future = storage_types::Clock::now(&first).timestamp_millis() + 86_400_000;
    let fixed = || {
        Arc::new(fixture::FixedClock(
            storage_types::TimestampMillis::from_timestamp(future),
        ))
    };
    let first = first.with_clock(fixed());
    let second = second.with_clock(fixed());
    put(&first, 1, "older").await;
    put(&second, 2, "newer").await;
    let losing = frame(&first, 1, 0, 1).await;
    let winning = frame(&second, 2, 0, 1).await;
    assert_eq!(
        item_clock(&losing),
        replication_types::event::order::HybridLogicalClock {
            physical_ms: future as u64,
            logical: 0
        }
    );
    assert_eq!(item_clock(&winning), item_clock(&losing));
    let receipt = deliver(&second, 2, Arc::clone(&losing)).await;
    assert_eq!(
        receipt.pages()[0].events[0].disposition,
        replication_types::event::disposition::EventDisposition::HistoryAccepted
    );
    assert_item(&second, Some("newer")).await;
    deliver(&first, 1, winning).await;
    assert_item(&first, Some("newer")).await;
    drop(first);
    let first = fixture::reopen(&first_path)
        .await
        .with_clock(Arc::new(fixture::FixedClock(
            storage_types::TimestampMillis::from_timestamp(future - 172_800_000),
        )));
    put(&first, 1, "after-import").await;
    let next = frame(&first, 1, 1, 2).await;
    assert_eq!(
        item_clock(&next),
        replication_types::event::order::HybridLogicalClock {
            physical_ms: future as u64,
            logical: 1
        }
    );
    deliver(&second, 2, next).await;
    assert_item(&second, Some("after-import")).await;
    let replay = deliver(&second, 2, losing).await;
    assert_eq!(replay.pages(), receipt.pages());
    assert_item(&second, Some("after-import")).await;
    delete(&first, 1).await;
    put(&second, 2, "put-wins-tie").await;
    let deleted = frame(&first, 1, 2, 3).await;
    let replacement = frame(&second, 2, 1, 2).await;
    assert_eq!(item_clock(&deleted), item_clock(&replacement));
    assert_eq!(
        deliver(&second, 2, deleted).await.pages()[0].events[0].disposition,
        replication_types::event::disposition::EventDisposition::HistoryAccepted
    );
    deliver(&first, 1, replacement).await;
    assert_item(&first, Some("put-wins-tie")).await;
    assert_item(&second, Some("put-wins-tie")).await;
}

fn item_clock(import: &RegionalImport) -> replication_types::event::order::HybridLogicalClock {
    let replication_types::event::content::CanonicalPayload::Item(item) =
        &import.events()[0].as_ref().payload
    else {
        panic!("fixture source is not an item");
    };
    let replication_types::event::order::WinnerOrder::Mutation { clock, .. } = item.order else {
        panic!("fixture item is not a mutation");
    };
    clock
}

#[tokio::test]
async fn given_persisted_missing_corrupt_or_exhausted_authoring_state_when_store_reopens_then_writes_fail_atomically_and_restored_history_resumes()
 {
    use replication_types::event::interval::IntervalCodec;
    use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageEnum};

    use crate::journal::record_fault_test_support::{self, PartitionFault};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.sqlite");
    let source = fixture::open(&path).await;
    let destination = fixture::open(&directory.path().join("destination.sqlite")).await;
    fixture::install_pair(&source, &destination).await;
    put(&source, 1, "accepted").await;
    let accepted = frame(&source, 1, 0, 1).await;
    let partition = accepted.manifest().origin.journal_partition;
    let journal = fixture::journal(1);
    let mut source = source;
    for fault in [
        PartitionFault::Missing,
        PartitionFault::CorruptFloor,
        PartitionFault::ClockExhausted,
        PartitionFault::PositionExhausted,
    ] {
        let original = record_fault_test_support::damage(&source, &journal, partition, fault).await;
        drop(source);
        source = fixture::reopen(&path).await;
        let rejected = execute_item_write(
            &source,
            OriginExecution {
                journal: Arc::clone(&journal),
                codec: fixture::codec(),
            },
            PutItemShape::try_from_request(PutItemRequest::new(
                TableName::new(fixture::NAME),
                fixture::item("must-abort"),
            ))
            .unwrap(),
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("persisted authoring fault {fault:?} accepted a write"));
        assert!(
            matches!(
                (fault, rejected.error.as_ref()),
                (
                    PartitionFault::Missing,
                    StorageEnum::InfrastructureResult(InfrastructureResultKind::Unavailable {
                        context: InfrastructureContext::RegionalSource
                    })
                ) | (
                    PartitionFault::CorruptFloor,
                    StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding {
                        context: InfrastructureContext::RecordEncoding
                    })
                ) | (
                    PartitionFault::PositionExhausted,
                    StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding {
                        context: InfrastructureContext::PublicationHistory
                    })
                ) | (
                    PartitionFault::ClockExhausted,
                    StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding {
                        context: InfrastructureContext::RegionalSource
                    })
                )
            ),
            "persisted authoring fault {fault:?}: {:?}",
            rejected.error
        );
        assert!(!rejected.error.is_commit_unknown());
        assert_item(&source, Some("accepted")).await;
        record_fault_test_support::restore(&source, &journal, original).await;
        let intact = frame(&source, 1, 0, 1).await;
        assert_eq!(
            fixture::codec().encode_interval(intact.manifest()).unwrap(),
            fixture::codec()
                .encode_interval(accepted.manifest())
                .unwrap()
        );
        assert_eq!(
            intact.events()[0].as_ref().bytes,
            accepted.events()[0].as_ref().bytes
        );
    }
    put(&source, 1, "after-recovery").await;
    let resumed = frame(&source, 1, 0, 2).await;
    assert_eq!(
        resumed.events().len(),
        2,
        "rejected writes allocated history positions"
    );
    assert_eq!(
        resumed.events()[0].as_ref().bytes,
        accepted.events()[0].as_ref().bytes
    );
    deliver(&destination, 2, resumed).await;
    assert_item(&destination, Some("after-recovery")).await;
}

#[tokio::test]
async fn given_local_and_global_tables_when_mixed_writes_commit_then_only_global_members_replicate()
{
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.sqlite");
    let lost_reply = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let source = fixture::open_with_probe(&source_path, Some(lost_reply.clone())).await;
    let destination = fixture::open(&directory.path().join("destination.sqlite")).await;
    fixture::install_pair(&source, &destination).await;
    fixture::create_table(&source, &TableName::new("local_table")).await;
    let batch = serde_json::from_value(serde_json::json!({"RequestItems": {
        "local_table": [{"PutRequest": {"Item": {"pk": {"S": "customer"}, "value": {"S": "local"}}}}],
        fixture::NAME: [{"PutRequest": {"Item": {"pk": {"S": "customer"}, "value": {"S": "batch"}}}}]
    }})).unwrap();
    let response = crate::origin::execute_batch_write(
        &source,
        OriginExecution {
            journal: fixture::journal(1),
            codec: fixture::codec(),
        },
        storage_provider::DynamoBatchWriteShape::try_from_request(batch).unwrap(),
    )
    .await
    .unwrap_or_else(|failure| panic!("batch: {:?}", failure.error));
    assert!(
        response
            .unprocessed_items
            .as_ref()
            .is_none_or(|items| items.is_empty())
    );
    let request: storage_types::TransactWriteItemsRequest = serde_json::from_value(serde_json::json!({
        "ClientRequestToken": "mixed-slice",
        "TransactItems": [
            {"Update": {"TableName": "local_table", "Key": {"pk": {"S": "customer"}}, "UpdateExpression": "SET #v = :v", "ExpressionAttributeNames": {"#v": "value"}, "ExpressionAttributeValues": {":v": {"S": "local-updated"}}}},
            {"Update": {"TableName": fixture::NAME, "Key": {"pk": {"S": "customer"}}, "UpdateExpression": "SET #v = :v", "ExpressionAttributeNames": {"#v": "value"}, "ExpressionAttributeValues": {":v": {"S": "transaction"}}}}
        ]
    })).unwrap();
    for _ in 0..2 {
        crate::origin::execute_transact_write(
            &source,
            OriginExecution {
                journal: fixture::journal(1),
                codec: fixture::codec(),
            },
            storage_provider::DynamoTransactWriteShape::try_from_request(request.clone()).unwrap(),
        )
        .await
        .unwrap_or_else(|failure| panic!("transaction: {:?}", failure.error));
    }
    let import = frame(&source, 1, 0, 2).await;
    assert_eq!(
        import.events().len(),
        2,
        "token replay and local members produce no extra events"
    );
    let replication_types::event::content::CanonicalPayload::Item(transaction) =
        &import.events()[1].as_ref().payload
    else {
        panic!("expected item")
    };
    assert!(transaction.transaction.is_some());
    assert_eq!(
        transaction
            .images
            .old
            .as_ref()
            .unwrap()
            .required_string_attribute("value")
            .unwrap(),
        "batch"
    );
    deliver(&destination, 2, import).await;
    assert_item(&source, Some("transaction")).await;
    assert_item(&destination, Some("transaction")).await;
    let local = source
        .get_item(GetItemRequest::new(
            TableName::new("local_table"),
            fixture::key(),
        ))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(
        local.required_string_attribute("value").unwrap(),
        "local-updated"
    );
    assert!(
        destination
            .get_item(GetItemRequest::new(
                TableName::new("local_table"),
                fixture::key()
            ))
            .await
            .is_err()
    );
    use replication_types::event::content::CanonicalEventCodec;
    let state = fixture::installed_state(&source, 1, fixture::NAME).await;
    let key_for = |value: &str| {
        let mut key = fixture::key();
        key.insert("pk", storage_types::AttributeValue::S(value.to_owned()));
        key
    };
    let first_key = "partial-recovery";
    let partition = fixture::partitions().select(
        state.configuration.lineage,
        &fixture::codec().encode_key(&key_for(first_key)).unwrap(),
    );
    let rejected_key = (0..4096)
        .map(|index| format!("partial-recovery-{index}"))
        .find(|value| {
            fixture::partitions().select(
                state.configuration.lineage,
                &fixture::codec().encode_key(&key_for(value)).unwrap(),
            ) == partition
        })
        .unwrap();
    let request: storage_types::BatchWriteItemRequest = serde_json::from_value(serde_json::json!({"RequestItems": {
        fixture::NAME: [
            {"PutRequest": {"Item": {"pk": {"S": first_key}, "value": {"S": "accepted-before-lost-reply"}}}},
            {"PutRequest": {"Item": {"pk": {"S": rejected_key}, "value": {"S": "still-unprocessed"}}, "Indexers": [], "AuxItemStreamTtlHours": 1}}
        ],
        "local_table": [{"PutRequest": {"Item": {"pk": {"S": first_key}, "value": {"S": "ordinary-accepted"}}}}]
    }})).unwrap();
    let mut expected = storage_types::VecMap::new();
    expected.insert(
        TableName::new(fixture::NAME),
        vec![
            request
                .request_items
                .get(&TableName::new(fixture::NAME))
                .unwrap()[1]
                .clone(),
        ],
    );
    let journal = fixture::journal_with_maximum_events(1, fixture::nz(1));
    lost_reply.arm();
    let failure = crate::origin::execute_batch_write(
        &source,
        OriginExecution {
            journal: Arc::clone(&journal),
            codec: fixture::codec(),
        },
        storage_provider::DynamoBatchWriteShape::try_from_request(request).unwrap(),
    )
    .await
    .expect_err("lost partial-batch reply was acknowledged");
    assert!(lost_reply.was_observed() && failure.error.is_commit_unknown());
    let crate::origin::OriginReceiptObservation::Durable(receipt) =
        crate::origin::reconcile_item_write(&source, Arc::clone(&journal), failure.recovery)
            .await
            .unwrap()
    else {
        panic!("partial batch lost its durable response");
    };
    let response: storage_types::BatchWriteItemResponse =
        serde_json::from_str(receipt.response.get()).unwrap();
    assert_eq!(
        serde_json::to_value(response.unprocessed_items).unwrap(),
        serde_json::to_value(Some(expected)).unwrap()
    );
    assert_eq!(receipt.events.len(), 1);
    let exact_response = receipt.response.get().to_owned();
    drop(source);
    let source = fixture::reopen(&source_path).await;
    let crate::origin::OriginReceiptObservation::Durable(reopened) =
        crate::origin::reconcile_item_write(&source, journal, failure.recovery)
            .await
            .unwrap()
    else {
        panic!("reopen lost partial batch response");
    };
    assert_eq!(reopened.response.get(), exact_response);
    assert_eq!(reopened.events, receipt.events);
    for (table, value) in [
        (fixture::NAME, "accepted-before-lost-reply"),
        ("local_table", "ordinary-accepted"),
    ] {
        let item = source
            .get_item(GetItemRequest::new(
                TableName::new(table),
                key_for(first_key),
            ))
            .await
            .unwrap()
            .item
            .unwrap();
        assert_eq!(item.required_string_attribute("value").unwrap(), value);
    }
    assert!(
        source
            .get_item(GetItemRequest::new(
                TableName::new(fixture::NAME),
                key_for(&rejected_key)
            ))
            .await
            .unwrap()
            .item
            .is_none()
    );
}

#[tokio::test]
async fn given_a_foreign_empty_fragment_when_imported_then_no_durable_receipt_is_created() {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination = fixture::open(&directory.path().join("destination.sqlite")).await;
    fixture::install_pair(&source, &destination).await;
    put(&source, 1, "source").await;
    let original = frame(&source, 1, 0, 1).await;
    let mut binding = original.bindings()[0].clone();
    binding.source_table = [254; 16].try_into().unwrap();
    let fragment = Arc::new(
        RegionalImport::try_new(
            (vec![binding], original.manifest().clone(), Vec::new()),
            fixture::codec().as_ref(),
        )
        .unwrap(),
    );
    let result = execute_import(
        &destination,
        fixture::journal(2),
        ImportExecution {
            import: fixture::bounded_import(Arc::clone(&fragment)),
            codec: fixture::codec(),
        },
    )
    .await;
    assert!(
        result.is_err(),
        "foreign empty fragment bypassed installed membership"
    );
    assert_item(&destination, None).await;
    assert!(matches!(
        reconcile_import_pages(
            &destination,
            fixture::journal(2),
            (fixture::bounded_import(fragment), fixture::codec())
        )
        .await
        .unwrap(),
        ImportReceiptObservation::Unresolved
    ));
    deliver(&destination, 2, original).await;
    assert_item(&destination, Some("source")).await;
}
