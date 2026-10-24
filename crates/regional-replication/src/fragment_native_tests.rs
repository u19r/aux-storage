use std::sync::Arc;

use replication_contract_tests::native::commit_fault::{NativeCommitBoundary, NativeCommitFault};
use replication_types::{
    configuration::membership::RegionalTableBinding, event::content::CanonicalEventCodec,
    regional_import::RegionalImport,
};
use storage_provider::{DynamoOperations, DynamoPort, native_admission::NativeAttemptSource};
use storage_types::{AttemptMode, AttributeValue, GetItemRequest, KeyAttributes, TableName};

use crate::{
    import::{ImportExecution, execute_import},
    import_receipt::{ImportReceiptObservation, reconcile_import_pages},
    test_support as fixture,
};

const SECOND: &str = "fragment_table";

#[tokio::test]
async fn given_two_table_transaction_when_fragments_abort_and_completion_loses_ack_then_only_complete_native_receipts_survive_reopen()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination_path = directory.path().join("destination.sqlite");
    let aborted = Arc::new(NativeCommitFault::new(NativeCommitBoundary::BeforeCommit));
    let destination = fixture::open_with_probe(&destination_path, Some(aborted.clone())).await;
    let pair = fixture::NativeCluster::start(&[&source, &destination]).await;
    pair.install(&source, fixture::NAME, &[2]).await;
    pair.install(&source, SECOND, &[2]).await;
    let source_operations = Arc::clone(&pair.operations[0]);
    pair.shutdown().await;
    let first = fixture::installed_state(&source, 1, fixture::NAME).await;
    let second = fixture::installed_state(&source, 1, SECOND).await;
    let codec = fixture::codec();
    let partition = fixture::partitions().select(
        first.configuration.lineage,
        &codec.encode_key(&key("customer")).unwrap(),
    );
    let second_key = (0..4096)
        .map(|index| format!("customer-{index}"))
        .find(|candidate| {
            fixture::partitions().select(
                second.configuration.lineage,
                &codec.encode_key(&key(candidate)).unwrap(),
            ) == partition
        })
        .unwrap();
    let journal = fixture::journal(1);
    let read_journal = Arc::clone(&journal);
    let after = source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { read_journal.load_source_head(attempt, partition).await })
        })
        .await
        .unwrap();
    source_operations.transact_write_items(serde_json::from_value(serde_json::json!({"ClientRequestToken": "two-table-fragments", "TransactItems": [
        {"Put": {"TableName": fixture::NAME, "Item": {"pk": {"S": "customer"}, "value": {"S": "first-fragment"}}}},
        {"Put": {"TableName": SECOND, "Item": {"pk": {"S": second_key}, "value": {"S": "second-fragment"}}}}
    ]})).unwrap()).await.unwrap();
    drop(source_operations);
    let read_journal = Arc::clone(&journal);
    let through = source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move { read_journal.load_source_head(attempt, partition).await })
        })
        .await
        .unwrap();
    assert_eq!(
        u64::from(through),
        u64::from(after) + 1,
        "one transaction split its shared source position"
    );
    let mut tables = vec![
        (first.configuration.table, first.configuration.lineage),
        (second.configuration.table, second.configuration.lineage),
    ];
    tables.sort_unstable();
    let request = crate::source_frame::SourceFrameRequest {
        origin: replication_types::identity::origin::OriginId {
            region: fixture::region(1),
            database_group: [1; 16].try_into().unwrap(),
            journal_generation: 1.into(),
            journal_partition: partition,
        },
        range: (after, through).try_into().unwrap(),
        membership: first.membership,
        tables,
    };
    let frame = source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                Ok(crate::source_frame::load_source_frame(
                    &journal,
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
    assert_eq!(frame.events().len(), 2);
    let mut bindings = [first, second]
        .into_iter()
        .map(|state| {
            let target = state
                .sources
                .iter()
                .find(|binding| binding.region == fixture::region(2))
                .unwrap();
            RegionalTableBinding {
                source_region: fixture::region(1),
                source_table: state.configuration.table,
                local_table: target.table,
                lineage: state.configuration.lineage,
                membership: state.membership,
                local_config: target.config,
            }
        })
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.source_table);
    let complete = Arc::new(
        RegionalImport::try_new(
            (bindings, frame.manifest().clone(), frame.events().to_vec()),
            &*codec,
        )
        .unwrap(),
    );
    let first_fragment = Arc::new(
        complete
            .select_fragment((0, 1).try_into().unwrap())
            .unwrap(),
    );
    let second_fragment = Arc::new(
        complete
            .select_fragment((1, 2).try_into().unwrap())
            .unwrap(),
    );
    aborted.arm();
    let error = execute_import(
        &destination,
        fixture::journal(2),
        ImportExecution {
            import: fixture::bounded_import(Arc::clone(&first_fragment)),
            codec: fixture::codec(),
        },
    )
    .await
    .err()
    .unwrap();
    assert!(aborted.was_observed() && !error.is_commit_unknown());
    require_items(&destination, &second_key, false, false).await;
    assert!(matches!(
        reconcile_import_pages(
            &destination,
            fixture::journal(2),
            (
                fixture::bounded_import(Arc::clone(&first_fragment)),
                fixture::codec()
            )
        )
        .await
        .unwrap(),
        ImportReceiptObservation::Unresolved
    ));
    let partial = fixture::deliver(&destination, 2, Arc::clone(&first_fragment)).await;
    crate::transport::test_support::measure_import_reply("partial", &partial);
    assert!(
        partial.completed().is_none(),
        "one fragment fabricated whole-page coverage"
    );
    require_items(&destination, &second_key, true, false).await;
    assert!(matches!(
        reconcile_import_pages(
            &destination,
            fixture::journal(2),
            (
                fixture::bounded_import(Arc::clone(&complete)),
                fixture::codec()
            )
        )
        .await
        .unwrap(),
        ImportReceiptObservation::Unresolved
    ));
    drop(destination);
    let lost_ack = Arc::new(NativeCommitFault::new(
        NativeCommitBoundary::AfterDurableAcknowledgement,
    ));
    let destination = fixture::open_with_probe(&destination_path, Some(lost_ack.clone())).await;
    destination
        .bind_work_limits(fixture::budgets().attempt)
        .unwrap();
    require_items(&destination, &second_key, true, false).await;
    lost_ack.arm();
    let error = execute_import(
        &destination,
        fixture::journal(2),
        ImportExecution {
            import: fixture::bounded_import(Arc::clone(&second_fragment)),
            codec: fixture::codec(),
        },
    )
    .await
    .err()
    .unwrap();
    assert!(lost_ack.was_observed() && error.is_commit_unknown());
    let ImportReceiptObservation::Durable(receipt) = reconcile_import_pages(
        &destination,
        fixture::journal(2),
        (
            fixture::bounded_import(Arc::clone(&second_fragment)),
            fixture::codec(),
        ),
    )
    .await
    .unwrap() else {
        panic!("committed fragment lost its exact receipt");
    };
    let completed = receipt
        .completed()
        .expect("all fragments did not seal complete table coverage")
        .to_vec();
    crate::transport::test_support::measure_import_reply("completed", &receipt);
    assert_eq!(completed.len(), 2);
    assert_eq!(
        completed
            .iter()
            .map(|page| page.events.len())
            .sum::<usize>(),
        2
    );
    require_items(&destination, &second_key, true, true).await;
    assert_eq!(
        fixture::deliver(&destination, 2, Arc::clone(&first_fragment))
            .await
            .completed()
            .unwrap(),
        completed
    );
    assert_eq!(
        fixture::deliver(&destination, 2, second_fragment)
            .await
            .completed()
            .unwrap(),
        completed
    );
    drop(destination);
    let destination = fixture::reopen(&destination_path).await;
    require_items(&destination, &second_key, true, true).await;
    let ImportReceiptObservation::Durable(receipt) = reconcile_import_pages(
        &destination,
        fixture::journal(2),
        (fixture::bounded_import(complete), fixture::codec()),
    )
    .await
    .unwrap() else {
        panic!("complete receipts disappeared on reopen");
    };
    assert_eq!(receipt.pages(), completed);
    crate::transport::test_support::measure_import_reply("reopened-complete", &receipt);
    let originals = frame.events().to_vec();
    destination
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                for original in originals {
                    let retained = fixture::journal(2)
                        .load_acceptance(attempt, &original.as_ref().id, &fixture::codec())
                        .await?
                        .unwrap();
                    assert_eq!(retained.event.as_ref().bytes, original.as_ref().bytes);
                }
                Ok(())
            })
        })
        .await
        .unwrap();
}

fn key(value: &str) -> KeyAttributes {
    KeyAttributes::from([("pk".into(), AttributeValue::S(value.into()))])
}

async fn require_items(store: &fixture::Store, second_key: &str, first: bool, second: bool) {
    for (table, key_value, value, present) in [
        (fixture::NAME, "customer", "first-fragment", first),
        (SECOND, second_key, "second-fragment", second),
    ] {
        let observed = store
            .get_item(GetItemRequest::new(TableName::new(table), key(key_value)))
            .await
            .unwrap()
            .item;
        assert_eq!(
            observed.map(|item| item.into_attribute_map().unwrap()),
            present.then(|| {
                [
                    ("pk".into(), AttributeValue::S(key_value.into())),
                    ("value".into(), AttributeValue::S(value.into())),
                ]
                .into_iter()
                .collect()
            })
        );
    }
}
