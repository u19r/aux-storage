use std::sync::Arc;

use infrastructure_provider::record::{DurableRecord, RecordChange};
use storage_provider::{DynamoOperations, DynamoPort};
use storage_types::{AttemptMode, PutItemRequest, ReadProtection, TableName};
use stream_provider::retained_image::ImageChargeAccount;

use super::JournalStore;
use crate::test_support as fixture;

#[tokio::test]
async fn given_full_customer_history_when_acceptance_stops_then_native_recovery_retains_headroom_and_refund_restores_acceptance()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination = fixture::open(&directory.path().join("destination.sqlite")).await;
    let mut policy = fixture::backlog();
    let ceiling = u64::try_from(fixture::budgets().attempt.data_bytes.get()).unwrap();
    policy.acceptance_bytes = (ceiling * 2).try_into().unwrap();
    policy.recovery_reserve_bytes = ceiling.try_into().unwrap();
    policy.hot_history_bytes = policy.acceptance_bytes;
    let cluster =
        fixture::NativeCluster::start_with_backlog(&[&source, &destination], policy).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("accepted"),
        ))
        .await
        .unwrap();
    let original = fixture::frame(&source, 1, 0, 1).await;
    let journal = Arc::clone(&cluster.journals[0]);
    // Real charged rows exercise shared storage accounting; never fabricate an
    // account balance.
    let mut charges = Vec::new();
    let mut ordinal = 0u32;
    loop {
        let (_, used) = account(&source, &journal).await;
        if used == ceiling - 128 {
            break;
        }
        let key = journal
            .record_key(
                [
                    b"fixture-capacity-".as_slice(),
                    ordinal.to_be_bytes().as_slice(),
                ]
                .concat(),
            )
            .unwrap();
        let key_bytes =
            u64::try_from(fixture::physical_limits().validate_key(&key).unwrap()).unwrap();
        let size = usize::try_from((ceiling - 128 - used - key_bytes).min(1044480)).unwrap();
        let charge = journal
            .records
            .prepare_bytes(key.clone(), None, vec![0; size].into())
            .unwrap();
        let owner = Arc::clone(&journal);
        source
            .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { owner.records.store(attempt, &[charge]).await })
            })
            .await
            .unwrap();
        charges.push(key);
        ordinal += 1;
    }
    let before = account(&source, &journal).await;
    let heads = source_heads(&source, &journal).await;
    assert_eq!(before.1, ceiling - 128);
    let error = cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item(&"rejected".repeat(1024)),
        ))
        .await
        .expect_err("customer crossed its retained-history ceiling");
    assert!(matches!(
        error.as_ref(),
        storage_types::StorageEnum::LimitExceeded { .. }
    ));
    fixture::assert_item(&source, Some("accepted")).await;
    let after = account(&source, &journal).await;
    assert_eq!(after.0.revision, before.0.revision);
    assert_eq!(
        after.0.bytes, before.0.bytes,
        "aborted customer attempt leaked history charges"
    );
    let retained = fixture::frame(&source, 1, 0, 1).await;
    assert_eq!(
        retained.events()[0].as_ref().bytes,
        original.events()[0].as_ref().bytes
    );
    assert_eq!(
        source_heads(&source, &journal).await,
        heads,
        "rejected customer allocated an original position"
    );
    let required_through = original.manifest().range.through();
    // Charged native setup grows beyond the customer stop without fabricating
    // the account.
    let recovery_key = journal
        .record_key(b"fixture-recovery-headroom".to_vec())
        .unwrap();
    let recovery = journal
        .records
        .prepare_bytes(recovery_key.clone(), None, vec![1; 65536].into())
        .unwrap();
    let owner = Arc::clone(&journal);
    source
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { owner.records.store(attempt, &[recovery]).await })
        })
        .await
        .unwrap();
    let (_, used) = account(&source, &journal).await;
    assert!(used > ceiling && used < ceiling * 2);
    for _ in 0..512 {
        cluster.drive_once().await;
        let owner = Arc::clone(&journal);
        let scope = fixture::delivery_scope(&retained);
        let through = source
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    Ok(owner
                        .load_delivery_progress(attempt, &scope)
                        .await?
                        .through())
                })
            })
            .await
            .unwrap();
        if through == required_through {
            break;
        }
    }
    fixture::assert_item(&destination, Some("accepted")).await;
    let owner = Arc::clone(&journal);
    let scope = fixture::delivery_scope(&retained);
    source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                assert_eq!(
                    u64::from(
                        owner
                            .load_delivery_progress(attempt, &scope)
                            .await?
                            .through()
                    ),
                    u64::from(required_through),
                    "reserved recovery failed to persist the source ACK above the customer stop"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    // Shared native refunds join the actual row removals in one caller-owned
    // attempt per wave.
    charges.push(recovery_key);
    for wave in charges.chunks(8) {
        let keys = wave.to_vec();
        let owner = Arc::clone(&journal);
        source
            .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    let rows = owner
                        .records
                        .load(attempt, &keys, ReadProtection::Conflict)
                        .await?;
                    let changes = rows
                        .into_iter()
                        .map(|row| {
                            let row = row.unwrap();
                            RecordChange::Delete {
                                key: row.key,
                                expected: row.revision,
                            }
                        })
                        .collect::<Vec<_>>();
                    owner.records.store(attempt, &changes).await
                })
            })
            .await
            .unwrap();
    }
    assert!(account(&source, &journal).await.1 < ceiling);
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("accepted-after-refund"),
        ))
        .await
        .unwrap();
    fixture::assert_item(&source, Some("accepted-after-refund")).await;
    cluster.shutdown().await;
}

async fn source_heads(store: &fixture::Store, journal: &Arc<JournalStore>) -> Vec<u64> {
    let journal = Arc::clone(journal);
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let partitions = (0..64u16).map(Into::into).collect::<Vec<_>>();
                Ok(journal
                    .load_source_heads(attempt, &partitions, ReadProtection::Snapshot)
                    .await?
                    .into_iter()
                    .map(u64::from)
                    .collect())
            })
        })
        .await
        .unwrap()
}

async fn account(store: &fixture::Store, journal: &Arc<JournalStore>) -> (DurableRecord, u64) {
    let journal = Arc::clone(journal);
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let rows = journal
                    .records
                    .load(
                        attempt,
                        &[journal.records.capacity.root_key().try_into()?],
                        ReadProtection::Snapshot,
                    )
                    .await?;
                let row = rows.into_iter().next().flatten().unwrap();
                let account = ImageChargeAccount::decode(
                    &row.bytes,
                    fixture::physical_limits().max_record_bytes().get(),
                )?;
                Ok((row, account.bytes()))
            })
        })
        .await
        .unwrap()
}
