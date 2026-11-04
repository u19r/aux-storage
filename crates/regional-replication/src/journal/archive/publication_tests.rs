use std::sync::Arc;

use storage_provider::{DynamoOperations, DynamoPort};
use storage_types::{AttemptMode, PutItemRequest, ReadProtection, TableName};

use crate::test_support as fixture;

#[tokio::test]
async fn given_a_verified_export_when_table_cleanup_cancels_it_then_stale_publication_cannot_restore_records_or_objects()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let peer = fixture::open(&directory.path().join("peer.sqlite")).await;
    let root = directory.path().join("archive");
    std::fs::create_dir(&root).unwrap();
    let cluster = fixture::NativeCluster::start_with_archive(&[&source, &peer], &root).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("stale-export"),
        ))
        .await
        .unwrap();
    let physical = fixture::installed_state(&source, 1, fixture::NAME)
        .await
        .physical;
    let journal = Arc::clone(&cluster.journals[0]);
    let archive = Arc::clone(journal.archive.get().unwrap());
    let plan = prepare_archive_export(&source, &journal).await;
    let verified = plan.publish(&archive).await.unwrap();
    let pointer = verified.hold().pointer.clone();
    cluster.shutdown().await;
    source
        .delete_table(storage_types::DeleteTableRequest {
            table_name: TableName::new(fixture::NAME),
        })
        .await
        .unwrap();
    while !archive
        .reclaim_table_objects(physical, std::num::NonZeroUsize::new(128).unwrap())
        .await
        .unwrap()
    {}
    let owner = Arc::clone(&journal);
    source
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { owner.cancel_table_archives(attempt, physical).await })
        })
        .await
        .unwrap();
    let owner = Arc::clone(&journal);
    assert!(
        source
            .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { owner.publish_archive_pointer(attempt, &verified).await })
            })
            .await
            .is_err()
    );
    source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                assert!(
                    journal
                        .find_archive_pointer(attempt, &pointer.key, ReadProtection::Snapshot)
                        .await?
                        .is_none()
                );
                assert!(
                    journal
                        .records
                        .load(attempt, &[pointer.key], ReadProtection::Snapshot)
                        .await?[0]
                        .is_some(),
                    "rejected stale publication must leave the hot original intact"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    let bytes = b"stale exporter after native cancellation";
    assert!(
        archive
            .publish_object(
                physical,
                replication_types::protocol::CanonicalDigest::of(bytes),
                bytes
            )
            .await
            .is_err()
    );
}

async fn prepare_archive_export(
    source: &fixture::Store,
    journal: &Arc<crate::journal::JournalStore>,
) -> Box<crate::archive::ArchivePlan> {
    for _ in 0..16 {
        let owner = Arc::clone(journal);
        let turn = source
            .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    owner.select_archive_family(attempt).await?;
                    owner
                        .prepare_archive_turn(
                            attempt,
                            crate::archive::ArchiveKind::Event,
                            (&fixture::limits(), &fixture::codec()),
                        )
                        .await
                })
            })
            .await
            .unwrap();
        if let crate::archive::ArchiveTurn::Export(plan) = turn {
            return plan;
        }
    }
    panic!("archive did not produce an export plan");
}

#[tokio::test]
async fn given_a_materialized_original_when_archive_handoff_finishes_then_original_is_cold_and_publication_metadata_stays_native()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let peer = fixture::open(&directory.path().join("peer.sqlite")).await;
    let root = directory.path().join("archive");
    std::fs::create_dir(&root).unwrap();
    let cluster = fixture::NativeCluster::start_with_archive(&[&source, &peer], &root).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("archive-original"),
        ))
        .await
        .unwrap();
    let frame = fixture::frame(&source, 1, 0, 1).await;
    let event = frame.events()[0].clone();
    let physical = fixture::installed_state(&source, 1, fixture::NAME)
        .await
        .physical;
    let journal = Arc::clone(&cluster.journals[0]);
    let owner = Arc::clone(&journal);
    let original = event.clone();
    let (body, metadata) = source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let (_, pointer) = owner
                    .load_acceptance_with_delivery(
                        attempt,
                        (physical, &original.as_ref().id),
                        &fixture::codec(),
                        ReadProtection::Snapshot,
                    )
                    .await?
                    .unwrap();
                let first = owner.record_key(crate::constants::CANONICAL_VIEW_SUFFIX.to_vec())?;
                let page = owner
                    .records
                    .scan(
                        attempt,
                        infrastructure_provider::page::RecordPageRequest {
                            range: owner.record_prefix_range(&first)?,
                            after: None,
                            max_bytes: owner.records.limits.max_bytes(),
                            protection: ReadProtection::Snapshot,
                        },
                    )
                    .await?;
                assert!(page.continuation.is_none());
                let receiver = page
                    .records
                    .iter()
                    .find(|row| {
                        owner.decode_receiver_pointer(row).unwrap().as_ref() == Some(&pointer)
                    })
                    .expect("the published receiver pointer must exist")
                    .key
                    .clone();
                Ok((
                    owner.table_identity_key(
                        crate::constants::EVENT_SUFFIX,
                        physical,
                        &original.as_ref().id,
                    )?,
                    vec![
                        owner.table_identity_key(
                            crate::constants::ACCEPTANCE_SUFFIX,
                            physical,
                            &original.as_ref().id,
                        )?,
                        receiver,
                        owner.source_key(
                            &frame.manifest().origin,
                            frame.manifest().range.through(),
                        )?,
                    ],
                ))
            })
        })
        .await
        .unwrap();
    let mut archived = false;
    for _ in 0..256 {
        // Allow a complete rescan when the new original sorts before the
        // previous cursor. Each turn advances the fixture's archive idle
        // interval.
        cluster.clock.advance(std::time::Duration::from_secs(1));
        cluster.drive_once().await;
        let owner = Arc::clone(&journal);
        let key = body.clone();
        archived = source
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    Ok(owner
                        .records
                        .load(
                            attempt,
                            std::slice::from_ref(&key),
                            ReadProtection::Snapshot,
                        )
                        .await?[0]
                        .is_none()
                        && owner
                            .find_archive_pointer(attempt, &key, ReadProtection::Snapshot)
                            .await?
                            .is_some())
                })
            })
            .await
            .unwrap();
        if archived {
            break;
        }
    }
    assert!(archived, "verified publication never released the original");
    let original = event.clone();
    source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let rows = journal
                    .records
                    .load(attempt, &metadata, ReadProtection::Snapshot)
                    .await?;
                assert_eq!(rows.len(), metadata.len());
                assert!(
                    rows.iter().all(Option::is_some),
                    "archival removed source, acceptance or receiver metadata"
                );
                let recovered = journal
                    .load_acceptance(
                        attempt,
                        (physical, &original.as_ref().id),
                        &fixture::codec(),
                    )
                    .await?
                    .unwrap();
                assert_eq!(recovered.event.as_ref().bytes, original.as_ref().bytes);
                Ok(())
            })
        })
        .await
        .unwrap();
    cluster.shutdown().await;
}
