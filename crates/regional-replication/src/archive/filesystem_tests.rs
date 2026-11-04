use std::{num::NonZeroUsize, sync::Arc};

use replication_types::protocol::CanonicalDigest;

use super::CanonicalArchive;

#[tokio::test]
async fn given_an_admitted_export_when_another_archive_revokes_then_it_waits_and_stale_exports_cannot_recreate_data()
 {
    let directory = tempfile::tempdir().unwrap();
    let bound = NonZeroUsize::new(65536).unwrap();
    let first = CanonicalArchive::try_new(directory.path(), [17; 16], bound)
        .await
        .unwrap();
    let second = CanonicalArchive::try_new(directory.path(), [17; 16], bound)
        .await
        .unwrap();
    let history = storage_types::TableHistoryId::from([1; 16]);
    let bytes = b"stale exporter";
    let digest = CanonicalDigest::of(bytes);
    first.publish_object(history, digest, bytes).await.unwrap();
    let lease = first.admit_table_publication(history).await.unwrap();
    let mut reclaim = tokio::spawn(async move {
        second
            .reclaim_table_objects(history, NonZeroUsize::new(2).unwrap())
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut reclaim)
            .await
            .is_err()
    );
    drop(lease);
    assert!(reclaim.await.unwrap().unwrap());
    assert!(first.publish_object(history, digest, bytes).await.is_err());
    assert!(
        first
            .fetch_object(history, digest, bytes.len())
            .await
            .is_err()
    );
    let reopened = CanonicalArchive::try_new(directory.path(), [17; 16], bound)
        .await
        .unwrap();
    assert!(
        reopened
            .publish_object(history, digest, bytes)
            .await
            .is_err()
    );
    assert!(
        reopened
            .reclaim_table_objects(history, NonZeroUsize::MIN)
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn given_verified_archive_objects_when_loading_then_measure_unchanged_names_and_content() {
    let directory = tempfile::tempdir().unwrap();
    let archive = CanonicalArchive::try_new(
        directory.path(),
        [17; 16],
        NonZeroUsize::new(65536).unwrap(),
    )
    .await
    .unwrap();
    for seed in [17u8, 61, 89] {
        let bytes: Arc<[u8]> = (0..4096)
            .map(|index| (index % 256) as u8 ^ seed)
            .collect::<Vec<_>>()
            .into();
        let digest = CanonicalDigest::of(&bytes);
        archive
            .publish_object(
                storage_types::TableHistoryId::from([17; 16]),
                digest,
                &bytes,
            )
            .await
            .unwrap();
        let expected: String = digest
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let namespace = directory.path().join(format!(
            "native-{}",
            uuid::Uuid::from_bytes([17; 16]).simple()
        ));
        assert_eq!(
            std::fs::read(
                namespace
                    .join(format!(
                        "table-{}",
                        uuid::Uuid::from_bytes([17; 16]).simple()
                    ))
                    .join(expected)
            )
            .unwrap(),
            bytes.as_ref()
        );
        for repetition in 0..5 {
            let guard = alloc_counter::AllocationGuard::start(
                module_path!(),
                "archive-fetch",
                file!(),
                line!(),
                None,
            );
            let recovered = archive
                .fetch_object(
                    storage_types::TableHistoryId::from([17; 16]),
                    digest,
                    bytes.len(),
                )
                .await
                .unwrap();
            let report = guard.finish();
            assert_eq!(recovered, bytes);
            eprintln!(
                "regional_archive_allocation {}",
                serde_json::json!({"seed":seed,"repetition":repetition,"allocations":report.allocation_count,"bytes":report.allocated_bytes})
            );
        }
    }
}

#[tokio::test]
async fn given_identical_objects_in_two_table_namespaces_when_one_is_removed_then_other_copy_survives_reopen()
 {
    let directory = tempfile::tempdir().unwrap();
    let identity = [17; 16];
    let bound = NonZeroUsize::new(65536).unwrap();
    let archive = CanonicalArchive::try_new(directory.path(), identity, bound)
        .await
        .unwrap();
    let first = storage_types::TableHistoryId::from([1; 16]);
    let second = storage_types::TableHistoryId::from([2; 16]);
    let unknown = storage_types::TableHistoryId::from([3; 16]);
    let bytes = b"independent canonical copies";
    let digest = CanonicalDigest::of(bytes);
    for scope in [first, second, storage_types::TableHistoryId::from([17; 16])] {
        archive.publish_object(scope, digest, bytes).await.unwrap();
        assert_eq!(
            archive
                .fetch_object(scope, digest, bytes.len())
                .await
                .unwrap()
                .as_ref(),
            bytes
        );
    }
    assert!(
        archive
            .fetch_object(unknown, digest, bytes.len())
            .await
            .is_err()
    );
    assert!(
        !archive
            .reclaim_table_objects(first, NonZeroUsize::MIN)
            .await
            .unwrap()
    );
    assert!(
        archive
            .reclaim_table_objects(first, NonZeroUsize::MIN)
            .await
            .unwrap()
    );
    assert!(archive.publish_object(first, digest, bytes).await.is_err());
    drop(archive);
    let archive = CanonicalArchive::try_new(directory.path(), identity, bound)
        .await
        .unwrap();
    assert!(
        archive
            .fetch_object(first, digest, bytes.len())
            .await
            .is_err()
    );
    assert!(archive.publish_object(first, digest, bytes).await.is_err());
    assert!(
        archive
            .reclaim_table_objects(first, NonZeroUsize::MIN)
            .await
            .unwrap()
    );
    for scope in [second, storage_types::TableHistoryId::from([17; 16])] {
        assert_eq!(
            archive
                .fetch_object(scope, digest, bytes.len())
                .await
                .unwrap()
                .as_ref(),
            bytes
        );
    }
}
