use std::{num::NonZeroUsize, sync::Arc};

use replication_types::protocol::CanonicalDigest;

use super::CanonicalArchive;

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
        archive.publish_object(digest, &bytes).await.unwrap();
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
            std::fs::read(namespace.join(expected)).unwrap(),
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
            let recovered = archive.fetch_object(digest, bytes.len()).await.unwrap();
            let report = guard.finish();
            assert_eq!(recovered, bytes);
            eprintln!(
                "regional_archive_allocation {}",
                serde_json::json!({"seed":seed,"repetition":repetition,"allocations":report.allocation_count,"bytes":report.allocated_bytes})
            );
        }
    }
}
