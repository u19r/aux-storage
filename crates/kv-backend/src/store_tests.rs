use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageEnum};

use super::{
    AttemptMode, KvAttempt, KvBackend, KvMutation, KvStore, ReadProtection,
    constants::{
        INFRASTRUCTURE_FORMAT_KEY, MAINTENANCE_IDENTITY_KEY, SYSTEM_STREAM_SCOPE_KEY,
        TTL_INDEX_FORMAT_KEY,
    },
    rocksdb::RocksDbBackend,
};

#[tokio::test]
async fn given_unsupported_final_format_when_store_initialization_fails_then_no_earlier_format_is_committed()
 {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let backend = RocksDbBackend::initialize(path.clone()).unwrap();
    backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                attempt.stage(vec![KvMutation::Put {
                    key: INFRASTRUCTURE_FORMAT_KEY.to_vec(),
                    value: vec![u8::MAX],
                }])
            })
        })
        .await
        .unwrap();

    let error = match KvStore::initialize_format(backend).await {
        Ok(_) => panic!("unsupported infrastructure format was accepted"),
        Err(error) => error,
    };
    assert!(matches!(
        error.as_ref(),
        StorageEnum::InfrastructureResult(InfrastructureResultKind::UnsupportedVersion {
            context: InfrastructureContext::PhysicalLayout,
        })
    ));

    let reopened = RocksDbBackend::initialize(path).unwrap();
    let observed = reopened
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                attempt
                    .get_batch(
                        vec![
                            INFRASTRUCTURE_FORMAT_KEY.to_vec(),
                            TTL_INDEX_FORMAT_KEY.to_vec(),
                            SYSTEM_STREAM_SCOPE_KEY.to_vec(),
                            MAINTENANCE_IDENTITY_KEY.to_vec(),
                        ],
                        ReadProtection::Snapshot,
                    )
                    .await
            })
        })
        .await
        .unwrap();
    assert_eq!(observed, vec![Some(vec![u8::MAX]), None, None, None]);
}
