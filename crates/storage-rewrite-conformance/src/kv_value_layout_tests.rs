//! Native reconstruction proof for the common KV value layout.

use std::num::{NonZeroU32, NonZeroUsize};

use kv_backend::{AttemptMode, KvAttempt, KvBackend, KvMutation, KvRange, ReadProtection};
use storage_types::{StorageEnum, context::WrappedError as _};

async fn verify_reconstruction<B: KvBackend>(backend: B) -> anyhow::Result<()> {
    let large = vec![42; 180_000];
    backend
        .with_attempt(AttemptMode::ReadWriteItems, move |attempt| {
            Box::pin(async move {
                attempt.stage(vec![
                    KvMutation::Put {
                        key: b"a".to_vec(),
                        value: large,
                    },
                    KvMutation::PutUnchunked {
                        key: b"b".to_vec(),
                        value: b"small123".to_vec(),
                    },
                ])
            })
        })
        .await?;
    backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                let values = attempt
                    .get_batch(
                        vec![b"a".to_vec(), b"missing".to_vec(), b"b".to_vec()],
                        ReadProtection::Snapshot,
                    )
                    .await?;
                assert_eq!(
                    values,
                    vec![Some(vec![42; 180_000]), None, Some(b"small123".to_vec())]
                );
                let page = attempt
                    .scan(
                        KvRange {
                            entry_bounds: None,
                            begin: b"a".to_vec(),
                            end: b"c".to_vec(),
                            reverse: false,
                            limit: NonZeroU32::new(3).unwrap(),
                            max_bytes: NonZeroUsize::new(200_000).unwrap(),
                        },
                        ReadProtection::Snapshot,
                    )
                    .await?;
                assert_eq!(page.entries.len(), 2);
                assert_eq!(page.entries[0].value, vec![42; 180_000]);
                assert_eq!(page.entries[1].value, b"small123");
                Ok(())
            })
        })
        .await?;
    backend
        .with_attempt(AttemptMode::ReadWriteItems, |attempt| {
            Box::pin(async move {
                attempt.native_stage(vec![KvMutation::Put {
                    key: b"bad".to_vec(),
                    value: vec![9],
                }])
            })
        })
        .await?;
    let result = backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                attempt
                    .get_batch(
                        vec![b"a".to_vec(), b"bad".to_vec()],
                        ReadProtection::Snapshot,
                    )
                    .await
            })
        })
        .await;
    let error = result.expect_err("invalid value layout must fail reconstruction");
    assert!(
        matches!(error.to_enum(), StorageEnum::InternalServerError { message } if message == "KV value has an invalid layout tag")
    );
    backend
        .with_attempt(AttemptMode::ReadWriteItems, |attempt| {
            Box::pin(async move {
                attempt.stage(vec![
                    KvMutation::Delete { key: b"a".to_vec() },
                    KvMutation::DeleteUnchunked { key: b"b".to_vec() },
                ])
            })
        })
        .await?;
    backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                assert_eq!(
                    attempt
                        .get_batch(vec![b"a".to_vec(), b"b".to_vec()], ReadProtection::Snapshot)
                        .await?,
                    vec![None, None],
                );
                let chunks = attempt
                    .native_scan(
                        KvRange {
                            entry_bounds: None,
                            begin: b"\0chunk/".to_vec(),
                            end: b"\0chunk0".to_vec(),
                            reverse: false,
                            limit: NonZeroU32::new(10).unwrap(),
                            max_bytes: NonZeroUsize::new(200_000).unwrap(),
                        },
                        ReadProtection::Snapshot,
                    )
                    .await?;
                assert!(
                    chunks.entries.is_empty(),
                    "variable-sized deletes must reclaim their chunks"
                );
                Ok(())
            })
        })
        .await?;
    Ok(())
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocksdb_mixed_values_when_reconstructed_then_inline_large_and_invalid_values_are_checked()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    verify_reconstruction(kv_backend::rocksdb::RocksDbBackend::initialize(
        directory.path().join("db"),
    )?)
    .await
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
async fn given_foundationdb_mixed_values_when_reconstructed_then_inline_chunked_and_invalid_values_are_checked()
-> anyhow::Result<()> {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE")?;
    let prefix = format!("test-kv-value-layout-{}", uuid::Uuid::now_v7());
    let backend = kv_backend::foundationdb::FoundationDbBackend::initialize(
        kv_backend::foundationdb::FoundationDbOptions {
            cluster_file: Some(&cluster),
            subspace_prefix: Some(prefix.as_bytes()),
            tenant_name: None,
            cache_read_version_ms: 0,
            report_conflicting_keys: false,
            transaction_byte_limit: None,
        },
    )
    .await?;
    verify_reconstruction(backend).await
}
