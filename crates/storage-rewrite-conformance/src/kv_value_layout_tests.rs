//! Native reconstruction proof for the common KV value layout.

use std::num::{NonZeroU32, NonZeroUsize};

use kv_backend::{AttemptMode, KvAttempt, KvBackend, KvMutation, KvRange, ReadProtection};
use storage_types::{AttemptPlan, DeadlineClass, StorageEnum, context::WrappedError as _};

async fn verify_reconstruction<B: KvBackend>(backend: B) -> anyhow::Result<()> {
    let large = vec![42; 180_000];
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            move |attempt| {
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
            },
        )
        .await?;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
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
            },
        )
        .await?;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt.native_stage(vec![KvMutation::Put {
                        key: b"bad".to_vec(),
                        value: vec![9],
                    }])
                })
            },
        )
        .await?;
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .get_batch(
                            vec![b"a".to_vec(), b"bad".to_vec()],
                            ReadProtection::Snapshot,
                        )
                        .await
                })
            },
        )
        .await;
    let error = result.expect_err("invalid value layout must fail reconstruction");
    assert!(
        matches!(error.to_enum(), StorageEnum::InternalServerError { message } if message == "KV value has an invalid layout tag")
    );
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt.stage(vec![
                        KvMutation::Delete { key: b"a".to_vec() },
                        KvMutation::DeleteUnchunked { key: b"b".to_vec() },
                    ])
                })
            },
        )
        .await?;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let key = b"observed-primary".to_vec();
                    let mut layouts = Vec::new();
                    assert_eq!(
                        attempt
                            .get_batch_primary_layouts(vec![key.clone()], &mut layouts)
                            .await?,
                        vec![None]
                    );
                    // Two replacements in one expansion must use the layout
                    // left by the preceding mutation, rather than the original
                    // absence.
                    attempt.stage_with_primary_layouts(
                        vec![
                            KvMutation::Put {
                                key: key.clone(),
                                value: vec![7; 180_000],
                            },
                            KvMutation::Put {
                                key: key.clone(),
                                value: b"small".to_vec(),
                            },
                        ],
                        &mut layouts,
                    )?;
                    assert_eq!(
                        attempt
                            .get_batch_primary_layouts(vec![key.clone()], &mut layouts)
                            .await?,
                        vec![Some(b"small".to_vec())]
                    );
                    assert!(layouts.iter().all(|(_, has_chunks)| !has_chunks));
                    // A later native read must preserve the layout established
                    // by the earlier write, including across dependent flushes.
                    attempt.stage_with_primary_layouts(
                        vec![KvMutation::Put {
                            key: key.clone(),
                            value: vec![9; 230_000],
                        }],
                        &mut layouts,
                    )?;
                    assert_eq!(
                        attempt
                            .get_batch_primary_layouts(vec![key.clone()], &mut layouts)
                            .await?,
                        vec![Some(vec![9; 230_000])]
                    );
                    if attempt.native_value_limit() == usize::MAX {
                        assert!(
                            layouts.is_empty(),
                            "unlimited values retain no cleanup keys"
                        );
                    } else {
                        assert_eq!(layouts.len(), 1);
                        assert!(layouts[0].1);
                    }
                    attempt.stage_with_primary_layouts(
                        vec![KvMutation::Put {
                            key: key.clone(),
                            value: b"last".to_vec(),
                        }],
                        &mut layouts,
                    )?;
                    assert_eq!(
                        attempt
                            .get_batch_primary_layouts(vec![key.clone()], &mut layouts)
                            .await?,
                        vec![Some(b"last".to_vec())]
                    );
                    assert!(layouts.iter().all(|(_, has_chunks)| !has_chunks));
                    attempt.stage_with_primary_layouts(
                        vec![KvMutation::Delete { key: key.clone() }],
                        &mut layouts,
                    )?;
                    assert_eq!(
                        attempt
                            .get_batch_primary_layouts(vec![key], &mut layouts)
                            .await?,
                        vec![None]
                    );
                    Ok(())
                })
            },
        )
        .await?;
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    assert_eq!(
                        attempt
                            .get_batch(
                                vec![b"a".to_vec(), b"b".to_vec(), b"observed-primary".to_vec()],
                                ReadProtection::Snapshot
                            )
                            .await?,
                        vec![None, None, None],
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
            },
        )
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
        storage_types::AttemptDeadlines::default(),
        256,
    )?)
    .await
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
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
            reply_byte_limit: storage_types::FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT,
            report_conflicting_keys: false,
            transaction_byte_limit: None,
            attempt_deadlines: storage_types::AttemptDeadlines::default(),
        },
    )
    .await?;
    verify_reconstruction(backend).await
}
