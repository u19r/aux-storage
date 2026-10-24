//! Native attempt invariants across the async worker boundary.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use kv_backend::{
    AttemptMode, KvAttempt, KvBackend, KvMutation, KvRange, ReadProtection, rocksdb::RocksDbBackend,
};
use storage_types::{StorageEnum, StorageError, context::WrappedError as _};
use tokio::sync::oneshot;

#[tokio::test]
async fn given_empty_protected_range_when_a_writer_queues_then_it_commits_after_the_range_attempt()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let backend = Arc::new(RocksDbBackend::initialize(directory.path().join("db"))?);
    let (observed, observation) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let first_backend = Arc::clone(&backend);
    let first = tokio::spawn(async move {
        first_backend
            .with_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    let page = attempt
                        .scan(
                            KvRange {
                                entry_bounds: None,
                                begin: b"a".to_vec(),
                                end: b"z".to_vec(),
                                reverse: false,
                                limit: NonZeroU32::new(1).unwrap(),
                                max_bytes: NonZeroUsize::new(1024).unwrap(),
                            },
                            ReadProtection::Conflict,
                        )
                        .await?;
                    assert!(page.entries.is_empty());
                    observed.send(()).unwrap();
                    resume.await.unwrap();
                    Ok(())
                })
            })
            .await
    });
    observation.await?;
    let (prepared, preparation) = oneshot::channel();
    let second_backend = Arc::clone(&backend);
    let mut second = tokio::spawn(async move {
        second_backend
            .with_attempt(AttemptMode::ReadWriteItems, move |attempt| {
                Box::pin(async move {
                    attempt.stage(vec![KvMutation::Put {
                        key: b"k".to_vec(),
                        value: b"queued".to_vec(),
                    }])?;
                    prepared.send(()).unwrap();
                    Ok(())
                })
            })
            .await
    });
    preparation.await?;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut second)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    first.await??;
    second.await??;
    backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                assert_eq!(
                    attempt.get(b"k", ReadProtection::Snapshot).await?,
                    Some(b"queued".to_vec())
                );
                Ok(())
            })
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn given_read_attempt_when_another_writer_commits_then_point_and_range_keep_one_snapshot()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let backend = Arc::new(RocksDbBackend::initialize(directory.path().join("db"))?);
    backend
        .with_attempt(AttemptMode::ReadWriteItems, |attempt| {
            Box::pin(async move {
                attempt.native_stage(vec![KvMutation::Put {
                    key: b"item".to_vec(),
                    value: b"before".to_vec(),
                }])
            })
        })
        .await?;
    let writer = Arc::clone(&backend);
    backend
        .with_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                assert_eq!(
                    attempt
                        .native_get(
                            b"item",
                            ReadProtection::Snapshot,
                            kv_backend::KvValueAllowance::new(1024, 1024)
                        )
                        .await?,
                    Some(b"before".to_vec())
                );
                writer
                    .with_attempt(AttemptMode::ReadWriteItems, |write| {
                        Box::pin(async move {
                            write.native_stage(vec![KvMutation::Put {
                                key: b"item".to_vec(),
                                value: b"after".to_vec(),
                            }])
                        })
                    })
                    .await?;
                assert_eq!(
                    attempt
                        .native_get(
                            b"item",
                            ReadProtection::Snapshot,
                            kv_backend::KvValueAllowance::new(1024, 1024)
                        )
                        .await?,
                    Some(b"before".to_vec())
                );
                let page = attempt
                    .native_scan(
                        KvRange {
                            entry_bounds: None,
                            begin: b"item".to_vec(),
                            end: b"item\0".to_vec(),
                            reverse: false,
                            limit: NonZeroU32::MIN,
                            max_bytes: NonZeroUsize::new(1024).unwrap(),
                        },
                        ReadProtection::Snapshot,
                    )
                    .await?;
                assert_eq!(page.entries.len(), 1);
                assert_eq!(page.entries[0].value, b"before");
                Ok(())
            })
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn given_staged_writes_when_read_before_commit_then_private_writes_and_final_writes_persist()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let backend = RocksDbBackend::initialize(directory.path().join("db"))?;
    backend
        .with_attempt(AttemptMode::ReadWriteItems, |attempt| {
            Box::pin(async move {
                attempt.native_stage(vec![KvMutation::Put {
                    key: b"first".to_vec(),
                    value: b"one".to_vec(),
                }])?;
                assert_eq!(
                    attempt
                        .native_get(
                            b"first",
                            ReadProtection::Conflict,
                            kv_backend::KvValueAllowance::new(1024, 1024)
                        )
                        .await?,
                    Some(b"one".to_vec())
                );
                attempt.native_stage(vec![KvMutation::Put {
                    key: b"second".to_vec(),
                    value: b"two".to_vec(),
                }])
            })
        })
        .await?;
    backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                let values = attempt
                    .native_get_batch(
                        &[b"first".to_vec(), b"second".to_vec()],
                        ReadProtection::Snapshot,
                        kv_backend::KvValueAllowance::new(1024, 2048),
                    )
                    .await?;
                assert_eq!(values, vec![Some(b"one".to_vec()), Some(b"two".to_vec())]);
                Ok(())
            })
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn given_final_batch_with_invalid_mutation_when_settled_then_all_writes_roll_back()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let backend = RocksDbBackend::initialize(directory.path().join("db"))?;
    let result = backend
        .with_attempt(AttemptMode::ReadWriteItems, |attempt| {
            Box::pin(async move {
                attempt.native_stage(vec![
                    KvMutation::Put {
                        key: b"item".to_vec(),
                        value: b"uncommitted".to_vec(),
                    },
                    KvMutation::DeleteRange {
                        begin: b"a".to_vec(),
                        end: b"z".to_vec(),
                    },
                ])
            })
        })
        .await;
    let error = result.expect_err("invalid physical mutation must abort");
    assert!(
        matches!(error.to_enum(), StorageEnum::InternalServerError { message } if message == "RocksDB does not chunk logical values")
    );
    backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                if attempt
                    .native_get(
                        b"item",
                        ReadProtection::Snapshot,
                        kv_backend::KvValueAllowance::new(1024, 1024),
                    )
                    .await?
                    .is_some()
                {
                    return Err(StorageError::internal("failed final batch leaked a write"));
                }
                Ok(())
            })
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn given_queued_writer_when_preparing_early_then_native_reads_observe_the_previous_commit()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let backend = Arc::new(RocksDbBackend::initialize(directory.path().join("db"))?);
    let (opened, open) = oneshot::channel();
    let (finish, finished) = oneshot::channel();
    let first_backend = Arc::clone(&backend);
    let first = tokio::spawn(async move {
        first_backend
            .with_attempt(AttemptMode::ReadWriteItems, move |attempt| {
                Box::pin(async move {
                    attempt.native_stage(vec![KvMutation::Put {
                        key: b"item".to_vec(),
                        value: b"committed".to_vec(),
                    }])?;
                    assert_eq!(
                        attempt
                            .native_get(
                                b"item",
                                ReadProtection::Conflict,
                                kv_backend::KvValueAllowance::new(1024, 1024)
                            )
                            .await?,
                        Some(b"committed".to_vec())
                    );
                    let _ = opened.send(());
                    let _ = finished.await;
                    Ok(())
                })
            })
            .await
    });
    open.await?;
    let (prepared, preparation) = oneshot::channel();
    let second = tokio::spawn(async move {
        backend
            .with_attempt(AttemptMode::ReadWriteItems, move |attempt| {
                Box::pin(async move {
                    let _ = prepared.send(());
                    // The second native snapshot must start after the first
                    // commits, although its request preparation already ran
                    // while queued.
                    assert_eq!(
                        attempt
                            .native_get(
                                b"item",
                                ReadProtection::Conflict,
                                kv_backend::KvValueAllowance::new(1024, 1024)
                            )
                            .await?,
                        Some(b"committed".to_vec())
                    );
                    Ok(())
                })
            })
            .await
    });
    let prepared_early = tokio::time::timeout(std::time::Duration::from_secs(1), preparation)
        .await
        .is_ok();
    let _ = finish.send(());
    first.await??;
    second.await??;
    assert!(
        prepared_early,
        "queued writer deferred request preparation until admission"
    );
    Ok(())
}

#[tokio::test]
async fn given_cancelled_write_when_store_is_dropped_then_native_settlement_releases_the_file_lock()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("cancelled-write-db");
    let backend = Arc::new(RocksDbBackend::initialize(path.clone())?);
    let (started, start) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let request_backend = Arc::clone(&backend);
    let request = tokio::spawn(async move {
        request_backend
            .with_attempt(AttemptMode::ReadWriteItems, move |attempt| {
                Box::pin(async move {
                    attempt.native_stage(vec![KvMutation::Put {
                        key: b"retained".to_vec(),
                        value: b"committed".to_vec(),
                    }])?;
                    started.send(()).unwrap();
                    resume.await.unwrap();
                    Ok(())
                })
            })
            .await
    });
    start.await?;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    drop(backend);
    assert!(
        RocksDbBackend::initialize(path.clone()).is_err(),
        "the pending supervisor must retain the native handle"
    );
    release.send(()).unwrap();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let reopened = loop {
        match RocksDbBackend::initialize(path.clone()) {
            Ok(backend) => break backend,
            Err(error) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(error.into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    };
    reopened
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                assert_eq!(
                    attempt
                        .native_get(
                            b"retained",
                            ReadProtection::Snapshot,
                            kv_backend::KvValueAllowance::new(1024, 1024)
                        )
                        .await?,
                    Some(b"committed".to_vec())
                );
                Ok(())
            })
        })
        .await?;
    Ok(())
}
