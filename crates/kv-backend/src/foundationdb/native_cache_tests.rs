//! Native client characterization against the disposable owned fixture.

use foundationdb::{Database, Transaction, options::TransactionOption};
#[cfg(feature = "test-support")]
use storage_types::DeadlineClass;

fn namespace_key(namespace: &'static [u8], name: &'static [u8]) -> Vec<u8> {
    use foundationdb_tuple::{Bytes, Element, pack};
    let mut key = pack(&(
        Element::Bytes(Bytes::from(&b"native-cache"[..])),
        Element::Bytes(Bytes::from(namespace)),
    ));
    key.extend_from_slice(name);
    key
}

const METADATA_KEY: &[u8] = b"\xff/metadataVersion";

async fn open_database() -> Database {
    super::network::ensure_network(super::network::NetworkPolicy {
        cache_read_version_ms: 100,
        reply_byte_limit: storage_types::FOUNDATIONDB_DEFAULT_REPLY_BYTE_LIMIT,
    })
    .unwrap();
    let database = Database::from_path(
        &std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned"),
    )
    .unwrap();
    // A newly configured fixture can change protocol while the client connects.
    // Only this read-only readiness observation retries; publications never
    // replay.
    database
        .run(|transaction, _| async move {
            transaction.get_read_version().await?;
            Ok::<_, foundationdb::FdbBindingError>(())
        })
        .await
        .unwrap();
    database
}

fn start_transaction(database: &Database, policy: Option<TransactionOption>) -> Transaction {
    let transaction = database.create_trx().unwrap();
    transaction
        .set_option(TransactionOption::Timeout(10_000))
        .unwrap();
    transaction
        .set_option(TransactionOption::ReadSystemKeys)
        .unwrap();
    if let Some(policy) = policy {
        transaction.set_option(policy).unwrap();
    }
    transaction
}

async fn read_metadata(transaction: &Transaction) -> Vec<u8> {
    let value = transaction.get(METADATA_KEY, false).await.unwrap().unwrap();
    assert_eq!(value.len(), 10);
    value.to_vec()
}

async fn print_status(database: &Database, label: &str) -> (u64, u64) {
    let status = database.get_client_status().await.unwrap();
    let status: serde_json::Value = serde_json::from_slice(&status).unwrap();
    let connections = status["DatabaseStatus"]["Connections"].as_array().unwrap();
    let sent = connections
        .iter()
        .map(|connection| connection["BytesSent"].as_u64().unwrap())
        .sum();
    let received = connections
        .iter()
        .map(|connection| connection["BytesReceived"].as_u64().unwrap())
        .sum();
    println!("native_client_status {label}: {status}");
    (sent, received)
}

#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_native_cache_when_versions_and_metadata_are_read_then_actual_behavior_is_reported() {
    let database = open_database().await;
    let publisher = start_transaction(&database, None);
    publisher
        .set_option(TransactionOption::AccessSystemKeys)
        .unwrap();
    publisher.update_metadata_version();
    publisher.set(&namespace_key(b"a", b"item"), b"first");
    publisher.commit().await.unwrap();
    print_status(&database, "before").await;
    let fresh = start_transaction(&database, None);
    let version = fresh.get_read_version().await.unwrap();
    let metadata = read_metadata(&fresh).await;
    let baseline = print_status(&database, "after_seed").await;
    let mut reused = 0;
    let mut versions = Vec::new();
    for _ in 0..16 {
        let cached = start_transaction(&database, Some(TransactionOption::UseGrvCache));
        let cached_version = cached.get_read_version().await.unwrap();
        reused += usize::from(cached_version == version);
        versions.push(cached_version);
        assert_eq!(read_metadata(&cached).await, metadata);
    }
    println!(
        "native_cache sample_count=16 seed_version_reuse={reused} distinct_versions={}",
        versions
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    );
    let cached_status = print_status(&database, "after_cached").await;
    println!(
        "native_cache cache_sample_socket_bytes_sent={} received={}",
        cached_status.0 - baseline.0,
        cached_status.1 - baseline.1
    );
    // Wait beyond the configured maximum age to observe expiry/background
    // refresh, never to claim a latency or throughput budget.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let after_wait = start_transaction(&database, Some(TransactionOption::UseGrvCache));
    let after_version = after_wait.get_read_version().await.unwrap();
    println!(
        "native_cache after_wait_version_equal_seed={}",
        after_version == version
    );
    read_metadata(&after_wait).await;
    let idle_status = print_status(&database, "after_expiry").await;
    println!(
        "native_cache idle_socket_bytes_sent={} received={}",
        idle_status.0 - cached_status.0,
        idle_status.1 - cached_status.1
    );
    let fresh = start_transaction(&database, None);
    let fresh_version = fresh.get_read_version().await.unwrap();
    assert!(fresh_version >= after_version);
    read_metadata(&fresh).await;
    let fresh_status = print_status(&database, "after_fresh").await;
    println!(
        "native_cache fresh_socket_bytes_sent={} received={}",
        fresh_status.0 - idle_status.0,
        fresh_status.1 - idle_status.1
    );
}

#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_protected_global_metadata_when_other_namespace_publishes_then_stale_writer_conflicts()
 {
    let database = open_database().await;
    let initial = start_transaction(&database, None);
    initial
        .set_option(TransactionOption::AccessSystemKeys)
        .unwrap();
    initial.update_metadata_version();
    initial.set(&namespace_key(b"a", b"item"), b"first");
    initial.commit().await.unwrap();
    let stale = start_transaction(&database, None);
    let previous_metadata = read_metadata(&stale).await;
    stale.set(&namespace_key(b"a", b"decision"), b"old");
    let publisher = start_transaction(&database, None);
    publisher
        .set_option(TransactionOption::AccessSystemKeys)
        .unwrap();
    publisher.update_metadata_version();
    publisher.set(&namespace_key(b"b", b"item"), b"second");
    let stamp = publisher.get_versionstamp();
    publisher.commit().await.unwrap();
    let stamp = stamp.await.unwrap();
    let current = start_transaction(&database, None);
    let current_metadata = read_metadata(&current).await;
    assert_ne!(current_metadata, previous_metadata);
    assert_eq!(current_metadata.as_slice(), &stamp[..]);
    assert_eq!(
        &current
            .get(&namespace_key(b"b", b"item"), false)
            .await
            .unwrap()
            .unwrap()[..],
        b"second"
    );
    assert_eq!(stale.commit().await.unwrap_err().code(), 1020);
    let check = start_transaction(&database, None);
    assert!(
        check
            .get(&namespace_key(b"a", b"decision"), false)
            .await
            .unwrap()
            .is_none()
    );
    println!(
        "global_metadata bytes=10 publication_atomic=true cross_namespace_conflict=1020 \
         full_stamp_equal=true"
    );
}

#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_concurrent_publications_when_committed_then_full_metadata_stamp_preserves_batch_order()
 {
    let database = open_database().await;
    let mut publications = Vec::new();
    for ordinal in 0u8..16 {
        let publisher = start_transaction(&database, None);
        publisher
            .set_option(TransactionOption::AccessSystemKeys)
            .unwrap();
        publisher.update_metadata_version();
        publisher.set(&[b'n', b'7', ordinal], &[ordinal]);
        publications.push(async move {
            let stamp = publisher.get_versionstamp();
            publisher.commit().await.unwrap();
            stamp.await.unwrap().to_vec()
        });
    }
    let mut stamps = futures::future::join_all(publications).await;
    stamps.sort();
    stamps.dedup();
    assert_eq!(stamps.len(), 16);
    let shared_version_pairs = stamps
        .windows(2)
        .filter(|pair| pair[0][..8] == pair[1][..8])
        .count();
    let current = start_transaction(&database, None);
    assert_eq!(read_metadata(&current).await, *stamps.last().unwrap());
    println!(
        "metadata_batch_order publications=16 unique_full_stamps=16 \
         same_version_adjacent_pairs={shared_version_pairs} final_token_is_maximum=true"
    );
}

#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_old_snapshot_when_observed_value_is_written_then_completion_distinguishes_conflict_from_no_write()
 {
    let database = open_database().await;
    let key = namespace_key(b"optimized", b"item");
    let initial = start_transaction(&database, None);
    initial.set(&key, b"first");
    initial.commit().await.unwrap();
    let previous = start_transaction(&database, None);
    let previous_version = previous.get_read_version().await.unwrap();
    assert_eq!(
        &previous.get(&key, false).await.unwrap().unwrap()[..],
        b"first"
    );
    drop(previous);

    let writer = start_transaction(&database, None);
    writer.set(&key, b"second");
    writer.commit().await.unwrap();

    // Pin the old version explicitly so the test does not race the client's
    // cache refresh. This has the same snapshot semantics as a cached version.
    let stale = start_transaction(&database, None);
    stale.set_read_version(previous_version);
    let observed = stale.get(&key, false).await.unwrap().unwrap();
    assert_eq!(&observed[..], b"first");
    stale.set(&key, &observed);
    let optimized = match stale.commit().await {
        Ok(committed) => {
            assert_eq!(committed.committed_version().unwrap(), -1);
            true
        }
        Err(error) => {
            assert_eq!(error.code(), 1020, "the stale mutation must conflict");
            false
        }
    };

    let fresh = start_transaction(&database, None);
    let current = fresh.get(&key, false).await.unwrap().unwrap();
    assert_eq!(
        &current[..],
        b"second",
        "the stale attempt must not change data"
    );
    assert_ne!(
        &current[..],
        &observed[..],
        "a fresh condition must use the new value"
    );
    fresh.set(&key, &observed);
    let restored_version = fresh.commit().await.unwrap().committed_version().unwrap();
    assert!(
        restored_version >= 0,
        "the changed-value set must actually commit"
    );
    let check = start_transaction(&database, None);
    assert_eq!(
        &check.get(&key, false).await.unwrap().unwrap()[..],
        b"first"
    );
    println!(
        "same_value_old_snapshot optimized_no_write={optimized} fresh_changed_observation=true \
         fresh_mutation_committed=true"
    );
}

#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fresh_observation_when_later_writer_commits_then_stale_attempt_keeps_later_value() {
    let database = open_database().await;
    let key = namespace_key(b"fresh-optimized", b"item");
    let initial = start_transaction(&database, None);
    initial.set(&key, b"first");
    initial.commit().await.unwrap();

    let fresh = start_transaction(&database, None);
    let observed = fresh.get(&key, false).await.unwrap().unwrap();
    let writer = start_transaction(&database, None);
    writer.set(&key, b"second");
    writer.commit().await.unwrap();
    fresh.set(&key, &observed);
    let optimized = match fresh.commit().await {
        Ok(committed) => {
            assert_eq!(committed.committed_version().unwrap(), -1);
            true
        }
        Err(error) => {
            assert_eq!(error.code(), 1020);
            false
        }
    };
    let check = start_transaction(&database, None);
    assert_eq!(
        &check.get(&key, false).await.unwrap().unwrap()[..],
        b"second"
    );
    println!("same_value_fresh_snapshot optimized_no_write={optimized} later_value_preserved=true");
}

#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_read_only_commit_when_completed_then_native_version_reports_no_mutation() {
    let database = open_database().await;
    let read = start_transaction(&database, None);
    read.get(&namespace_key(b"read-only", b"item"), false)
        .await
        .unwrap();
    assert_eq!(
        read.commit().await.unwrap().committed_version().unwrap(),
        -1
    );
}

pub(super) fn trace_transaction(transaction: &Transaction, label: &str) {
    transaction
        .set_option(TransactionOption::DebugTransactionIdentifier(
            label.to_owned(),
        ))
        .unwrap();
    transaction
        .set_option(TransactionOption::LogTransaction)
        .unwrap();
    transaction
        .set_option(TransactionOption::ServerRequestTracing)
        .unwrap();
}

#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fresh_metadata_when_native_tracing_is_enabled_then_controls_are_labelled() {
    let database = open_database().await;
    let seed = start_transaction(&database, None);
    seed.set_option(TransactionOption::AccessSystemKeys)
        .unwrap();
    seed.update_metadata_version();
    seed.set(&namespace_key(b"network", b"point"), b"positive-control");
    for ordinal in 0u8..4 {
        let mut key = namespace_key(b"network-range", b"row");
        key.push(ordinal);
        seed.set(&key, &[ordinal; 16_384]);
    }
    seed.commit().await.unwrap();
    let fresh = start_transaction(&database, None);
    trace_transaction(&fresh, "n7-fresh-metadata");
    fresh.get_read_version().await.unwrap();
    read_metadata(&fresh).await;
    drop(fresh);
    let point = start_transaction(&database, None);
    trace_transaction(&point, "n7-point-control");
    assert_eq!(
        &point
            .get(&namespace_key(b"network", b"point"), false)
            .await
            .unwrap()
            .unwrap()[..],
        b"positive-control"
    );
    drop(point);
    let range = start_transaction(&database, None);
    trace_transaction(&range, "n7-range-control");
    let begin = namespace_key(b"network-range", b"");
    let end = crate::range::prefix_upper_bound(&begin).unwrap();
    let mut options = foundationdb::RangeOption::from((begin.as_slice(), end.as_slice()));
    options.mode = foundationdb::options::StreamingMode::Exact;
    options.limit = Some(10);
    options.target_bytes = 1_000;
    let mut rows = 0;
    let mut replies = 0;
    loop {
        let values = range.get_range(&options, replies + 1, false).await.unwrap();
        replies += 1;
        rows += values.len();
        let Some(next) = options.next_range(&values) else {
            break;
        };
        options = next;
    }
    assert_eq!(rows, 4);
    assert!(replies > 1);
    println!("native_network range_rows={rows} sdk_range_completions={replies}");
    drop(range);
    // TraceBatch emits periodically. Keep the owned process alive through its
    // flush.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    println!(
        "native_network controls: fresh_metadata=10_bytes point=positive-control range_rows=4"
    );
    drop(database);
    super::shutdown_foundationdb_network().unwrap();
}

#[tokio::test]
#[ignore = "requires one isolated nextest process: just test-fdb-owned"]
async fn given_cached_metadata_when_native_tracing_is_enabled_then_socket_bytes_are_reported() {
    let database = open_database().await;
    let publisher = start_transaction(&database, None);
    publisher
        .set_option(TransactionOption::AccessSystemKeys)
        .unwrap();
    publisher.update_metadata_version();
    publisher.commit().await.unwrap();
    let seed = start_transaction(&database, None);
    let metadata = read_metadata(&seed).await;
    drop(seed);
    let before = print_status(&database, "network_cached_before").await;
    for ordinal in 0..16 {
        let cached = start_transaction(&database, Some(TransactionOption::UseGrvCache));
        trace_transaction(&cached, &format!("n7-cached-metadata-{ordinal:02}"));
        cached.get_read_version().await.unwrap();
        assert_eq!(read_metadata(&cached).await, metadata);
    }
    let after = print_status(&database, "network_cached_after").await;
    println!(
        "native_network cached_metadata: sdk_completions=16 socket_sent={} socket_received={}",
        after.0 - before.0,
        after.1 - before.1
    );
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    drop(database);
    super::shutdown_foundationdb_network().unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_initialized_cached_policy_when_native_attempt_has_no_mutation_then_fresh_validation_is_required()
 {
    use storage_types::{AttemptMode, AttemptPlan, ReadVersionPolicy};

    use crate::KvBackend as _;
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    let backend = super::FoundationDbBackend::initialize(super::FoundationDbOptions {
        cluster_file: Some(&cluster),
        cache_read_version_ms: 100,
        subspace_prefix: Some(b"cached-write-native"),
        ..Default::default()
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "native cached initialization: {error:#?}; {:?}",
            error.as_ref()
        )
    });
    assert_eq!(
        backend.ordinary_write_read_version_policy(),
        ReadVersionPolicy::Cached
    );
    let mut plan = AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write);
    plan.read_version = ReadVersionPolicy::Cached;
    let result = backend
        .with_attempt(plan.start(backend.attempt_deadlines()), |_| {
            Box::pin(async { Ok(7) })
        })
        .await;
    assert_eq!(
        result.unwrap_err().attempt_kind(),
        Some(storage_types::AttemptKind::FreshValidation)
    );
    plan.read_version = ReadVersionPolicy::Fresh;
    assert_eq!(
        backend
            .with_attempt(plan.start(backend.attempt_deadlines()), |_| {
                Box::pin(async { Ok(7) })
            })
            .await
            .unwrap(),
        7
    );
    let zero = super::FoundationDbBackend::initialize(super::FoundationDbOptions {
        cluster_file: Some(&cluster),
        cache_read_version_ms: 0,
        subspace_prefix: Some(b"cached-write-native"),
        ..Default::default()
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "native cached initialization: {error:#?}; {:?}",
            error.as_ref()
        )
    });
    assert_eq!(
        zero.ordinary_write_read_version_policy(),
        ReadVersionPolicy::Fresh
    );
    zero.delete_namespace_keys().await.unwrap();
}

#[cfg(feature = "test-support")]
async fn cached_backend(prefix: &'static [u8]) -> super::FoundationDbBackend {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    super::FoundationDbBackend::initialize(super::FoundationDbOptions {
        cluster_file: Some(&cluster),
        cache_read_version_ms: 100,
        subspace_prefix: Some(prefix),
        ..Default::default()
    })
    .await
    .unwrap()
}

#[cfg(feature = "test-support")]
fn cached_plan() -> storage_types::AttemptPlan {
    let mut request = storage_types::AttemptPlan::new(
        storage_types::AttemptMode::ReadWriteItems,
        storage_types::DeadlineClass::Write,
    );
    request.read_version = storage_types::ReadVersionPolicy::Cached;
    request
}

#[cfg(feature = "test-support")]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_native_cached_old_snapshot_when_equal_bytes_are_set_then_result_is_discarded_and_fresh_redecides()
 {
    use crate::{KvAttempt as _, KvBackend as _};
    let backend = cached_backend(b"cached-native-old").await;
    #[cfg(feature = "fault-injection")]
    let committed = std::sync::Arc::new(NativeCommitProbe::default());
    #[cfg(feature = "fault-injection")]
    let backend = backend.with_fault_probe(committed.clone());
    let database = open_database().await;
    let physical = backend_key(b"cached-native-old", b"item");
    let initial = start_transaction(&database, None);
    initial.set(&physical, b"first");
    initial.commit().await.unwrap();
    let old = start_transaction(&database, None);
    let version = old.get_read_version().await.unwrap();
    assert_eq!(
        &old.get(&physical, false).await.unwrap().unwrap()[..],
        b"first"
    );
    drop(old);
    let publisher = start_transaction(&database, None);
    publisher.set(&physical, b"second");
    publisher.commit().await.unwrap();
    backend.pin_next_read_version(version);
    let result = backend
        .with_attempt(
            cached_plan().start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let observed = attempt
                        .native_get(
                            b"item",
                            crate::ReadProtection::Conflict,
                            crate::KvValueAllowance::new(100, 100),
                        )
                        .await?
                        .unwrap();
                    assert_eq!(observed, b"first");
                    attempt.native_stage(vec![crate::KvMutation::Put {
                        key: b"item".to_vec(),
                        value: observed,
                    }])?;
                    Ok("discarded mutation result")
                })
            },
        )
        .await;
    assert_eq!(
        result.unwrap_err().attempt_kind(),
        Some(storage_types::AttemptKind::FreshValidation)
    );
    let check = start_transaction(&database, None);
    assert_eq!(
        &check.get(&physical, false).await.unwrap().unwrap()[..],
        b"second"
    );
    #[cfg(feature = "fault-injection")]
    println!(
        "native_cached_old_snapshot optimized_no_write={}",
        committed.0.load(std::sync::atomic::Ordering::SeqCst) == 1
    );
    let mut fresh = cached_plan();
    fresh.read_version = storage_types::ReadVersionPolicy::Fresh;
    let result = backend
        .with_attempt(fresh.start(backend.attempt_deadlines()), |attempt| {
            Box::pin(async move {
                let observed = attempt
                    .native_get(
                        b"item",
                        crate::ReadProtection::Conflict,
                        crate::KvValueAllowance::new(100, 100),
                    )
                    .await?
                    .unwrap();
                assert_eq!(observed, b"second");
                Ok("fresh rejected old condition")
            })
        })
        .await
        .unwrap();
    assert_eq!(result, "fresh rejected old condition");
    println!(
        "native_cached_old_snapshot typed_control=true fresh_redecision=true \
         actual_cache_hit=unclaimed optimization_branch=see_raw_sdk_characterization"
    );
    backend.delete_namespace_keys().await.unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_native_cached_poison_when_callback_returns_conflict_then_stronger_rejection_prevents_commit()
 {
    use storage_types::{AttemptKind, AttemptPhase, NativeUnavailable, StorageError};

    use crate::{KvAttempt as _, KvBackend as _};
    let backend = cached_backend(b"cached-native-poison").await;
    let database = open_database().await;
    for (error, expected) in [
        (
            storage_types::AttemptFailure::CAPACITY.into(),
            &[AttemptKind::Capacity, AttemptKind::TransactionBytes][..],
        ),
        (
            storage_types::AttemptFailure::Unavailable(NativeUnavailable::Deadline(
                AttemptPhase::Statement,
            ))
            .into(),
            &[AttemptKind::Unavailable, AttemptKind::Deadline][..],
        ),
        (
            storage_types::AttemptFailure::TRANSACTION_BYTES.into(),
            &[AttemptKind::Capacity, AttemptKind::TransactionBytes][..],
        ),
    ] {
        let result = backend
            .with_attempt(
                cached_plan().start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        attempt.native_stage(vec![crate::KvMutation::Put {
                            key: b"item".to_vec(),
                            value: b"must-not-commit".to_vec(),
                        }])?;
                        let rejected: storage_types::StorageResult<()> =
                            attempt.settlement().begin_call()?.complete(Err(error));
                        assert!(rejected.is_err());
                        Err::<(), _>(StorageError::from(storage_types::StorageEnum::Attempt(
                            storage_types::AttemptFailure::Conflict(
                                storage_types::ConflictCause::Logical(
                                    "synthetic accompanying cached conflict",
                                ),
                            ),
                        )))
                    })
                },
            )
            .await
            .unwrap_err();
        assert!(
            result
                .attempt_kind()
                .is_some_and(|kind| expected.contains(&kind)),
            "{result:?}"
        );
        let check = start_transaction(&database, None);
        assert!(
            check
                .get(&backend_key(b"cached-native-poison", b"item"), false)
                .await
                .unwrap()
                .is_none()
        );
    }
    backend.delete_namespace_keys().await.unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_native_cached_permanent_poison_when_callback_returns_conflict_then_fresh_control_is_forbidden()
 {
    use crate::{KvAttempt as _, KvBackend as _};
    let backend = cached_backend(b"cached-native-permanent").await;
    let result = backend
        .with_attempt(
            cached_plan().start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt.native_stage(vec![crate::KvMutation::Put {
                        key: b"item".to_vec(),
                        value: b"must-not-commit".to_vec(),
                    }])?;
                    let error = attempt
                        .native_stage_versionstamped_value(
                            b"invalid-stamp".to_vec(),
                            b"short".to_vec(),
                            100,
                        )
                        .unwrap_err();
                    assert!(
                        !error
                            .attempt_kind()
                            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown),
                        "invalid physical stamp must be permanent"
                    );
                    Err::<(), _>(
                        storage_types::StorageEnum::Attempt(
                            storage_types::AttemptFailure::Conflict(
                                storage_types::ConflictCause::Logical(
                                    "accompanying cached conflict",
                                ),
                            ),
                        )
                        .into(),
                    )
                })
            },
        )
        .await
        .unwrap_err();
    let database = open_database().await;
    let check = start_transaction(&database, None);
    assert!(
        check
            .get(&backend_key(b"cached-native-permanent", b"item"), false)
            .await
            .unwrap()
            .is_none()
    );
    backend.delete_namespace_keys().await.unwrap();
    assert!(
        !result
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown),
        "permanent native rejection must survive accompanying Conflict"
    );
    assert!(
        matches!(
            result.as_ref(),
            storage_types::StorageEnum::InfrastructureResult(
                storage_types::InfrastructureResultKind::InvalidBound {
                    context: storage_types::InfrastructureContext::NativeCapacity
                }
            )
        ),
        "the original physical failure must survive"
    );
}

#[cfg(feature = "test-support")]
fn backend_key(namespace: &[u8], key: &[u8]) -> Vec<u8> {
    use foundationdb_tuple::{Bytes, Element, pack};
    let mut physical = pack(&(Element::Bytes(Bytes::from(namespace)), Element::Nil));
    physical.extend_from_slice(key);
    physical
}

#[cfg(all(feature = "fault-injection", feature = "test-support"))]
#[derive(Default)]
struct NativeCommitProbe(std::sync::atomic::AtomicUsize);

#[cfg(all(feature = "fault-injection", feature = "test-support"))]
impl crate::KvFaultProbe for NativeCommitProbe {
    fn fail(&self, _: crate::KvFaultCall) -> Option<storage_types::StorageError> {
        None
    }
    fn observe(&self, event: crate::KvNativeEvent) {
        if event == crate::KvNativeEvent::Commit {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[cfg(all(feature = "fault-injection", feature = "test-support"))]
#[derive(Default)]
struct CorruptSnapshotEvents(std::sync::Mutex<Vec<crate::KvNativeEvent>>);

#[cfg(all(feature = "fault-injection", feature = "test-support"))]
impl crate::KvFaultProbe for CorruptSnapshotEvents {
    fn fail(&self, _: crate::KvFaultCall) -> Option<storage_types::StorageError> {
        None
    }
    fn observe(&self, event: crate::KvNativeEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[cfg(all(feature = "fault-injection", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_matching_old_cached_snapshot_when_public_write_reads_corruption_then_permanent_failure_survives()
 {
    use futures::FutureExt as _;
    use storage_provider::{DynamoOperations as _, DynamoPort as _};

    use crate::{KvAttempt as _, KvBackend as _};
    let namespace = storage_types::ambient::uuid_v4().to_string();
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    let events = std::sync::Arc::new(CorruptSnapshotEvents::default());
    let backend = super::FoundationDbBackend::initialize(super::FoundationDbOptions {
        cluster_file: Some(&cluster),
        cache_read_version_ms: 100,
        subspace_prefix: Some(namespace.as_bytes()),
        ..Default::default()
    })
    .await
    .unwrap()
    .with_fault_probe(events.clone());
    let store =
        crate::KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
            .await
            .unwrap()
            .with_metadata_cache_ttl(std::time::Duration::from_secs(60));
    println!("OWNED_FIXTURE created foundationdb namespace={namespace}");
    let outcome = std::panic::AssertUnwindSafe(async {
        let create = || serde_json::from_value(serde_json::json!({"TableName":"old_corruption",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]
        })).unwrap();
        store.create_table(create()).await.unwrap();
        let put = || storage_types::PutItemRequest::try_from(serde_json::json!({"TableName":"old_corruption","Item":{"pk":{"S":"one"}}})).unwrap();
        store.put_item(put()).await.unwrap();
        let cache = store.metadata_cache();
        let key = storage_provider::DynamoPort::with_attempt(&store, storage_types::AttemptPlan::new(storage_types::AttemptMode::ReadOnly, storage_types::DeadlineClass::PointRead).start(storage_types::AttemptDeadlineSource::attempt_deadlines(&store)), move |attempt| Box::pin(async move {
            let name = storage_types::TableName::new("old_corruption");
            let (table, _) = storage_provider::RequestTableBindings::default().resolve_table(attempt, &cache, &name).await?;
            let request = storage_types::GetItemRequest::try_from(serde_json::json!({"TableName":"old_corruption","Key":{"pk":{"S":"one"}}})).unwrap();
            crate::item_read::primary_item_key(crate::catalog::table_id_from_history(table.history_id)?, &table, &request.key)
        })).await.unwrap();
        store.backend.with_attempt(storage_types::AttemptPlan::new(storage_types::AttemptMode::ReadWrite, storage_types::DeadlineClass::Write).start(store.backend.attempt_deadlines()), move |attempt| Box::pin(async move {
            attempt.stage(vec![crate::KvMutation::Put { key, value: vec![255] }])
        })).await.unwrap();
        let database = open_database().await;
        let old = start_transaction(&database, None);
        let version = old.get_read_version().await.unwrap();
        drop(old);
        let publisher = store.clone().with_metadata_cache_ttl(std::time::Duration::from_secs(60));
        publisher.delete_table(serde_json::from_value(serde_json::json!({"TableName":"old_corruption"})).unwrap()).await.unwrap();
        publisher.create_table(create()).await.unwrap();
        events.0.lock().unwrap().clear();
        // Existing test hook pins an actual SDK snapshot; this proves the
        // Cached request policy, not that the SDK's GRV cache chose this version.
        store.backend.pin_next_read_version(version);
        let error = store.put_item(put()).await.unwrap_err();
        assert!(!error.class().is_retryable() && !error.is_commit_unknown());
        assert!(matches!(error.as_ref(), storage_types::StorageEnum::InternalServerError { message } if message == "KV primary item header is truncated"), "{:?}", error.as_ref());
        assert!(!error.attempt_kind().is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown), "corruption must not become FreshValidationRequired: {error:?}");
        let observed = std::mem::take(&mut *events.0.lock().unwrap());
        assert_eq!(observed.iter().filter(|event| matches!(event, crate::KvNativeEvent::AttemptRequested { .. })).count(), 1, "no Fresh replay: {observed:?}");
        assert!(observed.contains(&crate::KvNativeEvent::AttemptRequested { mode: storage_types::AttemptMode::ReadWriteItems, read_version: storage_types::ReadVersionPolicy::Cached }));
        assert!(!observed.iter().any(|event| matches!(event, crate::KvNativeEvent::Commit | crate::KvNativeEvent::Mutations { .. })), "no publication or mutation acknowledgement: {observed:?}");
        let current = publisher.get_item(storage_types::GetItemRequest::try_from(serde_json::json!({
            "TableName":"old_corruption", "Key":{"pk":{"S":"one"}}
        })).unwrap()).await.unwrap();
        assert!(current.item.is_none(), "failed old-snapshot write must leave current incarnation empty");
        println!("old_corruption public_write=true pinned_real_snapshot=true requested_policy=Cached actual_cache_hit=unclaimed permanent_failure=true attempts=1 commits=0");
    }).catch_unwind().await;
    store.backend.delete_namespace_keys().await.unwrap();
    assert!(!store.backend.namespace_has_keys().await.unwrap());
    println!("OWNED_FIXTURE deleted foundationdb namespace={namespace}");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
