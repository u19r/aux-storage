use std::num::NonZeroU64;

use kv_backend::{KvStore, rocksdb::RocksDbBackend};
use scheduled_bg_jobs::{JobId, Namespace};
use storage_common::{
    PARTITION_SAMPLE_PAGE_LIMIT,
    partition::{
        PartitionLoadSample, PartitionLoadSampleRecord, PartitionState, ResolvedPartitionFamily,
    },
};
use storage_provider::{
    DynamoPort,
    maintenance::{
        PartitionPort, PartitionReconciliationRepository, PartitionReconciliationRequest,
    },
};
use storage_types::{
    AttemptMode, BoundedVec, StreamName, TimestampMillis, UserStreamName, maintenance::StepResult,
};
use stream_provider::{StreamOperations, StreamPartitioningMode, StreamRowKind};

type Store = KvStore<RocksDbBackend>;

async fn load_family(store: &Store, stream: &StreamName) -> ResolvedPartitionFamily {
    let stream = stream.clone();
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |port| {
            Box::pin(async move { port.load_partition_family(&stream).await })
        })
        .await
        .expect("canonical family")
}

fn request(fence: u64, now: i64) -> PartitionReconciliationRequest {
    PartitionReconciliationRequest {
        destination: Namespace::try_new("partition-test").expect("namespace"),
        job: JobId::from([42; 16]),
        fence: NonZeroU64::new(fence).expect("positive fence"),
        captured_at: TimestampMillis::from_timestamp(now),
    }
}

fn sample(partition_id: u16, window: i64) -> PartitionLoadSampleRecord {
    PartitionLoadSampleRecord {
        partition_id,
        window_start_ms: window,
        publisher_id: "worker-1".to_owned(),
        sample: PartitionLoadSample {
            writes: 1_000,
            bytes: 512_000,
            routing_key_bucket_bitmap: 255,
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn given_hot_keyed_stream_when_fenced_reconciliation_splits_then_history_and_authority_survive()
 {
    let directory = std::env::temp_dir().join(format!(
        "partition-runtime-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store =
        Store::initialize_format(RocksDbBackend::initialize(directory.clone()).expect("driver"))
            .await
            .expect("initialized owner");
    let name = UserStreamName::new("partition-history");
    let stream = store
        .create_stream(name.clone(), None, StreamPartitioningMode::KeyOrdered)
        .await
        .expect("create stream");
    let initial = load_family(&store, &stream).await;
    let target = initial
        .route_ordered_log(b"key-0")
        .expect("route")
        .partition_id;
    let keys: Vec<_> = (0..10_000)
        .map(|i| format!("key-{i}"))
        .filter(|key| {
            initial
                .route_ordered_log(key.as_bytes())
                .expect("route")
                .partition_id
                == target
        })
        .take(8)
        .collect();
    assert_eq!(keys.len(), 8);
    let mut acknowledged = Vec::new();
    for key in &keys {
        acknowledged.push(
            store
                .append_item(name.clone(), &[7; 512], Some(key))
                .await
                .expect("append parent"),
        );
    }
    for pass in 0..3 {
        let now = 1_800_000_000_000 + pass * 1_000;
        store
            .publish_partition_samples(
                stream.clone(),
                BoundedVec::try_new(vec![sample(target, now)]).expect("bounded publication"),
            )
            .await
            .expect("publish bounded window");
        assert!(matches!(
            store
                .advance_partition_reconciliation(request(2, now))
                .await
                .expect("reconcile"),
            StepResult::Advanced(())
        ));
        assert!(matches!(
            store
                .advance_partition_reconciliation(request(2, now))
                .await
                .expect("finish sweep"),
            StepResult::Complete
        ));
    }
    let split = load_family(&store, &stream).await;
    assert_eq!(split.memberships.len(), 1);
    assert_eq!(split.partitions.len(), initial.partitions.len() + 2);
    assert_eq!(
        split
            .partitions
            .iter()
            .find(|p| p.partition_id == target)
            .expect("parent")
            .state,
        PartitionState::WriteClosed
    );
    assert!(matches!(
        store
            .advance_partition_reconciliation(request(1, 1_800_000_004_000))
            .await
            .expect("stale owner"),
        StepResult::Superseded
    ));
    let mut foreign = request(3, 1_800_000_004_000);
    foreign.destination =
        Namespace::try_new("another-task-destination").expect("foreign namespace");
    assert!(
        store
            .advance_partition_reconciliation(foreign)
            .await
            .is_err()
    );
    for key in &keys {
        acknowledged.push(
            store
                .append_item(name.clone(), &[8; 512], Some(key))
                .await
                .expect("append children"),
        );
    }
    let page = store
        .read_forward(StreamRowKind::User(stream.clone()), None, 100)
        .await
        .expect("read lineage");
    assert_eq!(page.items.len(), acknowledged.len());
    acknowledged.sort();
    assert_eq!(
        page.items.iter().map(|item| item.id).collect::<Vec<_>>(),
        acknowledged
    );
    let oversized = (0..=PARTITION_SAMPLE_PAGE_LIMIT)
        .map(|_| sample(target, 1_800_000_005_000))
        .collect();
    assert!(
        BoundedVec::<_, 0, { PARTITION_SAMPLE_PAGE_LIMIT as usize }>::try_new(oversized).is_err()
    );
    let mut invalid = sample(target, 1_800_000_005_000);
    invalid.publisher_id.clear();
    assert!(
        store
            .publish_partition_samples(
                stream.clone(),
                BoundedVec::try_new(vec![sample(target, 1_800_000_005_000), invalid])
                    .expect("bounded invalid-member input")
            )
            .await
            .is_err()
    );
    for batch in 0..3 {
        let records = (0..PARTITION_SAMPLE_PAGE_LIMIT)
            .map(|index| {
                let mut record = sample(target, 1_800_000_005_000);
                record.publisher_id = format!("pending-{batch}-{index}");
                record
            })
            .collect();
        store
            .publish_partition_samples(
                stream.clone(),
                BoundedVec::try_new(records).expect("bounded page"),
            )
            .await
            .expect("pending bounded telemetry");
    }
    store
        .delete_stream(name.clone())
        .await
        .expect("delete family");
    for _ in 0..3 {
        assert!(matches!(
            store
                .advance_partition_reconciliation(request(2, 1_800_000_006_000))
                .await
                .expect("bounded orphan cleanup"),
            StepResult::Advanced(())
        ));
    }
    store
        .advance_partition_reconciliation(request(2, 1_800_000_006_000))
        .await
        .expect("drain any automatically sampled appends");
    let deleted = stream.clone();
    let storage_provider::maintenance::PartitionSamplePage {
        family, records, ..
    } = store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |port| {
            Box::pin(async move { port.load_partition_samples(&deleted).await })
        })
        .await
        .expect("physical cleanup");
    assert!(family.is_none());
    assert!(records.is_empty());
    let recreated = store
        .create_stream(name, None, StreamPartitioningMode::KeyOrdered)
        .await
        .expect("new incarnation");
    assert_ne!(stream, recreated);
    assert!(load_family(&store, &recreated).await.memberships.is_empty());
    drop(store);
    std::fs::remove_dir_all(directory).expect("remove owned fixture");
}

fn runtime(
    store: bg_job_store::ScheduledJobStore,
    repository: std::sync::Arc<Store>,
    clock: bg_jobs_tokio::test_support::TestClock,
    owner: u8,
) -> bg_jobs_tokio::JobRuntime<bg_job_store::ScheduledJobStore> {
    use std::{num::NonZeroU16, sync::Arc, time::Duration};

    use scheduled_bg_jobs::{
        DriveBudget, DriveBudgetInput, EffectMode, EnqueueOptions, FairnessClass, FairnessPolicy,
        JobCatalog, JobSpec, OwnerId, QueueSelection, RetryPreset, RetrySelection,
    };
    let job = storage_common::StorageJob::PartitionReconciliation;
    let identity = request(1, 1_800_000_000_000);
    let handler = storage_provider::maintenance::PartitionReconciliationHandler::try_new(
        repository,
        Duration::from_secs(1),
    )
    .expect("handler");
    let mut catalog = JobCatalog::new();
    let now = scheduled_bg_jobs::Timestamp::from_millis(1_800_000_000_000);
    catalog
        .register_seeded(
            JobSpec::unit(job.kind(), NonZeroU16::MIN, job.queue(), handler).policy(
                EffectMode::Fenced,
                FairnessPolicy::low_volume(),
                RetrySelection::Preset(RetryPreset::Maintenance),
                Duration::from_secs(1),
            ),
            (),
            EnqueueOptions::new(FairnessClass::low_volume())
                .job_id(identity.job)
                .scheduled_at(now),
            now,
        )
        .expect("catalog");
    let budget = DriveBudget::try_from(DriveBudgetInput {
        queues: QueueSelection::only(vec![job.queue()]).expect("queue"),
        max_claims: 1,
        max_concurrency: 1,
        max_store_operations: crate::constants::MAINTENANCE_DRIVER_MAX_STORE_OPERATIONS,
        stop_claiming_after: Duration::from_secs(5),
        finish_after: Duration::from_secs(10),
    })
    .expect("budget");
    bg_jobs_tokio::test_support::runtime_builder(store, catalog, budget)
        .namespace(identity.destination)
        .owner(OwnerId::from([owner; OwnerId::BYTE_LENGTH]))
        .clock(Arc::new(clock))
        .build()
        .expect("runtime")
}

#[tokio::test]
async fn given_ten_job_nodes_when_claiming_one_partition_job_then_one_canonical_split_is_published()
{
    use std::{sync::Arc, time::Duration};

    use bg_job_store::ScheduledJobStoreSource;
    let directory = std::env::temp_dir().join(format!(
        "partition-claims-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store = Arc::new(
        Store::initialize_format(RocksDbBackend::initialize(directory.clone()).expect("driver"))
            .await
            .expect("owner"),
    );
    let stream = store
        .create_stream(
            UserStreamName::new("competing-workers"),
            None,
            StreamPartitioningMode::KeyOrdered,
        )
        .await
        .expect("stream");
    let initial = load_family(&store, &stream).await;
    let target = initial.partitions[0].partition_id;
    let clock = bg_jobs_tokio::test_support::TestClock::new(
        scheduled_bg_jobs::Timestamp::from_millis(1_800_000_000_000),
        scheduled_bg_jobs::runtime::MonotonicDeadline::ZERO,
    );
    let job_store = store
        .clone()
        .scheduled_job_store(Default::default())
        .expect("same owner job store");
    let runtimes: Vec<_> = (1..=10)
        .map(|owner| runtime(job_store.clone(), store.clone(), clock.clone(), owner))
        .collect();
    for runtime in &runtimes {
        runtime.ensure_seeds().await.expect("idempotent seed");
    }
    for pass in 0..3 {
        let now = 1_800_000_000_000 + pass * 1_000;
        store
            .publish_partition_samples(
                stream.clone(),
                BoundedVec::try_new(vec![sample(target, now)]).expect("bounded publication"),
            )
            .await
            .expect("sample");
        for _ in 0..2 {
            for result in
                futures::future::join_all(runtimes.iter().map(|runtime| runtime.drive_once())).await
            {
                match result {
                    Ok(_) | Err(scheduled_bg_jobs::JobSystemError::LeaseConflict) => {}
                    Err(error) => panic!("competing claim and dispatch: {error:?}"),
                }
            }
        }
        clock.advance(Duration::from_secs(1));
    }
    let split = load_family(&store, &stream).await;
    split.validate().expect("canonical coverage");
    assert_eq!(split.memberships.len(), 1);
    assert_eq!(split.partitions.len(), initial.partitions.len() + 2);
    drop(runtimes);
    drop(job_store);
    drop(store);
    std::fs::remove_dir_all(directory).expect("remove fixture");
}

#[tokio::test]
async fn given_keyed_appends_when_pressure_is_recorded_then_samples_join_the_item_transaction() {
    use storage_types::StorageError;
    use stream_provider::{StreamAttempt, StreamDataType, StreamPort, StreamRowValue};
    let directory = std::env::temp_dir().join(format!(
        "partition-sampling-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store =
        Store::initialize_format(RocksDbBackend::initialize(directory.clone()).expect("driver"))
            .await
            .expect("owner");
    let name = UserStreamName::new("automatic-samples");
    let stream = store
        .create_stream(name.clone(), None, StreamPartitioningMode::KeyOrdered)
        .await
        .expect("stream");
    let target = stream.clone();
    let rolled_back: storage_types::StorageResult<()> = store
        .with_stream_attempt(AttemptMode::ReadWrite, move |port| {
            Box::pin(async move {
                let family = StreamAttempt::load_partition_family(port, &target).await?;
                let partition = family.route_ordered_log(b"sample-key")?;
                let sample = partition
                    .sample_append(b"sample-key", 512, 64, TimestampMillis::now())
                    .expect("chosen sample");
                port.store_partition_sample(&target, &sample).await?;
                port.append_attempt_allocated_partitioned_user_item(
                    partition,
                    StreamRowValue::new(
                        target,
                        vec![7; 512],
                        StreamDataType::Binary,
                        TimestampMillis::now(),
                    ),
                )
                .await?;
                Err(StorageError::validation(
                    "reject after staging both effects",
                ))
            })
        })
        .await;
    assert!(rolled_back.is_err());
    assert!(
        store
            .read_forward(StreamRowKind::User(stream.clone()), None, 100)
            .await
            .expect("aborted row absent")
            .items
            .is_empty()
    );
    let mut observed = Vec::new();
    for _ in 0..64 {
        for _ in 0..64 {
            store
                .append_item(name.clone(), &[7; 512], Some("sample-key"))
                .await
                .expect("happy append");
        }
        let target = stream.clone();
        let storage_provider::maintenance::PartitionSamplePage { records, .. } = store
            .with_dynamo_attempt(AttemptMode::ReadWrite, move |port| {
                Box::pin(async move { port.load_partition_samples(&target).await })
            })
            .await
            .expect("pressure page");
        if !records.is_empty() {
            observed = records;
            break;
        }
    }
    assert!(
        !observed.is_empty(),
        "automatic sampling must publish pressure"
    );
    for record in &observed {
        assert_ne!(
            record.publisher_id, "00000000000000000000000000000040",
            "aborted sample absent"
        );
        assert_eq!(
            record.sample.writes,
            storage_common::PARTITION_LOAD_SAMPLE_RATE
        );
        assert_eq!(
            record.sample.bytes,
            storage_common::PARTITION_LOAD_SAMPLE_RATE * 512
        );
    }
    drop(store);
    std::fs::remove_dir_all(directory).expect("remove owned fixture");
}
