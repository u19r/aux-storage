//! Isolated native source-frame measurements; setup is outside measured
//! windows.
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use infrastructure_provider::limits::InfrastructureLimits;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::{event::content::CanonicalEventCodec, protocol::PayloadBounds};
use storage_provider::{DynamoPort, native_admission::NativeAttemptSource};
use storage_types::{AttemptMode, NativeWorkLimits};

use crate::{
    journal::JournalStore,
    source_frame::{SourceFrameRequest, load_source_frame},
    test_support as fixture,
};

const WIDTHS: [(usize, usize); 8] = [
    (1, 2048),
    (16, 128),
    (64, 32),
    (256, 8),
    (1024, 2),
    (7, 292),
    (63, 32),
    (255, 8),
];

#[derive(Default)]
struct NativeCalls(AtomicUsize);

impl sql_backend::SqlFaultProbe for NativeCalls {
    fn fail(&self, _: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        None
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "isolated release-profile performance campaign; fixture setup is substantial"]
async fn given_native_originals_when_selecting_varied_frames_then_measure_successful_selection() {
    let seed: usize = std::env::var("REGIONAL_PERFORMANCE_SEED")
        .unwrap_or_else(|_| "17".into())
        .parse()
        .unwrap();
    assert!([17, 61, 89].contains(&seed));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.sqlite");
    let source = fixture::open(&path).await;
    let destination = fixture::open(&directory.path().join("destination.sqlite")).await;
    fixture::install_pair(&source, &destination).await;
    for index in 0..1024 {
        let length = [800, 1150, 1500][(index + seed) % 3];
        let value = format!("seed-{seed}-{index}-{}", "x\"\\\n葉".repeat(length / 8));
        fixture::put(&source, 1, &value).await;
    }
    let state = fixture::installed_state(&source, 1, fixture::NAME).await;
    let after = fixture::source_position(&source, 1, 0).await;
    drop(source);
    drop(destination);
    let calls = Arc::new(NativeCalls::default());
    let source = fixture::open_with_probe(&path, Some(calls.clone())).await;
    source
        .bind_work_limits(NativeWorkLimits {
            data_bytes: fixture::nz(16 * 1024 * 1024 * 1024),
            call_guards: fixture::nz(100000),
        })
        .unwrap();
    let journal = wide_journal();
    let codec = Arc::new(CanonicalJsonCodec::new(PayloadBounds {
        event_bytes: fixture::nz(65536),
        manifest_bytes: fixture::nz(16 * 1024 * 1024),
    }));
    let mut limits = (*fixture::limits()).clone();
    limits.frame_events = fixture::nz(1024);
    limits.frame_bytes = fixture::nz(16 * 1024 * 1024);
    let limits = Arc::new(limits);
    for (width, iterations) in WIDTHS {
        if seed == 89 && ![7, 63, 255].contains(&width) {
            continue;
        }
        let request = Arc::new(SourceFrameRequest {
            origin: replication_types::identity::origin::OriginId {
                region: fixture::region(1),
                database_group: [1; 16].try_into().unwrap(),
                journal_generation: 1.into(),
                journal_partition: fixture::partitions().select(
                    state.configuration.lineage,
                    &codec.encode_key(&fixture::key()).unwrap(),
                ),
            },
            range: (after.into(), (after + width as u64).into())
                .try_into()
                .unwrap(),
            membership: state.membership,
            tables: vec![(state.configuration.table, state.configuration.lineage)],
        });
        measure_frames(
            &source,
            (&journal, &codec),
            (&limits, request),
            (width, iterations),
            &calls,
        )
        .await;
    }
    eprintln!(
        "regional_fixture {}",
        serde_json::json!({"seed": seed, "positions": 1024, "attributes": 2, "gsis": 0, "ttl": false, "value_length_bases": [800, 1150, 1500], "payload": "escaped/unicode, 8-byte repeat", "native_data_bytes": 16 * 1024_u64 * 1024 * 1024, "native_call_guards": 100000})
    );
}

fn wide_journal() -> Arc<JournalStore> {
    let journal = JournalStore::try_new(
        (
            fixture::region(1),
            [1; 16].try_into().unwrap(),
            1.into(),
            [7; 16].try_into().unwrap(),
        ),
        fixture::partitions(),
        InfrastructureLimits::try_new(
            fixture::nz(1024),
            fixture::nz(1048576),
            fixture::nz(8388608),
            fixture::nz(1024),
        )
        .unwrap(),
        (fixture::backlog(), 1000.try_into().unwrap()),
    )
    .unwrap();
    journal
        .require_original_capacity(fixture::budgets(), fixture::nz(1024))
        .unwrap();
    journal
        .native_admission
        .bind(Arc::new(
            crate::native_admission_test_support::FixtureAdmission::default(),
        ))
        .unwrap();
    Arc::new(journal)
}

async fn measure_frames(
    source: &fixture::Store,
    owners: (&Arc<JournalStore>, &Arc<CanonicalJsonCodec>),
    input: (&Arc<WorkLimits>, Arc<SourceFrameRequest>),
    case: (usize, usize),
    calls: &NativeCalls,
) {
    let (width, iterations) = case;
    let expected = read_frame(source, owners, (input.0, &input.1)).await;
    assert_eq!(expected.events().len(), width);
    assert_eq!(expected.manifest().coverage.len(), width);
    calls.0.store(0, Ordering::Relaxed);
    let cpu = cpu_nanos();
    let started = Instant::now();
    for _ in 0..iterations {
        let frame = read_frame(source, owners, (input.0, &input.1)).await;
        assert_eq!(frame.events().len(), width);
        assert_eq!(frame.manifest().digest, expected.manifest().digest);
        std::hint::black_box(frame);
    }
    let elapsed_ns = started.elapsed().as_nanos();
    let cpu_ns = cpu_nanos() - cpu;
    let native_calls = calls.0.load(Ordering::Relaxed);
    let actual = read_frame(source, owners, (input.0, &input.1)).await;
    assert_eq!(actual.manifest(), expected.manifest());
    for (actual, expected) in actual.events().iter().zip(expected.events()) {
        assert_eq!(actual.as_ref().bytes, expected.as_ref().bytes);
    }
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "native-frame",
        file!(),
        line!(),
        None,
    );
    std::hint::black_box(read_frame(source, owners, (input.0, &input.1)).await);
    let allocation = guard.finish();
    eprintln!(
        "regional_performance {}",
        serde_json::json!({"case": format!("native-frame-{width}"), "operations": iterations, "positions": width, "cpu_ns": cpu_ns, "elapsed_ns": elapsed_ns, "native_calls": native_calls, "current_thread_allocations": allocation.allocation_count, "current_thread_allocated_bytes": allocation.allocated_bytes})
    );
}

async fn read_frame(
    source: &fixture::Store,
    owners: (&Arc<JournalStore>, &Arc<CanonicalJsonCodec>),
    input: (&Arc<WorkLimits>, &Arc<SourceFrameRequest>),
) -> replication_provider::delivery::frame::SourceFrame {
    let journal = Arc::clone(owners.0);
    let codec = Arc::clone(owners.1);
    let limits = Arc::clone(input.0);
    let request = Arc::clone(input.1);
    source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                load_source_frame(&journal, attempt, (&request, &limits), &codec)
                    .await
                    .map_err(|error| {
                        storage_types::StorageError::internal(&format!(
                            "performance frame: {error:?}"
                        ))
                    })
            })
        })
        .await
        .unwrap()
}

fn cpu_nanos() -> u128 {
    let time = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    time.tv_sec as u128 * 1_000_000_000 + time.tv_nsec as u128
}
