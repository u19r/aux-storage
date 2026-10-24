use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bg_jobs_tokio::test_support::TestClock;
use futures::StreamExt;
use scheduled_bg_jobs::{Timestamp, runtime::MonotonicDeadline};
use tokio::{net::TcpListener, sync::watch};

use super::{
    TcpTransport,
    test_support::{Proxy, configuration},
};
use crate::{
    delivery::{load_delivery_progress, test_support::runtime},
    test_support as fixture,
};

#[tokio::test]
#[ignore = "requires toxiproxy-server; run just regional-replication-network-test"]
async fn given_regional_latency_when_draining_a_backlog_then_bounded_pages_reduce_round_trips() {
    measure_public_origin().await;
    let single = drain(false).await;
    let batched = drain(true).await;
    assert!(
        single.native_calls > 0 && batched.native_calls > 0,
        "native statement interception was inactive"
    );
    eprintln!(
        "150ms per direction, 16 mutations: one-position pages {:.1}ms/op; bounded job page \
         {:.1}ms/op; native SQL statements/op {:.1} -> {:.1}; recovery {:.1} -> {:.1} mutations/s",
        single.elapsed.as_secs_f64() * 1000.0 / 16.0,
        batched.elapsed.as_secs_f64() * 1000.0 / 16.0,
        single.native_calls as f64 / 16.0,
        batched.native_calls as f64 / 16.0,
        16.0 / single.elapsed.as_secs_f64(),
        16.0 / batched.elapsed.as_secs_f64(),
    );
    eprintln!(
        "encoded request payload bytes/mutation: {:.1} -> {:.1}; batched raw/packed bytes: {}/{}",
        single.packed_bytes as f64 / 16.0,
        batched.packed_bytes as f64 / 16.0,
        batched.raw_bytes,
        batched.packed_bytes
    );
    assert!(
        batched.elapsed * 2 < single.elapsed,
        "batching did not materially reduce the full delivery path"
    );
}

struct DeliveryMeasurement {
    elapsed: Duration,
    native_calls: usize,
    raw_bytes: usize,
    packed_bytes: usize,
}

#[derive(Default)]
struct NativeCalls(AtomicUsize);

impl sql_backend::SqlFaultProbe for NativeCalls {
    fn fail(&self, _call: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        None
    }
}

async fn measure_public_origin() {
    use storage_provider::DynamoOperations;
    use storage_types::TableName;
    let directory = tempfile::tempdir().unwrap();
    let archive = directory.path().join("archive");
    std::fs::create_dir(&archive).unwrap();
    let calls = Arc::new(NativeCalls::default());
    let source =
        fixture::open_with_probe(&directory.path().join("source.sqlite"), Some(calls.clone()))
            .await;
    let destination = fixture::open_with_probe(
        &directory.path().join("destination.sqlite"),
        Some(calls.clone()),
    )
    .await;
    let cluster =
        fixture::NativeCluster::start_with_archive(&[&source, &destination], &archive).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    let ordinary = TableName::new("ordinary-performance");
    fixture::create_table(&source, &ordinary).await;
    let before = archive_bytes(&archive);
    for (label, operations, table, concurrency) in [
        (
            "ordinary",
            &source as &dyn DynamoOperations,
            ordinary.clone(),
            1,
        ),
        (
            "regional host ordinary",
            cluster.operations[0].as_ref() as &dyn DynamoOperations,
            ordinary,
            1,
        ),
        (
            "regional serial",
            cluster.operations[0].as_ref() as &dyn DynamoOperations,
            TableName::new(fixture::NAME),
            1,
        ),
        (
            "regional contended",
            cluster.operations[0].as_ref() as &dyn DynamoOperations,
            TableName::new(fixture::NAME),
            4,
        ),
    ] {
        calls.0.store(0, Ordering::Relaxed);
        let started = Instant::now();
        futures::stream::iter((0..16).map(|index| {
            let request = storage_types::PutItemRequest::new(
                table.clone(),
                fixture::item(&format!("{label}-{index}")),
            );
            async move { operations.put_item(request).await.unwrap() }
        }))
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
        eprintln!(
            "{label}: {:.3}ms/op; native SQL statements/op {:.1}",
            started.elapsed().as_secs_f64() * 1000.0 / 16.0,
            calls.0.load(Ordering::Relaxed) as f64 / 16.0
        );
    }
    let frame = fixture::frame(&source, 1, 0, 32).await;
    assert_eq!(frame.manifest().links.len(), 32);
    let state = fixture::installed_state(&source, 1, fixture::NAME).await;
    let request = crate::history::read::CanonicalHistoryRequest {
        selection: crate::history::source_selection::CanonicalSourceSelection {
            serving_region: fixture::region(1),
            serving_group: [1; 16].try_into().unwrap(),
            table: TableName::new(fixture::NAME),
            lineage: state.configuration.lineage,
            membership: state.membership,
            origin: frame.manifest().origin.clone(),
            source: state
                .sources
                .iter()
                .find(|source| source.region == fixture::region(1))
                .unwrap()
                .clone(),
        },
        range: frame.manifest().range,
        cut: None,
    };
    let expected = {
        let history = cluster.history[0].read(request.clone()).await.unwrap();
        (history.manifest, history.originals)
    };
    for _ in 0..128 {
        cluster.drive_once().await;
        cluster.clock.advance(Duration::from_secs(1));
    }
    let growth = archive_bytes(&archive).checked_sub(before).unwrap();
    assert!(growth > 0, "registered archive work published no objects");
    eprintln!(
        "archive after 128 full-runtime turns: {growth} added bytes for 32 regional mutations \
         ({:.1} bytes/mutation)",
        growth as f64 / 32.0
    );
    {
        let history = cluster.history[0].read(request).await.unwrap();
        assert_eq!((history.manifest, history.originals), expected);
    }
    cluster.shutdown().await;
}

fn archive_bytes(root: &std::path::Path) -> u64 {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                archive_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}

async fn drain(batched: bool) -> DeliveryMeasurement {
    let directory = tempfile::tempdir().unwrap();
    let native_calls = Arc::new(NativeCalls::default());
    let source = Arc::new(
        fixture::open_with_probe(
            &directory.path().join("source.sqlite"),
            Some(native_calls.clone()),
        )
        .await,
    );
    let destination = Arc::new(
        fixture::open_with_probe(
            &directory.path().join("destination.sqlite"),
            Some(native_calls.clone()),
        )
        .await,
    );
    fixture::install_pair(&source, &destination).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let source_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_endpoint = source_listener.local_addr().unwrap();
    let proxy = Proxy::start(endpoint).await;
    proxy.latency(150, 0).await;
    let configured = configuration(1, source_endpoint, proxy.endpoint);
    let receiver = TcpTransport::try_new(configuration(2, endpoint, source_endpoint))
        .unwrap()
        .receiver(
            Arc::clone(&destination),
            fixture::journal(2),
            fixture::codec(),
        )
        .unwrap();
    let (stop, stopping) = watch::channel(false);
    let serving = tokio::spawn(receiver.serve(listener, stopping));
    for position in 1..=16 {
        fixture::put(&source, 1, &format!("value-{position}")).await;
    }
    let scope = fixture::delivery_scope(fixture::frame(&source, 1, 0, 1).await.as_ref());
    let client = TcpTransport::try_new(Arc::clone(&configured))
        .unwrap()
        .peer([2; 16].try_into().unwrap())
        .unwrap();
    let mut raw_bytes = 0;
    let mut packed_bytes = 0;
    let step = if batched { 16 } else { 1 };
    for after in (0..16).step_by(step) {
        let page = fixture::frame(&source, 1, after, after + step as u64).await;
        let encoded = super::client::encode_page(
            &page,
            (
                client.resources.identity.clone(),
                client.destination_identity(),
            ),
            configured.as_ref().as_ref().limits.frame_bytes.get(),
        )
        .unwrap();
        raw_bytes += encoded.raw_bytes as usize;
        packed_bytes += encoded.packed.len();
    }
    native_calls.0.store(0, Ordering::Relaxed);
    let started = Instant::now();
    if batched {
        let clock = Arc::new(TestClock::new(
            Timestamp::from_millis(1_800_000_000_000),
            MonotonicDeadline::ZERO,
        ));
        let runtime = runtime(Arc::clone(&source), configured, (Arc::clone(&clock), 7));
        runtime.ensure_seeds().await.unwrap();
        crate::delivery::test_support::drive_through(&[&runtime], &source, &scope, 16, &clock)
            .await;
    } else {
        for after in 0..16 {
            let page = fixture::frame(&source, 1, after, after + 1).await;
            let receipt = client.deliver(Arc::clone(&page)).await.unwrap();
            fixture::acknowledge(&source, page, receipt).await;
        }
    }
    let measurement = DeliveryMeasurement {
        elapsed: started.elapsed(),
        native_calls: native_calls.0.load(Ordering::Relaxed),
        raw_bytes,
        packed_bytes,
    };
    fixture::assert_item(&destination, Some("value-16")).await;
    let progress = load_delivery_progress(source.as_ref(), fixture::journal(1), scope)
        .await
        .unwrap();
    assert_eq!(
        u64::from(progress.through()),
        fixture::source_position(&source, 1, 16).await
    );
    assert!(progress.pending().is_empty());
    stop.send(true).unwrap();
    serving.await.unwrap().unwrap();
    measurement
}
