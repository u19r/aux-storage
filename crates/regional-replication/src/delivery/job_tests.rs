use std::sync::Arc;

use bg_jobs_tokio::test_support::TestClock;
use scheduled_bg_jobs::{Timestamp, runtime::MonotonicDeadline};
use tokio::{net::TcpListener, sync::watch};

use super::{load_delivery_progress, test_support::runtime};
use crate::{
    test_support as fixture,
    transport::{TcpTransport, test_support::configuration},
};

#[tokio::test]
async fn given_out_of_order_receipts_when_workers_restart_then_native_progress_closes_gaps_and_jobs_resume()
 {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.sqlite");
    let source = Arc::new(fixture::open(&source_path).await);
    let destination = Arc::new(fixture::open(&directory.path().join("destination.sqlite")).await);
    fixture::install_pair(&source, &destination).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let source_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_endpoint = source_listener.local_addr().unwrap();
    let configured = configuration(1, source_endpoint, endpoint);
    let transport = TcpTransport::try_new(Arc::clone(&configured)).unwrap();
    let client = transport.peer([2; 16].try_into().unwrap()).unwrap();
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
    fixture::put(&source, 1, "first").await;
    fixture::put(&source, 1, "second").await;
    let second = fixture::frame(&source, 1, 1, 2).await;
    let scope = fixture::delivery_scope(&second);
    let receipt = client.deliver(Arc::clone(&second)).await.unwrap();
    let pending = fixture::acknowledge(&source, second, receipt).await;
    assert_eq!(
        u64::from(pending.through()),
        fixture::source_position(&source, 1, 0).await
    );
    assert_eq!(pending.pending().len(), 1);
    let first = fixture::frame(&source, 1, 0, 1).await;
    let receipt = client.deliver(Arc::clone(&first)).await.unwrap();
    let closed = fixture::acknowledge(&source, Arc::clone(&first), receipt).await;
    assert_eq!(
        u64::from(closed.through()),
        fixture::source_position(&source, 1, 2).await
    );
    assert!(closed.pending().is_empty());
    let duplicate = client.deliver(Arc::clone(&first)).await.unwrap();
    assert!(fixture::acknowledge(&source, first, duplicate).await == closed);
    fixture::assert_item(&destination, Some("second")).await;
    drop(source);
    let source = Arc::new(fixture::reopen(&source_path).await);
    assert!(
        load_delivery_progress(source.as_ref(), fixture::journal(1), scope.clone())
            .await
            .unwrap()
            == closed
    );
    let now = Timestamp::from_millis(1_800_000_000_000);
    let clock = Arc::new(TestClock::new(now, MonotonicDeadline::ZERO));
    fixture::put(&source, 1, "job-7").await;
    let first_worker = runtime(
        Arc::clone(&source),
        Arc::clone(&configured),
        (clock.clone(), 7),
    );
    first_worker.ensure_seeds().await.unwrap();
    super::test_support::drive_through(&[&first_worker], &source, &scope, 3, &clock).await;
    fixture::assert_item(&destination, Some("job-7")).await;
    fixture::put(&source, 1, "job-8").await;
    let second_worker = runtime(
        Arc::clone(&source),
        Arc::clone(&configured),
        (clock.clone(), 8),
    );
    second_worker.ensure_seeds().await.unwrap();
    super::test_support::drive_through(
        &[&first_worker, &second_worker],
        &source,
        &scope,
        4,
        &clock,
    )
    .await;
    fixture::assert_item(&destination, Some("job-8")).await;
    let progress = load_delivery_progress(source.as_ref(), fixture::journal(1), scope.clone())
        .await
        .unwrap();
    assert_eq!(
        u64::from(progress.through()),
        fixture::source_position(&source, 1, 4).await
    );
    stop.send(true).unwrap();
    serving.await.unwrap().unwrap();
}
