use std::{
    future::Future,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
};

use async_trait::async_trait;
use storage_types::{
    ItemStreamVersion, StorageError, StorageResult, StreamItemId, StreamRetentionDuration,
    TableName, TimestampMillis,
};

use crate::{
    StreamDurationTrimBackend, StreamDurationTrimConfig, StreamDurationTrimPageRequest,
    StreamDurationTrimPageResult, StreamDurationTrimWorker, StreamTrimBoundary,
    StreamTrimDueMarker, StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimState,
    StreamTrimStateWrite,
};

#[derive(Clone, Default)]
struct FakeBackend {
    inner: Arc<Mutex<FakeBackendState>>,
}

#[derive(Default)]
struct FakeBackendState {
    markers: Vec<StreamTrimDueMarker>,
    state: Option<StreamTrimState>,
    boundaries: Option<StreamTrimScopeBoundaries>,
    page_result: Option<StreamDurationTrimPageResult>,
    trim_requests: Vec<StreamDurationTrimPageRequest>,
    finished_markers: Vec<StreamTrimDueMarker>,
    writes: Vec<StreamTrimStateWrite>,
    fail_next_finish: bool,
}

struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F>(future: F) -> F::Output
where F: Future {
    let waker = Waker::from(Arc::new(NoopWaker));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[async_trait]
impl StreamDurationTrimBackend for FakeBackend {
    async fn list_due_stream_trim_markers(
        &self,
        _due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        let inner = self.inner.lock().expect("fake backend lock");
        Ok(inner.markers.iter().take(limit).cloned().collect())
    }

    async fn load_stream_trim_state(
        &self,
        _scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>> {
        Ok(self.inner.lock().expect("fake backend lock").state.clone())
    }

    async fn load_stream_trim_boundaries(
        &self,
        _scope: &StreamTrimScope,
    ) -> StorageResult<StreamTrimScopeBoundaries> {
        Ok(self
            .inner
            .lock()
            .expect("fake backend lock")
            .boundaries
            .clone()
            .unwrap_or_else(StreamTrimScopeBoundaries::unbounded))
    }

    async fn trim_table_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        trim_page(&self.inner, request)
    }

    async fn trim_item_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        trim_page(&self.inner, request)
    }

    async fn finish_stream_trim_marker(
        &self,
        marker: StreamTrimDueMarker,
        write: Option<StreamTrimStateWrite>,
    ) -> StorageResult<()> {
        let mut inner = self.inner.lock().expect("fake backend lock");
        if inner.fail_next_finish {
            inner.fail_next_finish = false;
            return Err(StorageError::internal("simulated finish failure"));
        }
        inner.finished_markers.push(marker);
        if let Some(write) = write {
            inner.state = Some(write.state.clone());
            inner.writes.push(write);
        }
        Ok(())
    }
}

fn trim_page(
    inner: &Arc<Mutex<FakeBackendState>>,
    request: StreamDurationTrimPageRequest,
) -> StorageResult<StreamDurationTrimPageResult> {
    let mut inner = inner.lock().expect("fake backend lock");
    inner.trim_requests.push(request);
    Ok(inner
        .page_result
        .clone()
        .unwrap_or(StreamDurationTrimPageResult {
            deleted_rows: 0,
            first_remaining_version: None,
            first_remaining_timestamp: None,
        }))
}

fn table_scope() -> StreamTrimScope {
    StreamTrimScope::table("table/orders", TableName::new("orders"))
}

fn item_scope() -> StreamTrimScope {
    StreamTrimScope::item("item/orders/1", TableName::new("orders"), "hash-1")
}

fn stream_id(value: u64) -> StreamItemId {
    StreamItemId::from(ItemStreamVersion::new(value))
}

fn finite_state(scope: StreamTrimScope, policy_version: u64) -> StreamTrimState {
    StreamTrimState {
        scope,
        policy_version,
        retention: StreamRetentionDuration::FiniteHours(1),
        effective_retention: StreamRetentionDuration::FiniteHours(1),
        next_due_at: Some(TimestampMillis::from_timestamp(3_600_000)),
        oldest_retained_version: None,
        oldest_retained_timestamp: None,
        latest_version: None,
        latest_timestamp: None,
        updated_at: TimestampMillis::from_timestamp(0),
    }
}

fn worker(backend: FakeBackend) -> StreamDurationTrimWorker<FakeBackend> {
    StreamDurationTrimWorker::new(
        backend,
        StreamDurationTrimConfig {
            marker_page_size: 10,
            stream_page_size: 25,
        },
    )
}

#[test]
fn worker_skips_stale_markers_without_trimming() {
    let backend = FakeBackend::default();
    let scope = table_scope();
    let marker = StreamTrimDueMarker::new(TimestampMillis::from_timestamp(3_600_000), scope, 1);
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.markers = vec![marker.clone()];
        inner.state = Some(finite_state(marker.scope.clone(), 2));
    }

    let stats = block_on(worker(backend.clone()).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("worker should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    assert_eq!(stats.stale_markers, 1);
    assert_eq!(inner.trim_requests.len(), 0);
    assert_eq!(inner.finished_markers, vec![marker]);
}

#[test]
fn worker_skips_forever_scopes_and_clears_next_due() {
    let backend = FakeBackend::default();
    let scope = table_scope();
    let marker = StreamTrimDueMarker::new(TimestampMillis::from_timestamp(3_600_000), scope, 1);
    let mut state = finite_state(marker.scope.clone(), 1);
    state.retention = StreamRetentionDuration::Forever;
    state.effective_retention = StreamRetentionDuration::Forever;
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.markers = vec![marker];
        inner.state = Some(state);
    }

    let stats = block_on(worker(backend.clone()).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("worker should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    assert_eq!(stats.forever_markers, 1);
    assert_eq!(inner.trim_requests.len(), 0);
    assert_eq!(inner.writes[0].state.next_due_at, None);
    assert_eq!(inner.writes[0].next_marker, None);
}

#[test]
fn worker_applies_bounded_page_request_with_protective_version_ceiling() {
    let backend = FakeBackend::default();
    let scope = item_scope();
    let marker =
        StreamTrimDueMarker::new(TimestampMillis::from_timestamp(3_600_000), scope.clone(), 1);
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.markers = vec![marker];
        inner.state = Some(finite_state(scope, 1));
        inner.boundaries = Some(StreamTrimScopeBoundaries {
            latest_item_id: Some(stream_id(5)),
            protected_boundary: Some(StreamTrimBoundary {
                item_id: stream_id(4),
            }),
            retained_table_pointer_boundary: Some(StreamTrimBoundary {
                item_id: stream_id(3),
            }),
        });
        inner.page_result = Some(StreamDurationTrimPageResult {
            deleted_rows: 2,
            first_remaining_version: Some(ItemStreamVersion::new(3)),
            first_remaining_timestamp: Some(TimestampMillis::from_timestamp(3_700_000)),
        });
    }

    let stats = block_on(worker(backend.clone()).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("worker should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    let request = &inner.trim_requests[0];
    assert_eq!(stats.rows_deleted, 2);
    assert_eq!(request.page_limit, 25);
    assert_eq!(request.max_deleted_item_id, Some(stream_id(2)));
    assert_eq!(
        request.cutoff_timestamp,
        TimestampMillis::from_timestamp(3_600_000)
    );
    assert_eq!(
        inner.writes[0]
            .next_marker
            .as_ref()
            .map(|marker| marker.due_bucket),
        Some(TimestampMillis::from_timestamp(7_200_000))
    );
}

#[test]
fn worker_retries_safely_after_finish_failure() {
    let backend = FakeBackend::default();
    let scope = table_scope();
    let marker =
        StreamTrimDueMarker::new(TimestampMillis::from_timestamp(3_600_000), scope.clone(), 1);
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.markers = vec![marker.clone()];
        inner.state = Some(finite_state(scope, 1));
        inner.page_result = Some(StreamDurationTrimPageResult {
            deleted_rows: 1,
            first_remaining_version: Some(ItemStreamVersion::new(2)),
            first_remaining_timestamp: Some(TimestampMillis::from_timestamp(3_700_000)),
        });
        inner.fail_next_finish = true;
    }

    let first = block_on(worker(backend.clone()).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ));
    assert!(first.is_err());

    block_on(worker(backend.clone()).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("retry should finish");

    let inner = backend.inner.lock().expect("fake backend lock");
    assert_eq!(inner.trim_requests.len(), 2);
    assert_eq!(inner.finished_markers, vec![marker]);
    assert_eq!(inner.writes.len(), 1);
}
