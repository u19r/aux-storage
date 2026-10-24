use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use async_trait::async_trait;
use storage_types::{
    ItemStreamVersion, StorageError, StorageResult, StreamItemId, StreamRetentionDuration,
    SystemStreamScope, TableName, TimestampMillis,
};
use stream_provider::{
    MAX_SYSTEM_STREAM_TRIM_CONTINUATION_BYTES, MAX_SYSTEM_STREAM_TRIM_PAGE_LIMIT,
    MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS, SystemStreamTrimContinuation, SystemStreamTrimOperationId,
    SystemStreamTrimPageRequest, SystemStreamTrimPageResult,
};

use crate::{
    StreamDurationTrimConfig, StreamDurationTrimPageRequest, StreamDurationTrimPageResult,
    StreamDurationTrimWorker, StreamRetentionRepository, StreamTrimBoundary, StreamTrimDueMarker,
    StreamTrimPageLimit, StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimState,
    StreamTrimStateWrite, SystemStreamTrimExecutor, SystemStreamTrimRepository,
    maintenance::{StreamDurationTrimStats, StreamRetentionFinish},
};

#[derive(Clone, Default)]
struct FakeBackend {
    inner: Arc<Mutex<FakeBackendState>>,
}

#[derive(Default)]
struct FakeBackendState {
    markers: Vec<StreamTrimDueMarker>,
    apply_marker_writes: bool,
    state: Option<StreamTrimState>,
    states: HashMap<String, StreamTrimState>,
    boundaries: Option<StreamTrimScopeBoundaries>,
    protected_scopes: HashSet<String>,
    page_result: Option<StreamDurationTrimPageResult>,
    trim_requests: Vec<StreamDurationTrimPageRequest>,
    boundary_loads: usize,
    due_marker_loads: usize,
    system_page_result: Option<SystemStreamTrimPageResult>,
    alternate_system_cursors: bool,
    repeat_system_sequence: bool,
    fail_system_trim: bool,
    system_trim_requests: Vec<SystemStreamTrimPageRequest>,
    finished_markers: Vec<StreamTrimDueMarker>,
    writes: Vec<StreamTrimStateWrite>,
    fail_next_finish: bool,
}

struct BatchedScopeBackend {
    markers: Vec<StreamTrimDueMarker>,
    state_batch_sizes: Mutex<Vec<usize>>,
    boundary_batch_sizes: Mutex<Vec<usize>>,
    trim_batch_sizes: Mutex<Vec<usize>>,
    finish_batch_sizes: Mutex<Vec<usize>>,
}

#[async_trait]
impl StreamRetentionRepository for BatchedScopeBackend {
    async fn trim_stream_marker_wave(
        &self,
        wave: crate::maintenance::StreamRetentionWave,
    ) -> StorageResult<crate::maintenance::StreamDurationTrimStats> {
        wave.execute(&mut crate::stream_retention_tests::ModelTransaction::new(
            self,
        ))
        .await
    }

    async fn list_due_stream_trim_markers(
        &self,
        _due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        Ok(self.markers.iter().take(limit).cloned().collect())
    }

    async fn load_stream_trim_states(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<Option<StreamTrimState>>> {
        self.state_batch_sizes
            .lock()
            .expect("state batch sizes lock")
            .push(scopes.len());
        Ok(scopes
            .iter()
            .map(|scope| Some(finite_state(scope.clone(), 1)))
            .collect())
    }

    async fn load_stream_trim_boundaries(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<StreamTrimScopeBoundaries>> {
        self.boundary_batch_sizes
            .lock()
            .expect("boundary batch sizes lock")
            .push(scopes.len());
        Ok(scopes
            .iter()
            .map(|_| StreamTrimScopeBoundaries::unbounded())
            .collect())
    }
}

fn block_on<F>(future: F) -> F::Output
where F: Future {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[async_trait]
impl StreamRetentionRepository for FakeBackend {
    async fn trim_stream_marker_wave(
        &self,
        wave: crate::maintenance::StreamRetentionWave,
    ) -> StorageResult<crate::maintenance::StreamDurationTrimStats> {
        wave.execute(&mut crate::stream_retention_tests::ModelTransaction::new(
            self,
        ))
        .await
    }

    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        let mut inner = self.inner.lock().expect("fake backend lock");
        inner.due_marker_loads = inner.due_marker_loads.saturating_add(1);
        let mut markers = inner
            .markers
            .iter()
            .filter(|marker| !inner.apply_marker_writes || marker.due_bucket <= due_before)
            .cloned()
            .collect::<Vec<_>>();
        if inner.apply_marker_writes {
            markers.sort_by(|left, right| {
                left.due_bucket
                    .cmp(&right.due_bucket)
                    .then_with(|| left.scope.scope_id.cmp(&right.scope.scope_id))
                    .then_with(|| left.policy_version.cmp(&right.policy_version))
            });
        }
        Ok(markers.into_iter().take(limit).collect())
    }

    async fn load_stream_trim_states(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<Option<StreamTrimState>>> {
        let inner = self.inner.lock().expect("fake backend lock");
        Ok(scopes
            .iter()
            .map(|scope| {
                inner
                    .states
                    .get(&scope.scope_id)
                    .cloned()
                    .or_else(|| inner.state.clone())
            })
            .collect())
    }

    async fn load_stream_trim_boundaries(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<StreamTrimScopeBoundaries>> {
        let mut inner = self.inner.lock().expect("fake backend lock");
        inner.boundary_loads = inner.boundary_loads.saturating_add(1);
        Ok(scopes
            .iter()
            .map(|scope| {
                if inner.protected_scopes.contains(&scope.scope_id) {
                    return StreamTrimScopeBoundaries {
                        latest_item_id: Some(StreamItemId::default()),
                        protected_boundary: Some(StreamTrimBoundary {
                            item_id: stream_id(1),
                        }),
                        retained_table_pointer_boundary: None,
                    };
                }
                inner
                    .boundaries
                    .as_ref()
                    .map(copy_boundaries)
                    .unwrap_or_else(StreamTrimScopeBoundaries::unbounded)
            })
            .collect())
    }
}

#[async_trait]
impl SystemStreamTrimRepository for FakeBackend {
    async fn trim_system_stream_page(
        &self,
        request: SystemStreamTrimPageRequest,
    ) -> StorageResult<SystemStreamTrimPageResult> {
        let mut inner = self.inner.lock().expect("fake backend lock");
        inner.system_trim_requests.push(request);
        if inner.fail_system_trim {
            return Err(StorageError::internal("simulated system trim failure"));
        }
        let request = inner
            .system_trim_requests
            .last()
            .expect("request was just recorded");
        let operation_id = request.operation_id().clone();
        let continuation = request.continuation().cloned();
        if inner.alternate_system_cursors || inner.repeat_system_sequence {
            let next = match continuation
                .as_ref()
                .map(SystemStreamTrimContinuation::as_bytes)
            {
                None => system_continuation(&operation_id, 1, "cursor-a"),
                Some(b"cursor-a") => system_continuation(
                    &operation_id,
                    if inner.repeat_system_sequence { 1 } else { 2 },
                    "cursor-b",
                ),
                Some(b"cursor-b") => system_continuation(
                    &operation_id,
                    if inner.repeat_system_sequence { 1 } else { 3 },
                    "cursor-a",
                ),
                Some(_) => system_continuation(&operation_id, 1, "cursor-a"),
            };
            return Ok(SystemStreamTrimPageResult::new(operation_id, 1, Some(next)));
        }
        Ok(inner
            .system_page_result
            .clone()
            .unwrap_or_else(|| SystemStreamTrimPageResult::new(operation_id, 0, None)))
    }
}

struct CancellationProbe(Arc<AtomicBool>);

impl Drop for CancellationProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct PendingSystemBackend {
    dropped: Arc<AtomicBool>,
}

#[async_trait]
impl SystemStreamTrimRepository for PendingSystemBackend {
    async fn trim_system_stream_page(
        &self,
        _request: SystemStreamTrimPageRequest,
    ) -> StorageResult<SystemStreamTrimPageResult> {
        let _probe = CancellationProbe(self.dropped.clone());
        std::future::pending::<()>().await;
        unreachable!("pending backend future must not complete")
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
        .as_ref()
        .map(copy_page_result)
        .unwrap_or(StreamDurationTrimPageResult {
            deleted_rows: 0,
            first_remaining_version: None,
            first_remaining_timestamp: None,
        }))
}

fn copy_boundaries(boundaries: &StreamTrimScopeBoundaries) -> StreamTrimScopeBoundaries {
    StreamTrimScopeBoundaries {
        latest_item_id: boundaries.latest_item_id,
        protected_boundary: boundaries.protected_boundary,
        retained_table_pointer_boundary: boundaries.retained_table_pointer_boundary,
    }
}

fn copy_page_result(result: &StreamDurationTrimPageResult) -> StreamDurationTrimPageResult {
    StreamDurationTrimPageResult {
        deleted_rows: result.deleted_rows,
        first_remaining_version: result.first_remaining_version,
        first_remaining_timestamp: result.first_remaining_timestamp,
    }
}

fn table_scope() -> StreamTrimScope {
    StreamTrimScope::table("table/orders", TableName::new("orders"))
}

fn item_scope() -> StreamTrimScope {
    StreamTrimScope::item("item/orders/1", TableName::new("orders"), "hash-1")
}

fn system_scope() -> SystemStreamScope {
    SystemStreamScope::try_new("orders", 7).expect("system scope")
}

fn system_cutoff() -> TimestampMillis {
    TimestampMillis::from_timestamp(3_600_000)
}

fn system_operation_id() -> SystemStreamTrimOperationId {
    SystemStreamTrimOperationId::new(&system_scope(), system_cutoff())
}

fn system_continuation(
    operation_id: &SystemStreamTrimOperationId,
    sequence: u64,
    value: &str,
) -> SystemStreamTrimContinuation {
    SystemStreamTrimContinuation::try_new(operation_id.clone(), sequence, value.as_bytes())
        .expect("continuation")
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

struct TestWorker<'a> {
    backend: &'a FakeBackend,
}

fn trim_limit(limit: usize) -> StreamTrimPageLimit {
    StreamTrimPageLimit::try_from(limit).expect("valid stream trim page limit")
}

impl TestWorker<'_> {
    async fn run_due_page(
        &self,
        due_before: TimestampMillis,
        now: TimestampMillis,
    ) -> StorageResult<StreamDurationTrimStats> {
        StreamDurationTrimWorker::new(
            self.backend,
            StreamDurationTrimConfig {
                marker_page_size: trim_limit(10),
                stream_page_size: trim_limit(25),
            },
        )
        .run_due_page(due_before, now)
        .await
    }

    async fn run_system_stream_trim_page(
        &self,
        scope: SystemStreamScope,
        cutoff: TimestampMillis,
        continuation: Option<SystemStreamTrimContinuation>,
    ) -> StorageResult<SystemStreamTrimPageResult> {
        SystemStreamTrimExecutor::new(self.backend, 25)
            .run_page(scope, cutoff, continuation)
            .await
    }
}

fn worker(backend: &FakeBackend) -> TestWorker<'_> {
    TestWorker { backend }
}

#[test]
fn worker_batches_independent_scopes_and_serializes_each_scope() {
    let due_at = TimestampMillis::from_timestamp(3_600_000);
    let table_scope = StreamTrimScope::table("scope/table", TableName::new("orders"));
    let item_scope = StreamTrimScope::item("scope/item", TableName::new("orders"), "hash-1");
    let backend = BatchedScopeBackend {
        markers: vec![
            StreamTrimDueMarker::new(due_at, table_scope.clone(), 1),
            StreamTrimDueMarker::new(due_at, table_scope, 1),
            StreamTrimDueMarker::new(due_at, item_scope, 1),
        ],
        state_batch_sizes: Mutex::new(Vec::new()),
        boundary_batch_sizes: Mutex::new(Vec::new()),
        trim_batch_sizes: Mutex::new(Vec::new()),
        finish_batch_sizes: Mutex::new(Vec::new()),
    };

    let stats = block_on(
        StreamDurationTrimWorker::new(
            &backend,
            StreamDurationTrimConfig {
                marker_page_size: trim_limit(10),
                stream_page_size: trim_limit(25),
            },
        )
        .run_due_page(due_at, TimestampMillis::from_timestamp(7_200_000)),
    )
    .expect("worker should run");

    assert_eq!(stats.due_markers_scanned, 3);
    assert!(
        *backend
            .state_batch_sizes
            .lock()
            .expect("state batch sizes lock")
            == vec![2, 1]
    );
    assert!(
        *backend
            .boundary_batch_sizes
            .lock()
            .expect("boundary batch sizes lock")
            == vec![2, 1]
    );
    assert!(
        *backend
            .finish_batch_sizes
            .lock()
            .expect("finish batch sizes lock")
            == vec![2, 1]
    );
    assert!(
        *backend
            .trim_batch_sizes
            .lock()
            .expect("trim batch sizes lock")
            == vec![2, 1]
    );
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

    let stats = block_on(worker(&backend).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("worker should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    assert_eq!(stats.stale_markers, 1);
    assert_eq!(inner.trim_requests.len(), 0);
    assert!(inner.finished_markers == vec![marker]);
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

    let stats = block_on(worker(&backend).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("worker should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    assert_eq!(stats.forever_markers, 1);
    assert_eq!(inner.trim_requests.len(), 0);
    assert_eq!(inner.writes[0].state.next_due_at, None);
    assert!(inner.writes[0].next_marker.is_none());
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

    let stats = block_on(worker(&backend).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("worker should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    let request = &inner.trim_requests[0];
    assert_eq!(stats.rows_deleted, 2);
    assert_eq!(usize::from(request.page_limit), 25);
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
    assert_eq!(
        inner.writes[0].state.oldest_retained_version,
        Some(ItemStreamVersion::new(3))
    );
    assert_eq!(
        inner.writes[0].state.oldest_retained_timestamp,
        Some(TimestampMillis::from_timestamp(3_700_000))
    );
}

#[test]
fn worker_requeues_marker_when_zero_boundary_has_no_predecessor() {
    let backend = FakeBackend::default();
    let scope = item_scope();
    let marker =
        StreamTrimDueMarker::new(TimestampMillis::from_timestamp(3_600_000), scope.clone(), 1);
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.markers = vec![marker.clone()];
        inner.state = Some(finite_state(scope, 1));
        inner.boundaries = Some(StreamTrimScopeBoundaries {
            latest_item_id: Some(StreamItemId::default()),
            protected_boundary: Some(StreamTrimBoundary {
                item_id: stream_id(100),
            }),
            retained_table_pointer_boundary: None,
        });
    }

    let stats = block_on(worker(&backend).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("worker should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    assert_eq!(stats.protected_markers, 1);
    assert_eq!(inner.trim_requests.len(), 0);
    assert!(inner.finished_markers == vec![marker.clone()]);
    assert_eq!(
        inner.writes[0]
            .next_marker
            .as_ref()
            .map(|marker| marker.due_bucket),
        Some(TimestampMillis::from_timestamp(10_800_000))
    );
    assert_eq!(
        inner.writes[0].state.next_due_at,
        Some(TimestampMillis::from_timestamp(10_800_000))
    );
}

#[test]
fn worker_reschedules_zero_delete_protected_residual_page() {
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
            retained_table_pointer_boundary: None,
        });
        inner.page_result = Some(StreamDurationTrimPageResult {
            deleted_rows: 0,
            first_remaining_version: Some(ItemStreamVersion::new(1)),
            first_remaining_timestamp: Some(TimestampMillis::from_timestamp(0)),
        });
    }

    block_on(worker(&backend).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("worker should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    assert_eq!(inner.trim_requests.len(), 1);
    assert_eq!(inner.writes.len(), 1);
    assert_eq!(
        inner.writes[0]
            .next_marker
            .as_ref()
            .map(|marker| marker.due_bucket),
        Some(TimestampMillis::from_timestamp(10_800_000))
    );
    assert_eq!(
        inner.writes[0].state.next_due_at,
        Some(TimestampMillis::from_timestamp(10_800_000))
    );
}

#[test]
fn worker_does_not_starve_an_eligible_marker_behind_a_protected_page_prefix() {
    let backend = FakeBackend::default();
    let due_at = TimestampMillis::from_timestamp(3_600_000);
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.apply_marker_writes = true;
        for index in 0..=250 {
            let scope =
                StreamTrimScope::table(format!("fairness/{index:03}"), TableName::new("orders"));
            let marker = StreamTrimDueMarker::new(due_at, scope.clone(), 1);
            inner
                .states
                .insert(scope.scope_id.clone(), finite_state(scope.clone(), 1));
            if index < 250 {
                inner.protected_scopes.insert(scope.scope_id.clone());
            }
            inner.markers.push(marker);
        }
    }
    let worker = StreamDurationTrimWorker::new(
        &backend,
        StreamDurationTrimConfig {
            marker_page_size: trim_limit(250),
            stream_page_size: trim_limit(25),
        },
    );

    let first = block_on(worker.run_due_page(due_at, TimestampMillis::from_timestamp(7_200_000)))
        .expect("first worker page should run");
    let second = block_on(worker.run_due_page(due_at, TimestampMillis::from_timestamp(7_200_000)))
        .expect("second worker page should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    assert!(first.marker_page_full);
    assert_eq!(first.protected_markers, 250);
    assert!(!second.marker_page_full);
    assert_eq!(inner.trim_requests.len(), 1);
    assert_eq!(inner.trim_requests[0].scope.scope_id, "fairness/250");
}

#[test]
fn worker_does_not_move_retained_floor_backwards_on_older_page_observation() {
    let backend = FakeBackend::default();
    let scope = table_scope();
    let marker =
        StreamTrimDueMarker::new(TimestampMillis::from_timestamp(3_600_000), scope.clone(), 1);
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        let mut state = finite_state(scope, 1);
        state.oldest_retained_version = Some(ItemStreamVersion::new(11));
        state.oldest_retained_timestamp = Some(TimestampMillis::from_timestamp(4_000_000));
        inner.markers = vec![marker];
        inner.state = Some(state);
        inner.page_result = Some(StreamDurationTrimPageResult {
            deleted_rows: 1,
            first_remaining_version: Some(ItemStreamVersion::new(2)),
            first_remaining_timestamp: Some(TimestampMillis::from_timestamp(3_700_000)),
        });
    }

    block_on(worker(&backend).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("worker should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    let write = &inner.writes[0];
    assert_eq!(
        write.state.oldest_retained_version,
        Some(ItemStreamVersion::new(11))
    );
    assert_eq!(
        write.state.oldest_retained_timestamp,
        Some(TimestampMillis::from_timestamp(4_000_000))
    );
    assert_eq!(
        write.state.next_due_at,
        Some(TimestampMillis::from_timestamp(7_600_000))
    );
    assert_eq!(
        write.next_marker.as_ref().map(|marker| marker.due_bucket),
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

    let first = block_on(worker(&backend).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ));
    assert!(first.is_err());

    block_on(worker(&backend).run_due_page(
        TimestampMillis::from_timestamp(3_600_000),
        TimestampMillis::from_timestamp(7_200_000),
    ))
    .expect("retry should finish");

    let inner = backend.inner.lock().expect("fake backend lock");
    assert_eq!(inner.trim_requests.len(), 2);
    assert!(inner.finished_markers == vec![marker]);
    assert_eq!(inner.writes.len(), 1);
    assert_eq!(
        inner.writes[0].state.oldest_retained_version,
        Some(ItemStreamVersion::new(2))
    );
    assert_eq!(
        inner.writes[0].state.oldest_retained_timestamp,
        Some(TimestampMillis::from_timestamp(3_700_000))
    );
}

#[test]
fn worker_routes_system_trim_without_loading_or_accepting_frontier_state() {
    let backend = FakeBackend::default();
    let scope = system_scope();
    let operation_id = system_operation_id();
    let continuation = system_continuation(&operation_id, 1, "cursor-0");
    let next_continuation = system_continuation(&operation_id, 2, "cursor-1");
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.boundaries = Some(StreamTrimScopeBoundaries {
            latest_item_id: Some(stream_id(99)),
            protected_boundary: Some(StreamTrimBoundary {
                item_id: stream_id(98),
            }),
            retained_table_pointer_boundary: None,
        });
        inner.system_page_result = Some(SystemStreamTrimPageResult::new(
            operation_id,
            2,
            Some(next_continuation.clone()),
        ));
    }

    let page = block_on(worker(&backend).run_system_stream_trim_page(
        scope.clone(),
        TimestampMillis::from_timestamp(3_600_000),
        Some(continuation.clone()),
    ))
    .expect("system trim should run");

    let inner = backend.inner.lock().expect("fake backend lock");
    assert_eq!(page.reclaimed_records(), 2);
    assert_eq!(page.next_continuation(), Some(&next_continuation));
    assert_eq!(inner.boundary_loads, 0);
    assert_eq!(inner.due_marker_loads, 0);
    assert_eq!(inner.trim_requests.len(), 0);
    assert_eq!(inner.system_trim_requests.len(), 1);
    let request = &inner.system_trim_requests[0];
    assert_eq!(request.scope(), &scope);
    assert_eq!(
        request.cutoff_timestamp(),
        TimestampMillis::from_timestamp(3_600_000)
    );
    assert_eq!(request.page_limit(), 25);
    assert_eq!(request.continuation(), Some(&continuation));
}

#[test]
fn given_foreign_cursor_when_system_trim_runs_then_backend_is_not_called_and_request_has_no_frontier()
 {
    let backend = FakeBackend::default();
    let foreign_scope = SystemStreamScope::try_new("payments", 7).expect("foreign scope");
    let foreign_operation = SystemStreamTrimOperationId::new(&foreign_scope, system_cutoff());
    let foreign_cursor = system_continuation(&foreign_operation, 1, "foreign-cursor");

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        Some(foreign_cursor),
    ));
    assert!(result.is_err());

    let request = SystemStreamTrimPageRequest::try_new(system_scope(), system_cutoff(), 25, None)
        .expect("valid request");
    let debug = format!("{request:?}");
    assert!(!debug.contains("frontier"));
    assert!(!debug.contains("physical"));
    let request_with_cursor = SystemStreamTrimPageRequest::try_new(
        system_scope(),
        system_cutoff(),
        25,
        Some(system_continuation(&system_operation_id(), 1, "cursor")),
    )
    .expect("valid request with cursor");
    assert!(!format!("{request_with_cursor:?}").contains("cursor"));

    let inner = backend.inner.lock().expect("fake backend lock");
    assert!(inner.system_trim_requests.is_empty());
}

#[test]
fn given_invalid_scope_when_request_is_built_then_scope_is_rejected_and_cutoff_domain_is_preserved()
{
    assert!(SystemStreamScope::try_new("", 7).is_err());
    assert!(SystemStreamScope::try_new("orders", 0).is_err());

    // TimestampMillis intentionally represents the full signed millisecond
    // domain; there is no second provider-neutral "invalid cutoff" value.
    assert!(
        SystemStreamTrimPageRequest::try_new(
            system_scope(),
            TimestampMillis::from_timestamp(i64::MIN),
            1,
            None,
        )
        .is_ok()
    );
}

#[test]
fn worker_accepts_zero_reclaim_with_explicit_scan_progress() {
    let backend = FakeBackend::default();
    let operation_id = system_operation_id();
    let next_continuation = system_continuation(&operation_id, 2, "cursor-1");
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.system_page_result = Some(SystemStreamTrimPageResult::new_with_scan_progress(
            operation_id,
            0,
            100,
            Some(next_continuation.clone()),
        ));
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        Some(system_continuation(&system_operation_id(), 1, "cursor-0")),
    ))
    .expect("protected rows still advance a bounded scan");

    assert_eq!(result.reclaimed_records(), 0);
    assert_eq!(result.scanned_records(), 100);
    assert_eq!(result.next_continuation(), Some(&next_continuation));
}

#[test]
fn worker_rejects_system_trim_zero_delete_with_continuation() {
    let backend = FakeBackend::default();
    let operation_id = system_operation_id();
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.system_page_result = Some(SystemStreamTrimPageResult::new_with_scan_progress(
            operation_id.clone(),
            0,
            0,
            Some(system_continuation(&operation_id, 2, "cursor-1")),
        ));
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        Some(system_continuation(&operation_id, 1, "cursor-0")),
    ));

    assert!(result.is_err());
}

#[test]
fn worker_rejects_system_trim_reclaim_count_above_scan_progress() {
    let backend = FakeBackend::default();
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.system_page_result = Some(SystemStreamTrimPageResult::new_with_scan_progress(
            system_operation_id(),
            2,
            1,
            None,
        ));
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        None,
    ));

    assert!(result.is_err());
}

#[test]
fn worker_rejects_system_trim_scan_above_shared_bound() {
    let backend = FakeBackend::default();
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.system_page_result = Some(SystemStreamTrimPageResult::new_with_scan_progress(
            system_operation_id(),
            0,
            MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS + 1,
            None,
        ));
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        None,
    ));

    assert!(result.is_err());
}

#[test]
fn worker_rejects_system_trim_nonadvancing_continuation() {
    let backend = FakeBackend::default();
    let operation_id = system_operation_id();
    let continuation = system_continuation(&operation_id, 1, "cursor-0");
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.system_page_result = Some(SystemStreamTrimPageResult::new(
            operation_id,
            1,
            Some(continuation.clone()),
        ));
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        Some(continuation),
    ));

    assert!(result.is_err());
}

#[test]
fn given_foreign_result_when_system_trim_runs_then_worker_rejects_it() {
    let backend = FakeBackend::default();
    let foreign_scope = SystemStreamScope::try_new("payments", 7).expect("foreign scope");
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.system_page_result = Some(SystemStreamTrimPageResult::new(
            SystemStreamTrimOperationId::new(&foreign_scope, system_cutoff()),
            0,
            None,
        ));
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        None,
    ));

    assert!(result.is_err());
}

#[test]
fn given_foreign_result_cursor_when_system_trim_runs_then_worker_rejects_it() {
    let backend = FakeBackend::default();
    let foreign_scope = SystemStreamScope::try_new("payments", 7).expect("foreign scope");
    let foreign_operation = SystemStreamTrimOperationId::new(&foreign_scope, system_cutoff());
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.system_page_result = Some(SystemStreamTrimPageResult::new(
            system_operation_id(),
            1,
            Some(system_continuation(&foreign_operation, 1, "foreign-cursor")),
        ));
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        None,
    ));

    assert!(result.is_err());
}

#[test]
fn given_oversized_result_when_system_trim_runs_then_worker_rejects_it() {
    let backend = FakeBackend::default();
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.system_page_result = Some(SystemStreamTrimPageResult::new(
            system_operation_id(),
            26,
            None,
        ));
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        None,
    ));

    assert!(result.is_err());
}

#[test]
fn given_backend_error_when_system_trim_runs_then_worker_propagates_it() {
    let backend = FakeBackend::default();
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.fail_system_trim = true;
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        None,
    ));

    assert!(result.is_err());
    assert_eq!(
        backend
            .inner
            .lock()
            .expect("fake backend lock")
            .system_trim_requests
            .len(),
        1
    );
}

#[test]
fn given_trim_backend_when_used_as_trait_object_then_it_is_object_safe() {
    fn accepts_trait_object(_: &dyn StreamRetentionRepository) {}

    accepts_trait_object(&FakeBackend::default());
}

#[test]
fn given_pending_trim_when_worker_is_cancelled_then_backend_operation_is_dropped() {
    let dropped = Arc::new(AtomicBool::new(false));
    let backend = PendingSystemBackend {
        dropped: dropped.clone(),
    };
    let worker = SystemStreamTrimExecutor::new(&backend, 25);
    let mut future = Box::pin(worker.run_page(system_scope(), system_cutoff(), None));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    assert!(future.as_mut().poll(&mut context).is_pending());
    drop(future);

    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn given_alternating_cursor_when_sequence_advances_then_worker_cannot_prove_progress() {
    let backend = FakeBackend::default();
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.alternate_system_cursors = true;
    }

    let first = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        TimestampMillis::from_timestamp(3_600_000),
        None,
    ))
    .expect("first page should run");
    let second_cursor = first
        .next_continuation()
        .cloned()
        .expect("first page should continue");
    let second = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        TimestampMillis::from_timestamp(3_600_000),
        Some(second_cursor),
    ))
    .expect("second page should run");
    let third_cursor = second
        .next_continuation()
        .cloned()
        .expect("second page should continue");

    let third = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        TimestampMillis::from_timestamp(3_600_000),
        Some(third_cursor),
    ));

    assert!(
        third.is_ok(),
        "increasing opaque sequences do not prove payload progress"
    );
}

#[test]
fn given_reused_sequence_when_system_trim_continues_then_worker_rejects_it() {
    let backend = FakeBackend::default();
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.repeat_system_sequence = true;
    }

    let first = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        None,
    ))
    .expect("first page should run");
    let first_cursor = first
        .next_continuation()
        .cloned()
        .expect("first page should continue");

    let second = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        Some(first_cursor),
    ));

    assert!(
        second.is_err(),
        "a reused sequence cannot prove cursor progress"
    );
}

#[test]
fn given_max_sequence_when_system_trim_continues_then_worker_rejects_overflow() {
    let backend = FakeBackend::default();
    let operation_id = system_operation_id();
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.system_page_result = Some(SystemStreamTrimPageResult::new(
            operation_id.clone(),
            1,
            Some(system_continuation(&operation_id, 1, "wrapped")),
        ));
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        system_cutoff(),
        Some(system_continuation(&operation_id, u64::MAX, "last")),
    ));

    assert!(result.is_err());
}

#[test]
fn worker_accepts_terminal_empty_system_trim_page() {
    let backend = FakeBackend::default();
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
        inner.system_page_result = Some(SystemStreamTrimPageResult::new(
            system_operation_id(),
            0,
            None,
        ));
    }

    let result = block_on(worker(&backend).run_system_stream_trim_page(
        system_scope(),
        TimestampMillis::from_timestamp(3_600_000),
        None,
    ))
    .expect("empty terminal page is a valid completion");

    assert_eq!(result.reclaimed_records(), 0);
    assert_eq!(result.next_continuation(), None);
}

#[test]
fn system_trim_request_bounds_are_validated_and_cursor_debug_is_redacted() {
    let scope = system_scope();
    let cutoff = TimestampMillis::from_timestamp(3_600_000);
    assert!(SystemStreamTrimPageRequest::try_new(scope.clone(), cutoff, 0, None).is_err());
    assert!(
        SystemStreamTrimPageRequest::try_new(
            scope,
            cutoff,
            MAX_SYSTEM_STREAM_TRIM_PAGE_LIMIT + 1,
            None,
        )
        .is_err()
    );
    let operation_id = system_operation_id();
    assert!(SystemStreamTrimContinuation::try_new(operation_id.clone(), 0, b"cursor").is_err());
    assert!(SystemStreamTrimContinuation::try_new(operation_id.clone(), 1, Vec::new()).is_err());
    assert!(
        SystemStreamTrimContinuation::try_new(
            operation_id.clone(),
            1,
            vec![0; MAX_SYSTEM_STREAM_TRIM_CONTINUATION_BYTES + 1],
        )
        .is_err()
    );

    let continuation = system_continuation(&operation_id, 1, "cursor-0");
    assert!(!format!("{continuation:?}").contains("cursor-0"));
}

impl crate::stream_retention_tests::ModelRetentionIo for BatchedScopeBackend {
    async fn trim_stream_pages(
        &self,
        requests: Vec<StreamDurationTrimPageRequest>,
    ) -> StorageResult<Vec<StreamDurationTrimPageResult>> {
        self.trim_batch_sizes
            .lock()
            .expect("trim batch sizes lock")
            .push(requests.len());
        Ok(requests
            .into_iter()
            .map(|_| StreamDurationTrimPageResult {
                deleted_rows: 0,
                first_remaining_version: None,
                first_remaining_timestamp: None,
            })
            .collect())
    }

    async fn finish_stream_trim_markers(
        &self,
        finishes: Vec<StreamRetentionFinish>,
    ) -> StorageResult<()> {
        self.finish_batch_sizes
            .lock()
            .expect("finish batch sizes lock")
            .push(finishes.len());
        Ok(())
    }
}

impl crate::stream_retention_tests::ModelRetentionIo for FakeBackend {
    async fn trim_stream_pages(
        &self,
        requests: Vec<StreamDurationTrimPageRequest>,
    ) -> StorageResult<Vec<StreamDurationTrimPageResult>> {
        requests
            .into_iter()
            .map(|request| trim_page(&self.inner, request))
            .collect()
    }

    async fn finish_stream_trim_markers(
        &self,
        finishes: Vec<StreamRetentionFinish>,
    ) -> StorageResult<()> {
        let mut inner = self.inner.lock().expect("fake backend lock");
        if inner.fail_next_finish {
            inner.fail_next_finish = false;
            return Err(StorageError::internal("simulated finish failure"));
        }
        for finish in finishes {
            let marker = finish.marker;
            inner.finished_markers.push(marker.clone());
            if let Some(update) = finish.update {
                let write = update.write;
                if inner.apply_marker_writes {
                    inner.markers.retain(|candidate| {
                        candidate.due_bucket != marker.due_bucket
                            || candidate.scope.scope_id != marker.scope.scope_id
                            || candidate.policy_version != marker.policy_version
                    });
                    if let Some(next_marker) = write.next_marker.as_ref() {
                        inner.markers.push(next_marker.clone());
                    }
                }
                inner.state = Some(write.state.clone());
                inner
                    .states
                    .insert(write.state.scope.scope_id.clone(), write.state.clone());
                inner.writes.push(write);
            }
        }
        Ok(())
    }
}
