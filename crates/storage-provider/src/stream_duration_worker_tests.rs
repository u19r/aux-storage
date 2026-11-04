//! System retention runner validates native progress without a consumer
//! frontier.
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use async_trait::async_trait;
use storage_types::{StorageError, StorageResult, SystemStreamScope, TimestampMillis};
use stream_provider::{
    MAX_SYSTEM_STREAM_TRIM_CONTINUATION_BYTES, MAX_SYSTEM_STREAM_TRIM_PAGE_LIMIT,
    MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS, SystemStreamTrimContinuation, SystemStreamTrimOperationId,
    SystemStreamTrimPageRequest, SystemStreamTrimPageResult,
};

use crate::{SystemStreamTrimExecutor, SystemStreamTrimRepository};

#[derive(Clone, Default)]
struct FakeBackend {
    inner: Arc<Mutex<FakeBackendState>>,
}
#[derive(Default)]
struct FakeBackendState {
    system_page_result: Option<SystemStreamTrimPageResult>,
    alternate_system_cursors: bool,
    repeat_system_sequence: bool,
    fail_system_trim: bool,
    system_trim_requests: Vec<SystemStreamTrimPageRequest>,
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

struct TestWorker<'a> {
    backend: &'a FakeBackend,
}

impl TestWorker<'_> {
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
fn worker_routes_system_trim_without_loading_or_accepting_frontier_state() {
    let backend = FakeBackend::default();
    let scope = system_scope();
    let operation_id = system_operation_id();
    let continuation = system_continuation(&operation_id, 1, "cursor-0");
    let next_continuation = system_continuation(&operation_id, 2, "cursor-1");
    {
        let mut inner = backend.inner.lock().expect("fake backend lock");
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
    fn accepts_trait_object(_: &dyn SystemStreamTrimRepository) {}

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
