use std::sync::{Arc, Mutex};

use storage_types::{
    AttemptDeadlines, AttemptMode, AttemptPhase, AttemptPlan, DeadlineClass, NativeUnavailable,
    StorageError, StorageResult,
};

use crate::native_admission::{
    AttemptAdmission, NativeAdmission, NativeAdmissionBinding, NativeAdmissionFuture,
    NativeAdmissionOutcome, NativeAdmissionPermit, NativeWork, settle_native_attempt,
};

fn request(mode: AttemptMode) -> AttemptPlan {
    AttemptPlan::new(mode, DeadlineClass::Write)
}

struct RecordingPermit(Arc<Mutex<Option<bool>>>);

impl NativeAdmissionPermit for RecordingPermit {
    fn complete(
        self: Box<Self>,
        outcome: NativeAdmissionOutcome<'_>,
        _elapsed: std::time::Duration,
    ) {
        *self.0.lock().expect("recording lock") = Some(outcome.timed_out);
    }
}

async fn settle(result: StorageResult<()>) -> Option<bool> {
    let recorded = Arc::new(Mutex::new(None));
    let permit = Box::new(RecordingPermit(Arc::clone(&recorded)));
    let _ = settle_native_attempt(permit, async move { result }).await;
    *recorded.lock().expect("recording lock")
}

#[tokio::test]
async fn deadline_error_reports_a_timeout_to_admission() {
    let deadline: StorageError = storage_types::AttemptFailure::Unavailable(
        NativeUnavailable::Deadline(AttemptPhase::Acquire),
    )
    .into();
    assert_eq!(settle(Err(deadline)).await, Some(true));
}

#[tokio::test]
async fn other_outcomes_do_not_report_a_timeout() {
    let lost: StorageError =
        storage_types::AttemptFailure::Unavailable(NativeUnavailable::ConnectionLost).into();
    assert_eq!(settle(Err(lost)).await, Some(false));
    assert_eq!(
        settle(Err(StorageError::internal("failed"))).await,
        Some(false)
    );
    assert_eq!(settle(Ok(())).await, Some(false));
}

/// Records the work class of each acquisition.
#[derive(Default)]
struct RecordingAdmission {
    works: Mutex<Vec<&'static str>>,
    permit: Arc<Mutex<Option<bool>>>,
}

impl NativeAdmission for RecordingAdmission {
    fn connection_id(&self) -> &str {
        "test"
    }

    fn require_reserved_attempts(&self, _required: std::num::NonZeroUsize) -> StorageResult<()> {
        Ok(())
    }

    fn acquire(&self, work: NativeWork) -> NativeAdmissionFuture<'_> {
        let name = match work {
            NativeWork::Recovery => "recovery",
            NativeWork::PointRead => "point_read",
            NativeWork::RangeRead => "range_read",
            NativeWork::Write => "write",
            NativeWork::Background => "background",
        };
        self.works.lock().expect("works lock").push(name);
        let permit: Box<dyn NativeAdmissionPermit> =
            Box::new(RecordingPermit(Arc::clone(&self.permit)));
        Box::pin(async move { Ok(permit) })
    }

    fn drain_native(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

fn binding(admission: &Arc<RecordingAdmission>) -> Arc<NativeAdmissionBinding> {
    let binding = NativeAdmissionBinding::default();
    binding.bind(admission.clone()).expect("bind once");
    Arc::new(binding)
}

#[tokio::test]
async fn given_caller_lane_when_settled_then_no_permit_is_taken() {
    let unused = Arc::new(RecordingAdmission::default());
    let _bound = binding(&unused);
    let result = AttemptAdmission::Caller
        .settle(
            request(AttemptMode::ReadWrite).start(AttemptDeadlines::default()),
            async { Ok(1) },
        )
        .await;
    assert_eq!(result.expect("attempt result"), 1);
    assert!(unused.works.lock().unwrap().is_empty());
}

#[tokio::test]
async fn given_foreground_lane_when_each_mode_settles_then_read_only_takes_point_read_and_writes_take_write()
 {
    let admission = Arc::new(RecordingAdmission::default());
    let lane = AttemptAdmission::Foreground(binding(&admission));
    for mode in [
        AttemptMode::ReadOnly,
        AttemptMode::ReadWrite,
        AttemptMode::ReadWriteItems,
    ] {
        lane.settle(request(mode).start(AttemptDeadlines::default()), async {
            Ok(())
        })
        .await
        .expect("attempt");
    }
    assert_eq!(
        *admission.works.lock().unwrap(),
        ["point_read", "write", "write"]
    );
}

#[tokio::test]
async fn given_background_lane_when_settled_then_background_work_is_taken() {
    let admission = Arc::new(RecordingAdmission::default());
    let lane = AttemptAdmission::Background(binding(&admission));
    lane.settle(
        request(AttemptMode::ReadOnly).start(AttemptDeadlines::default()),
        async { Ok(()) },
    )
    .await
    .expect("attempt");
    assert_eq!(*admission.works.lock().unwrap(), ["background"]);
}

#[tokio::test]
async fn given_admitted_attempt_when_it_fails_then_the_permit_still_completes() {
    let admission = Arc::new(RecordingAdmission::default());
    let lane = AttemptAdmission::Foreground(binding(&admission));
    let result: StorageResult<()> = lane
        .settle(
            request(AttemptMode::ReadWrite).start(AttemptDeadlines::default()),
            async { Err(StorageError::internal("failed")) },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(*admission.permit.lock().unwrap(), Some(false));
}

#[tokio::test]
async fn given_foreground_lane_when_request_names_range_read_then_range_read_is_taken() {
    let admission = Arc::new(RecordingAdmission::default());
    let lane = AttemptAdmission::Foreground(binding(&admission));
    lane.settle(
        request(AttemptMode::ReadOnly)
            .work(NativeWork::RangeRead)
            .start(AttemptDeadlines::default()),
        async { Ok(()) },
    )
    .await
    .expect("attempt");
    assert_eq!(*admission.works.lock().unwrap(), ["range_read"]);
}

#[tokio::test]
async fn given_background_lane_when_request_names_work_then_only_recovery_is_kept() {
    let admission = Arc::new(RecordingAdmission::default());
    let lane = AttemptAdmission::Background(binding(&admission));
    for request in [
        request(AttemptMode::ReadOnly)
            .work(NativeWork::RangeRead)
            .start(AttemptDeadlines::default()),
        request(AttemptMode::ReadWrite).start(AttemptDeadlines::default()),
        request(AttemptMode::ReadWrite)
            .work(NativeWork::Recovery)
            .start(AttemptDeadlines::default()),
    ] {
        lane.settle(request, async { Ok(()) })
            .await
            .expect("attempt");
    }
    assert_eq!(
        *admission.works.lock().unwrap(),
        ["background", "background", "recovery"]
    );
}
