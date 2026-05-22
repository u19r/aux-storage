use crate::*;

#[derive(Default)]
struct RecordingCatchupAdapter {
    scan_outcomes: Vec<GsiCatchupOutcome>,
    stream_checkpoints: Vec<String>,
}

impl CatchupApplyAdapter for RecordingCatchupAdapter {
    type Error = CatchupStateError;

    fn apply_scan_observation(
        &mut self,
        apply_case: &GsiCatchupApplyCase,
    ) -> Result<GsiCatchupOutcome, Self::Error> {
        let outcome = plan_gsi_catchup_apply(apply_case);
        self.scan_outcomes.push(outcome);
        Ok(outcome)
    }

    fn apply_stream_record(
        &mut self,
        checkpoint: &StreamDrainCheckpoint,
    ) -> Result<(), Self::Error> {
        self.stream_checkpoints
            .push(checkpoint.as_str().to_string());
        Ok(())
    }
}

fn run_against_narrow_adapter(
    adapter: &mut impl CatchupApplyAdapter<Error = CatchupStateError>,
) -> Result<GsiCatchupOutcome, CatchupStateError> {
    let outcome = adapter.apply_scan_observation(&apply_case())?;
    let checkpoint = StreamDrainCheckpoint::new("tail-1")?;
    adapter.apply_stream_record(&checkpoint)?;
    Ok(outcome)
}

fn apply_case() -> GsiCatchupApplyCase {
    GsiCatchupApplyCase {
        current_version: 1,
        observation_version: 2,
        current_projects: false,
        observation_projects: true,
        history_available: true,
        scan_complete: false,
        drain_complete: false,
    }
}

#[test]
fn catchup_state_rejects_scan_before_boundary_is_protected() {
    let mut state = CatchupState::pending();

    assert_eq!(
        state.apply_scan_observation(&apply_case()),
        Err(CatchupStateError::BoundaryNotProtected)
    );
    state.capture_boundary("tail-1").unwrap();
    assert_eq!(
        state.apply_scan_observation(&apply_case()),
        Err(CatchupStateError::BoundaryNotProtected)
    );
}

#[test]
fn catchup_state_allows_activation_after_scan_and_stream_drain() {
    let mut state = CatchupState::pending();

    state.capture_boundary("tail-1").unwrap();
    state.protect_stream_boundary().unwrap();
    assert_eq!(
        state.apply_scan_observation(&apply_case()).unwrap(),
        GsiCatchupOutcome::AppliedProjection
    );
    state.mark_scan_complete().unwrap();
    state.apply_stream_record("tail-1").unwrap();
    state.mark_stream_drained().unwrap();

    assert_eq!(
        state.activate().unwrap(),
        GsiCatchupOutcome::ActivationAllowed
    );
    state.release_stream_boundary().unwrap();
    assert_eq!(state.protected_boundary, ProtectedBoundaryState::Released);
    state.begin_cleanup().unwrap();
    state.finish_cleanup().unwrap();
    assert_eq!(state.cleanup, CleanupState::Complete);
}

#[test]
fn catchup_state_rejects_cleanup_before_activation() {
    let mut state = CatchupState::pending();

    assert_eq!(state.begin_cleanup(), Err(CatchupStateError::NotActive));
    state.capture_boundary("tail-1").unwrap();
    state.protect_stream_boundary().unwrap();
    state.apply_scan_observation(&apply_case()).unwrap();
    state.mark_scan_complete().unwrap();
    state.apply_stream_record("tail-1").unwrap();
    state.mark_stream_drained().unwrap();
    state.activate().unwrap();

    state.begin_cleanup().unwrap();
    state.finish_cleanup().unwrap();
    assert_eq!(
        state.begin_cleanup(),
        Err(CatchupStateError::CleanupAlreadyComplete)
    );
}

#[test]
fn catchup_state_rejects_activation_before_stream_drain() {
    let mut state = CatchupState::pending();

    state.capture_boundary("tail-1").unwrap();
    state.protect_stream_boundary().unwrap();
    state.apply_scan_observation(&apply_case()).unwrap();
    state.mark_scan_complete().unwrap();

    assert_eq!(
        state.activate(),
        Err(CatchupStateError::StreamDrainNotComplete)
    );
}

#[test]
fn catchup_state_rejects_scan_observation_after_scan_complete() {
    let mut state = CatchupState::pending();

    state.capture_boundary("tail-1").unwrap();
    state.protect_stream_boundary().unwrap();
    state.apply_scan_observation(&apply_case()).unwrap();
    state.mark_scan_complete().unwrap();

    assert_eq!(
        state.apply_scan_observation(&apply_case()),
        Err(CatchupStateError::ScanAlreadyComplete)
    );
}

#[test]
fn catchup_state_rejects_empty_boundary_and_checkpoint() {
    let mut state = CatchupState::pending();

    assert_eq!(
        state.capture_boundary(""),
        Err(CatchupStateError::EmptyBoundary)
    );

    state.capture_boundary("tail-1").unwrap();
    state.protect_stream_boundary().unwrap();
    state.apply_scan_observation(&apply_case()).unwrap();
    state.mark_scan_complete().unwrap();
    assert_eq!(
        state.apply_stream_record(""),
        Err(CatchupStateError::EmptyCheckpoint)
    );
}

#[test]
fn catchup_apply_adapter_exposes_only_catchup_operations() {
    let mut adapter = RecordingCatchupAdapter::default();

    assert_eq!(
        run_against_narrow_adapter(&mut adapter).unwrap(),
        GsiCatchupOutcome::AppliedProjection
    );
    assert_eq!(
        adapter.scan_outcomes,
        vec![GsiCatchupOutcome::AppliedProjection]
    );
    assert_eq!(adapter.stream_checkpoints, vec!["tail-1".to_string()]);
}
