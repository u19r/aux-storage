use crate::*;

#[test]
fn rejects_stale_scan_image_before_it_can_recreate_projection() {
    let apply_case = GsiCatchupApplyCase {
        current_version: 5,
        observation_version: 4,
        current_projects: false,
        observation_projects: true,
        history_available: true,
        scan_complete: false,
        drain_complete: false,
    };

    assert_eq!(
        plan_gsi_catchup_apply(&apply_case),
        GsiCatchupOutcome::RejectedStaleObservation
    );
}
