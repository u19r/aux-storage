use crate::{
    BackfillControl, BackfillPolicy, CatchupSessionState, CatchupStateError, GsiBackfillPolicy,
    GsiCatchupApplyCase, GsiCatchupOutcome, GsiKeyMapping, GsiProjection, GsiScanObservation,
    GsiStreamRecord, GsiTombstoneEvidence, StreamDrainCheckpoint,
};

fn gsi_scan_observation() -> GsiScanObservation {
    GsiScanObservation {
        apply_case: GsiCatchupApplyCase {
            current_version: 1,
            observation_version: 1,
            current_projects: true,
            observation_projects: true,
            history_available: true,
            scan_complete: false,
            drain_complete: false,
        },
    }
}

#[test]
fn generic_control_uses_policy_name_and_shared_state() {
    let mut control = BackfillControl::new(GsiBackfillPolicy);

    assert_eq!(control.policy_name(), "gsi");
    control.capture_boundary("tail-1").unwrap();
    control.protect_stream_boundary().unwrap();
    assert_eq!(
        control
            .apply_scan_observation(&gsi_scan_observation())
            .unwrap(),
        GsiCatchupOutcome::AppliedProjection
    );
    control.mark_scan_complete().unwrap();
    control
        .apply_stream_record(&GsiStreamRecord {
            checkpoint: StreamDrainCheckpoint::new("tail-2").unwrap(),
        })
        .unwrap();
    control.mark_stream_drained().unwrap();
    assert_eq!(
        control.activate().unwrap(),
        GsiCatchupOutcome::ActivationAllowed
    );
    assert_eq!(control.state().session, CatchupSessionState::Active);
}

#[test]
fn gsi_policy_owns_projection_key_mapping_and_tombstone_evidence() {
    fn accepts_policy<
        P: BackfillPolicy<
                Projection = GsiProjection,
                KeyMapping = GsiKeyMapping,
                TombstoneEvidence = GsiTombstoneEvidence,
            >,
    >(
        _: P,
    ) {
    }

    accepts_policy(GsiBackfillPolicy);
    assert!(GsiProjection { projects: true }.projects);
    assert_eq!(
        GsiKeyMapping {
            source_key: "source".to_string(),
            gsi_key: Some("gsi".to_string()),
        }
        .gsi_key
        .as_deref(),
        Some("gsi")
    );
    let evidence = GsiTombstoneEvidence {
        hidden: true,
        isolated_from_query_prefix: true,
    };
    assert!(evidence.hidden);
    assert!(evidence.isolated_from_query_prefix);
}

#[test]
fn generic_control_owns_cleanup_ordering() {
    let mut control = BackfillControl::new(GsiBackfillPolicy);

    assert_eq!(control.begin_cleanup(), Err(CatchupStateError::NotActive));
    control.capture_boundary("tail-1").unwrap();
    control.protect_stream_boundary().unwrap();
    control
        .apply_scan_observation(&gsi_scan_observation())
        .unwrap();
    control.mark_scan_complete().unwrap();
    control
        .apply_stream_record(&GsiStreamRecord {
            checkpoint: StreamDrainCheckpoint::new("tail-2").unwrap(),
        })
        .unwrap();
    control.mark_stream_drained().unwrap();
    control.activate().unwrap();
    control.begin_cleanup().unwrap();
    control.finish_cleanup().unwrap();
}
