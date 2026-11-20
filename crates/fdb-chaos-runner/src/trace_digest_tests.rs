use std::fs;

use crate::trace_digest::{simulated_event_key, write_trace_digest};

#[test]
fn given_two_runs_that_differ_only_in_real_values_when_digesting_then_digests_are_equal() {
    let root = std::env::temp_dir().join(format!("fdb-chaos-digest-{}", std::process::id()));
    let event = |real: &str, id: &str| {
        format!(
            "<Event Severity=\"10\" Time=\"1.5\" DateTime=\"{real}\" Type=\"Commit\" \
             Machine=\"2.0.1.0:1\" ID=\"{id}\" RealTime=\"{real}\" />\n<Event Severity=\"10\" \
             Time=\"1.6\" Type=\"HugeArenaSample\" Machine=\"x\" Size=\"{real}\" />\n"
        )
    };
    let mut digests = Vec::new();
    for (run, (real, id)) in [("2026-a", "01"), ("2027-b", "02")].into_iter().enumerate() {
        let dir = root.join(run.to_string());
        fs::create_dir_all(dir.join("traces")).unwrap();
        fs::write(dir.join("traces/trace.a.0.1.xml"), event(real, id)).unwrap();
        fs::write(dir.join("fdbserver-output.log"), "Unseed: 42\n").unwrap();
        digests.push(write_trace_digest(&dir).unwrap());
    }
    assert_eq!(digests[0], digests[1]);
    assert_eq!(digests[0].events, 1);
    assert_eq!(digests[0].unseed.as_deref(), Some("42"));
    assert_eq!(
        simulated_event_key("<Event Severity=\"20\" Time=\"2.0\" Type=\"Commit\" Machine=\"m\" />"),
        Some("2.0 Commit m 20".to_string())
    );
    assert_ne!(
        simulated_event_key("<Event Severity=\"20\" Time=\"2.0\" Type=\"Commit\" Machine=\"m\" />"),
        simulated_event_key("<Event Severity=\"20\" Time=\"2.1\" Type=\"Commit\" Machine=\"m\" />")
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn given_workload_events_with_different_details_when_keyed_then_keys_differ() {
    let line = |error: &str| {
        format!(
            "<Event Severity=\"40\" Time=\"3.0\" DateTime=\"x\" Type=\"AuxError\" Machine=\"m\" \
             Error=\"{error}\" RustWorkload=\"1\" ThreadID=\"7\" />"
        )
    };
    assert_ne!(
        simulated_event_key(&line("a")),
        simulated_event_key(&line("b"))
    );
    assert_eq!(
        simulated_event_key(&line("a")),
        Some(
            "3.0 AuxError m 40 Severity=40 Time=3.0 Type=AuxError Machine=m Error=a RustWorkload=1"
                .to_string()
        )
    );
}

#[test]
fn given_trace_rollover_copies_when_reordered_then_only_original_events_determine_digest() {
    let root = std::env::temp_dir().join(format!("fdb-chaos-rolled-digest-{}", std::process::id()));
    let original = |detail: &str, severity: u8| {
        format!(
            "<Event Severity=\"10\" Time=\"1.0\" Type=\"AuxObservation\" Machine=\"m\" \
             OriginalTime=\"0.5\" Detail=\"{detail}\" RustWorkload=\"1\" />\n<Event \
             Severity=\"{severity}\" Time=\"2.0\" Type=\"Role\" Machine=\"m\" \
             TrackLatestType=\"Original\" />\n"
        )
    };
    let rolled = |time: &str, real: &str, event_type: &str| {
        format!(
            "<Event Severity=\"10\" Time=\"{time}\" Type=\"{event_type}\" Machine=\"m\" \
             OriginalTime=\"1.0\" OriginalDateTime=\"{real}\" TrackLatestType=\"Rolled\" \
             RustWorkload=\"1\" Detail=\"replayed\" />\n"
        )
    };
    let traces = [
        format!(
            "{}{}{}",
            original("first", 10),
            rolled("3.0", "2026-a", "Role"),
            rolled("3.0", "2026-a", "AuxObservation"),
        ),
        format!(
            "{}{}{}",
            original("first", 10),
            rolled("4.0", "2027-b", "AuxObservation"),
            rolled("4.0", "2027-b", "Role"),
        ),
        original("changed", 10),
        original("first", 20),
    ];
    let mut digests = Vec::new();
    for (run, trace) in traces.iter().enumerate() {
        let dir = root.join(run.to_string());
        fs::create_dir_all(dir.join("traces")).unwrap();
        fs::write(dir.join("traces/trace.a.0.1.xml"), trace).unwrap();
        digests.push(write_trace_digest(&dir).unwrap());
    }
    assert_eq!(digests[0], digests[1]);
    // An OriginalTime field alone does not make an original record a replay.
    assert_eq!(digests[0].events, 2);
    assert_ne!(digests[0].sha256, digests[2].sha256);
    assert_ne!(digests[0].sha256, digests[3].sha256);
    fs::remove_dir_all(root).unwrap();
}
