use std::time::{Duration, Instant};

use storage_types::PairId;

use super::{lag::PairLag, recorder_test_support::KeyRecorder};
use crate::constants::REGIONAL_LAG_METRIC;

fn lag_value(recorder: &KeyRecorder) -> f64 {
    let gauges = recorder.gauges_named(REGIONAL_LAG_METRIC);
    assert_eq!(gauges.len(), 1, "{gauges:?}");
    gauges[0].1
}

#[test]
fn given_two_pairs_when_observed_and_ended_then_gauge_is_the_largest_live_lag() {
    let recorder = KeyRecorder::default();
    metrics::with_local_recorder(&recorder, || {
        let (p1, p2) = (PairId::from_bytes([1; 16]), PairId::from_bytes([2; 16]));
        let t0 = Instant::now();
        let lag = PairLag::new("conn-a", Duration::from_secs(60));
        lag.observe_at(p1, 500, t0);
        assert_eq!(lag_value(&recorder), 500.0);
        lag.observe_at(p2, 2_000, t0);
        assert_eq!(lag_value(&recorder), 2000.0);
        lag.observe_at(p1, 100, t0);
        assert_eq!(lag_value(&recorder), 2000.0);
        lag.end_at(p2, t0);
        assert_eq!(lag_value(&recorder), 100.0);
        lag.end_at(p1, t0);
        assert_eq!(lag_value(&recorder), 0.0);
    });
}

#[test]
fn given_pair_observed_before_the_window_when_another_pair_is_observed_then_the_stale_pair_drops_out()
 {
    let recorder = KeyRecorder::default();
    metrics::with_local_recorder(&recorder, || {
        let (p1, p2) = (PairId::from_bytes([1; 16]), PairId::from_bytes([2; 16]));
        let t0 = Instant::now();
        let lag = PairLag::new("conn-a", Duration::from_secs(10));
        lag.observe_at(p1, 9_000, t0);
        lag.observe_at(p2, 10, t0 + Duration::from_secs(10));
        assert_eq!(lag_value(&recorder), 9000.0);
        lag.observe_at(
            p2,
            10,
            t0 + Duration::from_secs(10) + Duration::from_millis(1),
        );
        assert_eq!(lag_value(&recorder), 10.0);
    });
}

#[test]
fn given_lag_gauges_of_two_connections_then_each_has_one_series_labelled_by_connection_only() {
    let recorder = KeyRecorder::default();
    metrics::with_local_recorder(&recorder, || {
        let p1 = PairId::from_bytes([1; 16]);
        let t0 = Instant::now();
        let a = PairLag::new("conn-a", Duration::from_secs(60));
        let b = PairLag::new("conn-b", Duration::from_secs(60));
        a.observe_at(p1, 7, t0);
        b.observe_at(p1, 9, t0);
    });
    let mut gauges = recorder.gauges_named(REGIONAL_LAG_METRIC);
    gauges.sort_by(|left, right| left.1.total_cmp(&right.1));
    assert_eq!(
        gauges,
        [
            (vec![("connection".into(), "conn-a".into())], 7.0),
            (vec![("connection".into(), "conn-b".into())], 9.0)
        ]
    );
}
