use replication_types::event::interval::ClosedSourceInterval;

use super::coverage::DeliveryCoverage;

#[test]
fn given_arbitrary_delivery_order_when_coverage_is_persisted_then_exact_union_and_old_snapshots_survive()
 {
    let ranges = (0..6u64)
        .flat_map(|after| {
            ((after + 1)..=6).map(move |through| (after.into(), through.into()).try_into().unwrap())
        })
        .collect::<Vec<ClosedSourceInterval>>();
    for first in &ranges {
        for second in &ranges {
            for third in &ranges {
                assert_coverage_sequence([*first, *second, *third], &ranges);
            }
        }
    }
    let gap = ((u64::MAX - 2).into(), u64::MAX.into()).try_into().unwrap();
    let before = DeliveryCoverage::default().accept(gap, limit()).unwrap();
    let next = before
        .accept(
            (0.into(), (u64::MAX - 2).into()).try_into().unwrap(),
            limit(),
        )
        .unwrap();
    assert_eq!(u64::from(next.through()), u64::MAX);
    assert!(next.pending().is_empty());
    assert_eq!(before.pending(), &[gap]);
    assert!(next.accept(gap, limit()).unwrap() == next);
}

fn assert_coverage_sequence(sequence: [ClosedSourceInterval; 3], ranges: &[ClosedSourceInterval]) {
    let mut coverage = DeliveryCoverage::default();
    let mut accepted = 0u8;
    for range in sequence {
        let snapshot = serde_json::to_vec(&coverage).unwrap();
        let next = coverage.accept(range, limit()).unwrap();
        assert_eq!(snapshot, serde_json::to_vec(&coverage).unwrap());
        for position in (u64::from(range.after()) + 1)..=u64::from(range.through()) {
            accepted |= 1 << position;
        }
        for probe in ranges {
            let covered = ((u64::from(probe.after()) + 1)..=u64::from(probe.through()))
                .all(|position| accepted & (1 << position) != 0);
            assert_eq!(next.has_accepted(*probe), covered);
        }
        let through = (1..=6)
            .take_while(|position| accepted & (1 << position) != 0)
            .count() as u64;
        assert_eq!(u64::from(next.through()), through);
        next.validate(limit()).unwrap();
        let bytes = serde_json::to_vec(&next).unwrap();
        coverage = serde_json::from_slice(&bytes).unwrap();
        assert!(coverage == next);
    }
}

#[test]
fn given_invalid_or_full_coverage_when_accepting_then_errors_preserve_the_old_snapshot() {
    let one = std::num::NonZeroUsize::new(1).unwrap();
    let valid = DeliveryCoverage::default()
        .accept((2.into(), 3.into()).try_into().unwrap(), one)
        .unwrap();
    let snapshot = serde_json::to_vec(&valid).unwrap();
    assert!(
        valid
            .accept((5.into(), 6.into()).try_into().unwrap(), one)
            .is_err()
    );
    assert_eq!(snapshot, serde_json::to_vec(&valid).unwrap());
    for value in [
        serde_json::json!({"through": 3, "pending": [[2, 4]]}),
        serde_json::json!({"through": 0, "pending": [[2, 4], [4, 6]]}),
        serde_json::json!({"through": 0, "pending": [[5, 6], [2, 3]]}),
    ] {
        let invalid: DeliveryCoverage = serde_json::from_value(value).unwrap();
        let before = serde_json::to_vec(&invalid).unwrap();
        assert!(
            invalid
                .accept((0.into(), 1.into()).try_into().unwrap(), limit())
                .is_err()
        );
        assert_eq!(before, serde_json::to_vec(&invalid).unwrap());
    }
}

fn limit() -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(6).unwrap()
}
