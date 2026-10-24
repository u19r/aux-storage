use std::{num::NonZeroUsize, sync::Arc};

use super::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    record::{DurableRecord, DurableRecordKey, RecordAddress, RecordChange, RecordExpectation},
};

fn limits() -> InfrastructureLimits {
    let n = |value| NonZeroUsize::new(value).unwrap();
    InfrastructureLimits::try_new(n(128), n(64), n(512), n(8)).unwrap()
}

fn record(revision: u64, bytes: usize) -> DurableRecord {
    DurableRecord {
        key: DurableRecordKey::try_from(RecordAddress {
            namespace: "native".into(),
            resource: vec![1],
            generation: u64::MAX,
            suffix: vec![2],
        })
        .unwrap(),
        revision: revision.into(),
        bytes: Arc::from(vec![3; bytes]),
    }
}

#[test]
fn given_a_valid_borrowed_batch_when_constructed_then_original_effects_and_budgets_survive() {
    let changes = [RecordChange::Store {
        expected: RecordExpectation::Revision(7.into()),
        record: record(8, 64),
    }];
    let batch = RecordBatch::try_new(&changes, limits()).unwrap();
    assert!(std::ptr::eq(batch.as_ref(), changes.as_slice()));
    assert_eq!(batch.limits(), limits());
}
