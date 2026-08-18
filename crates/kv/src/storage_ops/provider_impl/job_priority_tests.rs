use bg_jobs::{BackgroundJobName, DatabaseJobKind, ImmediateJobKind, PeriodicJobKind};

use super::job_priority::uses_batch_priority;

#[test]
fn every_background_job_has_an_explicit_priority_class() {
    let expected_batch = [
        BackgroundJobName::Database {
            kind: DatabaseJobKind::GsiBackfill,
        },
        BackgroundJobName::Database {
            kind: DatabaseJobKind::QueuePayloadCleanup,
        },
        BackgroundJobName::Database {
            kind: DatabaseJobKind::StreamTtlCleanup,
        },
        BackgroundJobName::Database {
            kind: DatabaseJobKind::StreamTrim,
        },
        BackgroundJobName::Database {
            kind: DatabaseJobKind::TtlSweep,
        },
    ];
    let expected_default = [
        BackgroundJobName::Database {
            kind: DatabaseJobKind::GsiUpdate,
        },
        BackgroundJobName::Database {
            kind: DatabaseJobKind::PartitionFamilyReconcile,
        },
        BackgroundJobName::Periodic {
            kind: PeriodicJobKind::Maintenance,
        },
        BackgroundJobName::Immediate {
            kind: ImmediateJobKind::Task,
        },
    ];

    for name in expected_batch {
        assert!(uses_batch_priority(name), "{name} must use batch priority");
    }
    for name in expected_default {
        assert!(
            !uses_batch_priority(name),
            "{name} must use default priority"
        );
    }

    let classified = expected_batch.len() + expected_default.len();
    assert_eq!(classified, BackgroundJobName::all().len());
}
