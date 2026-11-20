use std::num::NonZeroU64;

use scheduled_bg_jobs::{JobId, Namespace};
use storage_types::{
    AttemptKind, Revision, StorageEnum, StorageError,
    maintenance::{MaintenanceKind, TargetIncarnation, TaskAuthority, TaskProgress, TaskScope},
    storage_serde,
};

use super::progress::*;

fn progress(target: &[u8], revision: u64) -> TaskProgress<()> {
    TaskProgress {
        authority: TaskAuthority {
            scope: TaskScope {
                destination: Namespace::try_new("progress-contract").expect("valid namespace"),
                target: TargetIncarnation::try_from(target.to_vec()).expect("valid incarnation"),
                work: MaintenanceKind::TtlSweep,
            },
            job: JobId::from([7; JobId::BYTE_LENGTH]),
            fence: NonZeroU64::new(1).expect("non-zero fence"),
        },
        revision: Revision::from(revision),
        state: (),
    }
}

fn assert_internal(error: &StorageError, expected: &str) {
    assert!(
        matches!(error.as_ref(), StorageEnum::InternalServerError { message } if message == expected),
        "unexpected error: {error}"
    );
    assert_eq!(error.attempt_kind(), None);
}

fn assert_conflict(result: Result<ProgressWrite, StorageError>) {
    let Err(error) = result else {
        panic!("the progress write must miss");
    };
    assert_eq!(error.attempt_kind(), Some(AttemptKind::Conflict));
}

#[test]
fn given_other_kind_when_writing_then_internal() {
    let observed = progress(b"database-a", 1);
    let next = progress(b"database-a", 2);
    let Err(error) = ProgressWrite::new(
        MaintenanceKind::GsiBackfill,
        &ProgressFence::observed(Some(&observed)),
        Some(Revision::from(1)),
        &next,
    ) else {
        panic!("a progress of another kind must be rejected");
    };
    assert_internal(&error, "maintenance task progress has the wrong kind");
}

#[test]
fn given_fence_revision_differs_when_writing_then_write_miss() {
    let observed = progress(b"database-a", 1);
    let next = progress(b"database-a", 2);
    assert_conflict(ProgressWrite::new(
        MaintenanceKind::TtlSweep,
        &ProgressFence::observed(Some(&observed)),
        Some(Revision::from(3)),
        &next,
    ));
}

#[test]
fn given_fence_target_differs_when_writing_then_write_miss() {
    let observed = progress(b"database-a", 1);
    let next = progress(b"database-b", 2);
    assert_conflict(ProgressWrite::new(
        MaintenanceKind::TtlSweep,
        &ProgressFence::observed(Some(&observed)),
        Some(Revision::from(1)),
        &next,
    ));
}

#[test]
fn given_present_progress_when_inserting_then_write_miss() {
    let observed = progress(b"database-a", 1);
    let next = progress(b"database-a", 2);
    assert_conflict(ProgressWrite::new(
        MaintenanceKind::TtlSweep,
        &ProgressFence::observed(Some(&observed)),
        None,
        &next,
    ));
}

#[test]
fn given_absent_progress_when_updating_then_write_miss() {
    let next = progress(b"database-a", 2);
    assert_conflict(ProgressWrite::new(
        MaintenanceKind::TtlSweep,
        &ProgressFence::observed::<()>(None),
        Some(Revision::from(1)),
        &next,
    ));
}

#[test]
fn given_matching_fence_when_writing_then_bytes_and_target_are_set() {
    let observed = progress(b"database-a", 1);
    let next = progress(b"database-a", 2);
    let Ok(write) = ProgressWrite::new(
        MaintenanceKind::TtlSweep,
        &ProgressFence::observed(Some(&observed)),
        Some(Revision::from(1)),
        &next,
    ) else {
        panic!("a matching fence must allow the write");
    };
    assert!(write.kind == MaintenanceKind::TtlSweep);
    assert_eq!(write.expected, Some(Revision::from(1)));
    assert_eq!(write.revision, Revision::from(2));
    assert_eq!(write.target, next.authority.scope.target);
    assert_eq!(write.bytes, storage_serde::to_bytes(&next).unwrap());
}

#[test]
fn given_stored_progress_of_other_kind_when_decoding_then_internal() {
    let next = progress(b"database-a", 2);
    let bytes = storage_serde::to_bytes(&next).unwrap();
    let Err(error) = decode_task_progress::<()>(&bytes, MaintenanceKind::GsiBackfill) else {
        panic!("a stored progress of another kind must be rejected");
    };
    assert_internal(&error, "stored maintenance task progress has another kind");
}
