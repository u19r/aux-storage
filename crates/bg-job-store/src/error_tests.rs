use scheduled_bg_jobs::store::StoreFailure;
use storage_provider::RawOrderedJobStoreError;
use storage_types::{AttemptFailure, AttemptPhase, NativeUnavailable, StorageError, expired_in};

use super::error::*;

#[test]
fn given_storage_failures_when_projected_then_by_class() {
    let project =
        |error: StorageError| ScheduledJobStoreError::from(RawOrderedJobStoreError::Backend(error));

    let unavailable = [
        StorageError::from(AttemptFailure::Unavailable(
            NativeUnavailable::ConnectionLost,
        )),
        expired_in(AttemptPhase::Statement),
        StorageError::from(AttemptFailure::FRESH_VALIDATION),
        StorageError::logical_conflict("test conflict"),
        StorageError::service_unavailable(7),
    ];
    for error in unavailable {
        let label = error.label();
        assert_eq!(
            project(error),
            ScheduledJobStoreError::Backend(StoreFailure::Unavailable),
            "{label}"
        );
    }

    let permanent = [
        StorageError::commit_unknown("lost"),
        StorageError::from(AttemptFailure::CAPACITY),
        StorageError::internal("secret detail"),
    ];
    for error in permanent {
        let label = error.label();
        assert_eq!(
            project(error),
            ScheduledJobStoreError::Backend(StoreFailure::Permanent),
            "{label}"
        );
    }

    assert_eq!(
        project(StorageError::unsupported("x")),
        ScheduledJobStoreError::Backend(StoreFailure::Unsupported)
    );
}
