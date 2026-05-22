use bg_jobs::JobLockError;
use storage_types::{StorageEnum, StorageError};

use crate::SysJobLockStore;

#[test]
fn given_conditional_check_failure_when_mapping_lock_error_then_treats_as_contention() {
    let error = StorageError::Base(StorageEnum::ConditionalCheckFailed);

    let mapped = SysJobLockStore::map_storage_error(error);

    assert!(matches!(mapped, JobLockError::Contention { .. }));
}
