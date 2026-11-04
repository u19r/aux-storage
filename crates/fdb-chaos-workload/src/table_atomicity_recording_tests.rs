use storage_types::{StorageEnum, StorageError};

use super::table_atomicity_recording::FailureDisposition;

#[test]
fn given_known_aborted_native_request_when_classified_then_it_requires_no_side_effects() {
    let error: StorageError = StorageEnum::TransactionConflict {
        message: "FoundationDB transaction conflict (code 1078)".into(),
    }
    .into();
    assert_eq!(
        FailureDisposition::from(&error),
        FailureDisposition::KnownAborted
    );
}

#[test]
fn given_lost_commit_acknowledgment_when_classified_then_both_outcomes_stay_possible() {
    let error = StorageError::sync_submission_ambiguous(None, "lost acknowledgment");
    assert_eq!(
        FailureDisposition::from(&error),
        FailureDisposition::Unknown
    );
}

#[test]
fn given_internal_error_with_retry_words_when_classified_then_it_still_fails_the_workload() {
    let error =
        StorageError::internal("transaction_conflict maybe_committed commit_unknown_result");
    assert_eq!(
        FailureDisposition::from(&error),
        FailureDisposition::Unexpected
    );
}
