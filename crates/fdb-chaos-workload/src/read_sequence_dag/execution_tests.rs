use std::{
    future::{Future, ready},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use super::{
    NormalizedResult, ReadSequenceDagWorkload,
    execution::{
        OperationFailure, OperationFuture, first_normalized_difference, run_with_buggified_retries,
        validate_mapped_result,
    },
};
use crate::imports::AttributeValue;

fn block_on_ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[test]
fn deliberate_mapped_result_mutation_is_rejected() {
    let expected = NormalizedResult::new();
    let mut mutated = expected.clone();
    mutated.insert("child".to_string(), vec![vec![Default::default()]]);

    assert!(validate_mapped_result("mapped", &expected, &mutated).is_err());
}

#[test]
fn mapped_result_mismatch_reports_the_first_stable_difference() {
    let expected = ReadSequenceDagWorkload::expected_result("open");
    let mut mapped = expected.clone();
    mapped
        .get_mut("query")
        .expect("query section")
        .first_mut()
        .expect("query invocation")
        .first_mut()
        .expect("query item")
        .insert(
            "payload".to_string(),
            AttributeValue::S("wrong".to_string()),
        );

    assert_eq!(
        first_normalized_difference(&expected, &mapped),
        Some(
            "\"query\"[0][0].\"payload\": expected Some(S(\"payload-a\")), mapped \
             Some(S(\"wrong\"))"
                .to_string()
        )
    );
}

#[test]
fn mapped_result_mismatch_is_not_retried_as_storage_failure() {
    let mut outcomes = vec![
        Err(OperationFailure::Fatal(
            "mapped result mismatch".to_string(),
        )),
        Ok(()),
    ]
    .into_iter();

    let (result, retries) = block_on_ready(run_with_buggified_retries(
        &mut (),
        true,
        &ReadyClock,
        |_: &mut ()| -> OperationFuture<'_> {
            Box::pin(ready(outcomes.next().expect("test operation outcome")))
        },
    ));

    assert_eq!(
        result,
        Err(OperationFailure::Fatal(
            "mapped result mismatch".to_string()
        ))
    );
    assert_eq!(retries, 0);
}

#[test]
fn retryable_storage_failure_uses_the_buggify_budget() {
    let mut outcomes = vec![
        Err(OperationFailure::RetryableStorage(
            "transaction conflict".to_string(),
        )),
        Ok(()),
    ]
    .into_iter();

    let (result, retries) = block_on_ready(run_with_buggified_retries(
        &mut (),
        true,
        &ReadyClock,
        |_: &mut ()| -> OperationFuture<'_> {
            Box::pin(ready(outcomes.next().expect("test operation outcome")))
        },
    ));

    assert_eq!(result, Ok(()));
    assert_eq!(retries, 1);
}

struct ReadyClock;
impl storage_types::Clock for ReadyClock {
    fn now(&self) -> storage_types::TimestampMillis {
        storage_types::TimestampMillis::from_timestamp(0)
    }
    fn sleep(
        &self,
        _: std::time::Duration,
    ) -> Pin<Box<dyn Future<Output = storage_types::StorageResult<()>> + Send + '_>> {
        Box::pin(ready(Ok(())))
    }
}

#[test]
fn given_unclassified_native_failure_when_classified_then_it_is_not_retried() {
    let error = storage_types::StorageError::internal("corrupt response");
    assert!(matches!(
        OperationFailure::from_storage(error),
        OperationFailure::Fatal(_)
    ));
}

#[test]
fn given_native_settlements_when_classified_then_only_known_conflicts_are_retryable() {
    let conflict = storage_types::StorageEnum::TransactionConflict {
        message: "conflict".into(),
    }
    .into();
    assert!(matches!(
        OperationFailure::from_storage(conflict),
        OperationFailure::RetryableStorage(_)
    ));
    let unknown = storage_types::StorageError::sync_submission_ambiguous(None, "unknown");
    assert!(matches!(
        OperationFailure::from_storage(unknown),
        OperationFailure::Fatal(_)
    ));
}

#[test]
fn given_persistent_conflicts_when_budget_exhausts_then_operation_still_fails() {
    let mut calls = 0;
    let (result, retries) = block_on_ready(run_with_buggified_retries(
        &mut calls,
        true,
        &ReadyClock,
        |calls| {
            *calls += 1;
            Box::pin(ready(Err(OperationFailure::RetryableStorage(
                "conflict".into(),
            ))))
        },
    ));
    assert!(result.is_err());
    assert_eq!(calls, storage_provider::DYNAMO_WRITE_RETRY.max_attempts());
    assert_eq!(u32::from(retries) + 1, calls);
}
