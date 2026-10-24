use alloc_counter::AllocationGuard;

use crate::context::ErrorContext;

#[derive(Debug, PartialEq, Eq)]
struct TestError(String);

impl ErrorContext<TestError, (), TestError> for Result<(), TestError> {
    fn context(self, context: impl Into<String>) -> Result<(), TestError> {
        match self {
            Ok(()) => Ok(()),
            Err(error) => Err(TestError(format!("{}: {}", context.into(), error.0))),
        }
    }
}

fn eager_with_context<F: FnOnce() -> String>(
    result: Result<(), TestError>,
    context: F,
) -> Result<(), TestError> {
    result.context(context())
}

type ContextApply = fn(Result<(), TestError>, fn() -> String) -> Result<(), TestError>;

fn measure_context_allocations(
    label: &'static str,
    apply: ContextApply,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "error_context_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..256 {
        let result = apply(Ok(()), || format!("unused context {}", 42));
        let _ = std::hint::black_box(result);
    }
    guard.finish()
}

fn apply_eager(result: Result<(), TestError>, context: fn() -> String) -> Result<(), TestError> {
    eager_with_context(result, context)
}

fn apply_lazy(result: Result<(), TestError>, context: fn() -> String) -> Result<(), TestError> {
    result.with_context(context)
}

#[test]
fn given_success_result_when_with_context_then_context_closure_is_not_evaluated() {
    let mut evaluated = false;
    let result = Ok::<_, TestError>(()).with_context(|| {
        evaluated = true;
        "unused".to_owned()
    });
    assert_eq!(result, Ok(()));
    assert!(!evaluated);
}

#[test]
fn given_error_result_when_with_context_then_error_context_is_preserved() {
    let result = Err::<(), _>(TestError("root".to_owned()))
        .with_context(|| "outer".to_owned())
        .expect_err("the original error must remain an error");
    assert_eq!(result, TestError("outer: root".to_owned()));
}

#[test]
fn given_success_results_when_with_context_is_profiled_then_lazy_path_allocates_nothing() {
    let eager = measure_context_allocations("eager", apply_eager);
    let lazy = measure_context_allocations("lazy", apply_lazy);
    let _ = alloc_counter::emit_report(&eager);
    let _ = alloc_counter::emit_report(&lazy);
    assert!(lazy.allocation_count < eager.allocation_count);
    assert!(lazy.allocated_bytes < eager.allocated_bytes);
}

#[test]
fn given_contextual_storage_error_when_cloned_then_cause_and_context_are_preserved_without_allocation()
 {
    use crate::{StorageError, context::WrappedError as _};

    let error = StorageError::service_unavailable(7).with_context("commit attempt");
    let guard =
        AllocationGuard::start(module_path!(), "shared_error_clone", file!(), line!(), None);
    for _ in 0..256 {
        let cloned = std::hint::black_box(error.clone());
        assert!(std::ptr::eq(error.to_enum(), cloned.to_enum()));
    }
    let report = guard.finish();
    assert_eq!(report.allocation_count, 0);
    assert_eq!(report.allocated_bytes, 0);
    let cloned = error.clone();
    assert_eq!(
        error.recursive_context(Vec::new()).1,
        vec!["commit attempt"]
    );
    assert_eq!(error.to_string(), cloned.to_string());
    assert!(
        cloned.base_error().is_none(),
        "context must not become a base-only retry"
    );
}

#[test]
fn given_shared_storage_error_when_consumed_then_the_original_opaque_json_cause_is_preserved() {
    use crate::{StorageEnum, StorageError};

    let cause = serde_json::from_str::<serde_json::Value>("{").expect_err("invalid JSON");
    let error = StorageError::from(cause).with_context("decode item");
    let guard = AllocationGuard::start(
        module_path!(),
        "shared_error_projection",
        file!(),
        line!(),
        None,
    );
    let first = error.clone().into_enum();
    let second = error.into_enum();
    let report = guard.finish();
    assert_eq!(report.allocation_count, 0);
    assert_eq!(report.allocated_bytes, 0);
    assert!(
        std::error::Error::source(&first)
            .expect("typed source")
            .is::<serde_json::Error>()
    );
    let (StorageEnum::Serialization(first), StorageEnum::Serialization(second)) = (first, second)
    else {
        panic!("serialization category changed");
    };
    assert!(std::ptr::eq(first.as_ref(), second.as_ref()));
    assert_eq!(first.line(), 1);
}

#[test]
fn given_base_storage_error_when_cloned_then_base_only_classification_is_unchanged() {
    use crate::{StorageEnum, StorageError};

    let error = StorageError::from(StorageEnum::ConditionalCheckFailed);
    let cloned = error.clone();
    assert!(matches!(
        cloned.base_error(),
        Some(StorageEnum::ConditionalCheckFailed)
    ));
    assert!(std::ptr::eq(
        error.base_error().expect("base"),
        cloned.base_error().expect("base")
    ));
}
