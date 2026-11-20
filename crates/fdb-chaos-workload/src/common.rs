use crate::{
    constants::{
        OPTION_ACTIVE_CLIENT_COUNT, OPTION_ARTIFACT_ROOT, OPTION_BUGGIFY,
        OPTION_HISTORY_SAMPLE_LIMIT, OPTION_KEY_COUNT, OPTION_MESSAGE_COUNT, OPTION_NODE_COUNT,
        OPTION_OPERATION_COUNT, OPTION_PROFILE, OPTION_RECREATE_COUNT, OPTION_SEED,
        OPTION_SHARED_KEY_COUNT, OPTION_SHARED_OPERATION_PERCENT, OPTION_WRITER_COUNT,
        QUEUE_IDEMPOTENT_CALL_ATTEMPTS, QUEUE_REDELIVERY_POLL_MILLIS,
    },
    imports::*,
};

pub(crate) fn option_or_default<T>(context: &WorkloadContext, name: &str, default: T) -> T
where T: std::str::FromStr {
    context.get_option(name).unwrap_or(default)
}

pub(crate) fn option_or_default_string(
    context: &WorkloadContext,
    name: &str,
    default: &str,
) -> String {
    context
        .get_option(name)
        .unwrap_or_else(|| default.to_string())
}

pub(crate) fn metric_val_u64(key: &'static str, value: u64) -> Metric<'static> {
    Metric {
        key,
        val: value as f64,
        avg: false,
        fmt: None,
    }
}

pub(crate) fn gsi_score(key: &str, value: &str) -> String {
    let hash = key.bytes().chain(value.bytes()).fold(0_u64, |acc, byte| {
        acc.wrapping_mul(131).wrapping_add(u64::from(byte))
    });
    (hash % 100_000).to_string()
}

pub(crate) fn string_attr(
    item: &impl storage_types::AttributeValueLookup,
    name: &str,
) -> Result<String, String> {
    match item.get_attribute_value(name) {
        Some(AttributeValue::S(value)) => Ok(value.clone()),
        Some(other) => Err(format!(
            "attribute {name} has non-string value of type {}",
            other.variant_name()
        )),
        None => Err(format!("attribute {name} is missing")),
    }
}

pub(crate) fn storage_error_detail(error: &StorageError) -> String {
    let (inner, context) = error.recursive_context(Vec::new());
    let mut detail = match inner {
        StorageEnum::InternalServerError { message } => {
            format!("internal_server_error: {message}")
        }
        StorageEnum::Attempt(failure @ storage_types::AttemptFailure::Conflict(_)) => {
            format!("transaction_conflict: {failure}")
        }
        StorageEnum::TransactionInProgress { message } => {
            format!("transaction_in_progress: {message}")
        }
        StorageEnum::Throttled { message } => {
            format!("throttled: {message}")
        }
        StorageEnum::ProvisionedThroughputExceeded { message } => {
            format!("provisioned_throughput_exceeded: {message}")
        }
        StorageEnum::AwsService { code, message } => {
            format!("aws_service code={code:?}: {message}")
        }
        _ => error.to_string(),
    };
    if !context.is_empty() {
        detail.push_str("; context=");
        detail.push_str(&context.join(" > "));
    }
    detail
}

pub(crate) fn is_condition_failure(error: &StorageError) -> bool {
    matches!(
        error.as_ref(),
        StorageEnum::ConditionalCheckFailed | StorageEnum::ConditionalCheckFailedWithItem { .. }
    ) || matches!(
        error.as_ref(),
        StorageEnum::TransactionCanceled { reasons }
            if reasons.iter().any(storage_types::CancellationReason::is_conditional_check_failed)
    )
}

pub(crate) fn consume_noop_options(context: &WorkloadContext) {
    let _: Option<String> = context.get_option(OPTION_PROFILE);
    let _: Option<String> = context.get_option(OPTION_OPERATION_COUNT);
    let _: Option<String> = context.get_option(OPTION_HISTORY_SAMPLE_LIMIT);
    let _: Option<String> = context.get_option(OPTION_ARTIFACT_ROOT);
    let _: Option<String> = context.get_option(OPTION_KEY_COUNT);
    let _: Option<String> = context.get_option(OPTION_ACTIVE_CLIENT_COUNT);
    let _: Option<String> = context.get_option(OPTION_SHARED_KEY_COUNT);
    let _: Option<String> = context.get_option(OPTION_SHARED_OPERATION_PERCENT);
    let _: Option<String> = context.get_option(OPTION_SEED);
    let _: Option<String> = context.get_option(OPTION_BUGGIFY);
    let _: Option<String> = context.get_option(OPTION_NODE_COUNT);
    let _: Option<String> = context.get_option(OPTION_MESSAGE_COUNT);
    let _: Option<String> = context.get_option(OPTION_WRITER_COUNT);
    let _: Option<String> = context.get_option(OPTION_RECREATE_COUNT);
}

/// Queue outcomes that a simulated client meets under fault injection and
/// handles as an SQS client does: it calls again.
pub(crate) enum QueueRetry {
    /// The attempt conflicted and nothing committed.
    Conflict,
    /// FoundationDB did not report the commit outcome.
    Ambiguous,
}

pub(crate) fn queue_retry_kind(error: &QueueError) -> Option<QueueRetry> {
    match error {
        QueueError::StorageError(error) if error.is_commit_unknown() => Some(QueueRetry::Ambiguous),
        QueueError::StorageError(error) if error.class().is_retryable() => {
            Some(QueueRetry::Conflict)
        }
        _ => None,
    }
}

/// One finished queue call as a workload counts it (W3-H5). An unknown
/// commit outcome is a correct answer of the store: count it, never trace
/// it as an error.
pub(crate) enum QueueCall<T> {
    Done(T),
    /// The attempt aborted; nothing committed. The caller may call again.
    Aborted,
    /// FoundationDB did not report whether the commit happened.
    Unknown,
    Failed(QueueError),
}

impl<T> From<Result<T, QueueError>> for QueueCall<T> {
    fn from(result: Result<T, QueueError>) -> Self {
        match result {
            Ok(value) => Self::Done(value),
            Err(error) => match queue_retry_kind(&error) {
                Some(QueueRetry::Ambiguous) => Self::Unknown,
                Some(QueueRetry::Conflict) => Self::Aborted,
                None => Self::Failed(error),
            },
        }
    }
}

/// Call an idempotent queue operation (a delete or a visibility change with
/// one receipt) again after a conflict or an unknown commit outcome.
pub(crate) async fn retry_idempotent_queue_call<T, F, Fut>(
    clock: &crate::simulation_clock::SimulationClock,
    mut call: F,
) -> Result<T, QueueError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, QueueError>>,
{
    let pause = std::time::Duration::from_millis(u64::from(QUEUE_REDELIVERY_POLL_MILLIS));
    for _ in 1..QUEUE_IDEMPOTENT_CALL_ATTEMPTS {
        match call().await {
            Err(error) if queue_retry_kind(&error).is_some() => {
                storage_types::Clock::sleep(clock, pause)
                    .await
                    .map_err(QueueError::StorageError)?;
            }
            result => return result,
        }
    }
    call().await
}

/// Resolve a queue URL, calling again after a retryable outcome such as an
/// expired statement deadline under simulated cluster recovery. The read has
/// no effects, so every retryable or unknown outcome may be repeated.
pub(crate) async fn queue_url_retrying(
    clock: Option<&std::sync::Arc<crate::simulation_clock::SimulationClock>>,
    manager: &queue::QueueManager,
    queue_name: &str,
) -> Result<String, String> {
    let clock = clock.ok_or("simulation phase clock is absent")?;
    retry_idempotent_queue_call(clock.as_ref(), || {
        manager.get_queue_url(queue::GetQueueUrlRequest {
            queue_name: queue_name.to_owned(),
            ..Default::default()
        })
    })
    .await
    .map(|response| response.queue_url)
    .map_err(|error| error.to_string())
}
