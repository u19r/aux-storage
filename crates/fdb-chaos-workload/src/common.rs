use crate::{
    constants::{
        OPTION_ACTIVE_CLIENT_COUNT, OPTION_ARTIFACT_ROOT, OPTION_BUGGIFY,
        OPTION_HISTORY_SAMPLE_LIMIT, OPTION_KEY_COUNT, OPTION_OPERATION_COUNT, OPTION_PROFILE,
        OPTION_SEED, OPTION_SHARED_KEY_COUNT, OPTION_SHARED_OPERATION_PERCENT,
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
        Some(other) => Err(format!("attribute {name} has non-string value: {other:?}")),
        None => Err(format!("attribute {name} is missing")),
    }
}

pub(crate) fn number_attr(
    item: &impl storage_types::AttributeValueLookup,
    name: &str,
) -> Result<String, String> {
    match item.get_attribute_value(name) {
        Some(AttributeValue::N(value)) => Ok(value.clone()),
        Some(other) => Err(format!("attribute {name} has non-number value: {other:?}")),
        None => Err(format!("attribute {name} is missing")),
    }
}

pub(crate) fn storage_error_detail(error: &StorageError) -> String {
    let (inner, context) = error.recursive_context(Vec::new());
    let mut detail = match inner {
        StorageEnum::InternalServerError { message } => {
            format!("internal_server_error: {message}")
        }
        StorageEnum::TransactionConflict { message } => {
            format!("transaction_conflict: {message}")
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
            if reasons.iter().any(|reason| reason == "ConditionalCheckFailed")
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
}
