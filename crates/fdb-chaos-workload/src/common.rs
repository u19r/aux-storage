use crate::imports::*;

pub(crate) const WORKLOAD_KV_SMOKE: &str = "kv_smoke";
pub(crate) const WORKLOAD_NOOP: &str = "noop";
pub(crate) const WORKLOAD_PARTITION_FAMILY: &str = "partition_family";
pub(crate) const WORKLOAD_PUBSUB_DELIVERY: &str = "pubsub_delivery";
pub(crate) const WORKLOAD_QUEUE_VISIBILITY: &str = "queue_visibility";
pub(crate) const WORKLOAD_TABLE_ATOMICITY: &str = "table_atomicity";
pub(crate) const OPTION_PROFILE: &str = "profile";
pub(crate) const OPTION_OPERATION_COUNT: &str = "operationCount";
pub(crate) const OPTION_HISTORY_SAMPLE_LIMIT: &str = "historySampleLimit";
pub(crate) const OPTION_ARTIFACT_ROOT: &str = "artifactRoot";
pub(crate) const OPTION_KEY_COUNT: &str = "keyCount";
pub(crate) const OPTION_ACTIVE_CLIENT_COUNT: &str = "activeClientCount";
pub(crate) const OPTION_SHARED_KEY_COUNT: &str = "sharedKeyCount";
pub(crate) const OPTION_SHARED_OPERATION_PERCENT: &str = "sharedOperationPercent";
pub(crate) const GSI_INDEX_NAME: &str = "by_category_score";
pub(crate) const GSI_CATEGORY_ATTR: &str = "category";
pub(crate) const GSI_SCORE_ATTR: &str = "score";
pub(crate) const TABLE_STREAM_DURATION_HOURS: u16 = 2;
pub(crate) const ITEM_STREAM_TTL_HOURS: u16 = 1;
pub(crate) const QUEUE_REDELIVERY_WAIT_SECONDS: u32 = 5;

pub(crate) type FdbChaosProvider = SortedKvDbStorageProvider<FoundationDbKvStore>;

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

pub(crate) fn item_trim_scope_id(stream_name: &StreamName) -> String {
    let mut scope_id = String::with_capacity("kv-stream:".len() + stream_name.as_ref().len() * 2);
    scope_id.push_str("kv-stream:");
    for byte in stream_name.as_ref() {
        scope_id.push(hex_nibble(byte >> 4));
        scope_id.push(hex_nibble(byte & 0x0f));
    }
    scope_id
}

pub(crate) fn hex_nibble(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'a' + (value - 10)),
        _ => '?',
    }
}

pub(crate) fn string_attr(
    item: &HashMap<String, AttributeValue>,
    name: &str,
) -> Result<String, String> {
    match item.get(name) {
        Some(AttributeValue::S(value)) => Ok(value.clone()),
        Some(other) => Err(format!("attribute {name} has non-string value: {other:?}")),
        None => Err(format!("attribute {name} is missing")),
    }
}

pub(crate) fn number_attr(
    item: &HashMap<String, AttributeValue>,
    name: &str,
) -> Result<String, String> {
    match item.get(name) {
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
}
