pub(crate) const BATCH_GET_ITEM_PERF_ITERATIONS: usize = 10_000;

pub(crate) const DYNAMODB_DECODE_PERF_ITERATIONS: usize = 1_000;

pub(crate) const DYNAMODB_DECODE_GET_BODY: &[u8] =
    br#"{"TableName":"table","Key":{"pk":{"S":"value"}}}"#;

pub(crate) const DYNAMODB_DECODE_PUT_BODY: &[u8] =
    br#"{"TableName":"table","Item":{"pk":{"S":"value"},"data":{"S":"payload"}}}"#;

pub(crate) const DYNAMODB_DECODE_ALLOCATION_ISOLATED_ENV: &str =
    "AUX_STORAGE_API_DYNAMODB_DECODE_ALLOCATION_ISOLATED";
