pub(super) const ROCKSDB_BATCH_WRITE_RETRIES: usize = 5;
pub(super) const ROCKSDB_CONDITIONAL_PUT_RETRIES: usize = 5;
pub(super) const ROCKSDB_CONDITIONAL_PUT_RETRY_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::RocksdbConditionalPutRetryMetric;
pub(super) const ROCKSDB_CONDITIONAL_PUT_FAILURE_METRIC: metrics_facade::CounterMetric =
    metrics_facade::CounterMetric::RocksdbConditionalPutFailureMetric;
