#![allow(dead_code)]

use std::time::Duration;

pub(crate) fn reset_provider(provider: &'static str) {
    storage_common::provider_perf::reset_provider(provider);
}

pub(crate) fn record(provider: &'static str, name: &'static str, elapsed: Duration) {
    storage_common::provider_perf::record(provider, name, elapsed);
}

pub(crate) fn record_amount(provider: &'static str, name: &'static str, amount: u64) {
    storage_common::provider_perf::record_amount(provider, name, amount);
}

pub(crate) fn snapshot_provider(
    provider: &'static str,
) -> Vec<storage_common::provider_perf::PerfCounterSnapshot> {
    storage_common::provider_perf::snapshot_provider(provider)
}
