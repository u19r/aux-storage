use storage_common::{ttl, ttl::TtlConfigRecord};
use storage_types::{TableName, TimeToLiveStatus};

use super::sweep::{adjust_ttl_shard_batch, usize_to_u32};
use crate::constants;

#[test]
fn adjust_batch_increases_when_utilization_low() {
    let table = TableName::new("AdjustIncreases");
    let gsi_name = ttl::ttl_gsi_name(&table);
    let mut config = TtlConfigRecord::new("ttl".to_string(), &gsi_name, TimeToLiveStatus::Enabled);
    config.adaptive_pk_batch_size = Some(usize_to_u32(constants::TTL_SWEEP_INITIAL_SHARD_BATCH));

    adjust_ttl_shard_batch(&table, &mut config, 10);
    adjust_ttl_shard_batch(&table, &mut config, 10);

    assert_eq!(
        config.adaptive_pk_batch_size,
        Some(usize_to_u32(constants::TTL_SWEEP_INITIAL_SHARD_BATCH + 1))
    );
}

#[test]
fn adjust_batch_decreases_when_utilization_high() {
    let table = TableName::new("AdjustDecreases");
    let gsi_name = ttl::ttl_gsi_name(&table);
    let mut config = TtlConfigRecord::new("ttl".to_string(), &gsi_name, TimeToLiveStatus::Enabled);
    let starting = constants::TTL_SWEEP_MIN_SHARD_BATCH + 2;
    config.adaptive_pk_batch_size = Some(usize_to_u32(starting));
    let interval_ms = constants::TTL_SWEEP_INTERVAL_MINUTES * 60_000;
    let runtime_ms = interval_ms.saturating_mul(6) / 10; // 60% utilization

    adjust_ttl_shard_batch(&table, &mut config, runtime_ms);
    adjust_ttl_shard_batch(&table, &mut config, runtime_ms);

    assert_eq!(
        config.adaptive_pk_batch_size,
        Some(usize_to_u32(starting - 1))
    );
}

#[test]
fn adjust_batch_no_change_when_within_band() {
    let table = TableName::new("AdjustStable");
    let gsi_name = ttl::ttl_gsi_name(&table);
    let mut config = TtlConfigRecord::new("ttl".to_string(), &gsi_name, TimeToLiveStatus::Enabled);
    config.adaptive_pk_batch_size = Some(usize_to_u32(constants::TTL_SWEEP_INITIAL_SHARD_BATCH));
    let interval_ms = constants::TTL_SWEEP_INTERVAL_MINUTES * 60_000;
    let runtime_ms = interval_ms / 2; // exactly 50%, upper bound

    adjust_ttl_shard_batch(&table, &mut config, runtime_ms);

    assert_eq!(
        config.adaptive_pk_batch_size,
        Some(usize_to_u32(constants::TTL_SWEEP_INITIAL_SHARD_BATCH))
    );
}
