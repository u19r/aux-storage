use storage_common::{ttl, ttl::TtlConfigRecord};
use storage_types::{TableName, TimeToLiveStatus};

use super::ttl_sweep::adjust_ttl_shard_batch;
use crate::constants;

#[test]
fn adjust_batch_increases_when_utilization_low_sqlite() {
    let table = TableName::new("AdjustIncreasesSqlite");
    let gsi_name = ttl::ttl_gsi_name(&table);
    let mut config = TtlConfigRecord::new("ttl".to_string(), &gsi_name, TimeToLiveStatus::Enabled);
    config.adaptive_pk_batch_size = Some(
        u32::try_from(constants::TTL_SWEEP_INITIAL_SHARD_BATCH).expect("initial batch fits in u32"),
    );

    adjust_ttl_shard_batch(&table, &mut config, 10);
    adjust_ttl_shard_batch(&table, &mut config, 10);

    assert_eq!(
        config.adaptive_pk_batch_size,
        Some(
            u32::try_from(constants::TTL_SWEEP_INITIAL_SHARD_BATCH + 1).expect("batch fits in u32")
        )
    );
}

#[test]
fn adjust_batch_decreases_when_utilization_high_sqlite() {
    let table = TableName::new("AdjustDecreasesSqlite");
    let gsi_name = ttl::ttl_gsi_name(&table);
    let mut config = TtlConfigRecord::new("ttl".to_string(), &gsi_name, TimeToLiveStatus::Enabled);
    let starting = constants::TTL_SWEEP_MIN_SHARD_BATCH + 2;
    config.adaptive_pk_batch_size = Some(u32::try_from(starting).expect("batch fits in u32"));
    let interval_ms = constants::TTL_SWEEP_INTERVAL_MINUTES * 60_000;
    let runtime_ms = interval_ms.saturating_mul(6) / 10;

    adjust_ttl_shard_batch(&table, &mut config, runtime_ms);
    adjust_ttl_shard_batch(&table, &mut config, runtime_ms);

    assert_eq!(
        config.adaptive_pk_batch_size,
        Some(u32::try_from(starting - 1).expect("batch fits in u32"))
    );
}

#[test]
fn adjust_batch_stays_within_bounds_sqlite() {
    let table = TableName::new("AdjustStableSqlite");
    let gsi_name = ttl::ttl_gsi_name(&table);
    let mut config = TtlConfigRecord::new("ttl".to_string(), &gsi_name, TimeToLiveStatus::Enabled);
    config.adaptive_pk_batch_size = Some(
        u32::try_from(constants::TTL_SWEEP_INITIAL_SHARD_BATCH).expect("initial batch fits in u32"),
    );
    let interval_ms = constants::TTL_SWEEP_INTERVAL_MINUTES * 60_000;
    let runtime_ms = interval_ms / 2;

    adjust_ttl_shard_batch(&table, &mut config, runtime_ms);

    assert_eq!(
        config.adaptive_pk_batch_size,
        Some(
            u32::try_from(constants::TTL_SWEEP_INITIAL_SHARD_BATCH)
                .expect("initial batch fits in u32")
        )
    );
}
