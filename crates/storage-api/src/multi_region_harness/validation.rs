use std::time::Duration;

use storage_types::{StorageError, StorageResult};

use super::runner::HarnessRunConfig;

pub(super) fn validate_run_config(config: &HarnessRunConfig) -> StorageResult<()> {
    if config.regions == 0 || config.regions > 5 {
        return Err(StorageError::validation(
            "multi-region harness requires between 1 and 5 regions",
        ));
    }
    if config.ops_per_sec == 0 {
        return Err(StorageError::validation(
            "multi-region harness ops_per_sec must be greater than zero",
        ));
    }
    if config.load_workers == 0 {
        return Err(StorageError::validation(
            "multi-region harness load_workers must be greater than zero",
        ));
    }
    if config.max_in_flight_convergence_checks == 0 {
        return Err(StorageError::validation(
            "multi-region harness max_in_flight_convergence_checks must be greater than zero",
        ));
    }
    if config.scenario == super::runner::HarnessScenario::Bootstrap
        && config.bootstrap_item_count == 0
    {
        return Err(StorageError::validation(
            "multi-region bootstrap harness bootstrap_item_count must be greater than zero",
        ));
    }
    if config.hot_key_percent > 100 || config.delete_percent > 100 || config.read_percent > 100 {
        return Err(StorageError::validation(
            "percent inputs must be between 0 and 100",
        ));
    }
    if u16::from(config.read_percent) + u16::from(config.delete_percent) > 100 {
        return Err(StorageError::validation(
            "read_percent + delete_percent must be at most 100",
        ));
    }
    Ok(())
}

pub(super) fn region_names(region_count: usize) -> Vec<String> {
    (0..region_count)
        .map(|index| format!("region-{}", char::from(b'a' + index as u8)))
        .collect()
}

pub(super) fn latency_duration(ms: u64, us: u64) -> Duration {
    Duration::from_millis(ms).saturating_add(Duration::from_micros(us))
}
