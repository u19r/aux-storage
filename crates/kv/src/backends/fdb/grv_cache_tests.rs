#![cfg(feature = "foundationdb-backend")]

use crate::backends::fdb::{
    network::{
        FoundationDbNetworkPolicy, validate_network_policy, validate_simulated_database_config,
    },
    store::FoundationDbConfig,
};

#[test]
fn foundationdb_network_policy_uses_cache_lag_only_when_enabled() {
    let disabled = FoundationDbNetworkPolicy::for_config(&FoundationDbConfig {
        cache_read_version_ms: 0,
        ..Default::default()
    });
    assert_eq!(
        disabled,
        FoundationDbNetworkPolicy {
            grv_cache_lag_ms: None
        }
    );

    let enabled = FoundationDbNetworkPolicy::for_config(&FoundationDbConfig {
        cache_read_version_ms: 100,
        ..Default::default()
    });
    assert_eq!(
        enabled,
        FoundationDbNetworkPolicy {
            grv_cache_lag_ms: Some(100),
        }
    );
}

#[test]
fn foundationdb_network_policy_allows_disabled_late_joiners_but_not_late_enablement() {
    validate_network_policy(
        FoundationDbNetworkPolicy {
            grv_cache_lag_ms: Some(100),
        },
        FoundationDbNetworkPolicy {
            grv_cache_lag_ms: None,
        },
    )
    .expect("disabled store should be allowed in a process that already enabled GRV cache");

    let error = validate_network_policy(
        FoundationDbNetworkPolicy {
            grv_cache_lag_ms: None,
        },
        FoundationDbNetworkPolicy {
            grv_cache_lag_ms: Some(100),
        },
    )
    .expect_err("late GRV cache enablement must be rejected");
    assert!(
        error.to_string().contains("first FoundationDB connection"),
        "unexpected error: {error}"
    );
}

#[test]
fn foundationdb_network_policy_rejects_mismatched_non_zero_lag_values() {
    let error = validate_network_policy(
        FoundationDbNetworkPolicy {
            grv_cache_lag_ms: Some(25),
        },
        FoundationDbNetworkPolicy {
            grv_cache_lag_ms: Some(100),
        },
    )
    .expect_err("mismatched non-zero lags must be rejected");
    assert!(
        error.to_string().contains("cache_read_version_ms mismatch"),
        "unexpected error: {error}"
    );
}

#[test]
fn foundationdb_simulated_database_rejects_process_network_options() {
    validate_simulated_database_config(&FoundationDbConfig {
        cache_read_version_ms: 0,
        ..Default::default()
    })
    .expect("simulated database should accept config without process network options");

    let error = validate_simulated_database_config(&FoundationDbConfig {
        cache_read_version_ms: 25,
        ..Default::default()
    })
    .expect_err("simulated database cannot configure process-level GRV cache");
    assert!(
        error
            .to_string()
            .contains("simulator already owns the FoundationDB network"),
        "unexpected error: {error}"
    );
}
