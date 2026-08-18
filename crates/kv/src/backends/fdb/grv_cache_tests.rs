#![cfg(feature = "foundationdb-backend")]

use crate::backends::fdb::{
    fdb_support_tests::connect_fdb_store_with_cache,
    network::{
        FoundationDbNetworkPolicy, validate_network_policy, validate_simulated_database_config,
    },
    store::FoundationDbConfig,
};

#[test]
fn foundationdb_config_defaults_to_fifty_millisecond_grv_cache_lag() {
    assert_eq!(FoundationDbConfig::default().cache_read_version_ms, 50);
}

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

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster"]
async fn foundationdb_live_read_option_selection_matches_consistency_and_explicit_zero() {
    let Some(cached_store) = connect_fdb_store_with_cache("fdb-grv-option-selection", 50).await
    else {
        eprintln!("Skipping FoundationDB GRV option test: unable to connect to local cluster");
        return;
    };

    assert!(cached_store.uses_grv_cache(false));
    assert!(!cached_store.uses_grv_cache(true));

    let eventual_transaction = cached_store
        .create_transaction()
        .expect("create eventual GRV-cache transaction");
    cached_store
        .configure_read_transaction(&eventual_transaction, Some("grv.option.eventual"), false)
        .expect("configure eventual GRV-cache transaction");
    eventual_transaction
        .get(b"__grv_option_eventual", false)
        .await
        .expect("execute eventual GRV-cache read");

    let consistent_transaction = cached_store
        .create_transaction()
        .expect("create consistent transaction");
    cached_store
        .configure_read_transaction(&consistent_transaction, Some("grv.option.consistent"), true)
        .expect("configure consistent transaction without GRV cache");
    consistent_transaction
        .get(b"__grv_option_consistent", false)
        .await
        .expect("execute consistent read");

    let Some(disabled_store) = connect_fdb_store_with_cache("fdb-grv-option-zero", 0).await else {
        eprintln!("Skipping explicit-zero GRV option test: unable to connect to local cluster");
        return;
    };
    assert!(!disabled_store.uses_grv_cache(false));
    assert!(!disabled_store.uses_grv_cache(true));
    let disabled_transaction = disabled_store
        .create_transaction()
        .expect("create explicit-zero transaction");
    disabled_store
        .configure_read_transaction(&disabled_transaction, Some("grv.option.zero"), false)
        .expect("configure explicit-zero transaction without GRV cache");
    disabled_transaction
        .get(b"__grv_option_zero", false)
        .await
        .expect("execute explicit-zero read");
}
