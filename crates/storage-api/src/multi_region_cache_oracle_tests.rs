use std::{collections::BTreeSet, path::PathBuf};

use storage_cache::{
    CacheReadOutcome, CacheState, ObservedRead, ReadRequest, compare_observed_read,
};
use storage_types::TableName;

use crate::multi_region_harness::{SimulationHarness, SimulationHarnessConfig};

fn db_only_oracle_state(db_present: BTreeSet<u8>) -> CacheState {
    let mut state = CacheState::authoritative_leader_base_state();
    state.db_present = db_present;
    state.cached_writes_only = false;
    state.item_authority = false;
    state.query_authority = false;
    state.gsi_query_authority = false;
    state
}

fn slot_pk(slot: u8) -> String {
    format!("slot-pk-{slot}")
}

fn slot_sk(slot: u8) -> String {
    format!("slot-sk-{slot}")
}

fn isolated_config(test_name: &str) -> SimulationHarnessConfig {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let sqlite_database_dir = PathBuf::from("/tmp")
        .join("auxfn-multi-region")
        .join("sqlite")
        .join(format!(
            "storage-api-oracle-{test_name}-{}-{unique}",
            std::process::id()
        ));

    SimulationHarnessConfig {
        sqlite_database_dir: Some(sqlite_database_dir),
        ..SimulationHarnessConfig::default()
    }
}

async fn observed_get(
    harness: &SimulationHarness,
    region_name: &str,
    table_name: &TableName,
    slot: u8,
) -> ObservedRead {
    let value = harness
        .get_item_value(region_name, table_name, &slot_pk(slot), &slot_sk(slot))
        .await
        .expect("read item from simulation harness");

    ObservedRead::Get {
        outcome: CacheReadOutcome::FallbackDb,
        slot_present: value.is_some(),
    }
}

async fn observed_batch_get(
    harness: &SimulationHarness,
    region_name: &str,
    table_name: &TableName,
    requested_keys: &BTreeSet<u8>,
) -> ObservedRead {
    let mut returned_present_keys = BTreeSet::new();
    let mut returned_absent_keys = BTreeSet::new();

    for slot in requested_keys {
        let value = harness
            .get_item_value(region_name, table_name, &slot_pk(*slot), &slot_sk(*slot))
            .await
            .expect("read batch slot from simulation harness");
        if value.is_some() {
            returned_present_keys.insert(*slot);
        } else {
            returned_absent_keys.insert(*slot);
        }
    }

    ObservedRead::BatchGet {
        outcome: CacheReadOutcome::FallbackDb,
        served_keys: BTreeSet::new(),
        fallback_keys: requested_keys.clone(),
        returned_present_keys,
        returned_absent_keys,
    }
}

#[tokio::test]
async fn simulation_eventual_get_matches_db_only_oracle_after_replication_converges() {
    let harness = SimulationHarness::new(isolated_config("eventual-get"))
        .await
        .expect("build simulation harness");
    let table_name = TableName::new("simulation-cache-oracle-get");
    harness
        .create_global_table(&table_name)
        .await
        .expect("create table");

    harness
        .put_item_value(
            "region-a",
            &table_name,
            &slot_pk(0),
            &slot_sk(0),
            "slot-0",
            0,
        )
        .await
        .expect("put slot 0");
    harness
        .put_item_value(
            "region-a",
            &table_name,
            &slot_pk(2),
            &slot_sk(2),
            "slot-2",
            0,
        )
        .await
        .expect("put slot 2");
    harness.run_until_idle(20).await.expect("drain replication");

    let oracle = db_only_oracle_state(BTreeSet::from([0_u8, 2]));
    let request_epoch = oracle.fresh_request_epoch();

    for region_name in ["region-a", "region-b"] {
        for slot in [0_u8, 1, 2, 3] {
            compare_observed_read(
                &oracle,
                &ReadRequest::Get {
                    slot,
                    strong: false,
                    request_epoch,
                },
                &observed_get(&harness, region_name, &table_name, slot).await,
            )
            .expect("region get should match db-only oracle");
        }
    }
}

#[tokio::test]
async fn simulation_batch_get_matches_db_only_oracle_after_partition_and_delete() {
    let harness = SimulationHarness::new(isolated_config("batch-get"))
        .await
        .expect("build simulation harness");
    let table_name = TableName::new("simulation-cache-oracle-batch");
    harness
        .create_global_table(&table_name)
        .await
        .expect("create table");

    for slot in [0_u8, 1, 2] {
        harness
            .put_item_value(
                "region-a",
                &table_name,
                &slot_pk(slot),
                &slot_sk(slot),
                &format!("slot-{slot}"),
                0,
            )
            .await
            .expect("seed slot");
    }
    harness.run_until_idle(10).await.expect("drain seed");

    harness.block_link("region-b", "region-a", true);
    harness
        .delete_item("region-b", &table_name, &slot_pk(1), &slot_sk(1))
        .await
        .expect("delete slot 1");
    harness.run_until_idle(6).await.expect("partial drain");
    harness.block_link("region-b", "region-a", false);
    harness.run_until_idle(20).await.expect("final drain");

    let requested = BTreeSet::from([0_u8, 1, 2, 3]);
    let oracle = db_only_oracle_state(BTreeSet::from([0_u8, 2]));
    let request_epoch = oracle.fresh_request_epoch();

    for region_name in ["region-a", "region-b"] {
        compare_observed_read(
            &oracle,
            &ReadRequest::BatchGet {
                requested_keys: requested.clone(),
                strong: false,
                request_epoch,
            },
            &observed_batch_get(&harness, region_name, &table_name, &requested).await,
        )
        .expect("region batch get should match db-only oracle");
    }
}
