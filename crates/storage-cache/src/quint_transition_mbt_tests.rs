#![allow(non_snake_case)]

use std::collections::BTreeSet;

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{CacheState, Transition, TransitionRange, model::NodeId};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct CacheTransitionMbtState {
    cache: CacheTransitionMbtSnapshot,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct CacheTransitionMbtSnapshot {
    #[serde(rename = "dbPresent")]
    db_present: BTreeSet<i64>,
    #[serde(rename = "preparedPuts")]
    prepared_puts: BTreeSet<i64>,
    #[serde(rename = "preparedDeletes")]
    prepared_deletes: BTreeSet<i64>,
    #[serde(rename = "leaderPayloadKeys")]
    leader_payload_keys: BTreeSet<i64>,
    #[serde(rename = "leaderNegativeKeys")]
    leader_negative_keys: BTreeSet<i64>,
    #[serde(rename = "leaderManifestKeys")]
    leader_manifest_keys: BTreeSet<i64>,
    #[serde(rename = "leaderCoveredSlots")]
    leader_covered_slots: BTreeSet<i64>,
    #[serde(rename = "followerPayloadKeys")]
    follower_payload_keys: BTreeSet<i64>,
    #[serde(rename = "followerNegativeKeys")]
    follower_negative_keys: BTreeSet<i64>,
    #[serde(rename = "followerManifestKeys")]
    follower_manifest_keys: BTreeSet<i64>,
    #[serde(rename = "followerCoveredSlots")]
    follower_covered_slots: BTreeSet<i64>,
    #[serde(rename = "servingNode")]
    serving_node: i64,
    #[serde(rename = "actualLeaderNode")]
    actual_leader_node: i64,
    #[serde(rename = "actualEpoch")]
    actual_epoch: i64,
    #[serde(rename = "cacheEpoch")]
    cache_epoch: i64,
    #[serde(rename = "itemAuthority")]
    item_authority: bool,
    #[serde(rename = "queryAuthority")]
    query_authority: bool,
    #[serde(rename = "gsiQueryAuthority")]
    gsi_query_authority: bool,
    #[serde(rename = "shardBusted")]
    shard_busted: bool,
    #[serde(rename = "shardAssigned")]
    shard_assigned: bool,
    #[serde(rename = "lastOperation")]
    last_operation: String,
    #[serde(rename = "lastSlot")]
    last_slot: i64,
    #[serde(rename = "lastLower")]
    last_lower: i64,
    #[serde(rename = "lastUpper")]
    last_upper: i64,
    #[serde(rename = "lastMask")]
    last_mask: i64,
    #[serde(rename = "lastOutcome")]
    last_outcome: String,
}

impl State<CacheTransitionMbtDriver> for CacheTransitionMbtState {
    fn from_driver(driver: &CacheTransitionMbtDriver) -> Result<Self> {
        let cache = &driver.cache;
        Ok(Self {
            cache: CacheTransitionMbtSnapshot {
                db_present: slots(&cache.db_present),
                prepared_puts: slots(&cache.prepared_puts),
                prepared_deletes: slots(&cache.prepared_deletes),
                leader_payload_keys: slots(&cache.leader.items.payload_keys),
                leader_negative_keys: slots(&cache.leader.items.negative_keys),
                leader_manifest_keys: slots(&cache.leader.items.manifest_keys),
                leader_covered_slots: slots(&cache.leader.items.covered_slots),
                follower_payload_keys: slots(&cache.follower.items.payload_keys),
                follower_negative_keys: slots(&cache.follower.items.negative_keys),
                follower_manifest_keys: slots(&cache.follower.items.manifest_keys),
                follower_covered_slots: slots(&cache.follower.items.covered_slots),
                serving_node: node(cache.serving_node),
                actual_leader_node: node(cache.actual_leader_node),
                actual_epoch: i64::from(cache.actual_epoch),
                cache_epoch: i64::from(cache.cache_epoch),
                item_authority: cache.item_authority,
                query_authority: cache.query_authority,
                gsi_query_authority: cache.gsi_query_authority,
                shard_busted: cache.shard_busted,
                shard_assigned: cache.shard_assigned,
                last_operation: driver.last_operation.clone(),
                last_slot: driver.last_slot,
                last_lower: driver.last_lower,
                last_upper: driver.last_upper,
                last_mask: driver.last_mask,
                last_outcome: driver.last_outcome.clone(),
            },
        })
    }
}

fn slots(values: &BTreeSet<u8>) -> BTreeSet<i64> {
    values.iter().map(|slot| i64::from(*slot)).collect()
}

fn node(value: NodeId) -> i64 {
    match value {
        NodeId::Leader => 0,
        NodeId::Follower => 1,
    }
}

#[derive(Debug)]
struct CacheTransitionMbtDriver {
    cache: CacheState,
    last_operation: String,
    last_slot: i64,
    last_lower: i64,
    last_upper: i64,
    last_mask: i64,
    last_outcome: String,
}

impl Default for CacheTransitionMbtDriver {
    fn default() -> Self {
        Self {
            cache: CacheState::authoritative_leader_base_state(),
            last_operation: "init".to_string(),
            last_slot: 0,
            last_lower: 0,
            last_upper: 0,
            last_mask: 0,
            last_outcome: "not_checked".to_string(),
        }
    }
}

impl Driver for CacheTransitionMbtDriver {
    type State = CacheTransitionMbtState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Step(operation: String, slot: i64, lower: i64, upper: i64, mask: i64) => {
                self.apply(&operation, slot, lower, upper, mask)?;
            },
            step(
                operation: String?,
                slot: i64?,
                lower: i64?,
                upper: i64?,
                mask: i64?,
            ) => {
                if let (Some(operation), Some(slot), Some(lower), Some(upper), Some(mask)) =
                    (operation, slot, lower, upper, mask)
                {
                    self.apply(&operation, slot, lower, upper, mask)?;
                }
            },
        })
    }
}

impl CacheTransitionMbtDriver {
    fn apply(&mut self, operation: &str, slot: i64, lower: i64, upper: i64, mask: i64) -> Result {
        self.last_operation = operation.to_string();
        self.last_slot = slot;
        self.last_lower = lower;
        self.last_upper = upper;
        self.last_mask = mask;

        let transition = match operation {
            "prepare_put" => Some(Transition::PreparePut {
                slot: u8::try_from(slot)?,
            }),
            "commit_put" => Some(Transition::LeaderCommitPut {
                slot: u8::try_from(slot)?,
            }),
            "ack_put" => Some(Transition::FollowerAcknowledgePut {
                slot: u8::try_from(slot)?,
            }),
            "prepare_delete" => Some(Transition::PrepareDelete {
                slot: u8::try_from(slot)?,
            }),
            "commit_delete" => Some(Transition::LeaderCommitDelete {
                slot: u8::try_from(slot)?,
            }),
            "ack_delete" => Some(Transition::FollowerAcknowledgeDelete {
                slot: u8::try_from(slot)?,
            }),
            "abort_prepared" => Some(Transition::AbortPrepared {
                slot: u8::try_from(slot)?,
            }),
            "fill_base" => Some(Transition::QueryFillBase {
                range: TransitionRange::new(u8::try_from(lower)?, u8::try_from(upper)?),
            }),
            "sync_follower" => Some(Transition::SyncFollowerFromLeader),
            "lose_epoch" => Some(Transition::LoseEpochAuthority),
            "gain_epoch" => Some(Transition::GainEpochAuthority),
            "lose_leader" => Some(Transition::LoseLeader),
            "regain_leader" => Some(Transition::RegainLeader),
            "promote_follower" => Some(Transition::PromoteFollowerCatchingUp),
            "finish_catch_up" => Some(Transition::FinishCatchUp),
            "bust_shard" => Some(Transition::BustShard),
            "assign_shard" => Some(Transition::AssignShard),
            "drain_shard" => Some(Transition::DrainShard),
            _ => None,
        };

        match transition.and_then(|transition| self.cache.try_apply(transition)) {
            Some(next) => {
                self.cache = next;
                self.last_outcome = "applied".to_string();
            }
            None => {
                self.last_outcome = "rejected".to_string();
            }
        }
        Ok(())
    }
}

#[quint_run(
    spec = "../../quint/distributed_cache_transition_mbt.qnt",
    init = "init",
    step = "step",
    max_samples = 128,
    max_steps = 16,
    seed = "0xca5e5eed"
)]
fn distributed_cache_transition_mbt_matches_rust_boundary() -> impl Driver {
    CacheTransitionMbtDriver::default()
}
