#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_types::{ItemStreamVersion, TableName};

use crate::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncCreateTableMutation, SyncItemBaseVersion,
    SyncMutationId, SyncMutationResponse, SyncProposalBatch, SyncProposalCoalescingDecision,
    SyncProposalCoalescingGate, SyncProposalId, SyncPutMutation, SyncReadSet,
    plan_proposal_coalescing,
};

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
struct MbtGate {
    #[serde(rename = "leftLifecycle")]
    left_lifecycle: bool,
    #[serde(rename = "rightLifecycle")]
    right_lifecycle: bool,
    #[serde(rename = "writeWriteConflict")]
    write_write_conflict: bool,
    #[serde(rename = "rightReadDependsOnLeftWrite")]
    right_read_depends_on_left_write: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncProposalCoalescingState {
    #[serde(rename = "lastGate")]
    last_gate: MbtGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncProposalCoalescingDriver> for SyncProposalCoalescingState {
    fn from_driver(driver: &SyncProposalCoalescingDriver) -> Result<Self> {
        Ok(Self {
            last_gate: driver.last_gate,
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncProposalCoalescingDriver {
    last_gate: MbtGate,
    last_decision: String,
}

impl Default for SyncProposalCoalescingDriver {
    fn default() -> Self {
        Self {
            last_gate: MbtGate {
                left_lifecycle: false,
                right_lifecycle: false,
                write_write_conflict: false,
                right_read_depends_on_left_write: false,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncProposalCoalescingDriver {
    type State = SyncProposalCoalescingState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                leftLifecycle: bool,
                rightLifecycle: bool,
                writeWriteConflict: bool,
                rightReadDependsOnLeftWrite: bool,
            ) => {
                self.check(MbtGate {
                    left_lifecycle: leftLifecycle,
                    right_lifecycle: rightLifecycle,
                    write_write_conflict: writeWriteConflict,
                    right_read_depends_on_left_write: rightReadDependsOnLeftWrite,
                });
            },
            step(
                leftLifecycle: bool?,
                rightLifecycle: bool?,
                writeWriteConflict: bool?,
                rightReadDependsOnLeftWrite: bool?,
            ) => {
                if let (
                    Some(left_lifecycle),
                    Some(right_lifecycle),
                    Some(write_write_conflict),
                    Some(right_read_depends_on_left_write),
                ) = (
                    leftLifecycle,
                    rightLifecycle,
                    writeWriteConflict,
                    rightReadDependsOnLeftWrite,
                ) {
                    self.check(MbtGate {
                        left_lifecycle,
                        right_lifecycle,
                        write_write_conflict,
                        right_read_depends_on_left_write,
                    });
                }
            },
        })
    }
}

impl SyncProposalCoalescingDriver {
    fn check(&mut self, gate: MbtGate) {
        let left = proposal("left", gate.left_lifecycle, "left-write", None, 1);
        let right_key = if gate.write_write_conflict {
            "left-write"
        } else {
            "right-write"
        };
        let right_read_key = gate
            .right_read_depends_on_left_write
            .then_some("left-write");
        let right = proposal("right", gate.right_lifecycle, right_key, right_read_key, 2);
        let decision = plan_proposal_coalescing(SyncProposalCoalescingGate {
            left: &left,
            right: &right,
        });
        self.last_decision = decision_name(decision).to_string();
        self.last_gate = gate;
    }
}

fn decision_name(decision: SyncProposalCoalescingDecision) -> &'static str {
    match decision {
        SyncProposalCoalescingDecision::Coalesce => "coalesce",
        SyncProposalCoalescingDecision::RejectLifecycleBoundary => "reject_lifecycle_boundary",
        SyncProposalCoalescingDecision::RejectWriteWriteConflict => "reject_write_write_conflict",
        SyncProposalCoalescingDecision::RejectStaleReadDependency => "reject_stale_read_dependency",
    }
}

fn proposal(
    id: &str,
    lifecycle: bool,
    write_key: &str,
    read_key: Option<&str>,
    version: u64,
) -> SyncProposalBatch {
    let mutation = if lifecycle {
        ResolvedSyncMutation::CreateTable(SyncCreateTableMutation {
            mutation_id: SyncMutationId::new(format!("{id}#create")).expect("mutation id"),
            table_name: TableName::new("table"),
            request_json: "{}".to_string(),
        })
    } else {
        ResolvedSyncMutation::Put(SyncPutMutation {
            mutation_id: SyncMutationId::new(format!("{id}#put")).expect("mutation id"),
            table_name: TableName::new("table"),
            key_json: key_json(write_key),
            item_json: format!(r#"{{"pk":{{"S":"{write_key}"}}}}"#),
            indexers: Vec::new(),
            old_item_json: None,
            old_indexers: None,
            target_item_stream_version: ItemStreamVersion::new(version),
            response: SyncMutationResponse::default(),
        })
    };
    let mut batch = SyncProposalBatch::new(
        SyncProposalId::new(id).expect("proposal id"),
        ResolvedSyncMutationBatch::new(vec![mutation]),
    );
    if let Some(read_key) = read_key {
        batch = batch.with_read_set(SyncReadSet::new(vec![SyncItemBaseVersion {
            table_name: TableName::new("table"),
            key_json: key_json(read_key),
            item_stream_version: None,
        }]));
    }
    batch
}

fn key_json(key: &str) -> String {
    format!(r#"{{"pk":{{"S":"{key}"}}}}"#)
}

#[quint_run(
    spec = "../../quint/sync_proposal_coalescing_mbt.qnt",
    max_samples = 32,
    max_steps = 8,
    seed = "0xc0a1e5ce"
)]
fn sync_proposal_coalescing_mbt_matches_rust_boundary() -> impl Driver {
    SyncProposalCoalescingDriver::default()
}
