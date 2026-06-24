#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    GetItemRequest, KeyAttributes, ReadSequenceForEach, ReadSequenceJoin, ReadSequenceJoinType,
    ReadSequenceOnMissing, ReadSequencePlannerInput, ReadSequenceRequest, ReadSequenceSelector,
    ReadSequenceStep, TableName, plan_read_sequence,
};

const MAX_SEQUENCE_STEPS: i64 = 3;
const MAX_FANOUT: i64 = 3;
const MAX_TOTAL_READS: i64 = 5;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct ReadSequenceCase {
    #[serde(rename = "stepCount")]
    step_count: i64,
    #[serde(rename = "dependencyOrderValid")]
    dependency_order_valid: bool,
    #[serde(rename = "selectorValid")]
    selector_valid: bool,
    #[serde(rename = "selectorProjected")]
    selector_projected: bool,
    #[serde(rename = "childFanout")]
    child_fanout: i64,
    #[serde(rename = "setFanout")]
    set_fanout: i64,
    #[serde(rename = "duplicateChildKeys")]
    duplicate_child_keys: bool,
    #[serde(rename = "hasMissingChild")]
    has_missing_child: bool,
    #[serde(rename = "joinMode")]
    join_mode: String,
    consistency: String,
    #[serde(rename = "readsGsi")]
    reads_gsi: bool,
    #[serde(rename = "immediateGsi")]
    immediate_gsi: bool,
    #[serde(rename = "backendSnapshot")]
    backend_snapshot: bool,
    #[serde(rename = "continuationKind")]
    continuation_kind: String,
    #[serde(rename = "tokenCarriesCursor")]
    token_carries_cursor: bool,
}

impl Default for ReadSequenceCase {
    fn default() -> Self {
        Self {
            step_count: 1,
            dependency_order_valid: true,
            selector_valid: true,
            selector_projected: true,
            child_fanout: 0,
            set_fanout: 1,
            duplicate_child_keys: false,
            has_missing_child: false,
            join_mode: "LEFT_ONE".to_string(),
            consistency: "EVENTUAL".to_string(),
            reads_gsi: false,
            immediate_gsi: false,
            backend_snapshot: false,
            continuation_kind: "none".to_string(),
            token_carries_cursor: false,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct ReadSequenceState {
    #[serde(rename = "lastCase")]
    last_case: ReadSequenceCase,
    #[serde(rename = "lastResult")]
    last_result: String,
    #[serde(rename = "plannedReads")]
    planned_reads: i64,
    #[serde(rename = "tokenRequired")]
    token_required: bool,
}

impl State<ReadSequenceDriver> for ReadSequenceState {
    fn from_driver(driver: &ReadSequenceDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            last_result: driver.last_result.clone(),
            planned_reads: driver.planned_reads,
            token_required: driver.token_required,
        })
    }
}

#[derive(Debug)]
struct ReadSequenceDriver {
    last_case: ReadSequenceCase,
    last_result: String,
    planned_reads: i64,
    token_required: bool,
}

impl Default for ReadSequenceDriver {
    fn default() -> Self {
        Self {
            last_case: ReadSequenceCase::default(),
            last_result: "NotChecked".to_string(),
            planned_reads: 0,
            token_required: false,
        }
    }
}

impl Driver for ReadSequenceDriver {
    type State = ReadSequenceState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                stepCount: i64,
                dependencyOrderValid: bool,
                selectorValid: bool,
                selectorProjected: bool,
                childFanout: i64,
                setFanout: i64,
                duplicateChildKeys: bool,
                hasMissingChild: bool,
                joinMode: String,
                consistency: String,
                readsGsi: bool,
                immediateGsi: bool,
                backendSnapshot: bool,
                continuationKind: String,
                tokenCarriesCursor: bool,
            ) => {
                self.check(ReadSequenceCase {
                    step_count: stepCount,
                    dependency_order_valid: dependencyOrderValid,
                    selector_valid: selectorValid,
                    selector_projected: selectorProjected,
                    child_fanout: childFanout,
                    set_fanout: setFanout,
                    duplicate_child_keys: duplicateChildKeys,
                    has_missing_child: hasMissingChild,
                    join_mode: joinMode,
                    consistency,
                    reads_gsi: readsGsi,
                    immediate_gsi: immediateGsi,
                    backend_snapshot: backendSnapshot,
                    continuation_kind: continuationKind,
                    token_carries_cursor: tokenCarriesCursor,
                })?;
            },
            step(
                stepCount: i64?,
                dependencyOrderValid: bool?,
                selectorValid: bool?,
                selectorProjected: bool?,
                childFanout: i64?,
                setFanout: i64?,
                duplicateChildKeys: bool?,
                hasMissingChild: bool?,
                joinMode: String?,
                consistency: String?,
                readsGsi: bool?,
                immediateGsi: bool?,
                backendSnapshot: bool?,
                continuationKind: String?,
                tokenCarriesCursor: bool?,
            ) => {
                if let (
                    Some(step_count),
                    Some(dependency_order_valid),
                    Some(selector_valid),
                    Some(selector_projected),
                    Some(child_fanout),
                    Some(set_fanout),
                    Some(duplicate_child_keys),
                    Some(has_missing_child),
                    Some(join_mode),
                    Some(consistency),
                    Some(reads_gsi),
                    Some(immediate_gsi),
                    Some(backend_snapshot),
                    Some(continuation_kind),
                    Some(token_carries_cursor),
                ) = (
                    stepCount,
                    dependencyOrderValid,
                    selectorValid,
                    selectorProjected,
                    childFanout,
                    setFanout,
                    duplicateChildKeys,
                    hasMissingChild,
                    joinMode,
                    consistency,
                    readsGsi,
                    immediateGsi,
                    backendSnapshot,
                    continuationKind,
                    tokenCarriesCursor,
                ) {
                    self.check(ReadSequenceCase {
                        step_count,
                        dependency_order_valid,
                        selector_valid,
                        selector_projected,
                        child_fanout,
                        set_fanout,
                        duplicate_child_keys,
                        has_missing_child,
                        join_mode,
                        consistency,
                        reads_gsi,
                        immediate_gsi,
                        backend_snapshot,
                        continuation_kind,
                        token_carries_cursor,
                    })?;
                }
            },
        })
    }
}

impl ReadSequenceDriver {
    fn check(&mut self, read_case: ReadSequenceCase) -> Result {
        self.planned_reads = total_reads_for(&read_case);
        self.token_required = needs_token(&read_case);
        self.last_result = expected_result(&read_case).to_string();
        if self.last_result == "Accepted" {
            assert_planner_accepts(&read_case)?;
        }
        self.last_case = read_case;
        Ok(())
    }
}

fn assert_planner_accepts(read_case: &ReadSequenceCase) -> Result {
    let child_fanout = u32::try_from(expanded_child_fanout(read_case))?;
    let request = planner_request(read_case);
    let mut input = ReadSequencePlannerInput::default();
    input.parent_counts.insert("root".to_string(), child_fanout);
    plan_read_sequence(&request, &input)?;
    Ok(())
}

fn planner_request(read_case: &ReadSequenceCase) -> ReadSequenceRequest {
    let mut sequence = vec![ReadSequenceStep {
        name: "root".to_string(),
        select: Default::default(),
        get: Some(GetItemRequest::new(
            TableName::new("Root"),
            KeyAttributes::new(),
        )),
        batch_get: None,
        query: None,
        for_each: None,
    }];
    if read_case.child_fanout > 0 {
        sequence.push(ReadSequenceStep {
            name: "child".to_string(),
            select: Default::default(),
            get: None,
            batch_get: None,
            query: None,
            for_each: Some(ReadSequenceForEach {
                from: ReadSequenceSelector("root.Item.child_id".to_string()),
                as_name: "child_id".to_string(),
                on_missing: ReadSequenceOnMissing::Null,
                get: Some(GetItemRequest::new(
                    TableName::new("Child"),
                    KeyAttributes::new(),
                )),
                batch_get: None,
                query: None,
                join: ReadSequenceJoin {
                    to: "root".to_string(),
                    as_name: "child".to_string(),
                    join_type: join_type_for(&read_case.join_mode),
                },
            }),
        });
    }
    ReadSequenceRequest {
        read_consistency: Default::default(),
        max_sequence_steps: Some(MAX_SEQUENCE_STEPS as u32),
        max_root_items: None,
        max_fanout_per_step: Some(MAX_FANOUT as u32),
        max_intermediate_items: None,
        max_total_read_items: None,
        max_child_query_items_per_parent: None,
        max_response_bytes: None,
        max_selector_bindings_per_step: None,
        max_selector_path_depth: None,
        next_sequence_token: None,
        return_consumed_capacity: None,
        sequence,
    }
}

fn join_type_for(join_mode: &str) -> ReadSequenceJoinType {
    match join_mode {
        "REQUIRED_ONE" => ReadSequenceJoinType::RequiredOne,
        "ARRAY" => ReadSequenceJoinType::Array,
        "INNER_ONE" => ReadSequenceJoinType::InnerOne,
        _ => ReadSequenceJoinType::LeftOne,
    }
}

fn child_read_count(read_case: &ReadSequenceCase) -> i64 {
    if read_case.child_fanout == 0 {
        0
    } else if read_case.duplicate_child_keys {
        1
    } else {
        expanded_child_fanout(read_case)
    }
}

fn total_reads_for(read_case: &ReadSequenceCase) -> i64 {
    1 + child_read_count(read_case)
}

fn expanded_child_fanout(read_case: &ReadSequenceCase) -> i64 {
    if read_case.child_fanout == 0 {
        0
    } else {
        read_case.child_fanout * read_case.set_fanout.max(1)
    }
}

fn needs_token(read_case: &ReadSequenceCase) -> bool {
    read_case.continuation_kind != "none" || total_reads_for(read_case) > MAX_TOTAL_READS
}

fn expected_result(read_case: &ReadSequenceCase) -> &'static str {
    if !read_case.dependency_order_valid {
        "RejectedDependencyOrder"
    } else if read_case.step_count > MAX_SEQUENCE_STEPS {
        "RejectedStepCap"
    } else if !read_case.selector_valid {
        "RejectedSelector"
    } else if !read_case.selector_projected {
        "RejectedUnprojectedSelector"
    } else if expanded_child_fanout(read_case) > MAX_FANOUT {
        "RejectedFanoutCap"
    } else if read_case.consistency == "STRONG" && read_case.reads_gsi {
        "RejectedStrongGsi"
    } else if read_case.consistency == "TRANSACTIONAL"
        && read_case.reads_gsi
        && !read_case.immediate_gsi
    {
        "RejectedTransactionalDelayedGsi"
    } else if read_case.consistency == "TRANSACTIONAL" && !read_case.backend_snapshot {
        "RejectedTransactionalUnsupported"
    } else if read_case.has_missing_child && read_case.join_mode == "REQUIRED_ONE" {
        "RejectedRequiredChildMissing"
    } else if needs_token(read_case) && !read_case.token_carries_cursor {
        "RejectedInvalidTokenState"
    } else {
        "Accepted"
    }
}

#[quint_run(
    spec = "../../quint/read_sequence_mbt.qnt",
    max_samples = 128,
    max_steps = 12,
    seed = "0x5e9"
)]
fn read_sequence_mbt_matches_phase0_contract() -> impl Driver {
    ReadSequenceDriver::default()
}
