use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    anomaly::{Anomaly, AnomalyKind},
    constants::{ARTIFACT_SCHEMA_VERSION, MAX_SERIALIZABLE_EVENTS_PER_SHARED_KEY},
    history::{HistoryEvent, OperationHistory, OperationKind, OperationOutcome},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SharedKeyRead {
    pub key: String,
    pub actual: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SharedKeyAudit {
    pub schema_version: u32,
    pub client_id: i32,
    pub reads: Vec<SharedKeyRead>,
}

impl SharedKeyAudit {
    #[must_use]
    pub fn new(client_id: i32, reads: Vec<SharedKeyRead>) -> Self {
        Self {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            client_id,
            reads,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SharedKeyCheckReport {
    pub schema_version: u32,
    pub checked_operation_count: usize,
    pub checked_history_read_count: usize,
    pub checked_order_constraint_count: usize,
    pub checked_read_count: usize,
    pub audit_count: usize,
    pub anomaly_count: usize,
    pub unclassified_key_count: usize,
    pub anomalies: Vec<Anomaly>,
    pub unclassified_keys: Vec<String>,
}

#[derive(Default)]
struct SharedKeyExpectations {
    possible_values: BTreeSet<String>,
    absent_possible: bool,
    has_unknown: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SharedKeyOperation {
    Write { value: String },
    PutIfAbsent { value: String },
    Delete,
    Read { actual: Option<String> },
    ConditionFailed,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SharedKeyOperationEntry {
    client_id: i32,
    sequence: u64,
    started_at_sim_us: u64,
    completed_at_sim_us: u64,
    operation: SharedKeyOperation,
}

impl SharedKeyOperationEntry {
    fn new(event: &HistoryEvent, operation: SharedKeyOperation) -> Self {
        Self {
            client_id: event.client_id,
            sequence: event.sequence,
            started_at_sim_us: event.started_at_sim_us,
            completed_at_sim_us: event.completed_at_sim_us,
            operation,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SerializableCheckResult {
    is_serializable: bool,
    order_constraint_count: usize,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SerializableSearchState {
    scheduled_mask: u64,
    value: Option<String>,
}

#[must_use]
pub fn check_shared_key_audits(
    histories: &[OperationHistory],
    audits: &[SharedKeyAudit],
) -> SharedKeyCheckReport {
    let mut expectations = BTreeMap::<String, SharedKeyExpectations>::new();
    let mut operations = BTreeMap::<String, Vec<SharedKeyOperationEntry>>::new();
    let mut checked_operation_count = 0;
    let mut checked_history_read_count = 0;
    for history in histories {
        for event in history.events() {
            if !event.key.starts_with("shared/") {
                continue;
            }
            let expectation = expectations.entry(event.key.clone()).or_default();
            match &event.outcome {
                OperationOutcome::Committed => match event.kind {
                    OperationKind::Put
                    | OperationKind::PutIfAbsent
                    | OperationKind::Update
                    | OperationKind::TransactWrite => {
                        if let Some(value) = &event.value {
                            expectation.possible_values.insert(value.clone());
                        }
                    }
                    OperationKind::Read => {}
                    OperationKind::Delete => {
                        expectation.absent_possible = true;
                    }
                },
                OperationOutcome::Unknown { .. } => {
                    expectation.has_unknown = true;
                }
                OperationOutcome::ConditionFailed { .. } | OperationOutcome::Failed { .. } => {}
            }
            if let Some(operation) = shared_key_operation(event) {
                checked_operation_count += 1;
                if matches!(operation, SharedKeyOperation::Read { .. }) {
                    checked_history_read_count += 1;
                }
                operations
                    .entry(event.key.clone())
                    .or_default()
                    .push(SharedKeyOperationEntry::new(event, operation));
            }
        }
    }

    let mut checked_read_count = 0;
    let mut checked_order_constraint_count = 0;
    let mut anomalies = Vec::new();
    let mut anomalous_keys = BTreeSet::new();
    let mut unclassified_keys = BTreeSet::new();
    for audit in audits {
        for read in &audit.reads {
            checked_read_count += 1;
            let Some(expectation) = expectations.get(&read.key) else {
                if read.actual.is_none() {
                    continue;
                }
                anomalies.push(shared_state_anomaly(
                    audit.client_id,
                    read,
                    "shared key is present without any committed history event".to_string(),
                ));
                anomalous_keys.insert(read.key.clone());
                continue;
            };
            if expectation.has_unknown {
                unclassified_keys.insert(read.key.clone());
                continue;
            }
            match &read.actual {
                Some(actual) if expectation.possible_values.contains(actual) => {}
                None if expectation.absent_possible || expectation.possible_values.is_empty() => {}
                _ => {
                    anomalies.push(shared_state_anomaly(
                        audit.client_id,
                        read,
                        format!(
                            "shared key final state is not explained by committed history; \
                             possible_values={:?}; absent_possible={}",
                            expectation.possible_values, expectation.absent_possible
                        ),
                    ));
                    anomalous_keys.insert(read.key.clone());
                }
            }
        }
    }
    for (key, mut key_operations) in operations {
        if anomalous_keys.contains(&key) || unclassified_keys.contains(&key) {
            continue;
        }
        if key_operations.len() > MAX_SERIALIZABLE_EVENTS_PER_SHARED_KEY {
            unclassified_keys.insert(key);
            continue;
        }
        let result =
            check_shared_key_serializable(&mut key_operations, audit_reads_for_key(audits, &key));
        checked_order_constraint_count += result.order_constraint_count;
        if !result.is_serializable {
            anomalies.push(Anomaly {
                kind: AnomalyKind::SharedHistoryNotSerializable,
                client_id: -1,
                key,
                expected: None,
                actual: None,
                detail: "no serial ordering respecting per-client operation order and known \
                         simulated real-time order explains shared-key reads and final audit state"
                    .to_string(),
            });
        }
    }

    SharedKeyCheckReport {
        schema_version: ARTIFACT_SCHEMA_VERSION,
        checked_operation_count,
        checked_history_read_count,
        checked_order_constraint_count,
        checked_read_count,
        audit_count: audits.len(),
        anomaly_count: anomalies.len(),
        unclassified_key_count: unclassified_keys.len(),
        anomalies,
        unclassified_keys: unclassified_keys.into_iter().collect(),
    }
}

fn shared_key_operation(event: &HistoryEvent) -> Option<SharedKeyOperation> {
    match (&event.kind, &event.outcome) {
        (
            OperationKind::Put | OperationKind::Update | OperationKind::TransactWrite,
            OperationOutcome::Committed,
        ) => event
            .value
            .clone()
            .map(|value| SharedKeyOperation::Write { value }),
        (OperationKind::PutIfAbsent, OperationOutcome::Committed) => event
            .value
            .clone()
            .map(|value| SharedKeyOperation::PutIfAbsent { value }),
        (OperationKind::PutIfAbsent, OperationOutcome::ConditionFailed { .. }) => {
            Some(SharedKeyOperation::ConditionFailed)
        }
        (OperationKind::Delete, OperationOutcome::Committed) => Some(SharedKeyOperation::Delete),
        (OperationKind::Read, OperationOutcome::Committed) => Some(SharedKeyOperation::Read {
            actual: event.value.clone(),
        }),
        _ => None,
    }
}

fn audit_reads_for_key(audits: &[SharedKeyAudit], key: &str) -> Vec<Option<String>> {
    audits
        .iter()
        .flat_map(|audit| audit.reads.iter())
        .filter(|read| read.key == key)
        .map(|read| read.actual.clone())
        .collect()
}

fn check_shared_key_serializable(
    key_operations: &mut [SharedKeyOperationEntry],
    final_reads: Vec<Option<String>>,
) -> SerializableCheckResult {
    key_operations.sort();
    let (predecessor_masks, order_constraint_count) = predecessor_masks(key_operations);
    let initial = SerializableSearchState {
        scheduled_mask: 0,
        value: None,
    };
    let mut visited = BTreeSet::new();
    let is_serializable = search_serializable_interleaving(
        key_operations,
        &predecessor_masks,
        &final_reads,
        initial,
        &mut visited,
    );
    SerializableCheckResult {
        is_serializable,
        order_constraint_count,
    }
}

fn predecessor_masks(key_operations: &[SharedKeyOperationEntry]) -> (Vec<u64>, usize) {
    let mut masks = vec![0; key_operations.len()];
    let mut constraint_count = 0;
    for (left_index, left) in key_operations.iter().enumerate() {
        for (right_index, right) in key_operations.iter().enumerate() {
            if left_index == right_index || !must_precede(left, right) {
                continue;
            }
            let bit = 1_u64 << left_index;
            if masks[right_index] & bit == 0 {
                masks[right_index] |= bit;
                constraint_count += 1;
            }
        }
    }
    (masks, constraint_count)
}

fn must_precede(left: &SharedKeyOperationEntry, right: &SharedKeyOperationEntry) -> bool {
    if left.client_id == right.client_id && left.sequence < right.sequence {
        return true;
    }
    left.completed_at_sim_us > 0
        && right.started_at_sim_us > 0
        && left.completed_at_sim_us <= right.started_at_sim_us
}

fn search_serializable_interleaving(
    key_operations: &[SharedKeyOperationEntry],
    predecessor_masks: &[u64],
    final_reads: &[Option<String>],
    state: SerializableSearchState,
    visited: &mut BTreeSet<SerializableSearchState>,
) -> bool {
    if !visited.insert(state.clone()) {
        return false;
    }
    let complete_mask = complete_mask(key_operations.len());
    if state.scheduled_mask == complete_mask {
        return final_reads.iter().all(|actual| actual == &state.value);
    }
    for (index, entry) in key_operations.iter().enumerate() {
        let bit = 1_u64 << index;
        if state.scheduled_mask & bit != 0 {
            continue;
        }
        if predecessor_masks[index] & !state.scheduled_mask != 0 {
            continue;
        }
        let Some(next_value) = apply_serial_operation(&state.value, &entry.operation) else {
            continue;
        };
        let next_state = SerializableSearchState {
            scheduled_mask: state.scheduled_mask | bit,
            value: next_value,
        };
        if search_serializable_interleaving(
            key_operations,
            predecessor_masks,
            final_reads,
            next_state,
            visited,
        ) {
            return true;
        }
    }
    false
}

fn complete_mask(len: usize) -> u64 {
    if len == 0 { 0 } else { (1_u64 << len) - 1 }
}

fn apply_serial_operation(
    current: &Option<String>,
    operation: &SharedKeyOperation,
) -> Option<Option<String>> {
    match operation {
        SharedKeyOperation::Write { value } => Some(Some(value.clone())),
        SharedKeyOperation::PutIfAbsent { value } if current.is_none() => Some(Some(value.clone())),
        SharedKeyOperation::PutIfAbsent { .. } => None,
        SharedKeyOperation::Delete => Some(None),
        SharedKeyOperation::Read { actual } if actual == current => Some(current.clone()),
        SharedKeyOperation::Read { .. } => None,
        SharedKeyOperation::ConditionFailed if current.is_some() => Some(current.clone()),
        SharedKeyOperation::ConditionFailed => None,
    }
}

fn shared_state_anomaly(client_id: i32, read: &SharedKeyRead, detail: String) -> Anomaly {
    Anomaly {
        kind: AnomalyKind::SharedFinalStateUnexplained,
        client_id,
        key: read.key.clone(),
        expected: None,
        actual: read.actual.clone(),
        detail,
    }
}
