use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationKind {
    Put,
    PutIfAbsent,
    Read,
    Update,
    Delete,
    TransactWrite,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationOutcome {
    Committed,
    ConditionFailed { error: String },
    Failed { error: String },
    Unknown { error: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HistoryEvent {
    pub sequence: u64,
    pub client_id: i32,
    #[serde(default)]
    pub started_at_sim_us: u64,
    #[serde(default)]
    pub completed_at_sim_us: u64,
    pub kind: OperationKind,
    pub key: String,
    pub value: Option<String>,
    pub outcome: OperationOutcome,
}

impl HistoryEvent {
    #[must_use]
    pub fn new(
        sequence: u64,
        client_id: i32,
        kind: OperationKind,
        key: String,
        value: Option<String>,
        outcome: OperationOutcome,
    ) -> Self {
        Self::with_sim_interval(sequence, client_id, 0, 0, kind, key, value, outcome)
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn with_sim_interval(
        sequence: u64,
        client_id: i32,
        started_at_sim_us: u64,
        completed_at_sim_us: u64,
        kind: OperationKind,
        key: String,
        value: Option<String>,
        outcome: OperationOutcome,
    ) -> Self {
        Self {
            sequence,
            client_id,
            started_at_sim_us,
            completed_at_sim_us,
            kind,
            key,
            value,
            outcome,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationHistory {
    events: Vec<HistoryEvent>,
}

impl OperationHistory {
    pub fn push(&mut self, event: HistoryEvent) {
        self.events.push(event);
    }

    #[must_use]
    pub fn events(&self) -> &[HistoryEvent] {
        &self.events
    }

    #[must_use]
    pub fn committed_count(&self) -> usize {
        self.events
            .iter()
            .filter(|event| event.outcome == OperationOutcome::Committed)
            .count()
    }

    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.events
            .iter()
            .filter(|event| matches!(event.outcome, OperationOutcome::Failed { .. }))
            .count()
    }

    #[must_use]
    pub fn condition_failed_count(&self) -> usize {
        self.events
            .iter()
            .filter(|event| matches!(event.outcome, OperationOutcome::ConditionFailed { .. }))
            .count()
    }

    #[must_use]
    pub fn unknown_count(&self) -> usize {
        self.events
            .iter()
            .filter(|event| matches!(event.outcome, OperationOutcome::Unknown { .. }))
            .count()
    }

    pub fn to_json_lines(&self) -> Result<String, serde_json::Error> {
        let mut lines = String::new();
        for event in &self.events {
            lines.push_str(&serde_json::to_string(event)?);
            lines.push('\n');
        }
        Ok(lines)
    }

    pub fn from_json_lines(input: &str) -> Result<Self, serde_json::Error> {
        let mut history = Self::default();
        for line in input.lines().filter(|line| !line.trim().is_empty()) {
            history.push(serde_json::from_str(line)?);
        }
        Ok(history)
    }
}

pub fn classify_operation_error(error: &str) -> OperationOutcome {
    if error.contains("commit_unknown_result") || error.contains("maybe_committed") {
        OperationOutcome::Unknown {
            error: error.to_string(),
        }
    } else {
        OperationOutcome::Failed {
            error: error.to_string(),
        }
    }
}
