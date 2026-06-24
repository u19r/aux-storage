use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::history::{HistoryEvent, OperationKind, OperationOutcome};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableModel {
    items: BTreeMap<String, String>,
}

impl TableModel {
    pub fn apply(&mut self, event: &HistoryEvent) {
        if event.outcome != OperationOutcome::Committed {
            return;
        }
        match event.kind {
            OperationKind::Put | OperationKind::Update | OperationKind::TransactWrite => {
                if let Some(value) = &event.value {
                    self.items.insert(event.key.clone(), value.clone());
                }
            }
            OperationKind::Read => {}
            OperationKind::PutIfAbsent => {
                if let Some(value) = &event.value {
                    self.items.entry(event.key.clone()).or_insert(value.clone());
                }
            }
            OperationKind::Delete => {
                self.items.remove(&event.key);
            }
        }
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.items.get(key).map(String::as_str)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PossibleTableModel {
    items: BTreeMap<String, BTreeSet<Option<String>>>,
}

impl PossibleTableModel {
    pub fn apply(&mut self, event: &HistoryEvent) {
        let current = self
            .items
            .get(&event.key)
            .cloned()
            .unwrap_or_else(|| BTreeSet::from([None]));
        let next = match &event.outcome {
            OperationOutcome::Committed => apply_committed_possibilities(&current, event),
            OperationOutcome::ConditionFailed { .. } => {
                apply_condition_failed_possibilities(&current, event)
            }
            OperationOutcome::Failed { .. } => current,
            OperationOutcome::Unknown { .. } => {
                let mut next = current.clone();
                next.extend(apply_committed_possibilities(&current, event));
                next
            }
        };
        self.items.insert(event.key.clone(), next);
    }

    #[must_use]
    pub fn allows(&self, key: &str, actual: Option<&str>) -> bool {
        let expected = actual.map(str::to_string);
        self.items
            .get(key)
            .map_or(actual.is_none(), |values| values.contains(&expected))
    }

    #[must_use]
    pub fn allows_present(&self, key: &str) -> bool {
        self.items
            .get(key)
            .is_some_and(|values| values.iter().any(Option::is_some))
    }

    #[must_use]
    pub fn describe_key(&self, key: &str) -> String {
        self.items
            .get(key)
            .cloned()
            .unwrap_or_else(|| BTreeSet::from([None]))
            .into_iter()
            .map(|value| value.unwrap_or_else(|| "<absent>".to_string()))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn apply_committed_possibilities(
    current: &BTreeSet<Option<String>>,
    event: &HistoryEvent,
) -> BTreeSet<Option<String>> {
    current
        .iter()
        .filter_map(|value| apply_committed_value(value, event))
        .collect()
}

fn apply_committed_value(current: &Option<String>, event: &HistoryEvent) -> Option<Option<String>> {
    match event.kind {
        OperationKind::Put | OperationKind::Update | OperationKind::TransactWrite => {
            event.value.clone().map(Some)
        }
        OperationKind::Read => Some(current.clone()),
        OperationKind::PutIfAbsent if current.is_none() => event.value.clone().map(Some),
        OperationKind::PutIfAbsent => None,
        OperationKind::Delete => Some(None),
    }
}

fn apply_condition_failed_possibilities(
    current: &BTreeSet<Option<String>>,
    event: &HistoryEvent,
) -> BTreeSet<Option<String>> {
    if event.kind != OperationKind::PutIfAbsent {
        return current.clone();
    }
    current
        .iter()
        .filter(|value| value.is_some())
        .cloned()
        .collect()
}
