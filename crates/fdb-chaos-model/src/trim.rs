use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    anomaly::{Anomaly, AnomalyKind},
    constants::ARTIFACT_SCHEMA_VERSION,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrimScopeKind {
    Table,
    Item,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TrimScopeExpectation {
    pub kind: TrimScopeKind,
    pub id: String,
}

impl TrimScopeExpectation {
    #[must_use]
    pub fn table(table_name: String) -> Self {
        Self {
            kind: TrimScopeKind::Table,
            id: table_name,
        }
    }

    #[must_use]
    pub fn item(key: String) -> Self {
        Self {
            kind: TrimScopeKind::Item,
            id: key,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TrimStateModel {
    scopes: BTreeSet<TrimScopeExpectation>,
    unclassified_scopes: BTreeSet<TrimScopeExpectation>,
}

impl TrimStateModel {
    pub fn expect_scope(&mut self, scope: TrimScopeExpectation) {
        self.scopes.insert(scope);
    }

    pub fn unclassify(&mut self, scope: TrimScopeExpectation) {
        self.unclassified_scopes.insert(scope);
    }

    #[must_use]
    pub fn classified_scopes(&self) -> Vec<TrimScopeExpectation> {
        self.scopes
            .difference(&self.unclassified_scopes)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn unclassified_scopes(&self) -> Vec<TrimScopeExpectation> {
        self.unclassified_scopes.iter().cloned().collect()
    }

    #[must_use]
    pub fn unclassified_count(&self) -> usize {
        self.unclassified_scopes.len()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TrimScopeReport {
    pub schema_version: u32,
    pub client_id: i32,
    pub classified_scopes: Vec<TrimScopeExpectation>,
    pub unclassified_scopes: Vec<TrimScopeExpectation>,
}

impl TrimScopeReport {
    #[must_use]
    pub fn new(
        client_id: i32,
        classified_scopes: Vec<TrimScopeExpectation>,
        unclassified_scopes: Vec<TrimScopeExpectation>,
    ) -> Self {
        Self {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            client_id,
            classified_scopes,
            unclassified_scopes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TrimProviderSnapshot {
    pub schema_version: u32,
    pub client_id: i32,
    pub table_scopes: Vec<String>,
    pub item_scopes: Vec<String>,
}

impl TrimProviderSnapshot {
    #[must_use]
    pub fn new(client_id: i32, table_scopes: Vec<String>, item_scopes: Vec<String>) -> Self {
        Self {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            client_id,
            table_scopes,
            item_scopes,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AggregateTrimCheckReport {
    pub schema_version: u32,
    pub checked_client_count: usize,
    pub expected_table_scope_count: usize,
    pub actual_table_scope_count: usize,
    pub expected_item_scope_count: usize,
    pub actual_item_scope_count: usize,
    pub unclassified_item_scope_count: usize,
    pub anomaly_count: usize,
    pub anomalies: Vec<Anomaly>,
}

pub fn check_aggregate_trim_scopes(
    reports: &[TrimScopeReport],
    snapshot: &TrimProviderSnapshot,
) -> AggregateTrimCheckReport {
    let mut expected_table_scopes = BTreeSet::new();
    let mut expected_item_scopes = BTreeSet::new();
    let mut unclassified_item_scopes = BTreeSet::new();
    for report in reports {
        for scope in &report.classified_scopes {
            match scope.kind {
                TrimScopeKind::Table => {
                    expected_table_scopes.insert(scope.id.clone());
                }
                TrimScopeKind::Item => {
                    expected_item_scopes.insert(scope.id.clone());
                }
            }
        }
        for scope in &report.unclassified_scopes {
            if scope.kind == TrimScopeKind::Item {
                unclassified_item_scopes.insert(scope.id.clone());
            }
        }
    }

    let actual_table_scopes = snapshot
        .table_scopes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let actual_item_scopes = snapshot
        .item_scopes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let missing_item_scopes = expected_item_scopes
        .difference(&actual_item_scopes)
        .cloned()
        .collect::<Vec<_>>();
    let unexpected_item_scopes = actual_item_scopes
        .difference(&expected_item_scopes)
        .filter(|scope| !unclassified_item_scopes.contains(*scope))
        .cloned()
        .collect::<Vec<_>>();

    let mut anomalies = Vec::new();
    if actual_table_scopes.len() != expected_table_scopes.len() {
        anomalies.push(Anomaly {
            kind: AnomalyKind::AuditValueMismatch,
            client_id: snapshot.client_id,
            key: "stream-trim/table-scope-count".to_string(),
            expected: Some(expected_table_scopes.len().to_string()),
            actual: Some(actual_table_scopes.len().to_string()),
            detail: format!(
                "aggregate trim expected {} table scope(s) from client histories; provider had {} \
                 compact table scope(s): {:?}",
                expected_table_scopes.len(),
                actual_table_scopes.len(),
                actual_table_scopes
            ),
        });
    }
    if !missing_item_scopes.is_empty() || !unexpected_item_scopes.is_empty() {
        anomalies.push(Anomaly {
            kind: AnomalyKind::AuditValueMismatch,
            client_id: snapshot.client_id,
            key: "stream-trim/item-scopes".to_string(),
            expected: Some(format!("{expected_item_scopes:?}")),
            actual: Some(format!("{actual_item_scopes:?}")),
            detail: format!(
                "aggregate trim item scope mismatch missing_count={} unexpected_count={} \
                 unclassified_count={} missing={:?} unexpected={:?}",
                missing_item_scopes.len(),
                unexpected_item_scopes.len(),
                unclassified_item_scopes.len(),
                missing_item_scopes,
                unexpected_item_scopes
            ),
        });
    }

    AggregateTrimCheckReport {
        schema_version: ARTIFACT_SCHEMA_VERSION,
        checked_client_count: reports.len(),
        expected_table_scope_count: expected_table_scopes.len(),
        actual_table_scope_count: actual_table_scopes.len(),
        expected_item_scope_count: expected_item_scopes.len(),
        actual_item_scope_count: actual_item_scopes.len(),
        unclassified_item_scope_count: unclassified_item_scopes.len(),
        anomaly_count: anomalies.len(),
        anomalies,
    }
}
