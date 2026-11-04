use storage_types::{StorageError, StorageResult, StreamRetentionDuration, TimestampMillis};

use super::{StreamTrimDueMarker, StreamTrimScope, StreamTrimState, StreamTrimStateWrite};

impl StreamTrimScope {
    pub fn table(scope_id: impl Into<String>, table_name: storage_types::TableName) -> Self {
        Self {
            scope_id: scope_id.into(),
            table_name,
        }
    }
}

impl StreamTrimState {
    pub fn require_valid_frontier(&self) -> StorageResult<()> {
        if self.policy_version != self.frontier.table_policy_version.get()
            || self.scope.scope_id
                != super::table_retention_scope_id(self.frontier.table_history_id)
        {
            return Err(StorageError::internal(
                "Table retention frontier has another authority",
            ));
        }
        self.frontier.require_valid_lineages()
    }

    pub fn next_policy_version(current: Option<&Self>) -> StorageResult<u64> {
        current.map_or(Ok(1), |state| {
            state
                .policy_version
                .checked_add(1)
                .ok_or_else(|| StorageError::validation("stream trim policy version overflow"))
        })
    }

    pub fn has_finite_due_work(&self) -> bool {
        self.retention != StreamRetentionDuration::Forever && self.next_due_at.is_some()
    }

    pub fn marker_matches(&self, marker: &StreamTrimDueMarker) -> bool {
        self.scheduled_marker().as_ref() == Some(marker)
    }

    /// The current state identifies exactly one physical due-index entry.
    pub fn scheduled_marker(&self) -> Option<StreamTrimDueMarker> {
        self.next_due_at
            .map(|due| StreamTrimDueMarker::new(due, self.scope.clone(), self.policy_version))
    }
}

impl StreamTrimStateWrite {
    /// Keep the prior trim floor while making existing rows eligible for an
    /// immediate evaluation under the replacement duration.
    pub(crate) fn preserve_existing_history(
        mut self,
        current: Option<&StreamTrimState>,
    ) -> StorageResult<Self> {
        let Some(current) = current else {
            return Ok(self);
        };
        if self.state.scope != current.scope {
            return Err(StorageError::internal(
                "stream retention replacement has another scope",
            ));
        }
        self.replaced_marker = current.next_due_at.map(|due_at| {
            StreamTrimDueMarker::new(due_at, current.scope.clone(), current.policy_version)
        });
        self.state.next_due_at = match self.state.retention {
            StreamRetentionDuration::Forever
                if self.state.frontier.has_pending_cleanup()?
                    || self.state.frontier.item_history_sweep.is_some() =>
            {
                Some(self.state.updated_at)
            }
            StreamRetentionDuration::Forever => None,
            StreamRetentionDuration::FiniteHours(_) => Some(self.state.updated_at),
        };
        self.next_marker = self.state.next_due_at.map(|due_at| {
            StreamTrimDueMarker::new(due_at, self.state.scope.clone(), self.state.policy_version)
        });
        Ok(self)
    }
}

impl StreamTrimDueMarker {
    pub fn new(due_at: TimestampMillis, scope: StreamTrimScope, policy_version: u64) -> Self {
        Self {
            due_at,
            scope,
            policy_version,
        }
    }
}
