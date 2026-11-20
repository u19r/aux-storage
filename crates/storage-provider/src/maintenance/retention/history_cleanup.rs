//! Durable per-lineage retention authority.

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use storage_types::{
    StorageError, StorageResult, SystemStreamLineageId, SystemStreamOffset, SystemStreamScope,
    SystemStreamShardId, TableHistoryId,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableHistoryTrimLineage {
    pub shard_id: SystemStreamShardId,
    pub lineage_id: SystemStreamLineageId,
    pub retired_through: Option<SystemStreamOffset>,
    pub cleaned_through: Option<SystemStreamOffset>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableHistoryTrimFrontier {
    pub table_history_id: TableHistoryId,
    pub table_policy_version: NonZeroU64,
    pub source_scope: SystemStreamScope,
    pub lineages: Vec<TableHistoryTrimLineage>,
}

impl TableHistoryTrimFrontier {
    pub(crate) fn has_pending_cleanup(&self) -> StorageResult<bool> {
        for lineage in &self.lineages {
            if let Some(retired) = &lineage.retired_through {
                let Some(cleaned) = &lineage.cleaned_through else {
                    return Ok(true);
                };
                if cleaned
                    .compare_order(retired)
                    .map_err(|_| invalid_observation())?
                    .is_lt()
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub fn initial(
        history_id: TableHistoryId,
        policy_version: NonZeroU64,
        source_high: &storage_types::SystemStreamCheckpoint,
    ) -> StorageResult<Self> {
        if history_id.as_ref() == &[0; 16] {
            return Err(invalid_observation());
        }
        Ok(Self {
            table_history_id: history_id,
            table_policy_version: policy_version,
            source_scope: source_high.scope().clone(),
            lineages: source_high
                .frontier()
                .iter()
                .map(|position| TableHistoryTrimLineage {
                    shard_id: position.shard_id().clone(),
                    lineage_id: position.lineage_id().clone(),
                    retired_through: None,
                    cleaned_through: None,
                })
                .collect(),
        })
    }

    /// Retire each lineage's history through its bound; a retired prefix
    /// never shrinks.
    pub(crate) fn retire_through(
        &mut self,
        bounds: &[storage_types::SystemStreamPosition],
    ) -> StorageResult<()> {
        for bound in bounds {
            let index = match self.lineages.iter().position(|lineage| {
                lineage.shard_id == *bound.shard_id() && lineage.lineage_id == *bound.lineage_id()
            }) {
                Some(index) => index,
                None => {
                    self.lineages.push(TableHistoryTrimLineage {
                        shard_id: bound.shard_id().clone(),
                        lineage_id: bound.lineage_id().clone(),
                        retired_through: None,
                        cleaned_through: None,
                    });
                    self.lineages.len() - 1
                }
            };
            let lineage = &mut self.lineages[index];
            let raises = match &lineage.retired_through {
                Some(retired) => bound
                    .offset()
                    .compare_order(retired)
                    .map_err(|_| invalid_observation())?
                    .is_gt(),
                None => true,
            };
            if raises {
                lineage.retired_through = Some(bound.offset().clone());
            }
        }
        Ok(())
    }

    pub(super) fn require_valid_lineages(&self) -> StorageResult<()> {
        if self.table_history_id.as_ref() == &[0; 16] {
            return Err(invalid_observation());
        }
        for (ordinal, lineage) in self.lineages.iter().enumerate() {
            if self.lineages[..ordinal].iter().any(|previous| {
                previous.shard_id == lineage.shard_id && previous.lineage_id == lineage.lineage_id
            }) {
                return Err(invalid_observation());
            }
            lineage.require_valid_progress()?;
        }
        Ok(())
    }
}

impl TableHistoryTrimLineage {
    fn require_valid_progress(&self) -> StorageResult<()> {
        let Some(retired) = self.retired_through.as_ref() else {
            return if self.cleaned_through.is_none() {
                Ok(())
            } else {
                Err(invalid_observation())
            };
        };
        let cleaned = self.cleaned_through.as_ref().unwrap_or(retired);
        if cleaned
            .compare_order(retired)
            .map_err(|_| invalid_observation())?
            .is_gt()
        {
            return Err(invalid_observation());
        }
        Ok(())
    }
}

fn invalid_observation() -> StorageError {
    StorageError::internal("Invalid table-controlled history cleanup observation")
}
