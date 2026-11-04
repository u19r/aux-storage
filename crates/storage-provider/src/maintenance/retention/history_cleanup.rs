//! One table frontier authorizes a bounded physical history-prefix deletion.

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use storage_types::{
    ItemHistoryAddress, ItemHistoryKind, ItemHistoryMetadata, ItemHistoryPublication,
    ItemStreamVersion, StorageError, StorageResult, SystemStreamLineageId, SystemStreamOffset,
    SystemStreamScope, SystemStreamShardId, TableHistoryId,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) item_history_sweep: Option<Box<super::orphan_cleanup::ItemHistorySweep>>,
}

/// The native port observes the first two surviving versions, not an arbitrary
/// page after an uncommitted cursor. A short page must establish its tail.
pub struct ItemHistoryMetadataPage {
    pub entries: Vec<ItemHistoryMetadata>,
    pub reached_tail: bool,
}

impl ItemHistoryMetadataPage {
    pub const MAX_ENTRIES: usize = 2;
}

/// Physical metadata observations join the cleanup owner's attempt. The
/// lowerer uses one additional native head to establish the prefix's tail.
/// One native namespace and its provider-owned exclusive continuation.
pub struct TableHistoryNamespace {
    pub stream_name: storage_types::StreamName,
    pub cursor: Vec<u8>,
}

pub trait ItemHistoryMetadataAttempt:
    infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    fn load_table_history_namespace(
        &mut self,
        history: TableHistoryId,
        after: Option<&[u8]>,
    ) -> impl Future<Output = StorageResult<Option<TableHistoryNamespace>>> + Send;

    fn load_item_history_metadata_pages(
        &mut self,
        histories: &[ItemHistoryAddress],
    ) -> impl Future<Output = StorageResult<Vec<ItemHistoryMetadataPage>>> + Send;
}

pub struct ItemHistoryCleanupObservation {
    history: ItemHistoryAddress,
    frontier: TableHistoryTrimFrontier,
    page: ItemHistoryMetadataPage,
    current_live_version: Option<ItemStreamVersion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryCleanupContinuation {
    Repeat,
    Complete,
}

pub enum ItemHistoryCleanupPlan {
    Complete,
    Delete {
        history: ItemHistoryAddress,
        expected_table_policy_version: NonZeroU64,
        through: ItemStreamVersion,
        continuation: HistoryCleanupContinuation,
    },
}

impl ItemHistoryCleanupObservation {
    pub fn try_new(
        history: ItemHistoryAddress,
        frontier: TableHistoryTrimFrontier,
        page: ItemHistoryMetadataPage,
        current_live_version: Option<ItemStreamVersion>,
    ) -> StorageResult<Self> {
        history.physical_stream_name()?;
        if history.history_id != frontier.table_history_id
            || history.history_id.as_ref() == &[0; 16]
            || history.stream_name.item_table_name().is_none()
            || current_live_version.is_some_and(|version| version.get() == 0)
        {
            return Err(invalid_observation());
        }
        frontier.require_valid_lineages()?;
        page.require_valid_prefix(&history)?;
        Ok(Self {
            history,
            frontier,
            page,
            current_live_version,
        })
    }

    pub fn plan_cleanup(self) -> StorageResult<ItemHistoryCleanupPlan> {
        let Some(oldest) = self.page.entries.first() else {
            return Ok(ItemHistoryCleanupPlan::Complete);
        };
        let first_unretired = self.find_first_unretired()?;
        self.require_exact_predecessor(first_unretired)?;
        let protected = self.protected_version(first_unretired);
        if protected.is_some_and(|version| oldest.version() >= version) {
            return Ok(ItemHistoryCleanupPlan::Complete);
        }
        let continuation = self.cleanup_continuation(first_unretired, protected);
        Ok(ItemHistoryCleanupPlan::Delete {
            history: self.history,
            expected_table_policy_version: self.frontier.table_policy_version,
            through: oldest.version(),
            continuation,
        })
    }

    fn find_first_unretired(&self) -> StorageResult<Option<&ItemHistoryMetadata>> {
        for entry in &self.page.entries {
            if matches!(entry.publication(), ItemHistoryPublication::Event { .. })
                && !self.frontier.is_retired(entry)?
            {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    fn require_exact_predecessor(
        &self,
        first_unretired: Option<&ItemHistoryMetadata>,
    ) -> StorageResult<()> {
        if let Some(previous) = first_unretired.and_then(ItemHistoryMetadata::previous_version)
            && !self
                .page
                .entries
                .iter()
                .any(|entry| entry.version() == previous && entry.kind() == ItemHistoryKind::Image)
        {
            return Err(StorageError::internal(
                "Required history predecessor image is unavailable",
            ));
        }
        Ok(())
    }

    fn protected_version(
        &self,
        first_unretired: Option<&ItemHistoryMetadata>,
    ) -> Option<ItemStreamVersion> {
        let boundary = if let Some(entry) = first_unretired {
            Some(entry.previous_version().unwrap_or(entry.version()))
        } else if self.page.reached_tail {
            None
        } else {
            self.page.entries.last().map(ItemHistoryMetadata::version)
        };
        boundary.into_iter().chain(self.current_live_version).min()
    }

    fn cleanup_continuation(
        &self,
        first_unretired: Option<&ItemHistoryMetadata>,
        protected: Option<ItemStreamVersion>,
    ) -> HistoryCleanupContinuation {
        let Some(next) = self.page.entries.get(1) else {
            return HistoryCleanupContinuation::Complete;
        };
        if first_unretired.is_some()
            || protected.is_some_and(|version| version <= next.version()) && self.page.reached_tail
        {
            return HistoryCleanupContinuation::Complete;
        }
        HistoryCleanupContinuation::Repeat
    }
}

impl ItemHistoryMetadataPage {
    fn require_valid_prefix(&self, history: &ItemHistoryAddress) -> StorageResult<()> {
        if self.entries.len() > Self::MAX_ENTRIES
            || !self.reached_tail && self.entries.len() != Self::MAX_ENTRIES
        {
            return Err(invalid_observation());
        }
        for (ordinal, entry) in self.entries.iter().enumerate() {
            if entry.address() != history
                || ordinal > 0 && self.entries[ordinal - 1].version() >= entry.version()
            {
                return Err(invalid_observation());
            }
        }
        Ok(())
    }
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
            item_history_sweep: None,
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

    pub fn is_retired(&self, entry: &ItemHistoryMetadata) -> StorageResult<bool> {
        let ItemHistoryPublication::Event {
            source_scope,
            table_position,
            ..
        } = entry.publication()
        else {
            return Ok(false);
        };
        if *source_scope != self.source_scope {
            return Ok(false);
        }
        let retired = self
            .lineages
            .iter()
            .find(|lineage| {
                &lineage.shard_id == table_position.shard_id()
                    && &lineage.lineage_id == table_position.lineage_id()
            })
            .and_then(|lineage| lineage.retired_through.as_ref());
        retired
            .map(|through| {
                table_position
                    .offset()
                    .compare_order(through)
                    .map(|order| !order.is_gt())
                    .map_err(|_| invalid_observation())
            })
            .unwrap_or(Ok(false))
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
