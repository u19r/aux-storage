use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use sha2::{Digest, Sha256};
use storage_types::{
    StorageError, StorageResult, SystemStreamCheckpoint, SystemStreamContractError,
    SystemStreamLineageId, SystemStreamPosition, SystemStreamScope, SystemStreamShardId,
};

/// A logical shard and lineage known by a provider.
///
/// The values are logical identifiers.  A provider must not populate them
/// with physical table, partition, or pointer IDs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SystemStreamLineage {
    shard_id: SystemStreamShardId,
    lineage_id: SystemStreamLineageId,
    parent_lineage_id: Option<SystemStreamLineageId>,
    sealed: bool,
}

impl SystemStreamLineage {
    /// Construct a catalog entry after the provider has validated its logical
    /// identifiers.
    pub fn try_new(
        shard_id: impl Into<String>,
        lineage_id: impl Into<String>,
        parent_lineage_id: Option<String>,
        sealed: bool,
    ) -> Result<Self, SystemStreamContractError> {
        let lineage_id = SystemStreamLineageId::try_new(lineage_id)?;
        let parent_lineage_id = parent_lineage_id
            .map(SystemStreamLineageId::try_new)
            .transpose()?;
        if parent_lineage_id.as_ref() == Some(&lineage_id) {
            return Err(SystemStreamContractError::BackendContract);
        }
        Ok(Self {
            shard_id: SystemStreamShardId::try_new(shard_id)?,
            lineage_id,
            parent_lineage_id,
            sealed,
        })
    }

    #[must_use]
    pub fn shard_id(&self) -> &SystemStreamShardId {
        &self.shard_id
    }

    #[must_use]
    pub fn lineage_id(&self) -> &SystemStreamLineageId {
        &self.lineage_id
    }

    #[must_use]
    pub fn parent_lineage_id(&self) -> Option<&SystemStreamLineageId> {
        self.parent_lineage_id.as_ref()
    }

    #[must_use]
    pub const fn sealed(&self) -> bool {
        self.sealed
    }

    pub fn position(
        &self,
        offset: impl Into<String>,
    ) -> Result<SystemStreamPosition, SystemStreamContractError> {
        SystemStreamPosition::try_new(self.shard_id.as_str(), self.lineage_id.as_str(), offset)
    }
}

/// Stable physical pointer namespace for one logical lineage. Catalog
/// validation rejects a collision before either backend can publish a split.
pub(crate) fn pointer_slot(lineage_id: &SystemStreamLineageId) -> [u8; 4] {
    let mut digest = Sha256::new();
    digest.update(crate::constants::SYSTEM_POINTER_SLOT_DOMAIN);
    digest.update(lineage_id.as_str().as_bytes());
    let digest = digest.finalize();
    [digest[0], digest[1], digest[2], digest[3]]
}

/// A provider-owned, validated snapshot of the logical lineage catalog.
///
/// The catalog is the precondition for a fresh split.  Its constructor rejects
/// duplicate identities, duplicate active shards, missing parents, cycles, and
/// active descendants of an unsealed parent before a provider can mutate state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamLineageCatalog {
    source_scope: SystemStreamScope,
    entries: Vec<SystemStreamLineage>,
}

impl SystemStreamLineageCatalog {
    /// Route one write to an active lineage using the same canonical route
    /// bytes for SQL and KV. A split changes only the active catalog, not
    /// this selection rule.
    pub fn writer_lineage(&self, routing_key: &[u8]) -> StorageResult<&SystemStreamLineage> {
        let mut active = self
            .entries
            .iter()
            .filter(|entry| !entry.sealed())
            .collect::<Vec<_>>();
        active.sort_unstable_by(|left, right| {
            left.shard_id()
                .cmp(right.shard_id())
                .then_with(|| left.lineage_id().cmp(right.lineage_id()))
        });
        if active.is_empty() {
            return Err(StorageError::internal(
                "SystemStream has no active writer lineage",
            ));
        }
        let digest = Sha256::digest(routing_key);
        let mut prefix = [0; 8];
        prefix.copy_from_slice(&digest[..8]);
        let index = (u64::from_be_bytes(prefix) % active.len() as u64) as usize;
        Ok(active[index])
    }

    /// Validate a provider catalog for one logical source scope.
    pub fn try_new(
        source_scope: SystemStreamScope,
        entries: Vec<SystemStreamLineage>,
    ) -> Result<Self, SystemStreamContractError> {
        validate_lineage_catalog(&entries)?;
        Ok(Self {
            source_scope,
            entries,
        })
    }

    #[must_use]
    pub fn source_scope(&self) -> &SystemStreamScope {
        &self.source_scope
    }

    #[must_use]
    pub fn entries(&self) -> &[SystemStreamLineage] {
        &self.entries
    }

    #[must_use]
    pub fn lineage(&self, lineage_id: &SystemStreamLineageId) -> Option<&SystemStreamLineage> {
        self.entries
            .iter()
            .find(|entry| entry.lineage_id() == lineage_id)
    }

    pub fn validate_scope(
        &self,
        requested: &SystemStreamScope,
    ) -> Result<(), SystemStreamContractError> {
        validate_scope(requested, &self.source_scope)
    }

    pub fn initial_checkpoint(&self) -> Result<SystemStreamCheckpoint, SystemStreamContractError> {
        let mut seen_shards = BTreeSet::new();
        let positions = self
            .entries
            .iter()
            .filter(|lineage| {
                lineage.parent_lineage_id().is_none()
                    && seen_shards.insert(lineage.shard_id().as_str())
            })
            .map(|lineage| lineage.position("0"))
            .collect::<Result<Vec<_>, _>>()?;
        SystemStreamCheckpoint::try_new(self.source_scope.clone(), positions)
    }

    /// Protect each checkpoint position and every descendant of a sealed
    /// position. The validated catalog bounds traversal and excludes cycles.
    pub fn protected_positions(
        &self,
        checkpoint: &SystemStreamCheckpoint,
    ) -> Result<Vec<SystemStreamPosition>, SystemStreamContractError> {
        checkpoint.validate_scope(&self.source_scope)?;
        self.expand_protected_positions(checkpoint.frontier())
    }

    /// Expand persisted minimum frontiers. Several sealed lineages may share
    /// one shard after a split, so this accepts the stored positions directly
    /// instead of forcing them back through a checkpoint's one-per-shard form.
    pub fn expand_protected_positions(
        &self,
        frontiers: &[SystemStreamPosition],
    ) -> Result<Vec<SystemStreamPosition>, SystemStreamContractError> {
        if frontiers.len() > storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
            return Err(SystemStreamContractError::BackendContract);
        }
        let mut positions = BTreeMap::<SystemStreamLineageId, SystemStreamPosition>::new();
        let mut pending = frontiers.to_vec();
        while let Some(position) = pending.pop() {
            let lineage = self
                .lineage(position.lineage_id())
                .ok_or(SystemStreamContractError::UnknownLineage)?;
            if lineage.shard_id() != position.shard_id() {
                return Err(SystemStreamContractError::UnknownLineage);
            }
            if let Some(previous) = positions.get_mut(position.lineage_id()) {
                if (position.offset().order()?, position.offset().as_str())
                    < (previous.offset().order()?, previous.offset().as_str())
                {
                    *previous = position;
                }
                continue;
            }
            if lineage.sealed() {
                for child in self
                    .entries
                    .iter()
                    .filter(|child| child.parent_lineage_id() == Some(lineage.lineage_id()))
                {
                    pending.push(child.position("0")?);
                }
            }
            positions.insert(position.lineage_id().clone(), position);
        }
        Ok(positions.into_values().collect())
    }

    #[must_use]
    pub fn has_unexpanded_sealed_frontier(&self, frontier: &[SystemStreamPosition]) -> bool {
        frontier.iter().any(|position| {
            self.entries.iter().any(|lineage| {
                lineage.sealed()
                    && lineage.shard_id() == position.shard_id()
                    && lineage.lineage_id() == position.lineage_id()
            })
        })
    }
}

fn validate_lineage_catalog(
    entries: &[SystemStreamLineage],
) -> Result<(), SystemStreamContractError> {
    if entries.is_empty() || entries.len() > storage_types::SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
        return Err(SystemStreamContractError::BackendContract);
    }

    let mut lineage_ids = HashSet::with_capacity(entries.len());
    let mut pointer_slots = HashSet::with_capacity(entries.len());
    let mut active_shards = HashSet::with_capacity(entries.len());
    let mut children_by_parent = HashMap::<&str, Vec<&SystemStreamLineage>>::new();
    for entry in entries {
        if !lineage_ids.insert(entry.lineage_id().as_str()) {
            return Err(SystemStreamContractError::BackendContract);
        }
        if !pointer_slots.insert(pointer_slot(entry.lineage_id())) {
            return Err(SystemStreamContractError::BackendContract);
        }
        if !entry.sealed() && !active_shards.insert(entry.shard_id().as_str()) {
            return Err(SystemStreamContractError::DuplicateShard);
        }
        if let Some(parent) = entry.parent_lineage_id() {
            children_by_parent
                .entry(parent.as_str())
                .or_insert_with(|| Vec::with_capacity(2))
                .push(entry);
        }
    }

    if active_shards.is_empty() {
        return Err(SystemStreamContractError::BackendContract);
    }

    for entry in entries {
        let Some(parent_id) = entry.parent_lineage_id() else {
            continue;
        };
        let parent = entries
            .iter()
            .find(|candidate| candidate.lineage_id() == parent_id)
            .ok_or(SystemStreamContractError::UnknownLineage)?;
        if !parent.sealed() || parent.shard_id() == entry.shard_id() {
            return Err(SystemStreamContractError::BackendContract);
        }

        let mut visited = HashSet::new();
        let mut current = Some(parent_id);
        while let Some(lineage_id) = current {
            if !visited.insert(lineage_id.as_str()) {
                return Err(SystemStreamContractError::BackendContract);
            }
            current = entries
                .iter()
                .find(|candidate| candidate.lineage_id() == lineage_id)
                .and_then(SystemStreamLineage::parent_lineage_id);
        }
    }
    for parent in entries.iter().filter(|entry| entry.sealed()) {
        let children = children_by_parent
            .get(parent.lineage_id().as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let [left, right] = children else {
            return Err(SystemStreamContractError::BackendContract);
        };
        if left.shard_id() == right.shard_id() {
            return Err(SystemStreamContractError::BackendContract);
        }
    }
    Ok(())
}

fn validate_scope(
    actual: &SystemStreamScope,
    expected: &SystemStreamScope,
) -> Result<(), SystemStreamContractError> {
    if actual.version() != expected.version() || actual.scope_id() != expected.scope_id() {
        return Err(SystemStreamContractError::WrongScope);
    }
    if actual.generation() != expected.generation() {
        return Err(SystemStreamContractError::WrongGeneration);
    }
    Ok(())
}

impl std::ops::Deref for SystemStreamLineageCatalog {
    type Target = [SystemStreamLineage];

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl From<SystemStreamLineageCatalog> for Vec<SystemStreamLineage> {
    fn from(catalog: SystemStreamLineageCatalog) -> Self {
        catalog.entries
    }
}
