//! Reads qualify the persisted graph before publishing retained content.

use std::num::NonZeroUsize;

use infrastructure_provider::{limits::InfrastructureLimits, record::DurableRecord};
use storage_types::{ImageAddress, StorageResult};

use super::{
    RetainedOwnerRecord, RetainedOwnershipAttempt,
    address::image_record_key,
    error::invalid_image,
    ownership_changes::OwnershipPhase,
    ownership_graph::{ImageOwnerEdge, ImageOwnershipHead},
    ownership_inventory::OwnershipRecords,
};

#[derive(Default)]
struct OwnershipReadPlan {
    heads: OwnershipRecords<HeadRead>,
    edges: OwnershipRecords<EdgeRead>,
}

struct HeadRead {
    address: ImageAddress,
    raw_bytes: usize,
    encoded_bytes: Option<usize>,
    created: bool,
    established_edges: u64,
}

struct EdgeRead {
    edge: ImageOwnerEdge,
    established: bool,
}

impl RetainedOwnerRecord {
    /// Receipt existence alone cannot qualify an incomplete ownership cutover.
    /// Pending transitions still refer to their first persisted graph
    /// observation.
    pub async fn require_graph_batch<A: RetainedOwnershipAttempt>(
        attempt: &mut A,
        receipts: &[Self],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            for wave in receipts.chunks(crate::constants::IMAGE_OWNER_READ_RECORDS) {
                Self::require_graph_wave(attempt, wave).await?;
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_graph_wave<A: RetainedOwnershipAttempt>(
        attempt: &mut A,
        receipts: &[Self],
    ) -> StorageResult<()> {
        let mut plan = OwnershipReadPlan::default();
        for receipt in receipts {
            plan.include_receipt(attempt, receipt)?;
        }
        plan.qualify(attempt).await
    }
}

impl OwnershipReadPlan {
    fn include_receipt<A: RetainedOwnershipAttempt>(
        &mut self,
        attempt: &mut A,
        receipt: &RetainedOwnerRecord,
    ) -> StorageResult<()> {
        let owner = receipt.key()?;
        let changes = attempt.ownership_changes();
        if !matches!(
            changes.phase,
            OwnershipPhase::Collecting | OwnershipPhase::Finalized
        ) {
            return Err(invalid_image());
        }
        let staged = changes.owners.get(&owner);
        if staged.is_some_and(|change| change.next.as_ref() != Some(receipt)) {
            return Err(invalid_image());
        }
        for (address, raw_bytes) in &receipt.bodies {
            let observation = changes.bodies.get(&image_record_key(*address)?);
            if observation
                .is_some_and(|body| body.address != *address || body.raw_bytes != *raw_bytes)
            {
                return Err(invalid_image());
            }
            let established = staged.is_none_or(|change| {
                change
                    .previous
                    .as_ref()
                    .is_some_and(|previous| previous.bodies.contains(&(*address, *raw_bytes)))
            });
            let head = HeadRead {
                address: *address,
                raw_bytes: *raw_bytes,
                encoded_bytes: observation.map(|body| body.encoded_bytes),
                created: observation.is_some_and(|body| body.created),
                established_edges: 0,
            };
            self.include_body(
                head,
                ImageOwnerEdge::try_new(*address, (&owner).try_into()?)?,
                established,
            )?;
        }
        Ok(())
    }

    fn include_body(
        &mut self,
        head: HeadRead,
        edge: ImageOwnerEdge,
        established: bool,
    ) -> StorageResult<()> {
        if head.raw_bytes == 0 || (head.created && established) {
            return Err(invalid_image());
        }
        let head_key = ImageOwnershipHead::key(head.address)?;
        if let Some(previous) = self.heads.get(&head_key)
            && (previous.address != head.address
                || previous.raw_bytes != head.raw_bytes
                || previous.encoded_bytes != head.encoded_bytes
                || previous.created != head.created)
        {
            return Err(invalid_image());
        }
        let observed = self.heads.try_get_or_insert(head_key, || head)?;
        let edge_key = edge.key()?;
        if let Some(previous) = self.edges.get(&edge_key) {
            if previous.established != established {
                return Err(invalid_image());
            }
            return Ok(());
        }
        if established {
            observed.established_edges = observed
                .established_edges
                .checked_add(1)
                .ok_or_else(invalid_image)?;
        }
        self.edges
            .try_insert(edge_key, EdgeRead { edge, established })?;
        Ok(())
    }

    async fn qualify<A: RetainedOwnershipAttempt>(self, attempt: &mut A) -> StorageResult<()> {
        if self.heads.is_empty() {
            return Ok(());
        }
        let mut keys = Vec::new();
        let count = self
            .heads
            .len()
            .checked_add(self.edges.len())
            .ok_or_else(super::error::capacity_rejected)?;
        keys.try_reserve(count)
            .map_err(|_| super::error::capacity_rejected())?;
        for key in self.heads.keys().chain(self.edges.keys()) {
            keys.push(key.try_into()?);
        }
        let limits = self.read_limits()?.project_read_keys(&keys)?;
        let protection = attempt.read_protection().shared();
        let rows = attempt.load_records(&keys, protection, limits).await?;
        limits.validate_observations(&keys, &rows)?;
        for (key, row) in keys.iter().zip(&rows) {
            if let Some(head) = self.heads.get(key) {
                head.require_observation(row.as_ref(), limits)?;
            }
            if let Some(edge) = self.edges.get(key) {
                edge.require_observation(row.as_ref(), limits)?;
            }
        }
        Ok(())
    }

    fn read_limits(&self) -> StorageResult<InfrastructureLimits> {
        let key = InfrastructureLimits::MAX_KEY_BYTES;
        let record = ImageOwnershipHead::encoded_length_bound(key)?
            .max(ImageOwnerEdge::encoded_length_bound()?);
        let count = self
            .heads
            .len()
            .checked_add(self.edges.len())
            .ok_or_else(invalid_image)?;
        let bytes = key
            .checked_add(record)
            .and_then(|bytes| bytes.checked_mul(count))
            .ok_or_else(invalid_image)?;
        InfrastructureLimits::try_new(
            NonZeroUsize::new(key).ok_or_else(invalid_image)?,
            NonZeroUsize::new(record).ok_or_else(invalid_image)?,
            NonZeroUsize::new(bytes).ok_or_else(invalid_image)?,
            NonZeroUsize::new(count).ok_or_else(invalid_image)?,
        )
    }
}

impl HeadRead {
    fn require_observation(
        &self,
        row: Option<&DurableRecord>,
        limits: InfrastructureLimits,
    ) -> StorageResult<()> {
        if self.created {
            if row.is_some() {
                return Err(invalid_image());
            }
            return Ok(());
        }
        let head = ImageOwnershipHead::decode(
            row.ok_or_else(invalid_image)?,
            self.address,
            limits.max_record_bytes().get(),
        )?;
        if head.raw_bytes != self.raw_bytes
            || head.owners.get() < self.established_edges
            || self
                .encoded_bytes
                .is_some_and(|bytes| head.encoded_bytes != bytes)
        {
            return Err(invalid_image());
        }
        Ok(())
    }
}

impl EdgeRead {
    fn require_observation(
        &self,
        row: Option<&DurableRecord>,
        limits: InfrastructureLimits,
    ) -> StorageResult<()> {
        match (self.established, row) {
            (true, Some(row)) => self
                .edge
                .require_observation(row, limits.max_record_bytes().get()),
            (false, None) => Ok(()),
            _ => Err(invalid_image()),
        }
    }
}
