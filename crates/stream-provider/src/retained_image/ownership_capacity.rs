//! Logical graph I/O bounds do not certify native transaction capacity.

use std::num::NonZeroUsize;

use infrastructure_provider::limits::InfrastructureLimits;
use storage_types::StorageResult;

use super::{
    error::invalid_image,
    ownership_batch::OwnershipNetPlan,
    ownership_graph::{ImageOwnerEdge, ImageOwnershipHead},
    payload::RetainedPayload,
};

impl OwnershipNetPlan {
    /// Covers the declared bodies, graph records and protected refund
    /// authority. Providers still enforce their real native attempt and
    /// chunking constraints.
    pub(super) fn work_limits(&self) -> StorageResult<InfrastructureLimits> {
        let key = InfrastructureLimits::MAX_KEY_BYTES;
        let mut record = ImageOwnershipHead::encoded_length_bound(key)?
            .max(ImageOwnerEdge::encoded_length_bound()?);
        let mut count = 0usize;
        for body in self.bodies.values() {
            record = record.max(RetainedPayload::encoded_length_bound(body.raw_bytes)?);
            // Body, head, charge receipt and account plus every touched edge.
            count = count
                .checked_add(body.owners.len())
                .and_then(|count| count.checked_add(4))
                .ok_or_else(invalid_image)?;
        }
        // Bound live observation waves independently of the complete net
        // inventory. This is the existing receipt/graph read quantum, not a
        // native allowance.
        let count = count.min(crate::constants::IMAGE_OWNER_READ_RECORDS);
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

/// Additional logical graph bytes for one original's owner references.
/// Native key/chunk/conflict lowering and other operation effects remain
/// separate.
pub fn ownership_graph_length_bound(
    bodies: usize,
    owners: usize,
    limits: InfrastructureLimits,
) -> StorageResult<usize> {
    let key_bytes = limits.max_key_bytes().get();
    let head = ImageOwnershipHead::encoded_length_bound(key_bytes)?;
    let edge = ImageOwnerEdge::encoded_length_bound()?;
    if key_bytes < ImageOwnerEdge::key_length_bound()?
        || head.max(edge) > limits.max_record_bytes().get()
    {
        return Err(invalid_image());
    }
    let head = head.checked_add(key_bytes).ok_or_else(invalid_image)?;
    let edge = edge.checked_add(key_bytes).ok_or_else(invalid_image)?;
    let edges = bodies
        .checked_mul(owners)
        .and_then(|count| count.checked_mul(edge))
        .ok_or_else(invalid_image)?;
    bodies
        .checked_mul(head)
        .and_then(|bytes| bytes.checked_add(edges))
        .ok_or_else(invalid_image)
}
