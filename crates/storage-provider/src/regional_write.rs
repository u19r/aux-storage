//! Paired local write decision (design B, R1, P2, P6): admission, order, marker
//! effect.

use storage_types::{
    ItemProvenance, PairState, RegionId, RegionalBinding, RegionalMarker, RegionalOrder,
    StorageError, StorageResult, TableName, TimestampMillis,
};

/// The order a local upsert stores, and whether it removes the key's marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionalUpsert {
    pub order: RegionalOrder,
    pub clear_marker: bool,
}

/// The marker a local delete stores (replacing any marker of the key).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionalDelete {
    pub marker: RegionalMarker,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DeleteCause {
    #[default]
    Customer,
    Expiration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteAdmission {
    Unpaired,
    Paired(RegionId),
}

pub(crate) fn admit(
    binding: Option<&RegionalBinding>,
    table: &TableName,
    cause: DeleteCause,
) -> StorageResult<WriteAdmission> {
    let Some(binding) = binding else {
        return Ok(WriteAdmission::Unpaired);
    };
    if binding.admits_local_writes() {
        return Ok(WriteAdmission::Paired(binding.local));
    }
    match (cause, binding.state) {
        (DeleteCause::Expiration, _) => Err(StorageError::internal(
            "a TTL delete reached a table that admits no local writes",
        )),
        (DeleteCause::Customer, PairState::Joining) => Err(StorageError::table_not_found(table)),
        (DeleteCause::Customer, _) => Err(StorageError::raw_validation(
            "Table replica is being removed",
        )),
    }
}

/// Current winner of the key: the live item, else its delete marker.
fn winner(
    local: RegionId,
    old_provenance: Option<&ItemProvenance>,
    old_marker: Option<RegionalMarker>,
) -> Option<RegionalOrder> {
    match old_provenance {
        Some(provenance) => Some(
            provenance
                .order()
                .unwrap_or_else(|| RegionalOrder::baseline(local)),
        ),
        None => old_marker.map(|marker| marker.order),
    }
}

pub(crate) fn local_upsert(
    local: RegionId,
    old_provenance: Option<&ItemProvenance>,
    old_marker: Option<RegionalMarker>,
    now: TimestampMillis,
) -> StorageResult<RegionalUpsert> {
    Ok(RegionalUpsert {
        order: RegionalOrder::next_local(now, winner(local, old_provenance, old_marker), local)?,
        clear_marker: old_marker.is_some(),
    })
}

/// None: a TTL delete of an absent item (no regional effect).
pub(crate) fn local_delete(
    local: RegionId,
    cause: DeleteCause,
    old_provenance: Option<&ItemProvenance>,
    old_marker: Option<RegionalMarker>,
    now: TimestampMillis,
) -> StorageResult<Option<RegionalDelete>> {
    let order = match cause {
        DeleteCause::Customer => {
            RegionalOrder::next_local(now, winner(local, old_provenance, old_marker), local)?
        }
        DeleteCause::Expiration => match old_provenance {
            None => return Ok(None),
            Some(_) => winner(local, old_provenance, None)
                .ok_or_else(|| StorageError::internal("expired item has no regional order"))?
                .expiry_child()?,
        },
    };
    Ok(Some(RegionalDelete {
        marker: RegionalMarker {
            order,
            received: false,
        },
    }))
}
