//! Provider-neutral evidence for one write attempt.
//!
//! The native layer owns row and tombstone encoding. This module owns the
//! identity and coverage rules that let the shared finish stage reject a
//! missing, duplicated, swapped, or wrong-role observation before a write is
//! lowered.

use storage_types::{
    ItemProvenance, ItemRevision, KeyAttributes, StorageEnum, StorageError, StorageResult,
    TableName, WireItem, context::WrappedError as _,
};

use crate::canonical_revision_key;

/// Whether evidence belongs to a write member or a condition-check member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteObservationRole {
    Write,
    Check,
}

/// Stable identity carried by a write observation.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteObservationIdentity {
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub ordinal: usize,
    pub role: WriteObservationRole,
}

impl WriteObservationIdentity {
    #[must_use]
    pub fn new(
        table_name: TableName,
        key: KeyAttributes,
        ordinal: usize,
        role: WriteObservationRole,
    ) -> Self {
        Self {
            table_name,
            key,
            ordinal,
            role,
        }
    }

    fn matches(&self, other: &Self) -> StorageResult<bool> {
        Ok(self.table_name == other.table_name
            && self.ordinal == other.ordinal
            && self.role == other.role
            && canonical_revision_key(&self.key)? == canonical_revision_key(&other.key)?)
    }
}

/// The three evidence shapes consumed by shared finishing. Durable revision
/// and provenance travel together; every prepared member demands an actual
/// observation, even when its live image is unnecessary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteObservationRequirements {
    OldItem,
    DurableMetadata,
    DurableOldItem,
}

impl WriteObservationRequirements {
    #[must_use]
    pub const fn durable(old_item: bool) -> Self {
        if old_item {
            Self::DurableOldItem
        } else {
            Self::DurableMetadata
        }
    }

    #[must_use]
    pub const fn requests_old_item(self) -> bool {
        matches!(self, Self::OldItem | Self::DurableOldItem)
    }

    #[must_use]
    pub const fn requests_durable_metadata(self) -> bool {
        matches!(self, Self::DurableMetadata | Self::DurableOldItem)
    }
}

/// One member's identity plus its native evidence requirements.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteObservationRequirement {
    pub identity: WriteObservationIdentity,
    pub fields: WriteObservationRequirements,
}

impl WriteObservationRequirement {
    #[must_use]
    pub fn new(identity: WriteObservationIdentity, fields: WriteObservationRequirements) -> Self {
        Self { identity, fields }
    }
}

/// Truthful evidence returned by a native attempt. A deleted state carries
/// history but never fabricates a live item image.
#[derive(Clone, Debug)]
pub enum WriteObservationState {
    Absent,
    Live {
        item: Box<WireItem>,
        revision: ItemRevision,
        provenance: ObservedProvenance,
    },
    /// A condition check needs the live image but does not continue history.
    LiveImage {
        item: Box<WireItem>,
    },
    /// A live row whose image was not requested by shared preparation. The
    /// native owner still returns durable metadata needed for revision and
    /// provenance decisions without publishing an unnecessary old image.
    LiveMetadata {
        revision: ItemRevision,
        provenance: ObservedProvenance,
    },
    Deleted {
        revision: ItemRevision,
        provenance: ObservedProvenance,
    },
}

/// SQL has canonical stored bytes; KV has an already decoded revision record.
/// Delay SQL decoding until conditions finish, and avoid a KV round trip.
#[derive(Clone, Debug)]
pub enum ObservedProvenance {
    Encoded(Vec<u8>),
    Decoded(ItemProvenance),
}

impl From<Vec<u8>> for ObservedProvenance {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Encoded(bytes)
    }
}

impl From<ItemProvenance> for ObservedProvenance {
    fn from(provenance: ItemProvenance) -> Self {
        Self::Decoded(provenance)
    }
}

impl ObservedProvenance {
    fn decode(self) -> StorageResult<ItemProvenance> {
        match self {
            Self::Encoded(bytes) => ItemProvenance::decode(&bytes),
            Self::Decoded(provenance) => Ok(provenance),
        }
    }
}

/// Identity-bound native evidence for one write or check member.
#[derive(Clone, Debug)]
pub struct WriteObservation {
    pub identity: WriteObservationIdentity,
    pub state: WriteObservationState,
}

impl WriteObservation {
    #[must_use]
    pub fn new(identity: WriteObservationIdentity, state: WriteObservationState) -> Self {
        Self { identity, state }
    }

    fn validate_and_complete(
        self,
        requirement: &WriteObservationRequirement,
    ) -> StorageResult<Self> {
        if !self.identity.matches(&requirement.identity)? {
            return Err(StorageError::internal(
                "write observation identity does not match its requirement",
            ));
        }
        let Self { identity, state } = self;
        let with_keys = |item: Box<WireItem>| {
            item.with_verified_keys(&identity.key)
                .map(Box::new)
                .map_err(|error| {
                    if matches!(
                        error.to_enum(),
                        StorageEnum::InternalServerError { message }
                            if message.starts_with("Key attribute mismatch for ")
                    ) {
                        StorageError::internal(
                            "write observation item key does not match its member identity",
                        )
                    } else {
                        error
                    }
                })
        };
        let state = match state {
            WriteObservationState::Live {
                item,
                revision,
                provenance,
            } => WriteObservationState::Live {
                item: with_keys(item)?,
                revision,
                provenance,
            },
            WriteObservationState::LiveImage { item } => WriteObservationState::LiveImage {
                item: with_keys(item)?,
            },
            state => state,
        };
        let observation = Self { identity, state };
        if requirement.fields.requests_old_item()
            && matches!(
                observation.state,
                WriteObservationState::LiveMetadata { .. }
            )
        {
            return Err(StorageError::internal(
                "write observation is missing its requested old item",
            ));
        }
        if (requirement.fields.requests_durable_metadata()
            || requirement.identity.role == WriteObservationRole::Write)
            && matches!(observation.state, WriteObservationState::LiveImage { .. })
        {
            return Err(StorageError::internal(
                "live write observation cannot omit durable history",
            ));
        }
        Ok(observation)
    }
}

/// Evidence after the shared identity and coverage gate has accepted it.
///
/// The inner observations are deliberately private. Native adapters can
/// construct raw observations, but only the shared validator can create this
/// value, so a positional old-image vector cannot bypass identity validation
/// on the production finish path.
#[derive(Clone, Debug)]
pub struct ValidatedWriteObservations {
    /// Accepted evidence in the prepared member's ordinal order.  Ordinals
    /// are request identities, not array indexes: a standalone physical
    /// write may carry an ordinal that is not zero, while a transaction uses
    /// the original request positions. Keeping the bounded collection
    /// ordered lets finish consume either shape without rebuilding a map.
    observations: Vec<WriteObservation>,
}

/// Durable history accepted for one write member in the committing attempt.
/// An absent key has neither a revision nor provenance; a deleted key keeps
/// both, without inventing an old item image.
#[derive(Clone, Debug)]
pub struct ObservedWriteHistory {
    pub ordinal: usize,
    pub revision: Option<ItemRevision>,
    pub provenance: Option<ItemProvenance>,
}

pub(crate) type EncodedWriteHistory = (usize, Option<ItemRevision>, Option<ObservedProvenance>);

pub(crate) fn decode_write_histories(
    encoded: Vec<EncodedWriteHistory>,
) -> StorageResult<Vec<ObservedWriteHistory>> {
    encoded
        .into_iter()
        .map(|(ordinal, revision, provenance)| {
            Ok(ObservedWriteHistory {
                ordinal,
                revision,
                provenance: provenance.map(ObservedProvenance::decode).transpose()?,
            })
        })
        .collect()
}

impl ObservedWriteHistory {
    /// Continue durable history from the protected observation. A physical
    /// stream position may raise the floor, but cannot reset a tombstone.
    pub fn next_revision_at_least(&self, floor: u64) -> StorageResult<ItemRevision> {
        ItemRevision::next_at_least(self.revision, floor)
    }
}

impl ValidatedWriteObservations {
    /// Return the identity-bound evidence after the shared gate has accepted
    /// it. This is used by a native planner that must pass the evidence to the
    /// next shared owner without copying its wire payloads.
    pub fn into_observations(self) -> Vec<WriteObservation> {
        self.observations
    }

    /// Consume the validated evidence in bounded source-ordinal slots. Native
    /// plans are ordered by request ordinal, whereas the prepared batch stores
    /// members grouped by verb; slots prevent those representations from
    /// swapping old images when verbs are mixed.
    pub(crate) fn into_finish_inputs(self) -> ObservedWriteFinish {
        let mut wires = Vec::with_capacity(self.observations.len());
        let mut histories = Vec::with_capacity(self.observations.len());
        for observation in self.observations {
            let ordinal = observation.identity.ordinal;
            let (wire, revision, provenance) = match observation.state {
                WriteObservationState::Live {
                    item,
                    revision,
                    provenance,
                } => (Some(*item), Some(revision), Some(provenance)),
                WriteObservationState::LiveImage { item } => (Some(*item), None, None),
                WriteObservationState::LiveMetadata {
                    revision,
                    provenance,
                }
                | WriteObservationState::Deleted {
                    revision,
                    provenance,
                } => (None, Some(revision), Some(provenance)),
                WriteObservationState::Absent => (None, None, None),
            };
            wires.push((ordinal, Some(wire)));
            if observation.identity.role == WriteObservationRole::Write {
                histories.push((ordinal, revision, provenance));
            }
        }
        ObservedWriteFinish { wires, histories }
    }
}

pub(crate) struct ObservedWriteFinish {
    pub wires: Vec<(usize, Option<Option<WireItem>>)>,
    pub histories: Vec<EncodedWriteHistory>,
}

/// Validate one complete observation set before shared finish consumes it.
/// The native result must retain request order. This is intentionally linear
/// in the member count; write batches are small and the identity checks
/// preserve the original member ordinal.
pub fn validate_observation_coverage(
    requirements: &[WriteObservationRequirement],
    observations: impl IntoIterator<Item = WriteObservation>,
) -> StorageResult<ValidatedWriteObservations> {
    let mut ordered_requirements = requirements.iter().collect::<Vec<_>>();
    ordered_requirements.sort_unstable_by_key(|requirement| requirement.identity.ordinal);
    if ordered_requirements
        .windows(2)
        .any(|requirements| requirements[0].identity.ordinal == requirements[1].identity.ordinal)
    {
        return Err(StorageError::internal(
            "write observation requirements contain a duplicate ordinal",
        ));
    }

    // Keep one bounded slot per prepared member. The slot index is an
    // implementation detail; the identity carried by each accepted
    // observation remains the source ordinal used by finish.
    let mut slots = (0..ordered_requirements.len())
        .map(|_| None)
        .collect::<Vec<Option<WriteObservation>>>();
    for observation in observations {
        let ordinal = observation.identity.ordinal;
        let slot = ordered_requirements
            .binary_search_by_key(&ordinal, |requirement| requirement.identity.ordinal)
            .map_err(|_| {
                StorageError::internal("write observation contains an unexpected member")
            })?;
        let requirement = ordered_requirements[slot];
        if slots[slot].is_some() {
            return Err(StorageError::internal(
                "write observations contain a duplicate member",
            ));
        }
        slots[slot] = Some(observation.validate_and_complete(requirement)?);
    }
    let observations = slots
        .into_iter()
        .map(|observation| {
            observation.ok_or_else(|| {
                StorageError::internal("write observation count does not match its requirements")
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(ValidatedWriteObservations { observations })
}
