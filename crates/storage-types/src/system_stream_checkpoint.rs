//! Versioned logical scope and checkpoint types for the public SystemStream
//! API.
//!
//! The physical table, partition, and provider cursor are deliberately absent
//! from these types.  Providers translate the logical frontier to their private
//! storage representation after validating the scope and lineage.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use thiserror::Error;
use utoipa::ToSchema;

use crate::constants::{
    CHECKPOINT_TOKEN_PREFIX, SYSTEM_STREAM_CHECKPOINT_MAX_ENCODED_BYTES,
    SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES, SYSTEM_STREAM_CHECKPOINT_MAX_STATE_BYTES,
    SYSTEM_STREAM_CHECKPOINT_TOKEN_ID_BYTES, SYSTEM_STREAM_LINEAGE_ID_MAX_BYTES,
    SYSTEM_STREAM_OFFSET_MAX_BYTES, SYSTEM_STREAM_SCOPE_ID_MAX_BYTES, SYSTEM_STREAM_SCOPE_VERSION,
    SYSTEM_STREAM_SHARD_ID_MAX_BYTES,
};

mod codec;
mod token;

pub use token::SystemStreamCheckpointToken;

/// Errors returned while parsing or validating the logical SystemStream
/// contract.  Error messages intentionally contain no caller-supplied token,
/// key, value, or identifier.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SystemStreamContractError {
    #[error("SystemStream SourceScope is required")]
    MissingSourceScope,
    #[error("SystemStream Checkpoint requires SourceScope")]
    MissingCheckpointScope,
    #[error("LastEvaluatedKey is not supported for SystemStream")]
    LegacyLastEvaluatedKey,
    #[error("SystemStream fields require SystemStream=true")]
    UnexpectedSystemStreamFields,
    #[error("TableName and SystemStream cannot be used together")]
    ConflictingStreamSources,
    #[error("SystemStream response contains mixed legacy and logical fields")]
    MixedSystemStreamFields,
    #[error("SystemStream response contains physical pointer fields")]
    PhysicalSystemStreamFields,
    #[error("table stream response requires its observed frontier")]
    MissingTableFrontier,
    #[error("table stream frontier has invalid retained bounds")]
    InvalidTableFrontier,
    #[error("SystemStream scope version is unsupported")]
    UnsupportedScopeVersion,
    #[error("SystemStream scope id is invalid")]
    InvalidScopeId,
    #[error("SystemStream generation is invalid")]
    InvalidGeneration,
    #[error("SystemStream shard id is invalid")]
    InvalidShardId,
    #[error("SystemStream lineage id is invalid")]
    InvalidLineageId,
    #[error("SystemStream offset is invalid")]
    InvalidOffset,
    #[error("SystemStream checkpoint is malformed")]
    MalformedCheckpoint,
    #[error("SystemStream checkpoint token version is unsupported")]
    UnsupportedCheckpointVersion,
    #[error("SystemStream checkpoint is not canonical")]
    NonCanonicalCheckpoint,
    #[error("SystemStream checkpoint scope does not match the request")]
    WrongScope,
    #[error("SystemStream checkpoint generation does not match the request")]
    WrongGeneration,
    #[error("SystemStream checkpoint frontier is empty")]
    EmptyFrontier,
    #[error("SystemStream checkpoint frontier is too large")]
    FrontierTooLarge,
    #[error("SystemStream checkpoint repeats a shard")]
    DuplicateShard,
    #[error("SystemStream checkpoint names an unknown lineage")]
    UnknownLineage,
    #[error("SystemStream checkpoint token is not issued for this source")]
    UnknownCheckpoint,
    #[error("SystemStream record is missing logical position")]
    MissingRecordPosition,
    #[error("SystemStream record is missing logical source table name")]
    MissingSourceTableName,
    #[error("SystemStream source table name is invalid")]
    InvalidSourceTableName,
    #[error("SystemStream logical lineage is not available from the backend")]
    BackendContract,
    #[error("SystemStream checkpoint exceeds its encoded size bound")]
    CheckpointTooLarge,
    #[error("SystemStream lineage catalog is invalid")]
    InvalidLineageCatalog,
    #[error("SystemStream lineage is duplicated")]
    DuplicateLineage,
    #[error("SystemStream lineage parent or child topology is invalid")]
    InvalidSplitTopology,
    #[error("SystemStream lineage catalog has no matching frontier position")]
    MissingPosition,
    #[error("SystemStream frontier position is duplicated")]
    DuplicatePosition,
    #[error("SystemStream capture frontier is not in canonical order")]
    NonCanonicalFrontier,
    #[error("SystemStream lineage catalog digest is invalid")]
    InvalidLineageCatalogDigest,
    #[error("SystemStream capture digest is invalid")]
    InvalidDigest,
    #[error("SystemStream sealed lineage cutoff is invalid")]
    InvalidSealBoundary,
}

macro_rules! bounded_string_type {
    ($name:ident, $max:ident, $error:ident, $description:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, ToSchema)]
        #[schema(value_type = String, description = $description)]
        #[serde(try_from = "String")]
        pub struct $name(String);

        impl $name {
            /// Validate and create the logical identifier.
            pub fn try_new(value: impl Into<String>) -> Result<Self, SystemStreamContractError> {
                let value = value.into();
                if value.is_empty()
                    || value.trim().is_empty()
                    || value.len() > $max
                    || value.chars().any(char::is_control)
                {
                    return Err(SystemStreamContractError::$error);
                }
                Ok(Self(value))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = SystemStreamContractError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::try_new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where S: serde::Serializer {
                serializer.serialize_str(self.as_str())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

bounded_string_type!(
    SystemStreamScopeId,
    SYSTEM_STREAM_SCOPE_ID_MAX_BYTES,
    InvalidScopeId,
    "Logical SystemStream source scope identifier"
);
bounded_string_type!(
    SystemStreamShardId,
    SYSTEM_STREAM_SHARD_ID_MAX_BYTES,
    InvalidShardId,
    "Logical SystemStream shard identifier"
);
bounded_string_type!(
    SystemStreamLineageId,
    SYSTEM_STREAM_LINEAGE_ID_MAX_BYTES,
    InvalidLineageId,
    "Logical SystemStream shard lineage identifier"
);
bounded_string_type!(
    SystemStreamOffset,
    SYSTEM_STREAM_OFFSET_MAX_BYTES,
    InvalidOffset,
    "Provider-opaque SystemStream record offset"
);

impl SystemStreamOffset {
    pub fn compare_order(
        &self,
        other: &Self,
    ) -> Result<std::cmp::Ordering, SystemStreamContractError> {
        match (self.versionstamp(), other.versionstamp()) {
            (Some(left), Some(right)) => Ok(left.cmp(&right)),
            (None, None) => Ok(self.order()?.cmp(&other.order()?)),
            _ => Err(SystemStreamContractError::InvalidOffset),
        }
    }

    pub fn versionstamp(&self) -> Option<[u8; 12]> {
        let (version, suffix) = self.0.split_once("-vs")?;
        let version = version.parse::<u64>().ok()?;
        let suffix = u32::from_str_radix(suffix, 16).ok()?;
        let mut stamp = [0; 12];
        stamp[..8].copy_from_slice(&version.to_be_bytes());
        stamp[8..].copy_from_slice(&suffix.to_be_bytes());
        Some(stamp)
    }

    pub fn order(&self) -> Result<u64, SystemStreamContractError> {
        self.0
            .split_once('-')
            .map_or(self.0.as_str(), |(prefix, _)| prefix)
            .parse()
            .map_err(|_| SystemStreamContractError::InvalidOffset)
    }
}

/// Logical source authority.  `Version` identifies this wire contract; the
/// generation fences recreation of the same logical source scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct SystemStreamScope {
    version: u8,
    scope_id: SystemStreamScopeId,
    generation: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct SystemStreamScopeWire {
    version: u8,
    scope_id: String,
    generation: u64,
}

impl SystemStreamScope {
    /// Create the current version of a logical source scope.
    pub fn try_new(
        scope_id: impl Into<String>,
        generation: u64,
    ) -> Result<Self, SystemStreamContractError> {
        Self::try_new_versioned(SYSTEM_STREAM_SCOPE_VERSION, scope_id, generation)
    }

    /// Create a scope while explicitly checking the wire version.
    pub fn try_new_versioned(
        version: u8,
        scope_id: impl Into<String>,
        generation: u64,
    ) -> Result<Self, SystemStreamContractError> {
        if version != SYSTEM_STREAM_SCOPE_VERSION {
            return Err(SystemStreamContractError::UnsupportedScopeVersion);
        }
        if generation == 0 {
            return Err(SystemStreamContractError::InvalidGeneration);
        }
        Ok(Self {
            version,
            scope_id: SystemStreamScopeId::try_new(scope_id)?,
            generation,
        })
    }

    #[must_use]
    pub const fn version(&self) -> u8 {
        self.version
    }

    #[must_use]
    pub fn scope_id(&self) -> &SystemStreamScopeId {
        &self.scope_id
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
}

impl<'de> Deserialize<'de> for SystemStreamScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let wire = SystemStreamScopeWire::deserialize(deserializer)?;
        Self::try_new_versioned(wire.version, wire.scope_id, wire.generation)
            .map_err(D::Error::custom)
    }
}

/// Stable logical shard/lineage position carried by each SystemStream record.
/// No physical table, partition, or raw provider key is represented here.
/// Providers retain this tuple across retry, redelivery, reclaim, and split
/// continuation.  `Offset` is ordered only within its `(ShardId, LineageId)`;
/// positions from different shards have no total ordering relationship.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct SystemStreamPosition {
    shard_id: SystemStreamShardId,
    lineage_id: SystemStreamLineageId,
    offset: SystemStreamOffset,
}

impl SystemStreamPosition {
    pub fn next_versionstamp(&self) -> Result<Self, SystemStreamContractError> {
        let mut stamp = self
            .offset()
            .versionstamp()
            .ok_or(SystemStreamContractError::InvalidOffset)?;
        for byte in stamp.iter_mut().rev() {
            let (next, carry) = byte.overflowing_add(1);
            *byte = next;
            if !carry {
                return self.with_versionstamp(stamp);
            }
        }
        Err(SystemStreamContractError::InvalidOffset)
    }

    pub fn with_versionstamp(&self, stamp: [u8; 12]) -> Result<Self, SystemStreamContractError> {
        let version = u64::from_be_bytes([
            stamp[0], stamp[1], stamp[2], stamp[3], stamp[4], stamp[5], stamp[6], stamp[7],
        ]);
        let suffix = u32::from_be_bytes([stamp[8], stamp[9], stamp[10], stamp[11]]);
        Self::try_new(
            self.shard_id().as_str(),
            self.lineage_id().as_str(),
            format!("{version:020}-vs{suffix:08x}"),
        )
    }

    /// Validate and create a logical record position.
    pub fn try_new(
        shard_id: impl Into<String>,
        lineage_id: impl Into<String>,
        offset: impl Into<String>,
    ) -> Result<Self, SystemStreamContractError> {
        Ok(Self {
            shard_id: SystemStreamShardId::try_new(shard_id)?,
            lineage_id: SystemStreamLineageId::try_new(lineage_id)?,
            offset: SystemStreamOffset::try_new(offset)?,
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
    pub fn offset(&self) -> &SystemStreamOffset {
        &self.offset
    }
}

/// A validated logical checkpoint frontier.  Entries are canonically ordered
/// by shard and lineage and contain exactly one entry for each shard.  The
/// frontier is a set of independent per-shard positions, not a global cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamCheckpoint {
    scope: SystemStreamScope,
    frontier: Vec<SystemStreamPosition>,
}

impl SystemStreamCheckpoint {
    /// Validate a logical checkpoint frontier and canonicalize its order.
    pub fn try_new(
        scope: SystemStreamScope,
        mut frontier: Vec<SystemStreamPosition>,
    ) -> Result<Self, SystemStreamContractError> {
        if frontier.is_empty() {
            return Err(SystemStreamContractError::EmptyFrontier);
        }
        if frontier.len() > SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
            return Err(SystemStreamContractError::FrontierTooLarge);
        }
        frontier.sort_unstable();
        if frontier
            .windows(2)
            .any(|positions| positions[0].shard_id == positions[1].shard_id)
        {
            return Err(SystemStreamContractError::DuplicateShard);
        }
        Ok(Self { scope, frontier })
    }

    #[must_use]
    pub fn scope(&self) -> &SystemStreamScope {
        &self.scope
    }

    #[must_use]
    pub fn frontier(&self) -> &[SystemStreamPosition] {
        &self.frontier
    }

    /// Check that a checkpoint can be resumed against the requested logical
    /// source authority.
    pub fn validate_scope(
        &self,
        expected: &SystemStreamScope,
    ) -> Result<(), SystemStreamContractError> {
        if self.scope.version != expected.version || self.scope.scope_id != expected.scope_id {
            return Err(SystemStreamContractError::WrongScope);
        }
        if self.scope.generation != expected.generation {
            return Err(SystemStreamContractError::WrongGeneration);
        }
        Ok(())
    }

    /// Check every `(shard, lineage)` pair against the provider's canonical
    /// lineage catalog.  Lineage-id membership alone is insufficient: a
    /// lineage from another shard must not authorize this frontier.
    pub fn validate_frontier_catalog<'a, I>(
        &self,
        catalog: I,
    ) -> Result<(), SystemStreamContractError>
    where
        I: IntoIterator<Item = &'a SystemStreamPosition>,
    {
        let catalog: Vec<_> = catalog.into_iter().collect();
        let all_known = if self.frontier.len() < 8 || catalog.len() < 32 {
            self.frontier.iter().all(|position| {
                catalog.iter().any(|candidate| {
                    candidate.shard_id == position.shard_id
                        && candidate.lineage_id == position.lineage_id
                })
            })
        } else {
            let mut catalog = catalog;
            catalog.sort_unstable_by(|left, right| {
                left.shard_id
                    .cmp(&right.shard_id)
                    .then_with(|| left.lineage_id.cmp(&right.lineage_id))
            });
            self.frontier.iter().all(|position| {
                catalog
                    .binary_search_by(|candidate| {
                        candidate
                            .shard_id
                            .cmp(&position.shard_id)
                            .then_with(|| candidate.lineage_id.cmp(&position.lineage_id))
                    })
                    .is_ok()
            })
        };
        if !all_known {
            return Err(SystemStreamContractError::UnknownLineage);
        }
        Ok(())
    }

    /// Encode the durable checkpoint state in one deterministic, backend-
    /// neutral format.  This state is stored under the opaque token by a
    /// backend; it is never sent to callers as the `Checkpoint` field.
    pub fn encode_state(&self) -> Result<Vec<u8>, SystemStreamContractError> {
        let mut state = Vec::new();
        state.push(SYSTEM_STREAM_SCOPE_VERSION);
        codec::put_string(&mut state, self.scope.scope_id.as_str())?;
        state.extend(self.scope.generation.to_be_bytes());
        let frontier_len = u16::try_from(self.frontier.len())
            .map_err(|_| SystemStreamContractError::FrontierTooLarge)?;
        state.extend(frontier_len.to_be_bytes());
        for position in &self.frontier {
            codec::put_string(&mut state, position.shard_id.as_str())?;
            codec::put_string(&mut state, position.lineage_id.as_str())?;
            codec::put_string(&mut state, position.offset.as_str())?;
        }
        if state.len() > SYSTEM_STREAM_CHECKPOINT_MAX_STATE_BYTES {
            return Err(SystemStreamContractError::CheckpointTooLarge);
        }
        Ok(state)
    }

    /// Decode and validate the deterministic backend state format.
    pub fn decode_state(state: &[u8]) -> Result<Self, SystemStreamContractError> {
        if state.is_empty() {
            return Err(SystemStreamContractError::MalformedCheckpoint);
        }
        if state.len() > SYSTEM_STREAM_CHECKPOINT_MAX_STATE_BYTES {
            return Err(SystemStreamContractError::CheckpointTooLarge);
        }
        let mut cursor = 0;
        let version = codec::take_u8(state, &mut cursor)?;
        if version != SYSTEM_STREAM_SCOPE_VERSION {
            return Err(SystemStreamContractError::UnsupportedScopeVersion);
        }
        let scope_id = codec::take_string(state, &mut cursor, SYSTEM_STREAM_SCOPE_ID_MAX_BYTES)
            .map_err(|_| SystemStreamContractError::InvalidScopeId)?;
        let generation = codec::take_u64(state, &mut cursor)?;
        let scope = SystemStreamScope::try_new_versioned(version, scope_id, generation)?;
        let frontier_len = usize::from(codec::take_u16(state, &mut cursor)?);
        if frontier_len == 0 {
            return Err(SystemStreamContractError::EmptyFrontier);
        }
        if frontier_len > SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
            return Err(SystemStreamContractError::FrontierTooLarge);
        }
        let mut frontier = Vec::with_capacity(frontier_len);
        for _ in 0..frontier_len {
            let shard_id = codec::take_string(state, &mut cursor, SYSTEM_STREAM_SHARD_ID_MAX_BYTES)
                .map_err(|_| SystemStreamContractError::InvalidShardId)?;
            let lineage_id =
                codec::take_string(state, &mut cursor, SYSTEM_STREAM_LINEAGE_ID_MAX_BYTES)
                    .map_err(|_| SystemStreamContractError::InvalidLineageId)?;
            let offset = codec::take_string(state, &mut cursor, SYSTEM_STREAM_OFFSET_MAX_BYTES)
                .map_err(|_| SystemStreamContractError::InvalidOffset)?;
            frontier.push(SystemStreamPosition::try_new(shard_id, lineage_id, offset)?);
        }
        if cursor != state.len() {
            return Err(SystemStreamContractError::MalformedCheckpoint);
        }
        let canonical_frontier = frontier
            .windows(2)
            .all(|positions| positions[0] <= positions[1]);
        let checkpoint = Self::try_new(scope, frontier)?;
        if !canonical_frontier {
            return Err(SystemStreamContractError::NonCanonicalCheckpoint);
        }
        Ok(checkpoint)
    }
}

impl serde::Serialize for SystemStreamCheckpoint {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes = self.encode_state().map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> serde::Deserialize<'de> for SystemStreamCheckpoint {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8> as serde::Deserialize>::deserialize(deserializer)?;
        Self::decode_state(&bytes).map_err(serde::de::Error::custom)
    }
}
