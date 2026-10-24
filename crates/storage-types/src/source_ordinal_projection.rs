//! Frozen physical selections bind to the exact retained source field order.
use serde::{Deserialize, Serialize};

use crate::{FrozenImageProjection, StorageError, StorageResult, WireItem};

#[derive(Clone, Serialize, Deserialize)]
pub enum SourceOrdinalProjection {
    All,
    Fields(SourceProjectionOrdinals),
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "Vec<u32>", into = "Vec<u32>")]
pub struct SourceProjectionOrdinals(Vec<u32>);

impl TryFrom<Vec<u32>> for SourceProjectionOrdinals {
    type Error = StorageError;
    fn try_from(ordinals: Vec<u32>) -> StorageResult<Self> {
        if ordinals.len() > crate::constants::MAX_FROZEN_PROJECTION_FIELDS
            || ordinals.iter().enumerate().any(|(position, ordinal)| {
                u64::from(*ordinal) >= crate::MAX_ITEM_SIZE_BYTES as u64
                    || ordinals[..position].contains(ordinal)
            })
        {
            return Err(StorageError::internal("Invalid source projection ordinals"));
        }
        Ok(Self(ordinals))
    }
}
impl From<SourceProjectionOrdinals> for Vec<u32> {
    fn from(ordinals: SourceProjectionOrdinals) -> Self {
        ordinals.0
    }
}
impl AsRef<[u32]> for SourceProjectionOrdinals {
    fn as_ref(&self) -> &[u32] {
        &self.0
    }
}

impl FrozenImageProjection {
    pub fn bind_ordinals(&self, source: &WireItem) -> StorageResult<SourceOrdinalProjection> {
        match self {
            Self::All => Ok(SourceOrdinalProjection::All),
            Self::Fields(fields) => {
                let bytes = source.dynamo_json_bytes()?;
                let ordinals = crate::wire_item::ordinal_projection::bind_source_fields(
                    &bytes,
                    fields.as_ref(),
                )?;
                Ok(SourceOrdinalProjection::Fields(
                    SourceProjectionOrdinals::try_from(ordinals)?,
                ))
            }
        }
    }
}

impl SourceOrdinalProjection {
    /// Thirteen framing bytes, bounded digits and commas between ordinals.
    pub fn encoded_length_bound() -> usize {
        crate::constants::MAX_SOURCE_ORDINAL_PROJECTION_BYTES
    }

    pub fn project(&self, source: &WireItem) -> StorageResult<WireItem> {
        match self {
            Self::All => Ok(source.clone()),
            Self::Fields(ordinals) => {
                let bytes = source.dynamo_json_bytes()?;
                crate::wire_item::ordinal_projection::project_source_fields(
                    &bytes,
                    ordinals.as_ref(),
                )
                .map(WireItem::dynamo_json)
            }
        }
    }
}
