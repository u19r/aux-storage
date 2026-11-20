//! Self-contained Dynamo stream images. Native keys own table incarnation and
//! lineage position; entries never refer to item history or predecessors.

use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize};
use storage_types::{
    BoundTableInfo, IndexerDeclaration, KeyAttributes, MaxIndexers, RegionalMark, StorageError,
    StorageResult, StoredWireItem, StreamUserIdentity, StreamViewType, TimestampMillis,
    VerifiedWireItem, WireItem, storage_serde,
};

use crate::DynamoMutationRoute;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamChange {
    Insert,
    Modify,
    Remove,
}

/// Images are complete at acceptance. MODIFY remains explicit even when the
/// selected view omits the old image, including the first write after enable.
#[derive(Clone, Serialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
#[serde(transparent)]
pub struct StreamEntry(StreamEntryValue);

#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
struct StreamEntryValue {
    change: StreamChange,
    keys: KeyAttributes,
    new_image: Option<StoredWireItem>,
    old_image: Option<StoredWireItem>,
    created_at: TimestampMillis,
    user_identity: Option<StreamUserIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    regional: Option<RegionalMark>,
}

/// Accepted primary observations and declarations from one bound native
/// attempt. The new image declaration is validated by the request owner; the
/// old image and its declaration come from the protected, decoded primary
/// observation.
#[derive(Serialize)]
pub struct StreamEntryInput<'a> {
    change: StreamChange,
    keys: &'a KeyAttributes,
    new_image: Option<StreamImageRef<'a>>,
    old_image: Option<StreamImageRef<'a>>,
    created_at: TimestampMillis,
    user_identity: Option<StreamUserIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    regional: Option<RegionalMark>,
}

#[derive(Serialize)]
struct StreamImageRef<'a> {
    item: storage_types::dynamo_json::ItemRef<'a>,
    indexers: &'a [String],
}

impl<'a> StreamEntryInput<'a> {
    pub fn try_new(
        table: &'a BoundTableInfo,
        route: &'a DynamoMutationRoute,
        new_image: Option<(&'a VerifiedWireItem<Arc<WireItem>>, &'a IndexerDeclaration)>,
        old_image: Option<(&'a VerifiedWireItem, &'a [String])>,
        created_at: TimestampMillis,
        user_identity: Option<StreamUserIdentity>,
    ) -> StorageResult<Self> {
        if route.source_table_name.as_str() != table.table_name.as_ref() {
            return Err(StorageError::internal(
                "stream mutation does not match its bound table",
            ));
        }
        let view = &table
            .binding
            .stream
            .as_ref()
            .ok_or_else(|| StorageError::internal("Stream entry has no bound stream"))?
            .view;
        let change = match (new_image.is_some(), old_image.is_some()) {
            (true, false) => StreamChange::Insert,
            (true, true) => StreamChange::Modify,
            (false, true) => StreamChange::Remove,
            (false, false) => return Err(invalid_entry()),
        };
        let new_image = new_image.map(|(item, declaration)| StreamImageRef {
            item: storage_types::dynamo_json::ItemRef(item),
            indexers: declaration.names(),
        });
        let old_image = match view {
            StreamViewType::OldImage | StreamViewType::NewAndOldImages => {
                old_image.map(|(item, indexers)| StreamImageRef {
                    item: storage_types::dynamo_json::ItemRef(item),
                    indexers,
                })
            }
            StreamViewType::KeysOnly | StreamViewType::NewImage => None,
        };
        Ok(Self {
            change,
            keys: &route.key,
            new_image,
            old_image,
            created_at,
            user_identity,
            regional: None,
        })
    }

    /// Mark a paired-table entry with its order and role.
    #[must_use]
    pub fn with_regional(mut self, regional: Option<RegionalMark>) -> Self {
        self.regional = regional;
        self
    }

    /// A paired entry with no public change: an absent-key origin delete or an
    /// echo that removed no item. Public readers skip it; regional delivery
    /// sends it.
    pub fn hidden(
        table: &BoundTableInfo,
        route: &'a DynamoMutationRoute,
        created_at: TimestampMillis,
        mut mark: RegionalMark,
    ) -> StorageResult<Self> {
        if route.source_table_name.as_str() != table.table_name.as_ref() {
            return Err(StorageError::internal(
                "stream mutation does not match its bound table",
            ));
        }
        mark.hidden = true;
        Ok(Self {
            change: StreamChange::Remove,
            keys: &route.key,
            new_image: None,
            old_image: None,
            created_at,
            user_identity: None,
            regional: Some(mark),
        })
    }

    pub fn created_at(&self) -> TimestampMillis {
        self.created_at
    }

    pub fn encode(&self) -> StorageResult<Vec<u8>> {
        storage_serde::to_bytes(self)
    }
}

impl StreamEntry {
    pub fn encode(&self) -> StorageResult<Vec<u8>> {
        storage_serde::to_bytes(&self.0)
    }

    /// The physical reader admits native bytes before decoding. Stored images
    /// are untrusted again, so validate their keys and persisted declarations.
    pub fn decode(bytes: &[u8]) -> StorageResult<Self> {
        Self::validate_value(storage_serde::from_bytes(bytes)?)
    }

    fn validate_value(mut value: StreamEntryValue) -> StorageResult<Self> {
        if value.keys.is_empty()
            || matches!(value.change, StreamChange::Insert | StreamChange::Modify)
                != value.new_image.is_some()
            || value.change == StreamChange::Insert && value.old_image.is_some()
            || value.regional.is_some_and(|mark| {
                mark.hidden
                    && (value.change != StreamChange::Remove
                        || value.new_image.is_some()
                        || value.old_image.is_some())
            })
        {
            return Err(invalid_entry());
        }
        let capacity = MaxIndexers::try_new(storage_types::MAX_INDEXERS_CAPACITY)?;
        for image in value.new_image.iter_mut().chain(&mut value.old_image) {
            if !image.item.has_verified_keys(&value.keys)? {
                return Err(invalid_entry());
            }
            let declaration =
                IndexerDeclaration::try_new(std::mem::take(&mut image.indexers), capacity)?;
            declaration.require_values(&image.item)?;
            image.indexers = declaration.into_names();
        }
        Ok(Self(value))
    }

    pub fn change(&self) -> StreamChange {
        self.0.change
    }

    pub fn keys(&self) -> &KeyAttributes {
        &self.0.keys
    }

    pub fn new_image(&self) -> Option<&StoredWireItem> {
        self.0.new_image.as_ref()
    }

    pub fn old_image(&self) -> Option<&StoredWireItem> {
        self.0.old_image.as_ref()
    }

    pub fn created_at(&self) -> TimestampMillis {
        self.0.created_at
    }

    pub fn user_identity(&self) -> Option<&StreamUserIdentity> {
        self.0.user_identity.as_ref()
    }

    pub fn regional(&self) -> Option<RegionalMark> {
        self.0.regional
    }

    /// Public stream readers return this entry.
    pub fn is_public(&self) -> bool {
        !self.0.regional.is_some_and(|mark| mark.hidden)
    }
}

fn invalid_entry() -> StorageError {
    StorageError::internal("Invalid canonical stream entry")
}

impl<'de> Deserialize<'de> for StreamEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::validate_value(StreamEntryValue::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}
