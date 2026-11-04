//! A current item owns its complete immutable native value, with no body
//! locator.

use crate::{
    IndexerDeclaration, MaxIndexers, StorageError, StorageResult, StoredWireItem, TableHistoryId,
    WireItem,
    constants::{MATERIALIZED_ITEM_FORMAT, MATERIALIZED_ITEM_HEADER_BYTES},
};

pub struct MaterializedItemRecord<'a> {
    payload: &'a [u8],
}

impl<'a> MaterializedItemRecord<'a> {
    /// Escaped image bytes and declaration framing are independent of the
    /// logical DynamoDB page budget. One legal row must always make progress.
    pub const fn encoded_length_bound() -> usize {
        crate::LogicalItemSize::maximum_json_projection_bytes()
            + 9 * crate::MAX_INDEXER_DECLARATION_BYTES
            + 5
            + MATERIALIZED_ITEM_HEADER_BYTES
    }

    pub fn encode(
        history: TableHistoryId,
        item: &WireItem,
        declaration: &IndexerDeclaration,
    ) -> StorageResult<Vec<u8>> {
        let payload =
            serde_json::to_vec(&(declaration.names(), crate::dynamo_json::ItemRef(item)))?;
        let length = u32::try_from(payload.len()).map_err(|_| invalid_record())?;
        let size = MATERIALIZED_ITEM_HEADER_BYTES
            .checked_add(payload.len())
            .ok_or_else(invalid_record)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| crate::InfrastructureResultKind::NativeCapacityRejected)?;
        bytes.push(MATERIALIZED_ITEM_FORMAT);
        bytes.extend_from_slice(history.as_ref());
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    pub fn decode(bytes: &'a [u8], history: TableHistoryId, maximum: usize) -> StorageResult<Self> {
        if bytes.len() > maximum
            || bytes.len() < MATERIALIZED_ITEM_HEADER_BYTES
            || bytes[0] != MATERIALIZED_ITEM_FORMAT
            || &bytes[1..17] != history.as_ref()
        {
            return Err(invalid_record());
        }
        let length =
            u32::from_be_bytes(bytes[17..21].try_into().map_err(|_| invalid_record())?) as usize;
        let payload = &bytes[MATERIALIZED_ITEM_HEADER_BYTES..];
        if payload.len() != length {
            return Err(invalid_record());
        }
        Ok(Self { payload })
    }

    pub fn decoded_bytes(&self) -> usize {
        self.payload.len()
    }

    pub fn into_stored(self, capacity: MaxIndexers) -> StorageResult<StoredWireItem> {
        let (names, image): (Vec<String>, crate::dynamo_json::Item) =
            serde_json::from_slice(self.payload)?;
        let declaration = IndexerDeclaration::try_new(names, capacity)?;
        image.0.logical_size()?;
        declaration.require_values(&image.0)?;
        Ok(StoredWireItem {
            item: image.0,
            indexers: declaration.into_names(),
        })
    }
}

fn invalid_record() -> StorageError {
    StorageError::internal("Invalid materialized current item")
}
