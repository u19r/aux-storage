//! Native primary authority header and retained descriptor encoding.

use storage_provider::ItemPrimaryEffects;
use storage_types::{ItemProvenance, ItemRevision, StorageError, StorageResult};

use crate::constants::{PRIMARY_ITEM_FORMAT, PRIMARY_ITEM_HEADER_BYTES};

/// A live item's revision authority travels with its image. A separate
/// tombstone key stores authority only after deletion.
pub(crate) struct PrimaryItemEnvelope<'a> {
    pub revision: ItemRevision,
    pub ttl_epoch: Option<i64>,
    pub provenance: &'a [u8],
    pub image: &'a [u8],
}

impl<'a> PrimaryItemEnvelope<'a> {
    pub fn decode(bytes: &'a [u8]) -> StorageResult<Self> {
        let header = bytes
            .get(..PRIMARY_ITEM_HEADER_BYTES)
            .ok_or_else(|| StorageError::internal("KV primary item header is truncated"))?;
        if header[0] != PRIMARY_ITEM_FORMAT {
            return Err(StorageError::internal("KV primary item format is invalid"));
        }
        let revision_bytes: [u8; 8] = header[1..9]
            .try_into()
            .map_err(|_| StorageError::internal("KV primary revision is invalid"))?;
        let revision = ItemRevision::try_from(u64::from_be_bytes(revision_bytes))?;
        let ttl_bytes: [u8; 8] = header[10..18]
            .try_into()
            .map_err(|_| StorageError::internal("KV primary TTL epoch is invalid"))?;
        let ttl_value = i64::from_be_bytes(ttl_bytes);
        let ttl_epoch = match header[9] {
            0 if ttl_value == 0 => None,
            1 => Some(ttl_value),
            _ => {
                return Err(StorageError::internal(
                    "KV primary item TTL epoch is invalid",
                ));
            }
        };
        let provenance_len_bytes: [u8; 4] = header[18..22]
            .try_into()
            .map_err(|_| StorageError::internal("KV primary provenance length is invalid"))?;
        let provenance_len = usize::try_from(u32::from_be_bytes(provenance_len_bytes))
            .map_err(|_| StorageError::internal("KV primary provenance length is too large"))?;
        let image_start = PRIMARY_ITEM_HEADER_BYTES
            .checked_add(provenance_len)
            .ok_or_else(|| StorageError::internal("KV primary item length overflowed"))?;
        let provenance = bytes
            .get(PRIMARY_ITEM_HEADER_BYTES..image_start)
            .ok_or_else(|| StorageError::internal("KV primary item provenance is truncated"))?;
        let image = bytes
            .get(image_start..)
            .filter(|image| !image.is_empty())
            .ok_or_else(|| StorageError::internal("KV primary item image is missing"))?;
        Ok(Self {
            revision,
            ttl_epoch,
            provenance,
            image,
        })
    }

    pub fn decode_provenance(&self) -> StorageResult<ItemProvenance> {
        ItemProvenance::decode(self.provenance)
    }
}

pub(crate) fn encode_primary_item(
    image: &[u8],
    effects: &ItemPrimaryEffects,
) -> StorageResult<Vec<u8>> {
    let provenance = effects.provenance().as_bytes();
    let length = u32::try_from(provenance.len())
        .map_err(|_| StorageError::internal("KV primary item provenance is too large"))?;
    let size = PRIMARY_ITEM_HEADER_BYTES
        .checked_add(provenance.len())
        .and_then(|bytes| bytes.checked_add(image.len()))
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    bytes.push(PRIMARY_ITEM_FORMAT);
    bytes.extend_from_slice(&effects.revision().get().to_be_bytes());
    bytes.push(u8::from(effects.ttl_epoch().is_some()));
    bytes.extend_from_slice(&effects.ttl_epoch().unwrap_or_default().to_be_bytes());
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(provenance);
    bytes.extend_from_slice(image);
    Ok(bytes)
}
