//! Native source keys qualify committed system history references.
use storage_types::{StorageError, StorageResult, SystemStreamPosition};

pub(crate) fn decode_position(
    origin: &SystemStreamPosition,
    key: &[u8],
) -> StorageResult<SystemStreamPosition> {
    let prefix = super::key_layout::record_prefix(origin);
    let suffix = key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(invalid_position)?;
    if origin.offset().versionstamp().is_some() {
        return origin
            .with_versionstamp(suffix.try_into().map_err(|_| invalid_position())?)
            .map_err(|_| invalid_position());
    }
    let (order, text) = suffix.split_at_checked(8).ok_or_else(invalid_position)?;
    let text = std::str::from_utf8(text).map_err(|_| invalid_position())?;
    let position = SystemStreamPosition::try_new(
        origin.shard_id().as_str(),
        origin.lineage_id().as_str(),
        text,
    )
    .map_err(|_| invalid_position())?;
    if order
        != position
            .offset()
            .order()
            .map_err(|_| invalid_position())?
            .to_be_bytes()
    {
        return Err(invalid_position());
    }
    Ok(position)
}

fn invalid_position() -> StorageError {
    StorageError::internal("Invalid feed source key")
}

/// Private feed format: table incarnation (16 bytes), canonical encoded value
/// length (big-endian u32), creation time (big-endian i64 milliseconds).
/// Entry and reference publish atomically in the same native attempt.
pub(crate) struct FeedReference {
    pub(crate) history_id: storage_types::TableHistoryId,
    pub(crate) encoded_length: usize,
    pub(crate) created_at: storage_types::TimestampMillis,
}

impl FeedReference {
    pub(crate) fn encode(
        history_id: storage_types::TableHistoryId,
        created_at: storage_types::TimestampMillis,
        value: &[u8],
    ) -> StorageResult<Vec<u8>> {
        let length = u32::try_from(value.len())
            .map_err(|_| StorageError::internal("Canonical entry length exceeds feed format"))?;
        let mut bytes = Vec::with_capacity(28);
        bytes.extend_from_slice(history_id.as_ref());
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(&created_at.timestamp_millis().to_be_bytes());
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> StorageResult<Self> {
        let bytes: &[u8; 28] = bytes
            .try_into()
            .map_err(|_| StorageError::internal("Invalid feed reference length"))?;
        Ok(Self {
            history_id: storage_types::TableHistoryId::from(
                <[u8; 16]>::try_from(&bytes[..16])
                    .map_err(|_| StorageError::internal("Invalid feed incarnation"))?,
            ),
            encoded_length: u32::from_be_bytes(
                bytes[16..20]
                    .try_into()
                    .map_err(|_| StorageError::internal("Invalid feed length"))?,
            ) as usize,
            created_at: i64::from_be_bytes(
                bytes[20..28]
                    .try_into()
                    .map_err(|_| StorageError::internal("Invalid feed timestamp"))?,
            )
            .into(),
        })
    }
}
