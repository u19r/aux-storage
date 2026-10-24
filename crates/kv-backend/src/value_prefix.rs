//! Protected logical prefixes do not hydrate unrelated payload chunks.

use std::num::NonZeroUsize;

use storage_types::{StorageError, StorageResult};

use crate::{
    KvAttempt, ReadProtection,
    constants::KV_CHUNK_BYTES,
    value_layout::{
        ChunkAddress, ValueHeader, load_value_heads, parse_header, require_chunk_length,
    },
};

pub struct KvValuePrefix {
    pub(crate) length: usize,
    pub(crate) bytes: Vec<u8>,
}

pub(crate) async fn load_value_prefixes<A: KvAttempt + ?Sized>(
    attempt: &A,
    keys: &[Vec<u8>],
    protection: ReadProtection,
    prefix_bytes: NonZeroUsize,
) -> StorageResult<Vec<Option<KvValuePrefix>>> {
    if prefix_bytes.get() > KV_CHUNK_BYTES {
        return Err(invalid_prefix());
    }
    let mut output = Vec::new();
    let mut pending = Vec::new();
    output
        .try_reserve_exact(keys.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    pending
        .try_reserve_exact(keys.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let heads = load_value_heads(
        attempt,
        keys,
        protection,
        crate::KvValueAllowance::for_unqualified_values(keys.len()).for_head_reads(keys.len())?,
    )
    .await?;
    for (index, head) in heads.into_iter().enumerate() {
        match head.as_deref().map(parse_header).transpose()? {
            None => output.push(None),
            Some(ValueHeader::Inline(value)) => {
                output.push(Some(KvValuePrefix {
                    length: value.len(),
                    bytes: copy_prefix(value, prefix_bytes)?,
                }));
            }
            Some(ValueHeader::Chunked { length, .. }) => {
                if length < prefix_bytes.get() {
                    return Err(invalid_prefix());
                }
                output.push(None);
                pending.push((index, length));
            }
        }
    }
    if !pending.is_empty() {
        let chunks = prepare_prefix_chunks(keys, &pending)?;
        let maximum = pending
            .iter()
            .try_fold(0usize, |total, (_, length)| {
                total.checked_add((*length).min(KV_CHUNK_BYTES))
            })
            .ok_or_else(invalid_prefix)?;
        let values = load_value_heads(
            attempt,
            &chunks,
            protection,
            crate::KvValueAllowance::for_chunks(maximum),
        )
        .await?;
        for ((index, length), value) in pending.into_iter().zip(values) {
            let value = value.ok_or_else(invalid_prefix)?;
            require_chunk_length(&value, length, 0)?;
            output[index] = Some(KvValuePrefix {
                length,
                bytes: copy_prefix(&value, prefix_bytes)?,
            });
        }
    }
    Ok(output)
}

fn copy_prefix(value: &[u8], prefix_bytes: NonZeroUsize) -> StorageResult<Vec<u8>> {
    let prefix = value.get(..prefix_bytes.get()).ok_or_else(invalid_prefix)?;
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(prefix.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    copied.extend_from_slice(prefix);
    Ok(copied)
}

fn prepare_prefix_chunks(
    keys: &[Vec<u8>],
    pending: &[(usize, usize)],
) -> StorageResult<Vec<Vec<u8>>> {
    let mut addresses = Vec::new();
    addresses
        .try_reserve_exact(pending.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for (index, _) in pending {
        addresses.push(ChunkAddress::prepare(keys[*index].as_slice())?);
    }
    let mut chunks = Vec::new();
    chunks
        .try_reserve_exact(addresses.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for address in addresses {
        chunks.push(address.chunk_key(0)?);
    }
    Ok(chunks)
}

fn invalid_prefix() -> StorageError {
    StorageError::internal("KV value cannot supply its declared prefix")
}
