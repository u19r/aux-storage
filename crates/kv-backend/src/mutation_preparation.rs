//! The complete input inventory qualifies before physical payload expansion.

use storage_types::{StorageError, StorageResult};

use crate::{
    KvMutation, KvVersionstampedMutation,
    value_encoding::PreparedValue,
    value_layout::{ChunkAddress, chunk_prefix},
};

enum PreparedMutation {
    ChunkablePut {
        address: ChunkAddress<Vec<u8>>,
        value: PreparedValue,
    },
    InlinePut {
        key: Vec<u8>,
        value: PreparedValue,
    },
    ChunkableDelete(ChunkAddress<Vec<u8>>),
    InlineDelete(Vec<u8>),
    InlineClearRange {
        begin: Vec<u8>,
        end: Vec<u8>,
    },
}

struct PreparedVersionstampedPut {
    prefix: Vec<u8>,
    user_bytes: [u8; 2],
    key_length: u16,
    value: PreparedValue,
}

pub(crate) fn expand_mutations(
    changes: Vec<KvMutation>,
    native_limit: usize,
    layouts: &mut [(Vec<u8>, bool)],
) -> StorageResult<Vec<KvMutation>> {
    let mut prepared = Vec::new();
    prepared
        .try_reserve_exact(changes.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for change in changes {
        prepared.push(PreparedMutation::prepare(change, native_limit)?);
    }
    let count = prepared.iter().try_fold(0usize, |count, change| {
        count
            .checked_add(change.mutation_count())
            .ok_or_else(|| StorageError::internal("KV mutation count overflowed"))
    })?;
    let mut expanded = Vec::new();
    expanded
        .try_reserve_exact(count)
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for change in prepared {
        change.expand(&mut expanded, layouts)?;
    }
    Ok(expanded)
}

pub(crate) fn expand_versionstamped_puts(
    puts: Vec<(Vec<u8>, [u8; 2], Vec<u8>)>,
    native_limit: usize,
) -> StorageResult<Vec<KvVersionstampedMutation>> {
    let mut prepared = Vec::new();
    prepared
        .try_reserve_exact(puts.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for put in puts {
        prepared.push(PreparedVersionstampedPut::prepare(put, native_limit)?);
    }
    let count = prepared.iter().try_fold(0usize, |count, put| {
        count
            .checked_add(put.value.chunk_count() + 1)
            .ok_or_else(|| StorageError::internal("KV versionstamped mutation count overflowed"))
    })?;
    let mut expanded = Vec::new();
    expanded
        .try_reserve_exact(count)
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for put in prepared {
        put.expand(&mut expanded)?;
    }
    Ok(expanded)
}

impl PreparedVersionstampedPut {
    fn prepare(put: (Vec<u8>, [u8; 2], Vec<u8>), native_limit: usize) -> StorageResult<Self> {
        let (prefix, user_bytes, value) = put;
        let length = prefix
            .len()
            .checked_add(12)
            .ok_or_else(|| StorageError::internal("KV versionstamped key length overflowed"))?;
        let key_length = u16::try_from(length)
            .map_err(|_| StorageError::internal("KV versionstamped key exceeds chunk layout"))?;
        Ok(Self {
            prefix,
            user_bytes,
            key_length,
            value: PreparedValue::prepare(value, native_limit)?,
        })
    }

    fn expand(self, expanded: &mut Vec<KvVersionstampedMutation>) -> StorageResult<()> {
        let (head, chunks) = self.value.encode()?;
        for (number, value) in chunks.into_iter().enumerate() {
            let prefix = chunk_prefix(&self.prefix, self.key_length)?;
            let number = u16::try_from(number).map_err(|_| {
                StorageError::internal("KV versionstamped chunk count exceeds layout")
            })?;
            let mut suffix = Vec::new();
            suffix
                .try_reserve_exact(2)
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            suffix.extend_from_slice(&number.to_be_bytes());
            expanded.push(KvVersionstampedMutation {
                prefix,
                user_bytes: self.user_bytes,
                suffix,
                value,
            });
        }
        expanded.push(KvVersionstampedMutation {
            prefix: self.prefix,
            user_bytes: self.user_bytes,
            suffix: Vec::new(),
            value: head,
        });
        Ok(())
    }
}

impl PreparedMutation {
    fn prepare(change: KvMutation, native_limit: usize) -> StorageResult<Self> {
        match change {
            KvMutation::Put { key, value } if native_limit != usize::MAX => {
                let value = PreparedValue::prepare(value, native_limit)?;
                Ok(Self::ChunkablePut {
                    address: ChunkAddress::prepare(key)?,
                    value,
                })
            }
            KvMutation::Put { key, value } | KvMutation::PutUnchunked { key, value } => {
                Ok(Self::InlinePut {
                    key,
                    value: PreparedValue::prepare_inline(value, native_limit)?,
                })
            }
            KvMutation::Delete { key } if native_limit != usize::MAX => {
                Ok(Self::ChunkableDelete(ChunkAddress::prepare(key)?))
            }
            KvMutation::Delete { key } | KvMutation::DeleteUnchunked { key } => {
                Ok(Self::InlineDelete(key))
            }
            KvMutation::ClearUnchunkedRange { begin, end } => {
                Ok(Self::InlineClearRange { begin, end })
            }
            KvMutation::DeleteRange { .. } => Err(StorageError::internal(
                "KV chunk ranges are internal mutations",
            )),
        }
    }

    fn mutation_count(&self) -> usize {
        match self {
            Self::ChunkablePut { value, .. } => value.chunk_count() + 2,
            Self::ChunkableDelete(_) => 2,
            Self::InlinePut { .. } | Self::InlineDelete(_) | Self::InlineClearRange { .. } => 1,
        }
    }

    fn expand(
        self,
        expanded: &mut Vec<KvMutation>,
        layouts: &mut [(Vec<u8>, bool)],
    ) -> StorageResult<()> {
        match self {
            Self::ChunkablePut { address, value } => {
                let has_chunks = value.chunk_count() != 0;
                let previous = layouts
                    .iter_mut()
                    .find(|(key, _)| key.as_slice() == address.key());
                if previous.as_ref().is_none_or(|(_, has_chunks)| *has_chunks) {
                    expanded.push(address.clear_range()?);
                }
                let (head, chunks) = value.encode()?;
                if let Some((_, previous)) = previous {
                    *previous = has_chunks;
                }
                for (number, value) in chunks.into_iter().enumerate() {
                    expanded.push(KvMutation::Put {
                        key: address.chunk_key(number)?,
                        value,
                    });
                }
                expanded.push(KvMutation::Put {
                    key: address.into_key(),
                    value: head,
                });
            }
            Self::InlinePut { key, value } => {
                let (value, _) = value.encode()?;
                expanded.push(KvMutation::Put { key, value });
            }
            Self::ChunkableDelete(address) => {
                let previous = layouts
                    .iter_mut()
                    .find(|(key, _)| key.as_slice() == address.key());
                if previous.as_ref().is_none_or(|(_, has_chunks)| *has_chunks) {
                    expanded.push(address.clear_range()?);
                }
                if let Some((_, has_chunks)) = previous {
                    *has_chunks = false;
                }
                expanded.push(KvMutation::Delete {
                    key: address.into_key(),
                });
            }
            Self::InlineDelete(key) => expanded.push(KvMutation::Delete { key }),
            Self::InlineClearRange { begin, end } => {
                expanded.push(KvMutation::ClearUnchunkedRange { begin, end });
            }
        }
        Ok(())
    }
}
