/// One staged physical change. Key/value encoding belongs to the KV family.
/// Deliberately lacks `Debug` so key material is not accidentally logged.
pub enum KvMutation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Store a bounded format that has never used value chunks. The lowerer
    /// rejects values exceeding the native inline allowance.
    PutUnchunked {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Delete a key whose stored format can never require value chunks.
    /// An observation that a variable-size value is currently small is
    /// insufficient.
    DeleteUnchunked {
        key: Vec<u8>,
    },
    /// Clear an internal value-chunk span before replacing its head.
    DeleteRange {
        begin: Vec<u8>,
        end: Vec<u8>,
    },
}

/// One key whose 10-byte commit versionstamp is inserted after `prefix`.
/// `user_bytes` distinguish writes made by the same native transaction.
/// The driver owns only the native placeholder and commit mechanism.
pub struct KvVersionstampedMutation {
    pub prefix: Vec<u8>,
    pub user_bytes: [u8; 2],
    pub suffix: Vec<u8>,
    pub value: Vec<u8>,
}

impl KvMutation {
    pub(crate) fn charge_work(
        &self,
        settlement: &storage_types::NativeAttemptSettlement,
    ) -> storage_types::StorageResult<()> {
        let (key, value) = match self {
            Self::Put { key, value } | Self::PutUnchunked { key, value } => {
                (key.len(), value.len())
            }
            Self::Delete { key } | Self::DeleteUnchunked { key } => (key.len(), 0),
            Self::DeleteRange { begin, end } => (begin.len(), end.len()),
        };
        let bytes = key
            .checked_add(value)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        settlement.charge_data(bytes)
    }
}

#[cfg(feature = "foundationdb")]
impl KvVersionstampedMutation {
    pub(crate) fn charge_work(
        &self,
        settlement: &storage_types::NativeAttemptSettlement,
    ) -> storage_types::StorageResult<()> {
        let bytes = self
            .prefix
            .len()
            .checked_add(self.suffix.len())
            .and_then(|bytes| bytes.checked_add(self.value.len()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        settlement.charge_data(bytes)
    }
}
