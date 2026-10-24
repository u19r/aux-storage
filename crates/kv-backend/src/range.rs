use std::num::{NonZeroU32, NonZeroUsize};

/// An ordered, half-open native key range.
#[derive(Clone)]
pub struct KvRange {
    pub begin: Vec<u8>,
    pub end: Vec<u8>,
    pub reverse: bool,
    pub limit: NonZeroU32,
    /// Logical key plus reconstructed value bytes. The first entry may exceed
    /// this budget so a caller can always advance its continuation key.
    pub max_bytes: NonZeroUsize,
    pub entry_bounds: Option<KvEntryBounds>,
}

impl KvRange {
    pub(crate) fn charge_work(
        &self,
        settlement: &storage_types::NativeAttemptSettlement,
    ) -> storage_types::StorageResult<()> {
        let bytes = self
            .begin
            .len()
            .checked_add(self.end.len())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        settlement.charge_data(bytes)
    }

    pub(crate) fn require_entry(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> storage_types::StorageResult<()> {
        if let Some(bounds) = self.entry_bounds {
            return bounds.require_entry(key, value);
        }
        let maximum = crate::constants::KV_UNQUALIFIED_RANGE_HEAD_BYTES;
        crate::KvValueAllowance::new(maximum, maximum).require_value_bytes(value.len())
    }

    pub fn try_for_prefix(
        begin: Vec<u8>,
        limit: NonZeroU32,
        max_bytes: NonZeroUsize,
    ) -> storage_types::StorageResult<Self> {
        let mut end = Vec::new();
        end.try_reserve_exact(begin.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        end.extend_from_slice(&begin);
        let end = increment_prefix(end)
            .ok_or_else(|| storage_types::StorageError::internal("KV prefix has no upper bound"))?;
        Ok(Self {
            begin,
            end,
            reverse: false,
            limit,
            max_bytes,
            entry_bounds: None,
        })
    }
}

/// Family-specific response limits, checked against borrowed native heads.
#[derive(Clone, Copy)]
pub struct KvEntryBounds {
    key_bytes: NonZeroUsize,
    value_bytes: NonZeroUsize,
}

impl KvEntryBounds {
    pub fn new(key_bytes: NonZeroUsize, value_bytes: NonZeroUsize) -> Self {
        Self {
            key_bytes,
            value_bytes,
        }
    }

    fn require_entry(&self, key: &[u8], value: &[u8]) -> storage_types::StorageResult<()> {
        if key.len() > self.key_bytes.get()
            || crate::value_layout::logical_value_len(value)? > self.value_bytes.get()
        {
            return Err(storage_types::StorageError::internal(
                "Native KV range exceeds its entry bounds",
            ));
        }
        Ok(())
    }
}

/// One physical key with its reconstructed logical value, not a domain item.
pub struct KvEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KvEntry {
    #[must_use]
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}

impl TryFrom<(&[u8], &[u8])> for KvEntry {
    type Error = storage_types::StorageError;

    fn try_from((key, value): (&[u8], &[u8])) -> Result<Self, Self::Error> {
        let mut owned_key = Vec::new();
        let mut owned_value = Vec::new();
        owned_key
            .try_reserve_exact(key.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        owned_value
            .try_reserve_exact(value.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        owned_key.extend_from_slice(key);
        owned_value.extend_from_slice(value);
        Ok(Self::new(owned_key, owned_value))
    }
}

/// Smallest key strictly above every key beginning with this prefix.
pub(crate) fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    increment_prefix(prefix.to_vec())
}

fn increment_prefix(mut end: Vec<u8>) -> Option<Vec<u8>> {
    while let Some(byte) = end.pop() {
        if byte != u8::MAX {
            end.push(byte + 1);
            return Some(end);
        }
    }
    None
}

/// A bounded page from one native attempt. `more` never implies commit.
pub struct KvRangePage {
    pub entries: Vec<KvEntry>,
    pub more: bool,
}

/// Native rows share admission, fallible ownership and first-row progress.
#[derive(Default)]
pub(crate) struct KvRangeBuffer {
    entries: Vec<KvEntry>,
    bytes: usize,
}

impl KvRangeBuffer {
    pub(crate) fn append(
        &mut self,
        range: &KvRange,
        key: &[u8],
        value: &[u8],
    ) -> storage_types::StorageResult<bool> {
        range.require_entry(key, value)?;
        let bytes = key.len().saturating_add(value.len());
        if !self.can_append(range, bytes) {
            return Ok(false);
        }
        self.entries
            .try_reserve(1)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let entry = KvEntry::try_from((key, value))?;
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.push(entry);
        Ok(true)
    }

    pub(crate) fn into_page(self, more: bool) -> KvRangePage {
        KvRangePage {
            entries: self.entries,
            more,
        }
    }

    fn can_append(&self, range: &KvRange, bytes: usize) -> bool {
        self.entries.len() < range.limit.get() as usize
            && (self.entries.is_empty()
                || self.bytes.saturating_add(bytes) <= range.max_bytes.get())
    }
}

#[cfg(feature = "foundationdb")]
impl KvRangeBuffer {
    pub(crate) fn remaining_rows(&self, range: &KvRange) -> usize {
        range.limit.get() as usize - self.entries.len()
    }

    pub(crate) fn remaining_bytes(&self, range: &KvRange) -> usize {
        range.max_bytes.get().saturating_sub(self.bytes).max(1)
    }

    pub(crate) fn is_full(&self, range: &KvRange) -> bool {
        self.remaining_rows(range) == 0 || self.bytes >= range.max_bytes.get()
    }
}
