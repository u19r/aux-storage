//! Physical mutation plans own all addresses and trailers before native
//! staging.
use foundationdb::{
    Transaction,
    options::{MutationType, TransactionOption},
};
use storage_types::StorageResult;

use super::{error::map_operation_error, input_limits::NativeInputLimits};
use crate::{KvMutation, KvVersionstampedMutation};

pub(super) struct PreparedMutations(Vec<KvMutation>);
pub(super) struct PreparedStampedMutations(Vec<(Vec<u8>, Vec<u8>)>);

impl NativeInputLimits {
    pub(super) fn prepare_mutations(
        &self,
        mut changes: Vec<KvMutation>,
    ) -> StorageResult<PreparedMutations> {
        self.mutations(&changes)?;
        for change in &mut changes {
            match change {
                KvMutation::Put { key, .. }
                | KvMutation::PutUnchunked { key, .. }
                | KvMutation::Delete { key }
                | KvMutation::DeleteUnchunked { key } => {
                    *key = self.prefix_key(std::mem::take(key))?;
                }
                KvMutation::DeleteRange { begin, end }
                | KvMutation::ClearUnchunkedRange { begin, end } => {
                    *begin = self.prefix_key(std::mem::take(begin))?;
                    *end = self.prefix_key(std::mem::take(end))?;
                }
            }
        }
        Ok(PreparedMutations(changes))
    }

    pub(super) fn prepare_stamped_mutations(
        &self,
        changes: Vec<KvVersionstampedMutation>,
    ) -> StorageResult<PreparedStampedMutations> {
        for change in &changes {
            self.versionstamped_key(change)?;
        }
        let mut physical = Vec::new();
        physical
            .try_reserve_exact(changes.len())
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        for change in changes {
            physical.push(self.prepare_stamped_key(change)?);
        }
        Ok(PreparedStampedMutations(physical))
    }

    pub(super) fn prepare_stamped_value(
        &self,
        key: Vec<u8>,
        mut value: Vec<u8>,
        offset: u32,
    ) -> StorageResult<(Vec<u8>, Vec<u8>)> {
        self.point(&key)?;
        Self::versionstamped_value(&value, offset)?;
        value
            .try_reserve_exact(std::mem::size_of::<u32>())
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        let key = self.prefix_key(key)?;
        value.extend_from_slice(&offset.to_le_bytes());
        Ok((key, value))
    }
}

/// The fault probe and the provider prefix bytes, to report each physical
/// mutation.
pub(super) type StageObserver<'a> = (Option<&'a dyn crate::KvFaultProbe>, usize);

impl PreparedMutations {
    pub(super) fn stage(self, transaction: &Transaction, (probe, prefix_bytes): StageObserver<'_>) {
        for change in self.0 {
            if let Some(probe) = probe {
                observe_mutation(probe, prefix_bytes, &change);
            }
            match change {
                KvMutation::Put { key, value } | KvMutation::PutUnchunked { key, value } => {
                    transaction.set(&key, &value)
                }
                KvMutation::Delete { key } | KvMutation::DeleteUnchunked { key } => {
                    transaction.clear(&key)
                }
                KvMutation::DeleteRange { begin, end }
                | KvMutation::ClearUnchunkedRange { begin, end } => {
                    transaction.clear_range(&begin, &end)
                }
            }
        }
    }
}

/// Each set and clear adds a write conflict key, a clear range a write
/// conflict range.
fn observe_mutation(probe: &dyn crate::KvFaultProbe, prefix_bytes: usize, change: &KvMutation) {
    let (kind, key, native_bytes) = match change {
        KvMutation::Put { key, value } | KvMutation::PutUnchunked { key, value } => {
            (crate::KvAccessKind::Write, key, key.len() + value.len())
        }
        KvMutation::Delete { key } | KvMutation::DeleteUnchunked { key } => {
            (crate::KvAccessKind::Clear, key, key.len())
        }
        KvMutation::DeleteRange { begin, end } | KvMutation::ClearUnchunkedRange { begin, end } => {
            (
                crate::KvAccessKind::ClearRange,
                begin,
                begin.len() + end.len(),
            )
        }
    };
    probe.observe_access(crate::KvAccess {
        kind,
        key: key.get(prefix_bytes..).unwrap_or_default(),
        native_bytes,
        conflict: true,
    });
}

impl PreparedStampedMutations {
    /// Each key has no write conflict range.
    pub(super) fn stage(
        self,
        transaction: &Transaction,
        (probe, prefix_bytes): StageObserver<'_>,
    ) -> StorageResult<()> {
        for (key, value) in self.0 {
            if let Some(probe) = probe {
                super::attempt::observe_native_write(
                    probe,
                    prefix_bytes,
                    (&key, value.len()),
                    false,
                );
            }
            transaction
                .set_option(TransactionOption::NextWriteNoWriteConflictRange)
                .map_err(map_operation_error)?;
            transaction.atomic_op(&key, &value, MutationType::SetVersionstampedKey);
        }
        Ok(())
    }
}
