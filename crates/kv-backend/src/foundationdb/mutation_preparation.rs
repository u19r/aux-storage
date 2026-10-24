//! Physical mutation plans own all addresses and trailers before native
//! staging.
use foundationdb::{
    Transaction,
    options::{MutationType, TransactionOption},
};
use storage_types::{InfrastructureResultKind, StorageResult};

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
                KvMutation::DeleteRange { begin, end } => {
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
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
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
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        let key = self.prefix_key(key)?;
        value.extend_from_slice(&offset.to_le_bytes());
        Ok((key, value))
    }
}

impl PreparedMutations {
    pub(super) fn stage(self, transaction: &Transaction) {
        for change in self.0 {
            match change {
                KvMutation::Put { key, value } | KvMutation::PutUnchunked { key, value } => {
                    transaction.set(&key, &value)
                }
                KvMutation::Delete { key } | KvMutation::DeleteUnchunked { key } => {
                    transaction.clear(&key)
                }
                KvMutation::DeleteRange { begin, end } => transaction.clear_range(&begin, &end),
            }
        }
    }
}

impl PreparedStampedMutations {
    pub(super) fn stage(self, transaction: &Transaction) -> StorageResult<()> {
        for (key, value) in self.0 {
            transaction
                .set_option(TransactionOption::NextWriteNoWriteConflictRange)
                .map_err(map_operation_error)?;
            transaction.atomic_op(&key, &value, MutationType::SetVersionstampedKey);
        }
        Ok(())
    }
}
