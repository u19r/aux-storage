//! Exact old/new descriptor transitions share one protected receipt CAS owner.

use infrastructure_provider::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    record::{ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordChange},
};
use storage_types::{ReadProtection, StorageResult, TableHistoryId};

use super::{
    RetainedOwnerKind, RetainedOwnerRecord,
    custody_record::CustodyRecordCodec,
    error::{capacity_rejected, invalid_image},
};

pub struct RetainedOwnerTransition {
    identity: RetainedOwnerRecord,
    previous: Option<RetainedOwnerRecord>,
    next: Option<RetainedOwnerRecord>,
}

impl RetainedOwnerTransition {
    pub fn try_new(
        kind: RetainedOwnerKind,
        locator: DurableRecordKey,
        history: TableHistoryId,
        records: (Option<RetainedOwnerRecord>, Option<RetainedOwnerRecord>),
    ) -> StorageResult<Self> {
        if history.as_ref() == &[0; 16]
            || (!kind.is_mutable() && records.0.is_some() && records.1.is_some())
        {
            return Err(invalid_image());
        }
        let identity = RetainedOwnerRecord {
            kind,
            locator,
            history,
            descriptor: [0; 32],
            bodies: Vec::new(),
        };
        for record in [records.0.as_ref(), records.1.as_ref()]
            .into_iter()
            .flatten()
        {
            if record.kind != kind
                || record.locator != identity.locator
                || record.history != history
                || record.bodies.is_empty()
            {
                return Err(invalid_image());
            }
        }
        Ok(Self {
            identity,
            previous: records.0,
            next: records.1,
        })
    }

    pub async fn prepare_batch<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        transitions: Vec<Self>,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if transitions.is_empty() {
                return Ok(());
            }
            let OwnerTransitionInventory {
                keys,
                mut changes,
                limits,
            } = OwnerTransitionInventory::try_new(&transitions)?;
            let count = limits.max_records().get();
            for (wave, keys) in transitions.chunks(count).zip(keys.chunks(count)) {
                let read_limits = limits.project_read_keys(keys)?;
                let observed = attempt
                    .load_records(keys, ReadProtection::Conflict, read_limits)
                    .await?;
                read_limits.validate_observations(keys, &observed)?;
                for (transition, row) in wave.iter().zip(observed) {
                    changes.extend(transition.prepare_change(row, limits)?);
                }
            }
            let [waves] = RecordBatch::plan_waves([changes.as_slice()], limits)?;
            for batch in waves {
                if !matches!(
                    attempt.store_records(&batch).await?,
                    ConditionalWriteResult::Applied
                ) {
                    return Err(storage_types::StorageEnum::TransactionConflict {
                        message: "Image descriptor ownership changed".into(),
                    }
                    .into());
                }
            }
            for transition in transitions {
                if transition.previous.is_some() || transition.next.is_some() {
                    attempt
                        .ownership_changes()
                        .stage_owner(transition.previous, transition.next)?;
                }
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    fn prepare_change(
        &self,
        observed: Option<DurableRecord>,
        limits: InfrastructureLimits,
    ) -> StorageResult<Option<RecordChange>> {
        let key = self.identity.key()?;
        match (&self.previous, &observed) {
            (None, None) => {}
            (Some(previous), Some(row)) => {
                if !previous.kind.is_mutable() && row.revision != storage_types::Revision::INITIAL {
                    return Err(invalid_image());
                }
                let receipt: RetainedOwnerRecord =
                    CustodyRecordCodec::decode(&row.bytes, limits.max_record_bytes().get())?;
                if receipt != *previous {
                    return Err(invalid_image());
                }
            }
            _ => return Err(invalid_image()),
        }
        Ok(match self.next.as_ref() {
            Some(next) => Some(CustodyRecordCodec::prepare(
                key,
                observed.as_ref().map(|row| row.revision),
                next,
                limits.max_record_bytes().get(),
            )?),
            None => observed.map(|row| RecordChange::Delete {
                key,
                expected: row.revision,
            }),
        })
    }
}

/// Unique receipt reads and maximum output slots are prepared before native
/// work.
struct OwnerTransitionInventory {
    keys: Vec<DurableRecordKey>,
    changes: Vec<RecordChange>,
    limits: InfrastructureLimits,
}

impl OwnerTransitionInventory {
    fn try_new(transitions: &[RetainedOwnerTransition]) -> StorageResult<Self> {
        let mut keys = Vec::new();
        keys.try_reserve(transitions.len())
            .map_err(|_| capacity_rejected())?;
        for transition in transitions {
            keys.push(transition.identity.key()?);
        }
        if keys
            .iter()
            .enumerate()
            .any(|(slot, key)| keys[..slot].contains(key))
        {
            return Err(invalid_image());
        }
        let limits = RetainedOwnerRecord::receipt_limits(
            transitions.iter().map(|transition| &transition.identity),
        )?;
        let mut changes = Vec::new();
        changes
            .try_reserve(transitions.len())
            .map_err(|_| capacity_rejected())?;
        Ok(Self {
            keys,
            changes,
            limits,
        })
    }
}
