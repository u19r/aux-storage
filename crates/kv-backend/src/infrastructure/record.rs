//! Physical record lowering lends the existing catalog and native attempt.

use std::num::NonZeroUsize;

use infrastructure_provider::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    page::{RecordPage, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordChange, RecordExpectation,
    },
};
use storage_types::{
    InfrastructureContext, InfrastructureResultKind, ReadProtection, StorageResult,
};

use super::{codec, key_layout};
use crate::{
    KvAttempt, KvMutation, catalog::KvCatalogAttempt, constants::INFRASTRUCTURE_RECORD_HEADER_BYTES,
};

impl<A: KvAttempt> PhysicalRecordAttempt for KvCatalogAttempt<'_, A> {
    fn read_protection(&self) -> ReadProtection {
        self.protection
    }

    fn reject_settlement(&mut self) {
        self.attempt.root_attempt().reject_settlement();
    }

    fn reject_settlement_error(&mut self, error: &storage_types::StorageError) {
        self.attempt.root_attempt().settlement().reject_error(error);
    }

    async fn has_records_in_namespace(
        &mut self,
        namespace: &str,
        protection: ReadProtection,
    ) -> StorageResult<bool> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let range = crate::KvRange::try_for_prefix(
                key_layout::encode_namespace_prefix(namespace)?,
                std::num::NonZeroU32::MIN,
                NonZeroUsize::MIN,
            )?;
            catalog
                .attempt
                .root_attempt()
                .has_key_in_range(&range.begin, &range.end, protection)
                .await
        }
        .await;
        operation.complete(result)
    }

    async fn load_records(
        &mut self,
        keys: &[DurableRecordKey],
        protection: ReadProtection,
        limits: InfrastructureLimits,
    ) -> StorageResult<Vec<Option<DurableRecord>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let budget = observation_value_budget(keys.iter(), limits)?;
            let physical = prepare_keys(keys.iter())?;
            let maximum = limits
                .max_record_bytes()
                .get()
                .checked_add(INFRASTRUCTURE_RECORD_HEADER_BYTES)
                .ok_or(InfrastructureResultKind::InvalidBound {
                    context: InfrastructureContext::Bounds,
                })?;
            let values = catalog
                .attempt
                .root_attempt()
                .get_batch_admitted(
                    physical,
                    protection,
                    crate::KvValueAllowance::new(maximum, budget.get()),
                )
                .await?;
            codec::decode_records(keys.iter(), values, limits)
        }
        .await;
        operation.complete(result)
    }

    async fn scan_records(
        &mut self,
        request: RecordPageRequest,
        limits: InfrastructureLimits,
    ) -> StorageResult<RecordPage> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            request.validate(limits)?;
            let page = catalog
                .attempt
                .root_attempt()
                .scan(
                    key_layout::encode_range(&request, limits)?,
                    request.protection,
                )
                .await?;
            RecordPage::try_new(
                page.entries.into_iter().map(codec::decode_entry),
                page.more,
                limits,
                &request,
            )
        }
        .await;
        operation.complete(result)
    }

    async fn store_records(
        &mut self,
        batch: &RecordBatch<'_>,
    ) -> StorageResult<ConditionalWriteResult> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            if batch.as_ref().is_empty() {
                return Ok(ConditionalWriteResult::Applied);
            }
            let keys = prepare_keys(batch.as_ref().iter().map(RecordChange::key))?;
            let changes = prepare_changes(batch.as_ref())?;
            let prefix = NonZeroUsize::new(INFRASTRUCTURE_RECORD_HEADER_BYTES).ok_or(
                InfrastructureResultKind::Encoding {
                    context: InfrastructureContext::Revision,
                },
            )?;
            let values = catalog
                .attempt
                .root_attempt()
                .get_batch_prefix(keys, ReadProtection::Conflict, prefix)
                .await?;
            if values.len() != batch.as_ref().len() {
                return Err(InfrastructureResultKind::Encoding {
                    context: InfrastructureContext::Observation,
                }
                .into());
            }
            let observed = codec::decode_revisions(values, batch.limits())?;
            if let Some(conflict) = find_conflict(batch.as_ref(), observed) {
                return Ok(conflict);
            }
            catalog.attempt.root_attempt().stage(changes)?;
            Ok(ConditionalWriteResult::Applied)
        }
        .await;
        operation.complete(result)
    }
}

fn find_conflict(
    changes: &[RecordChange],
    observations: Vec<Option<storage_types::Revision>>,
) -> Option<ConditionalWriteResult> {
    changes
        .iter()
        .zip(observations)
        .find_map(|(change, observed)| {
            let expected = match change {
                RecordChange::Store {
                    expected: RecordExpectation::Absent,
                    ..
                } => None,
                RecordChange::Store {
                    expected: RecordExpectation::Revision(revision),
                    ..
                }
                | RecordChange::Delete {
                    expected: revision, ..
                } => Some(*revision),
            };
            (observed != expected).then_some(ConditionalWriteResult::Conflict { observed })
        })
}

fn observation_value_budget<'a>(
    mut keys: impl ExactSizeIterator<Item = &'a DurableRecordKey>,
    limits: InfrastructureLimits,
) -> StorageResult<NonZeroUsize> {
    let count = keys.len();
    let invalid = || InfrastructureResultKind::InvalidBound {
        context: InfrastructureContext::RecordBatch,
    };
    let key_bytes = keys.try_fold(0usize, |total, key| -> StorageResult<usize> {
        total
            .checked_add(limits.validate_key(key)?)
            .ok_or_else(|| invalid().into())
    })?;
    limits.check_batch(count, key_bytes)?;
    let bytes = count
        .checked_mul(INFRASTRUCTURE_RECORD_HEADER_BYTES)
        .and_then(|headers| {
            limits
                .max_bytes()
                .get()
                .checked_sub(key_bytes)?
                .checked_add(headers)
        })
        .ok_or_else(invalid)?;
    Ok(NonZeroUsize::new(bytes).unwrap_or(NonZeroUsize::MIN))
}

fn prepare_keys<'a>(
    keys: impl ExactSizeIterator<Item = &'a DurableRecordKey>,
) -> StorageResult<Vec<Vec<u8>>> {
    let mut physical = Vec::new();
    physical
        .try_reserve_exact(keys.len())
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    for key in keys {
        physical.push(key_layout::encode_key(key)?);
    }
    Ok(physical)
}

fn prepare_changes(changes: &[RecordChange]) -> StorageResult<Vec<KvMutation>> {
    let mut mutations = Vec::new();
    mutations
        .try_reserve_exact(changes.len())
        .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
    for change in changes {
        mutations.push(encode_change(change)?);
    }
    Ok(mutations)
}

fn encode_change(change: &RecordChange) -> StorageResult<KvMutation> {
    let key = key_layout::encode_key(change.key())?;
    Ok(match change {
        RecordChange::Store { record, .. } => KvMutation::Put {
            key,
            value: codec::encode_record(record)?,
        },
        RecordChange::Delete { .. } => KvMutation::Delete { key },
    })
}

impl<A: KvAttempt> stream_provider::retained_image::RetainedOwnershipAttempt
    for KvCatalogAttempt<'_, A>
{
    fn ownership_changes(
        &mut self,
    ) -> &mut stream_provider::retained_image::RetainedOwnershipChanges {
        self.attempt.root_attempt().ownership_changes()
    }
}
