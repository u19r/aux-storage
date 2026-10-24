//! Deleted-incarnation receipt disposal precedes physical descriptor
//! reclamation.

use std::num::NonZeroUsize;

use infrastructure_provider::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    page::{BoundedRecordRange, RecordPage, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordAddress, RecordChange,
    },
};
use storage_types::{ReadProtection, StorageResult, TableHistoryId};

use super::{
    RetainedOwnerKind, RetainedOwnerRecord, custody_record::CustodyRecordCodec,
    error::invalid_image,
};

pub struct RetainedOwnerRetirement {
    kind: RetainedOwnerKind,
    history: TableHistoryId,
}

impl RetainedOwnerRetirement {
    pub fn try_new(kind: RetainedOwnerKind, history: TableHistoryId) -> StorageResult<Self> {
        if !matches!(
            kind,
            RetainedOwnerKind::Primary | RetainedOwnerKind::Projection
        ) || history.as_ref() == &[0; 16]
        {
            return Err(invalid_image());
        }
        Ok(Self { kind, history })
    }

    /// The caller must already protect the deleted table incarnation marker.
    /// No body deletion follows from disposal of one descriptor receipt.
    pub async fn release_page<A: super::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<bool> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (page, limits) = self.load_page(attempt, None).await?;
            let found = !page.records.is_empty();
            let (changes, owners): (Vec<_>, Vec<_>) = page
                .records
                .into_iter()
                .map(|row| self.prepare_retirement(row, limits))
                .collect::<StorageResult<Vec<_>>>()?
                .into_iter()
                .unzip();
            self.store_retirements(attempt, &changes, limits).await?;
            for owner in owners {
                owner.stage_retirement(attempt)?;
            }
            Ok(found)
        }
        .await;
        settlement_guard.complete(result)
    }

    /// The caller protects removal of this exact writable index binding.
    /// The durable cursor must commit with these receipt deletions.
    pub async fn release_projection_page<A: super::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        incarnation: &storage_types::maintenance::TargetIncarnation,
        name: &storage_types::IndexName,
        after: Option<DurableRecordKey>,
    ) -> StorageResult<Option<DurableRecordKey>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if self.kind != RetainedOwnerKind::Projection {
                return Err(invalid_image());
            }
            let mut prefix = Vec::new();
            prefix.extend_from_slice(
                &u16::try_from(incarnation.as_ref().len())
                    .map_err(|_| invalid_image())?
                    .to_be_bytes(),
            );
            prefix.extend_from_slice(incarnation.as_ref());
            prefix.extend_from_slice(
                &u16::try_from(name.as_ref().len())
                    .map_err(|_| invalid_image())?
                    .to_be_bytes(),
            );
            prefix.extend_from_slice(name.as_ref().as_bytes());
            let (page, limits) = self.load_page(attempt, after).await?;
            let mut changes = Vec::new();
            let mut owners = Vec::new();
            for row in page.records {
                let (change, owner) = self.prepare_retirement(row, limits)?;
                if owner.locator.as_ref().suffix.starts_with(&prefix) {
                    changes.push(change);
                    owners.push(owner);
                }
            }
            self.store_retirements(attempt, &changes, limits).await?;
            for owner in owners {
                owner.stage_retirement(attempt)?;
            }
            Ok(page.continuation)
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn load_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        after: Option<DurableRecordKey>,
    ) -> StorageResult<(RecordPage, InfrastructureLimits)> {
        if after.as_ref().is_some_and(|key| {
            RetainedOwnerKind::from_key(key) != Some(self.kind)
                || key.as_ref().resource != self.history.as_ref()
        }) {
            return Err(invalid_image());
        }
        let limits = self.page_limits()?;
        let bound = |tag| -> StorageResult<DurableRecordKey> {
            RecordAddress {
                namespace: crate::constants::RETAINED_OWNER_NAMESPACE.into(),
                resource: self.history.as_ref().to_vec(),
                generation: 0,
                suffix: vec![tag],
            }
            .try_into()
        };
        let range = BoundedRecordRange::try_new(
            bound(self.kind.tag())?,
            bound(self.kind.tag() + 1)?,
            limits.max_records(),
        )?;
        let request = RecordPageRequest {
            range,
            after,
            max_bytes: limits.max_bytes(),
            protection: ReadProtection::Conflict,
        };
        request.validate(limits)?;
        let page = attempt
            .scan_records(
                RecordPageRequest {
                    range: request.range.clone(),
                    after: request.after.clone(),
                    max_bytes: request.max_bytes,
                    protection: request.protection,
                },
                limits,
            )
            .await?;
        let continuation = page.continuation;
        let checked = RecordPage::try_new(
            page.records.into_iter().map(Ok),
            continuation.is_some(),
            limits,
            &request,
        )?;
        if checked.continuation != continuation {
            return Err(invalid_image());
        }
        Ok((checked, limits))
    }

    fn page_limits(&self) -> StorageResult<InfrastructureLimits> {
        let count = NonZeroUsize::new(crate::constants::IMAGE_OWNER_RECLAIM_RECORDS)
            .ok_or_else(invalid_image)?;
        let key_bytes = crate::constants::RETAINED_OWNER_NAMESPACE.len() + 16 + 16 + 33;
        let locator_bytes = match self.kind {
            RetainedOwnerKind::Primary => crate::constants::PRIMARY_IMAGE_LOCATOR_BYTES,
            RetainedOwnerKind::Projection => crate::constants::GSI_IMAGE_LOCATOR_BYTES,
            _ => return Err(invalid_image()),
        };
        let record_bytes = RetainedOwnerRecord::encoded_length_bound(locator_bytes, usize::MAX)?;
        let bytes = key_bytes
            .checked_add(record_bytes)
            .and_then(|bytes| bytes.checked_mul(count.get()))
            .ok_or_else(invalid_image)?;
        InfrastructureLimits::try_new(
            NonZeroUsize::new(key_bytes).ok_or_else(invalid_image)?,
            NonZeroUsize::new(record_bytes).ok_or_else(invalid_image)?,
            NonZeroUsize::new(bytes).ok_or_else(invalid_image)?,
            count,
        )
    }

    async fn store_retirements<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        changes: &[RecordChange],
        limits: InfrastructureLimits,
    ) -> StorageResult<()> {
        if changes.is_empty() {
            return Ok(());
        }
        if !matches!(
            attempt
                .store_records(&RecordBatch::try_new(changes, limits)?)
                .await?,
            ConditionalWriteResult::Applied
        ) {
            return Err(storage_types::StorageEnum::TransactionConflict {
                message: "Image descriptor retirement changed".into(),
            }
            .into());
        }
        Ok(())
    }

    fn prepare_retirement(
        &self,
        row: DurableRecord,
        limits: InfrastructureLimits,
    ) -> StorageResult<(RecordChange, RetainedOwnerRecord)> {
        let history = self.history;
        let owner: RetainedOwnerRecord =
            CustodyRecordCodec::decode(&row.bytes, limits.max_record_bytes().get())?;
        let locator = owner.locator.as_ref();
        let namespace = match self.kind {
            RetainedOwnerKind::Primary => crate::constants::PRIMARY_IMAGE_LOCATOR_NAMESPACE,
            RetainedOwnerKind::Projection => crate::constants::GSI_IMAGE_LOCATOR_NAMESPACE,
            _ => return Err(invalid_image()),
        };
        if owner.kind != self.kind
            || owner.history != history
            || owner.key()? != row.key
            || locator.namespace != namespace
            || locator.resource != history.as_ref()
            || locator.generation != 0
            || locator.suffix.is_empty()
            || owner.bodies.is_empty()
            || owner.bodies.len() > 2
        {
            return Err(invalid_image());
        }
        self.require_locator(&owner.locator)?;
        for (index, (address, length)) in owner.bodies.iter().enumerate() {
            if address.history() != history
                || *length == 0
                || owner.bodies[..index].iter().any(|(previous, _)| {
                    previous.identity().as_ref() >= address.identity().as_ref()
                })
            {
                return Err(invalid_image());
            }
        }
        Ok((
            RecordChange::Delete {
                key: row.key,
                expected: row.revision,
            },
            owner,
        ))
    }

    fn require_locator(&self, locator: &DurableRecordKey) -> StorageResult<()> {
        let suffix = locator.as_ref().suffix.as_slice();
        let prefix = suffix.get(..2).ok_or_else(invalid_image)?;
        let length = usize::from(u16::from_be_bytes(
            prefix.try_into().map_err(|_| invalid_image())?,
        ));
        let remaining = suffix
            .len()
            .checked_sub(2 + length)
            .ok_or_else(invalid_image)?;
        let maximum = match self.kind {
            RetainedOwnerKind::Primary => storage_types::MAX_PARTITION_KEY_BYTES,
            RetainedOwnerKind::Projection => storage_types::MAX_TARGET_INCARNATION_BYTES,
            _ => return Err(invalid_image()),
        };
        if length == 0 || length > maximum {
            return Err(invalid_image());
        }
        if self.kind == RetainedOwnerKind::Primary && remaining > storage_types::MAX_SORT_KEY_BYTES
        {
            return Err(invalid_image());
        }
        if self.kind == RetainedOwnerKind::Projection {
            let name_prefix = suffix
                .get(2 + length..4 + length)
                .ok_or_else(invalid_image)?;
            let name_bytes = usize::from(u16::from_be_bytes(
                name_prefix.try_into().map_err(|_| invalid_image())?,
            ));
            let name = suffix
                .get(4 + length..4 + length + name_bytes)
                .ok_or_else(invalid_image)?;
            if name.is_empty()
                || name_bytes > storage_types::MAX_INDEX_NAME_BYTES
                || std::str::from_utf8(name).is_err()
                || remaining != 2 + name_bytes + 32
            {
                return Err(invalid_image());
            }
        }
        Ok(())
    }
}
