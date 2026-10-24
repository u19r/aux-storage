use std::num::NonZeroUsize;

use storage_types::{
    InfrastructureContext, InfrastructureResultKind, ReadProtection, StorageResult,
};

use crate::record::{DurableRecord, DurableRecordKey};

/// One opaque namespace/resource/generation scope with a half-open suffix
/// range. Dynamo table ranges cannot represent this scope or its physical page
/// budget.
#[derive(Clone, Debug)]
pub struct BoundedRecordRange {
    first: DurableRecordKey,
    last_exclusive: DurableRecordKey,
    max_records: NonZeroUsize,
}
impl BoundedRecordRange {
    pub fn try_new(
        first: DurableRecordKey,
        last_exclusive: DurableRecordKey,
        max_records: NonZeroUsize,
    ) -> StorageResult<Self> {
        validate_record_range(&first, &last_exclusive)?;
        Ok(Self {
            first,
            last_exclusive,
            max_records,
        })
    }
    pub fn contains(&self, key: &DurableRecordKey) -> bool {
        &self.first <= key && key < &self.last_exclusive
    }
    pub fn bounds(&self) -> (&DurableRecordKey, &DurableRecordKey, NonZeroUsize) {
        (&self.first, &self.last_exclusive, self.max_records)
    }
}

pub struct RecordPageRequest {
    pub range: BoundedRecordRange,
    pub after: Option<DurableRecordKey>,
    pub max_bytes: NonZeroUsize,
    pub protection: ReadProtection,
}

impl RecordPageRequest {
    pub fn validate(&self, limits: crate::limits::InfrastructureLimits) -> StorageResult<()> {
        let (first, last, count) = self.range.bounds();
        limits.check_batch(count.get(), self.max_bytes.get())?;
        limits.validate_key(first)?;
        limits.validate_key(last)?;
        if let Some(after) = &self.after {
            limits.validate_key(after)?;
        }
        if self
            .after
            .as_ref()
            .is_some_and(|key| !self.range.contains(key))
        {
            return Err(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::RecordPage,
            }
            .into());
        }
        Ok(())
    }
}

pub struct RecordPage {
    pub records: Vec<DurableRecord>,
    /// None marks the end of the range; otherwise resume strictly after this
    /// key.
    pub continuation: Option<DurableRecordKey>,
}

impl RecordPage {
    /// Both physical families admit the same bounded, strictly ordered page.
    /// Decoding stays lazy so rows beyond the admitted page need no allocation.
    pub fn try_new(
        records: impl IntoIterator<Item = StorageResult<DurableRecord>>,
        native_more: bool,
        limits: crate::limits::InfrastructureLimits,
        request: &RecordPageRequest,
    ) -> StorageResult<Self> {
        let mut retained: Vec<DurableRecord> = Vec::new();
        let mut bytes = 0usize;
        let mut more = native_more;
        for record in records {
            let record = record?;
            request.check_order(&record.key, retained.last().map(|record| &record.key))?;
            let next_bytes = bytes.checked_add(limits.validate_record(&record)?).ok_or(
                InfrastructureResultKind::InvalidBound {
                    context: InfrastructureContext::RecordPage,
                },
            )?;
            if retained.len() == request.range.bounds().2.get()
                || next_bytes > request.max_bytes.get()
            {
                more = true;
                break;
            }
            bytes = next_bytes;
            retained
                .try_reserve(1)
                .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
            retained.push(record);
        }
        let continuation = if more {
            let last = retained
                .last()
                .ok_or(InfrastructureResultKind::InvalidBound {
                    context: InfrastructureContext::RecordPage,
                })?;
            Some(DurableRecordKey::try_from(&last.key)?)
        } else {
            None
        };
        Ok(Self {
            records: retained,
            continuation,
        })
    }
}

impl RecordPageRequest {
    fn check_order(
        &self,
        key: &DurableRecordKey,
        previous: Option<&DurableRecordKey>,
    ) -> StorageResult<()> {
        if !self.range.contains(key)
            || self.after.as_ref().is_some_and(|after| key <= after)
            || previous.is_some_and(|previous| key <= previous)
        {
            return Err(InfrastructureResultKind::Encoding {
                context: InfrastructureContext::RecordPage,
            }
            .into());
        }
        Ok(())
    }
}

pub struct DeletedRecordPage {
    pub deleted: usize,
    /// None marks the end of the range; otherwise resume strictly after this
    /// key.
    pub continuation: Option<DurableRecordKey>,
}

fn validate_record_range(
    first: &DurableRecordKey,
    last_exclusive: &DurableRecordKey,
) -> StorageResult<()> {
    let a = first.as_ref();
    let b = last_exclusive.as_ref();
    if a.namespace != b.namespace
        || a.resource != b.resource
        || a.generation != b.generation
        || first >= last_exclusive
    {
        return Err(InfrastructureResultKind::InvalidBound {
            context: InfrastructureContext::RecordPage,
        }
        .into());
    }
    Ok(())
}
