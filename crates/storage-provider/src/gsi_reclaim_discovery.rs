//! Protected round-robin discovery advances with the selected retirement page.

use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPage, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange, RecordExpectation},
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageResult};

use crate::gsi_reclaim_record::{GsiReclaimRecord, invalid_marker};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryCursor {
    format: u8,
    after: DurableRecordKey,
}

pub(crate) struct GsiReclaimDiscovery {
    previous: Option<Revision>,
    after: Option<DurableRecordKey>,
}

impl GsiReclaimDiscovery {
    pub(crate) async fn load(attempt: &mut impl PhysicalRecordAttempt) -> StorageResult<Self> {
        let key = GsiReclaimRecord::scope_key(vec![0])?;
        let mut rows = attempt
            .load_records(
                std::slice::from_ref(&key),
                ReadProtection::Conflict,
                GsiReclaimRecord::limits()?,
            )
            .await?;
        if rows.len() != 1 {
            return Err(invalid_marker());
        }
        let Some(row) = rows.pop().flatten() else {
            return Ok(Self {
                previous: None,
                after: None,
            });
        };
        if row.key != key {
            return Err(invalid_marker());
        }
        GsiReclaimRecord::limits()?.validate_record(&row)?;
        let cursor: DiscoveryCursor =
            serde_json::from_slice(&row.bytes).map_err(|_| invalid_marker())?;
        if cursor.format != 1 {
            return Err(invalid_marker());
        }
        GsiReclaimRecord::require_marker_key(&cursor.after)?;
        Ok(Self {
            previous: Some(row.revision),
            after: Some(cursor.after),
        })
    }

    pub(crate) async fn select_marker(
        &self,
        attempt: &mut impl PhysicalRecordAttempt,
    ) -> StorageResult<Option<DurableRecord>> {
        let selected = Self::scan_marker(attempt, self.after.clone()).await?;
        if selected.is_some() || self.after.is_none() {
            return Ok(selected);
        }
        Self::scan_marker(attempt, None).await
    }

    pub(crate) fn prepare_advance(
        &self,
        selected: DurableRecordKey,
    ) -> StorageResult<RecordChange> {
        GsiReclaimRecord::require_marker_key(&selected)?;
        let cursor = DiscoveryCursor {
            format: 1,
            after: selected,
        };
        let record = DurableRecord {
            key: GsiReclaimRecord::scope_key(vec![0])?,
            revision: self
                .previous
                .map_or(Ok(Revision::INITIAL), Revision::checked_next)?,
            bytes: serde_json::to_vec(&cursor)
                .map_err(|_| invalid_marker())?
                .into(),
        };
        GsiReclaimRecord::limits()?.validate_record(&record)?;
        Ok(RecordChange::Store {
            expected: self
                .previous
                .map_or(RecordExpectation::Absent, RecordExpectation::Revision),
            record,
        })
    }

    async fn scan_marker(
        attempt: &mut impl PhysicalRecordAttempt,
        after: Option<DurableRecordKey>,
    ) -> StorageResult<Option<DurableRecord>> {
        let limits = GsiReclaimRecord::limits()?;
        let range = BoundedRecordRange::try_new(
            GsiReclaimRecord::scope_key(vec![1])?,
            GsiReclaimRecord::scope_key(vec![2])?,
            limits.max_records(),
        )?;
        let request = RecordPageRequest {
            range,
            after,
            max_bytes: limits.max_bytes(),
            protection: ReadProtection::Conflict,
        };
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
        let mut checked = RecordPage::try_new(
            page.records.into_iter().map(Ok),
            continuation.is_some(),
            limits,
            &request,
        )?;
        if checked.continuation != continuation {
            return Err(invalid_marker());
        }
        Ok(checked.records.pop())
    }
}
