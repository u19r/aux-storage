//! Exact-incarnation cursor records share the catalog transition's native
//! attempt.
use std::num::NonZeroUsize;

use infrastructure_provider::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{
        ConditionalWriteResult, DurableRecord, DurableRecordKey, RecordAddress, RecordChange,
        RecordExpectation,
    },
};
use serde::{Deserialize, Serialize};
use storage_types::{
    BoundTableInfo, ReadProtection, Revision, StorageResult, TableHistoryId, TableName,
};
#[derive(Serialize, Deserialize)]
pub(super) struct TableStreamInitialization {
    pub(super) table: TableName,
    pub(super) history: TableHistoryId,
    pub(super) after: Option<String>,
}

impl TableStreamInitialization {
    pub(super) fn scope_key(suffix: Vec<u8>) -> StorageResult<DurableRecordKey> {
        RecordAddress {
            namespace: crate::constants::TABLE_STREAM_INITIALIZATION_NAMESPACE.into(),
            resource: vec![1],
            generation: 0,
            suffix,
        }
        .try_into()
    }

    pub(super) fn key(&self) -> StorageResult<DurableRecordKey> {
        let mut suffix = vec![1];
        suffix.extend_from_slice(self.history.as_ref());
        Self::scope_key(suffix)
    }

    pub(super) fn prepare_store(&self, previous: Option<Revision>) -> StorageResult<RecordChange> {
        Ok(RecordChange::Store {
            expected: previous.map_or(RecordExpectation::Absent, RecordExpectation::Revision),
            record: DurableRecord {
                key: self.key()?,
                revision: previous.map_or(Ok(Revision::INITIAL), |value| value.checked_next())?,
                bytes: serde_json::to_vec(self)?.into(),
            },
        })
    }
}

pub(super) fn record_limits() -> StorageResult<InfrastructureLimits> {
    InfrastructureLimits::try_new(
        crate::constants::TABLE_STREAM_INITIALIZATION_KEY_BYTES,
        crate::CATALOG_READ_BYTES,
        crate::CATALOG_READ_BYTES
            .saturating_add(crate::constants::TABLE_STREAM_INITIALIZATION_KEY_BYTES.get()),
        NonZeroUsize::MIN,
    )
}

pub(super) async fn store_change(
    attempt: &mut (impl PhysicalRecordAttempt + ?Sized),
    change: RecordChange,
) -> StorageResult<()> {
    let changes = [change];
    let batch = RecordBatch::try_new(&changes, record_limits()?)?;
    if !matches!(
        attempt.store_records(&batch).await?,
        ConditionalWriteResult::Applied
    ) {
        return Err(storage_types::StorageEnum::TransactionConflict {
            message: "Stream initialization changed".into(),
        }
        .into());
    }
    Ok(())
}

pub(crate) async fn register_initialization(
    attempt: &mut (impl PhysicalRecordAttempt + ?Sized),
    table: &BoundTableInfo,
) -> StorageResult<()> {
    let record = TableStreamInitialization {
        table: table.table_name.clone(),
        history: table.history_id,
        after: None,
    };
    let previous = attempt
        .load_records(&[record.key()?], ReadProtection::Conflict, record_limits()?)
        .await?;
    let revision = previous
        .into_iter()
        .next()
        .flatten()
        .map(|row| row.revision);
    store_change(attempt, record.prepare_store(revision)?).await
}

pub(super) async fn load_pending_initialization(
    attempt: &mut impl PhysicalRecordAttempt,
) -> StorageResult<Option<(DurableRecord, TableStreamInitialization)>> {
    let page = attempt
        .scan_records(
            RecordPageRequest {
                range: BoundedRecordRange::try_new(
                    TableStreamInitialization::scope_key(vec![1])?,
                    TableStreamInitialization::scope_key(vec![2])?,
                    NonZeroUsize::MIN,
                )?,
                after: None,
                max_bytes: record_limits()?.max_bytes(),
                protection: ReadProtection::Conflict,
            },
            record_limits()?,
        )
        .await?;
    let Some(row) = page.records.into_iter().next() else {
        return Ok(None);
    };
    let initialization: TableStreamInitialization = serde_json::from_slice(&row.bytes)?;
    if initialization.key()? != row.key {
        return Err(storage_types::StorageError::internal(
            "Invalid stream initialization identity",
        ));
    }
    Ok(Some((row, initialization)))
}
