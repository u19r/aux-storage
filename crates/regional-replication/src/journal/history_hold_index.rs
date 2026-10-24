//! Active operation source holds share the native reader inventory and epochs.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordChange, RecordExpectation},
};
use replication_types::{identity::resource::TableLineage, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult};

use super::{JournalStore, history_hold::SourceHold, reader_hold::RetentionHoldReference};
use crate::{history::hold_discovery::ReaderHoldObservation, native_record::corrupt_record};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct SourceHoldReference {
    lineage: TableLineage,
    source: DurableRecordKey,
}

impl JournalStore {
    pub(in crate::journal) async fn require_absent_capture_indexes<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &replication_types::configuration::proposal::ControlProposal,
        sources: &[DurableRecordKey],
        holds: &[Option<infrastructure_provider::record::DurableRecord>],
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if sources.len() != holds.len() {
                return Err(corrupt_record());
            }
            let keys = sources
                .iter()
                .zip(holds)
                .filter(|(_, hold)| hold.is_none())
                .map(|(source, _)| {
                    self.source_hold_index_key(&SourceHoldReference {
                        lineage: proposal.operation().lineage,
                        source: source.clone(),
                    })
                })
                .collect::<StorageResult<Vec<_>>>()?;
            if keys.is_empty() {
                return Ok(());
            }
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            if rows.iter().any(Option::is_some) {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) fn source_hold_index_key(
        &self,
        reference: &SourceHoldReference,
    ) -> StorageResult<DurableRecordKey> {
        self.require_source_hold_address(reference)?;
        let mut suffix = crate::constants::READER_HOLD_SUFFIX.to_vec();
        suffix.push(3);
        suffix.extend_from_slice(
            CanonicalDigest::of_json(&reference.source)
                .map_err(|_| corrupt_record())?
                .as_ref(),
        );
        self.record_key(suffix)
    }

    fn require_source_hold_address(&self, reference: &SourceHoldReference) -> StorageResult<()> {
        let base = self.record_key(crate::constants::SOURCE_HOLD_SUFFIX.to_vec())?;
        let base = base.as_ref();
        let address = reference.source.as_ref();
        if address.namespace != base.namespace
            || address.resource != base.resource
            || address.generation != base.generation
            || !address.suffix.starts_with(&base.suffix)
            || address.suffix.len() != base.suffix.len() + 64
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn decode_indexed_source_hold(
        &self,
        record: &infrastructure_provider::record::DurableRecord,
        reference: &SourceHoldReference,
    ) -> StorageResult<SourceHold> {
        self.require_source_hold_address(reference)?;
        let hold = self.decode_source_hold(record)?;
        if record.key != reference.source || hold.lineage != reference.lineage {
            return Err(corrupt_record());
        }
        Ok(hold)
    }

    pub(in crate::journal) async fn load_indexed_source_holds<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (
            &replication_types::configuration::proposal::ControlProposal,
            &[replication_types::identity::origin::OriginId],
        ),
        protection: ReadProtection,
    ) -> StorageResult<Vec<SourceHold>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (proposal, origins) = input;
            let references = origins
                .iter()
                .map(|origin| {
                    self.source_hold_key(origin, proposal)
                        .map(|source| SourceHoldReference {
                            lineage: proposal.operation().lineage,
                            source,
                        })
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let keys = references
                .iter()
                .map(|reference| reference.source.clone())
                .collect::<Vec<_>>();
            let rows = self.records.load(attempt, &keys, protection).await?;
            let holds = rows
                .into_iter()
                .zip(&references)
                .zip(origins)
                .map(|((row, reference), origin)| {
                    let row = row.ok_or_else(corrupt_record)?;
                    let hold = self.decode_indexed_source_hold(&row, reference)?;
                    hold.require_scope(origin, proposal)?;
                    Ok(hold)
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let keys = references
                .iter()
                .map(|reference| self.source_hold_index_key(reference))
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self.records.load(attempt, &keys, protection).await?;
            for ((hold, reference), row) in holds.iter().zip(references).zip(rows) {
                if hold.is_released() {
                    if row.is_some() {
                        return Err(corrupt_record());
                    }
                    continue;
                }
                let row = row.ok_or_else(corrupt_record)?;
                if self.decode_retention_hold(&row)? != RetentionHoldReference::Capture(reference) {
                    return Err(corrupt_record());
                }
            }
            Ok(holds)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn observe_capture_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: SourceHoldReference,
        protection: ReadProtection,
    ) -> StorageResult<ReaderHoldObservation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            self.require_source_hold_address(&reference)?;
            let row = self
                .records
                .load(attempt, std::slice::from_ref(&reference.source), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let hold = self.decode_indexed_source_hold(&row, &reference)?;
            let range = hold.observe_live_interval()?;
            Ok(ReaderHoldObservation::Capture { range })
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn prepare_source_hold_indexes<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        lineage: TableLineage,
        changes: &[RecordChange],
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let growing = changes
                .iter()
                .filter_map(|change| match change {
                    RecordChange::Store {
                        record,
                        expected: RecordExpectation::Absent,
                    } => Some(
                        self.decode_source_hold(record)
                            .and_then(|hold| hold.observe_live_interval()),
                    ),
                    _ => None,
                })
                .collect::<StorageResult<Vec<_>>>()?;
            if !growing.is_empty() {
                let ranges = growing
                    .iter()
                    .map(crate::history::source_retention::SourceRetentionRange::Capture)
                    .collect::<Vec<_>>();
                self.require_reservation_admission(
                    attempt,
                    super::trim_reservation::ReservationAdmission::Sources(&ranges),
                )
                .await?;
            }
            let references = changes
                .iter()
                .map(|change| {
                    let source = match change {
                        RecordChange::Store { record, .. } => &record.key,
                        RecordChange::Delete { key, .. } => key,
                    };
                    SourceHoldReference {
                        lineage,
                        source: source.clone(),
                    }
                })
                .collect::<Vec<_>>();
            let keys = references
                .iter()
                .map(|reference| self.source_hold_index_key(reference))
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            let mut indexes = Vec::with_capacity(changes.len());
            for (((reference, key), row), change) in
                references.into_iter().zip(keys).zip(rows).zip(changes)
            {
                if let Some(row) = &row
                    && self.decode_retention_hold(row)?
                        != RetentionHoldReference::Capture(reference.clone())
                {
                    return Err(corrupt_record());
                }
                if let Some(change) =
                    self.prepare_source_hold_index((reference, key, row), change)?
                {
                    indexes.push(change);
                }
            }
            Ok(indexes)
        }
        .await;
        settlement_guard.complete(result)
    }

    fn prepare_source_hold_index(
        &self,
        input: (
            SourceHoldReference,
            DurableRecordKey,
            Option<infrastructure_provider::record::DurableRecord>,
        ),
        change: &RecordChange,
    ) -> StorageResult<Option<RecordChange>> {
        let (reference, key, row) = input;
        if let RecordChange::Store { expected, record } = change {
            let hold = self.decode_indexed_source_hold(record, &reference)?;
            if *expected == RecordExpectation::Absent {
                if row.is_some() || hold.is_released() {
                    return Err(corrupt_record());
                }
                return self
                    .records
                    .prepare(key, None, &RetentionHoldReference::Capture(reference))
                    .map(Some);
            }
            if row.is_none() {
                return Err(corrupt_record());
            }
            if !hold.is_released() {
                return Ok(None);
            }
        }
        let row = row.ok_or_else(corrupt_record)?;
        Ok(Some(RecordChange::Delete {
            key,
            expected: row.revision,
        }))
    }
}
