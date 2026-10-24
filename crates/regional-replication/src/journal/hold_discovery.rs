//! Restartable observation of generation-wide native reader obligations.
use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::DurableRecordKey,
};
use replication_provider::runtime_configuration::limits::WorkLimits;
use replication_types::identity::resource::TableLineage;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult};

use super::{
    JournalStore,
    reader_hold::{ReaderHoldKind, RetentionHoldReference},
};
use crate::history::hold_discovery::{
    ReaderHoldCursor, ReaderHoldDiscoveryAction, ReaderHoldDiscoveryRequest, ReaderHoldObservation,
    ReaderHoldPage, RetentionInventoryStamp,
};

impl JournalStore {
    pub(crate) async fn load_reader_hold_page<A: CatalogAttempt + PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &ReaderHoldDiscoveryRequest,
        budget: (&WorkLimits, crate::transport::SourceReservation),
    ) -> StorageResult<ReaderHoldPage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let stamp = self
                .load_retention_inventory_stamp(attempt, request)
                .await?;
            let first = self.record_key(crate::constants::READER_HOLD_SUFFIX.to_vec())?;
            let all = self.record_prefix_range(&first)?;
            let range = BoundedRecordRange::try_new(
                first,
                all.bounds().1.clone(),
                std::num::NonZeroUsize::MIN,
            )?;
            let after = self.qualify_hold_continuation(request, &stamp, &range)?;
            let page = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range,
                        after,
                        max_bytes: self.records.limits.max_bytes(),
                        protection: ReadProtection::Snapshot,
                    },
                )
                .await?;
            let hold = match page.records.into_iter().next() {
                Some(row) => {
                    self.resolve_retention_hold(attempt, &row, stamp.lineage)
                        .await?
                }
                None => None,
            };
            let continuation = page.continuation.map(|after| ReaderHoldCursor {
                stamp: stamp.clone(),
                after,
            });
            let response = ReaderHoldPage {
                stamp,
                hold,
                continuation,
                _source: budget.1,
            };
            crate::history::response_budget::encoded_length(&response, budget.0.frame_bytes.get())?;
            Ok(response)
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn load_retention_inventory_stamp<A: CatalogAttempt + PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &ReaderHoldDiscoveryRequest,
    ) -> StorageResult<RetentionInventoryStamp> {
        let catalog = attempt
            .read_catalog_stamp(&request.table)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&request.table))?;
        let physical = attempt.read_table_at_stamp(&request.table, catalog).await?;
        let observed = self
            .observe_history_table(attempt, &physical, ReadProtection::Snapshot)
            .await?;
        let state = observed.state();
        let retention = self
            .observe_retention_revision(
                attempt,
                state.configuration.lineage,
                ReadProtection::Snapshot,
            )
            .await?;
        Ok(RetentionInventoryStamp {
            trim: observed.authority.revision(),
            region: self.region.clone(),
            group: self.group,
            generation: self.generation,
            tenant: state.configuration.tenant,
            table: state.configuration.table,
            physical: state.physical,
            lineage: state.configuration.lineage,
            configuration: state.configuration.config,
            membership: state.membership,
            state_revision: observed.state.revision,
            retention,
        })
    }

    fn qualify_hold_continuation(
        &self,
        request: &ReaderHoldDiscoveryRequest,
        stamp: &RetentionInventoryStamp,
        range: &BoundedRecordRange,
    ) -> StorageResult<Option<DurableRecordKey>> {
        let ReaderHoldDiscoveryAction::Continue(cursor) = &request.retention_holds else {
            return Ok(None);
        };
        if cursor.stamp != *stamp || !range.contains(&cursor.after) {
            return Err(StorageError::validation(
                "Reader hold inventory changed or cursor scope differs; restart discovery",
            ));
        }
        Ok(Some(cursor.after.clone()))
    }

    async fn resolve_retention_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &infrastructure_provider::record::DurableRecord,
        lineage: TableLineage,
    ) -> StorageResult<Option<ReaderHoldObservation>> {
        let observation = self
            .resolve_native_retention_hold(attempt, row, ReadProtection::Snapshot)
            .await?;
        let selected = match &observation {
            ReaderHoldObservation::Source { cut, .. } => cut.lineage == lineage,
            ReaderHoldObservation::Capture { range } => range.lineage == lineage,
            ReaderHoldObservation::ImportFragmentPending { binding, .. } => {
                binding.lineage == lineage
            }
            _ => true,
        };
        Ok(selected.then_some(observation))
    }

    pub(in crate::journal) async fn resolve_native_retention_hold<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &infrastructure_provider::record::DurableRecord,
        protection: ReadProtection,
    ) -> StorageResult<ReaderHoldObservation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            match self.decode_retention_hold(row)? {
                RetentionHoldReference::Adoption(operation) => {
                    self.observe_adoption_inventory(attempt, operation, protection)
                        .await
                }
                RetentionHoldReference::Reenable(operation) => {
                    self.observe_reenable_custody(attempt, operation, protection)
                        .await
                }
                RetentionHoldReference::Capture(reference) => {
                    self.observe_capture_hold(attempt, reference, protection)
                        .await
                }
                RetentionHoldReference::ImportFragment(reference) => {
                    self.observe_fragment_hold(attempt, &reference, protection)
                        .await
                }
                RetentionHoldReference::Archive(hold) => {
                    self.observe_archive_hold(attempt, &hold, protection).await
                }
                RetentionHoldReference::Reader(reference) => match reference.kind {
                    ReaderHoldKind::Receiver => {
                        self.observe_receiver_hold(attempt, reference, protection)
                            .await
                    }
                    ReaderHoldKind::Source => {
                        self.observe_source_hold(attempt, reference, protection)
                            .await
                    }
                },
            }
        }
        .await;
        settlement_guard.complete(result)
    }
}
