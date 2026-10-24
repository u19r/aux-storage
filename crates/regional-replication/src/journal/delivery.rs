use std::borrow::Cow;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    delivery::coverage::DeliveryCoverage,
    regional_application::observation::{ImportPageScope, TablePageAcceptance},
    runtime_configuration::limits::WorkLimits,
};
use replication_types::{
    configuration::{installed::RegionalTablePhase, table::RegionalMode},
    regional_import::RegionalImport,
};
use serde::{Deserialize, Serialize};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, Revision, StorageResult};

use super::JournalStore;
use crate::{
    constants::{DELIVERY_PROGRESS_SUFFIX, DELIVERY_RECEIPT_SUFFIX},
    delivery::DeliveryScope,
    native_record::corrupt_record,
    source_frame::load_source_frame,
    transport::RemoteImportReceipt,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryReceipt<'page> {
    pages: Cow<'page, [TablePageAcceptance]>,
}

impl JournalStore {
    pub async fn load_delivery_progress<A: CatalogAttempt + PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &DeliveryScope,
    ) -> StorageResult<DeliveryCoverage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            self.require_delivery_scope(attempt, scope, ReadProtection::Snapshot)
                .await?;
            Ok(self
                .observe_delivery_progress(attempt, scope, ReadProtection::Snapshot)
                .await?
                .0)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn load_delivery_head<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &DeliveryScope,
    ) -> StorageResult<replication_types::identity::origin::OriginPosition> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            if self.is_local_origin(&scope.origin) {
                return self
                    .load_source_head(attempt, scope.origin.journal_partition)
                    .await;
            }
            let received = self
                .load_received_coverage(
                    attempt,
                    (scope.local_table, scope.table, scope.lineage),
                    &scope.origin,
                    ReadProtection::Snapshot,
                )
                .await?;
            Ok(received.through())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn record_delivery<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&DeliveryScope, &RegionalImport, &RemoteImportReceipt),
        configuration: (&CanonicalJsonCodec, &WorkLimits),
    ) -> StorageResult<DeliveryCoverage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (scope, import, receipt) = input;
            scope.require_import(import)?;
            self.require_delivery_scope(attempt, scope, ReadProtection::Shared)
                .await?;
            self.require_original_frame(attempt, (scope, import), configuration)
                .await?;
            self.acknowledge_delivery(attempt, (scope, import, receipt))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) async fn acknowledge_delivery<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&DeliveryScope, &RegionalImport, &RemoteImportReceipt),
    ) -> StorageResult<DeliveryCoverage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (scope, import, receipt) = input;
            if !receipt.matches_import(import) || !receipt.matches_destination(&scope.destination) {
                return Err(corrupt_record());
            }
            ImportPageScope::Selected
                .require_pages(import, receipt.pages())
                .map_err(|_| corrupt_record())?;
            self.store_delivery_coverage(attempt, (scope, receipt), import.manifest().range)
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn store_delivery_coverage<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (&DeliveryScope, &RemoteImportReceipt),
        range: replication_types::event::interval::ClosedSourceInterval,
    ) -> StorageResult<DeliveryCoverage> {
        let (scope, receipt) = input;
        let (coverage, revision) = self
            .observe_delivery_progress(attempt, scope, ReadProtection::Conflict)
            .await?;
        let key = self.identity_key(DELIVERY_RECEIPT_SUFFIX, &(scope, &receipt.pages()[0].page))?;
        let rows = self
            .records
            .load(
                attempt,
                std::slice::from_ref(&key),
                ReadProtection::Conflict,
            )
            .await?;
        if let Some(record) = &rows[0] {
            if record.revision != Revision::INITIAL {
                return Err(corrupt_record());
            }
            if self
                .records
                .decode::<DeliveryReceipt>(record)?
                .pages
                .as_ref()
                != receipt.pages()
            {
                return Err(corrupt_record());
            }
        }
        let next = coverage
            .accept(range, self.records.limits.max_records())
            .map_err(|_| corrupt_record())?;
        let progress_key = self.identity_key(DELIVERY_PROGRESS_SUFFIX, scope)?;
        let mut changes = Vec::new();
        if rows[0].is_none() {
            changes.push(self.records.prepare(
                key,
                None,
                &DeliveryReceipt {
                    pages: Cow::Borrowed(receipt.pages()),
                },
            )?);
        }
        if next != coverage || revision.is_none() {
            changes.push(self.records.prepare(progress_key, revision, &next)?);
        }
        if changes.is_empty() {
            return Ok(next);
        }
        self.records.store(attempt, &changes).await?;
        Ok(next)
    }

    pub(in crate::journal) async fn observe_delivery_progress<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &DeliveryScope,
        protection: ReadProtection,
    ) -> StorageResult<(DeliveryCoverage, Option<Revision>)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(DELIVERY_PROGRESS_SUFFIX, scope)?;
            let rows = self.records.load(attempt, &[key], protection).await?;
            let Some(record) = &rows[0] else {
                return Ok((DeliveryCoverage::default(), None));
            };
            let coverage: DeliveryCoverage = self.records.decode(record)?;
            coverage
                .validate(self.records.limits.max_records())
                .map_err(|_| corrupt_record())?;
            Ok((coverage, Some(record.revision)))
        }
        .await;
        settlement.complete(result)
    }

    async fn require_delivery_scope<A: CatalogAttempt + PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &DeliveryScope,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let table = self
            .observe_table_state(attempt, scope.local_table, protection)
            .await?
            .ok_or_else(corrupt_record)?;
        let state = table.state();
        self.require_local_source(state)?;
        let RegionalMode::Global { members, .. } = &state.configuration.regional else {
            return Err(corrupt_record());
        };
        let source = state
            .sources
            .iter()
            .find(|source| {
                source.region == scope.origin.region
                    && source.database_group == scope.origin.database_group
                    && source.generation == scope.origin.journal_generation
                    && source.table == scope.table
            })
            .ok_or_else(corrupt_record)?;
        source
            .partitions
            .require_partition(scope.origin.journal_partition)
            .map_err(|_| corrupt_record())?;
        if !members.contains(&scope.origin.region)
            || scope.destination.region == scope.origin.region
            || state.configuration.lineage != scope.lineage
            || state.membership != scope.membership
            || !members.contains(&scope.destination.region)
            || scope.destination.region == self.region
            || !state.sources.contains(&scope.destination)
            || !matches!(
                state.phase,
                RegionalTablePhase::Active | RegionalTablePhase::Sealing
            )
        {
            return Err(corrupt_record());
        }
        let stamp = attempt
            .read_catalog_stamp(&state.name)
            .await?
            .ok_or_else(corrupt_record)?;
        if stamp.history_id != state.physical {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) async fn require_original_frame<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&DeliveryScope, &RegionalImport),
        configuration: (&CanonicalJsonCodec, &WorkLimits),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (scope, import) = input;
            let (codec, limits) = configuration;
            let request = scope.source_request(import.manifest().range);
            let issued = load_source_frame(self, attempt, (&request, limits), codec)
                .await
                .map_err(storage_types::StorageError::from)?;
            if issued.manifest() != import.manifest()
                || !issued
                    .events()
                    .iter()
                    .zip(import.events())
                    .all(|(original, received)| original.as_ref().bytes == received.as_ref().bytes)
                || issued.events().len() != import.events().len()
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
