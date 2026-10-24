use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    regional_application::{
        error::RegionalApplicationError,
        observation::{EventApplication, EventObservation, TablePageAcceptance},
        port::{RegionalMutationStore, WinnerMutation},
    },
};
use replication_types::{
    configuration::{installed::InstalledTableState, membership::RegionalTableBinding},
    event::{
        content::{CanonicalEvent, CanonicalEventCodec, CanonicalPayload},
        disposition::CanonicalAcceptanceReceipt,
    },
    regional_import::RegionalImport,
};
use storage_provider::{
    BoundDeleteItem, CachePublication, DynamoAttempt, PutItemShape, TableMetadataCache,
    prepare_accepted_delete, prepare_accepted_upsert, resolve_table_in_attempt,
};
use storage_types::{BoundTableInfo, PutItemRequest, StorageResult, TimestampMillis};

use super::observation::{materialized_winner, verify_expiration};
use crate::{journal::JournalStore, native_record::corrupt_record};

pub(super) struct NativeImportStore<'operation> {
    pub journal: &'operation JournalStore,
    pub codec: &'operation CanonicalJsonCodec,
    pub cache: &'operation TableMetadataCache,
    pub import: &'operation RegionalImport,
    pub timestamp: TimestampMillis,
    pub publications: Vec<CachePublication>,
    pub tables: Vec<ImportTable>,
    pub completed: Option<Vec<TablePageAcceptance>>,
    pub events: crate::journal::PreparedImportEvents,
}

pub(super) struct ImportTable {
    state: InstalledTableState,
    physical: Arc<BoundTableInfo>,
    authority: super::ImportAuthority,
}

pub(super) struct ObservedPrimary {
    table: Arc<BoundTableInfo>,
    primary: storage_provider::ItemWriteObservation,
    lifecycle: storage_backfill::GsiProjectionLifecycle,
}

impl NativeImportStore<'_> {
    pub(super) async fn load_configuration<A: DynamoAttempt + PhysicalRecordAttempt>(
        &mut self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        self.tables
            .try_reserve_exact(self.import.bindings().len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        self.publications
            .try_reserve_exact(self.import.bindings().len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        self.journal
            .require_retired_origin_bounds(attempt, self.import)
            .await?;
        let mut identities = Vec::new();
        identities
            .try_reserve_exact(self.import.bindings().len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        identities.extend(
            self.import
                .bindings()
                .iter()
                .map(|binding| binding.local_table),
        );
        let configured = self
            .journal
            .observe_table_states(attempt, &identities, storage_types::ReadProtection::Shared)
            .await?;
        for (binding, configured) in self.import.bindings().iter().zip(configured) {
            let configured = configured.ok_or_else(corrupt_record)?;
            self.load_table_binding(attempt, binding, configured.state())
                .await?;
        }
        Ok(())
    }

    async fn load_table_binding<A: DynamoAttempt + PhysicalRecordAttempt>(
        &mut self,
        attempt: &mut A,
        binding: &RegionalTableBinding,
        state: &InstalledTableState,
    ) -> StorageResult<()> {
        self.journal.require_local_source(state)?;
        let authority = self
            .journal
            .require_import_binding(attempt, state, (binding, self.import, self.codec))
            .await?;
        if self
            .tables
            .iter()
            .any(|table| table.state.configuration.table == binding.local_table)
        {
            return Ok(());
        }
        let (physical, publication) =
            resolve_table_in_attempt(attempt, self.cache, &state.name).await?;
        if physical.history_id != state.physical {
            return Err(corrupt_record());
        }
        self.tables.push(ImportTable {
            state: state.clone(),
            physical,
            authority,
        });
        self.publications.extend(publication);
        Ok(())
    }

    pub(super) async fn prepare_events<A: DynamoAttempt + PhysicalRecordAttempt>(
        &mut self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        let mut declarations = Vec::new();
        declarations
            .try_reserve_exact(self.import.events().len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (binding, event) in self.import.event_bindings() {
            let table = self
                .tables
                .iter()
                .find(|table| table.state.configuration.table == binding.local_table)
                .ok_or_else(corrupt_record)?;
            let history = matches!(event.as_ref().payload, CanonicalPayload::Item(_))
                .then_some(table.physical.history_id);
            declarations.push((event, history));
        }
        self.events = self
            .journal
            .prepare_import_events(attempt, &declarations, self.codec)
            .await?;
        Ok(())
    }
}

impl<A: DynamoAttempt + PhysicalRecordAttempt> RegionalMutationStore<A> for NativeImportStore<'_> {
    type Observation = Option<ObservedPrimary>;

    async fn store_import_pages(
        &mut self,
        attempt: &mut A,
        pages: &[TablePageAcceptance],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.events.require_accepted_inventory()?;
            self.completed = self
                .journal
                .seal_import_pages(attempt, (self.import, pages), self.codec)
                .await?;
            if self.completed.is_none() {
                return Ok(());
            }
            for table in &self.tables {
                self.journal
                    .record_joining_coverage(attempt, &table.state, self.import, self.codec)
                    .await?;
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn load_event_observation(
        &mut self,
        attempt: &mut A,
        binding: &RegionalTableBinding,
        event: &CanonicalEvent,
    ) -> StorageResult<EventObservation<Option<ObservedPrimary>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let table = self
                .tables
                .iter()
                .find(|table| table.state.configuration.table == binding.local_table)
                .ok_or_else(corrupt_record)?;
            if matches!(
                event.as_ref().id,
                replication_types::event::identity::CanonicalEventId::Origin(_)
            ) && binding.source_table != event.source_table()
            {
                return Err(corrupt_record());
            }
            if let Some(accepted) = self.events.observe_event(event)? {
                return Ok(EventObservation {
                    application: EventApplication::RetainHistory,
                    schema: None,
                    accepted: Some(accepted),
                    winner: None,
                    native: None,
                });
            }
            verify_expiration(self.journal, attempt, (event, self.codec, &self.events)).await?;
            table
                .load_payload_observation(attempt, event, self.codec)
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn store_winner_effects(
        &mut self,
        attempt: &mut A,
        input: (WinnerMutation<'_>, Option<ObservedPrimary>),
    ) -> StorageResult<()> {
        let (incoming, observation) = input;
        let provenance = self
            .codec
            .encode_winner_provenance(incoming.provenance)
            .map_err(RegionalApplicationError::from)?;
        let observed = observation.ok_or_else(corrupt_record)?;
        match &incoming.item.images.new {
            Some(image) => {
                let request = PutItemRequest::builder()
                    .table_name(observed.table.table_name.clone())
                    .item(image.clone())
                    .indexers(incoming.item.images.new_indexers.clone())
                    .build();
                let policy = attempt.read_ttl_config(&observed.table).await?;
                let shape = PutItemShape::try_from_request(request)?
                    .bind_table(observed.table, policy.as_ref())?;
                if self
                    .codec
                    .encode_key(&shape.key)
                    .map_err(RegionalApplicationError::from)?
                    != incoming.item.canonical_key
                {
                    return Err(corrupt_record());
                }
                let (accepted, _) = shape
                    .accept_old(observed.primary, observed.lifecycle)?
                    .into_result()?;
                prepare_accepted_upsert(attempt, accepted, self.timestamp, provenance).await?;
            }
            None => {
                let shape =
                    BoundDeleteItem::for_maintenance(observed.table, incoming.item.key.clone())?;
                let accepted = shape
                    .accept_old(observed.primary, observed.lifecycle)?
                    .into_result()?;
                prepare_accepted_delete(attempt, &accepted, self.timestamp, provenance).await?;
            }
        }
        attempt.flush_dynamo_write().await
    }

    async fn store_acceptance(
        &mut self,
        attempt: &mut A,
        event: &CanonicalEvent,
        receipt: &CanonicalAcceptanceReceipt,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let original = self.events.begin_publication(event)?;
            let accepted = self
                .journal
                .accept_import_event(attempt, (event, receipt, self.timestamp), original)
                .await?;
            self.events.complete_publication(accepted)
        }
        .await;
        settlement.complete(result)
    }
}

fn history_observation() -> EventObservation<Option<ObservedPrimary>> {
    EventObservation {
        application: EventApplication::Materialize,
        schema: None,
        accepted: None,
        winner: None,
        native: None,
    }
}

impl ImportTable {
    async fn load_payload_observation<A: DynamoAttempt>(
        &self,
        attempt: &mut A,
        event: &CanonicalEvent,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<EventObservation<Option<ObservedPrimary>>> {
        let item = match &event.as_ref().payload {
            CanonicalPayload::Item(item) => item,
            CanonicalPayload::System(_) | CanonicalPayload::TableStream { .. } => {
                return Ok(history_observation());
            }
        };
        if let super::ImportAuthority::Retained(schemas) = &self.authority {
            return Ok(EventObservation {
                application: EventApplication::RetainHistory,
                schema: schemas.contains(&item.schema).then_some(item.schema),
                accepted: None,
                winner: None,
                native: None,
            });
        }
        storage_types::validate_transact_key(&self.physical.info, &item.key)
            .map_err(|_| corrupt_record())?;
        let table = Arc::clone(&self.physical);
        let (primary, lifecycle) = attempt.observe_for_write(&table, &item.key).await?;
        let winner = materialized_winner(&primary, (item.lineage, &item.canonical_key), codec)?;
        Ok(EventObservation {
            application: EventApplication::Materialize,
            schema: self
                .state
                .schemas
                .contains(&item.schema)
                .then_some(item.schema),
            accepted: None,
            winner,
            native: Some(ObservedPrimary {
                table,
                primary,
                lifecycle,
            }),
        })
    }
}
