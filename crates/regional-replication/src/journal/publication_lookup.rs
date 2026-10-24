//! Earliest actual publication lookup; complete immutable links remain
//! independently retained.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_types::{
    event::{identity::CanonicalEventId, interval::CanonicalEventLink},
    protocol::CanonicalDigest,
};
use storage_types::{ReadProtection, StorageResult};

use super::{JournalStore, retained_links::RetainedSourceLink};
use crate::{
    history::{
        publication_scope::SourcePublicationScope, source_selection::CanonicalSourceSelection,
        table_cut::CanonicalTableCut,
    },
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn require_retained_publication<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &storage_types::TableName,
            &CanonicalTableCut,
            &CanonicalEventId,
            CanonicalDigest,
        ),
        encoding: (
            &replication_provider::runtime_configuration::limits::WorkLimits,
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
        ),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if self
                .observe_retained_publication(attempt, input, encoding)
                .await?
                .is_none()
            {
                return Err(storage_types::StorageEnum::HistoryUnavailable.into());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn observe_retained_publication<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &storage_types::TableName,
            &CanonicalTableCut,
            &CanonicalEventId,
            CanonicalDigest,
        ),
        encoding: (
            &replication_provider::runtime_configuration::limits::WorkLimits,
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
        ),
    ) -> StorageResult<Option<RetainedSourceLink>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (table, vector, identity, digest) = input;
            let Some((component, publication)) = self
                .load_retained_publication_lookup(attempt, vector, (identity, digest))
                .await?
            else {
                return Ok(None);
            };
            let cut = &vector.sources[component];
            let position = u64::from(publication.link.position);
            let range = replication_types::event::interval::ClosedSourceInterval::try_from((
                (position - 1).into(),
                position.into(),
            ))
            .map_err(|_| corrupt_record())?;
            let selection = CanonicalSourceSelection {
                serving_region: self.region.clone(),
                serving_group: self.group,
                table: table.clone(),
                lineage: vector.lineage,
                membership: vector.membership,
                source: cut.source.clone(),
                origin: cut.origin.clone(),
            };
            let frame = self
                .load_history_frame(attempt, (&selection, range), encoding)
                .await?;
            if frame.manifest().origin != publication.origin
                || !frame.manifest().links.contains(&publication.link)
            {
                return Err(corrupt_record());
            }
            Ok(Some(publication))
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn require_original_floor_visibility<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        inventory: (
            &crate::history::source_floor::SourcePublicationFloors,
            replication_types::configuration::membership::MembershipRevision,
        ),
        event: (&CanonicalEventId, CanonicalDigest),
        encoding: (
            &replication_provider::runtime_configuration::limits::WorkLimits,
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
        ),
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (publication, floor) = self
                .load_floor_publication(attempt, inventory.0, event)
                .await?
                .ok_or(storage_types::StorageEnum::HistoryUnavailable)?;
            let position = u64::from(publication.link.position);
            let range = replication_types::event::interval::ClosedSourceInterval::try_from((
                (position - 1).into(),
                position.into(),
            ))
            .map_err(|_| corrupt_record())?;
            let request = crate::source_frame::SourceFrameRequest {
                origin: publication.origin.clone(),
                range,
                membership: inventory.1,
                tables: vec![(publication.link.table, publication.link.lineage)],
            };
            let frame = crate::source_frame::load_source_frame(
                self,
                attempt,
                (&request, encoding.0),
                encoding.1,
            )
            .await
            .map_err(storage_types::StorageError::from)?;
            if frame.manifest().origin != publication.origin
                || !frame.manifest().links.contains(&publication.link)
            {
                return Err(corrupt_record());
            }
            Ok(publication.link.position > floor)
        }
        .await;
        settlement.complete(result)
    }

    async fn load_floor_publication<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        floors: &crate::history::source_floor::SourcePublicationFloors,
        event: (&CanonicalEventId, CanonicalDigest),
    ) -> StorageResult<
        Option<(
            RetainedSourceLink,
            replication_types::identity::origin::OriginPosition,
        )>,
    > {
        let keys = floors
            .scopes()
            .map(|floor| self.publication_lookup_key(event.0, floor.scope()))
            .collect::<StorageResult<Vec<_>>>()?;
        let waves = self.records.read_waves(&keys, |key| key)?;
        let mut scopes = floors.scopes();
        let mut selected: Option<(
            RetainedSourceLink,
            replication_types::identity::origin::OriginPosition,
        )> = None;
        for wave in waves {
            let rows = self
                .records
                .load(attempt, &keys[wave], ReadProtection::Snapshot)
                .await?;
            for row in rows {
                let floor = scopes.next().ok_or_else(corrupt_record)?;
                let Some(row) = row else {
                    continue;
                };
                let publication = self.decode_publication_lookup(&row, (event.0, floor.scope()))?;
                if publication.link.digest != event.1 {
                    return Err(corrupt_record());
                }
                let is_expired = publication.link.position <= floor.through;
                if selected
                    .as_ref()
                    .is_none_or(|(old, old_floor)| old.link.position > *old_floor && is_expired)
                {
                    selected = Some((publication, floor.through));
                }
            }
        }
        if scopes.next().is_some() {
            return Err(corrupt_record());
        }
        Ok(selected)
    }

    pub(in crate::journal) fn publication_lookup_key(
        &self,
        identity: &CanonicalEventId,
        publisher: SourcePublicationScope<'_>,
    ) -> StorageResult<DurableRecordKey> {
        if !identity.is_bound_to(publisher.lineage) {
            return Err(corrupt_record());
        }
        self.identity_key(
            crate::constants::PUBLICATION_LOOKUP_SUFFIX,
            &(identity, publisher.origin, publisher.table),
        )
    }

    pub(in crate::journal) fn prepare_publication_lookup(
        &self,
        row: Option<&DurableRecord>,
        publication: RetainedSourceLink,
    ) -> StorageResult<Option<RecordChange>> {
        let key = self.publication_lookup_key(
            &publication.link.event,
            SourcePublicationScope::new(
                publication.link.lineage,
                &publication.origin,
                publication.link.table,
            ),
        )?;
        if let Some(row) = row {
            let existing = self.decode_publication_lookup(
                row,
                (
                    &publication.link.event,
                    SourcePublicationScope::new(
                        publication.link.lineage,
                        &publication.origin,
                        publication.link.table,
                    ),
                ),
            )?;
            if !self.is_earlier_publication(&existing.link, &publication.link)? {
                return Ok(None);
            }
        }
        self.records
            .prepare(key, row.map(|row| row.revision), &publication)
            .map(Some)
    }

    pub(in crate::journal) fn is_earlier_publication(
        &self,
        current: &CanonicalEventLink,
        incoming: &CanonicalEventLink,
    ) -> StorageResult<bool> {
        if current.event != incoming.event
            || current.digest != incoming.digest
            || current.table != incoming.table
            || current.lineage != incoming.lineage
        {
            return Err(corrupt_record());
        }
        let old = (current.position, current.ordinal);
        let new = (incoming.position, incoming.ordinal);
        if old == new && current != incoming {
            return Err(corrupt_record());
        }
        Ok(new < old)
    }

    async fn load_retained_publication_lookup<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        vector: &CanonicalTableCut,
        event: (&CanonicalEventId, CanonicalDigest),
    ) -> StorageResult<Option<(usize, RetainedSourceLink)>> {
        let keys = vector
            .sources
            .iter()
            .map(|cut| self.publication_lookup_key(event.0, cut.into()))
            .collect::<StorageResult<Vec<_>>>()?;
        let waves = self.records.read_waves(&keys, |key| key)?;
        let mut selected = None;
        for wave in waves {
            let rows = self
                .records
                .load(attempt, &keys[wave.clone()], ReadProtection::Snapshot)
                .await?;
            for (component, row) in wave.zip(rows) {
                let cut = &vector.sources[component];
                let Some(row) = row else {
                    continue;
                };
                let publication = self.decode_publication_lookup(&row, (event.0, cut.into()))?;
                if publication.link.digest != event.1 {
                    return Err(corrupt_record());
                }
                if selected.is_none()
                    && publication.link.position > cut.after
                    && publication.link.position <= cut.through
                {
                    selected = Some((component, publication));
                }
            }
        }
        Ok(selected)
    }

    fn decode_publication_lookup(
        &self,
        row: &DurableRecord,
        scope: (&CanonicalEventId, SourcePublicationScope<'_>),
    ) -> StorageResult<RetainedSourceLink> {
        let publication: RetainedSourceLink = self.records.decode(row)?;
        if publication.link.event != *scope.0
            || SourcePublicationScope::new(
                publication.link.lineage,
                &publication.origin,
                publication.link.table,
            ) != scope.1
            || row.key != self.publication_lookup_key(scope.0, scope.1)?
        {
            return Err(corrupt_record());
        }
        self.require_publication_origin(&publication.origin, &publication.link)?;
        Ok(publication)
    }
}
