use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    event::interval::CanonicalEventLink,
    identity::origin::{OriginId, OriginPosition},
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult};

use super::{partition::PartitionAllocation, store::JournalStore};
use crate::native_record::corrupt_record;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePosition {
    pub origin: OriginId,
    pub position: OriginPosition,
    pub events: Vec<CanonicalEventLink>,
}

/// Original immutable native position and its stored-content receipt.
pub(crate) struct SealedSourcePosition {
    pub(crate) source: SourcePosition,
    pub(crate) receipt: replication_types::protocol::CanonicalDigest,
}

impl JournalStore {
    pub(crate) fn require_source_byte_allowances(
        &self,
        native: replication_provider::runtime_configuration::limits::NativeBudgets,
        maximum_events: std::num::NonZeroUsize,
    ) -> StorageResult<()> {
        let expected =
            super::source_declaration::SourcePublicationLimits::new(native, maximum_events);
        if self.source_byte_allowances.get() != Some(&expected) {
            return Err(storage_types::StorageError::validation(
                "source allowances differ from the initialized journal",
            ));
        }
        Ok(())
    }

    pub(in crate::journal) fn source_proof_allowance(
        &self,
        manifest: &replication_types::event::interval::SourceIntervalManifest,
        codec: &replication_provider::canonical_event_codec::CanonicalJsonCodec,
    ) -> StorageResult<usize> {
        let maximum = self
            .source_byte_allowances
            .get()
            .ok_or_else(|| {
                storage_types::StorageError::internal("source original allowance is not bound")
            })?
            .original_bytes
            .get();
        let declaration = codec
            .interval_content_length(manifest, maximum)
            .map_err(|_| {
                storage_types::StorageError::validation(
                    "source declaration exceeds its qualified allowance",
                )
            })?;
        maximum.checked_sub(declaration).ok_or_else(|| {
            storage_types::StorageError::validation(
                "source declaration exceeds its qualified allowance",
            )
        })
    }

    pub(in crate::journal) fn require_source_original_bytes(
        &self,
        originals: usize,
    ) -> StorageResult<()> {
        let limits = self.source_byte_allowances.get().ok_or_else(|| {
            storage_types::StorageError::internal("source original allowance is not bound")
        })?;
        let total = originals
            .checked_add(limits.declaration_bytes.get())
            .ok_or_else(corrupt_record)?;
        if total > limits.original_bytes.get() {
            return Err(storage_types::InfrastructureResultKind::NativeCapacityRejected.into());
        }
        Ok(())
    }

    pub(crate) fn bound_source_range(
        &self,
        range: replication_types::event::interval::ClosedSourceInterval,
        maximum: std::num::NonZeroUsize,
    ) -> StorageResult<replication_types::event::interval::ClosedSourceInterval> {
        let after = u64::from(range.after());
        let available = u64::from(range.through()) - after;
        let limit = maximum.min(self.records.limits.max_records());
        let width = available.min(u64::try_from(limit.get()).map_err(|_| corrupt_record())?);
        replication_types::event::interval::ClosedSourceInterval::try_from((
            range.after(),
            (after + width).into(),
        ))
        .map_err(|_| corrupt_record())
    }

    /// Accept exact originals and publish every local position in this attempt.
    pub(in crate::journal) async fn store_source_publications<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        publications: Vec<(PartitionAllocation, Vec<CanonicalEventLink>)>,
        accepted: &[(
            &replication_types::event::content::CanonicalEvent,
            &replication_types::event::disposition::CanonicalAcceptanceReceipt,
            Option<storage_types::TableHistoryId>,
        )],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let mut prepared = super::source_acceptance::PreparedSourceAcceptance::default();
            for input in accepted {
                let publication = publications
                    .iter()
                    .find_map(|(allocation, links)| {
                        links
                            .iter()
                            .find(|link| link.event == input.0.as_ref().id)
                            .map(|link| (&allocation.origin, link))
                    })
                    .ok_or_else(corrupt_record)?;
                prepared.include(self, (input.0.into(), input.1, input.2), publication)?;
            }
            self.store_prepared_source_publications(settlement.as_mut(), publications, prepared)
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn store_prepared_source_publications<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        publications: Vec<(PartitionAllocation, Vec<CanonicalEventLink>)>,
        mut accepted: super::source_acceptance::PreparedSourceAcceptance,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let sources = self.prepare_source_positions(publications)?;
            accepted.require_sources(self, &sources)?;
            let publications = self
                .prepare_source_publication_changes(attempt, &sources)
                .await?;
            let mut changes = accepted.prepare(self, attempt).await?;
            changes.extend(publications);
            self.records.store(attempt, &changes).await
        }
        .await;
        settlement.complete(result)
    }

    fn prepare_source_positions(
        &self,
        publications: Vec<(PartitionAllocation, Vec<CanonicalEventLink>)>,
    ) -> StorageResult<Vec<SourcePosition>> {
        let mut partitions = std::collections::BTreeSet::new();
        let mut sources = Vec::with_capacity(publications.len());
        for (allocation, links) in publications {
            if links
                .iter()
                .any(|link| link.publication_clock != allocation.clock())
                || allocation.origin != self.origin(allocation.origin.journal_partition)?
                || !partitions.insert(allocation.origin.journal_partition)
            {
                return Err(corrupt_record());
            }
            let mut source = SourcePosition {
                origin: allocation.origin,
                position: allocation.position,
                events: links,
            };
            source.validate()?;
            self.require_source_declaration(&mut source)?;
            sources.push(source);
        }
        Ok(sources)
    }

    async fn prepare_source_publication_changes<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        sources: &[SourcePosition],
    ) -> StorageResult<Vec<infrastructure_provider::record::RecordChange>> {
        let publications = sources
            .iter()
            .flat_map(|source| source.events.iter().map(|link| (&source.origin, link)));
        let mut changes = self
            .prepare_retained_publications(attempt, publications)
            .await?;
        for source in sources {
            changes.push(self.records.prepare(
                self.source_key(&source.origin, source.position)?,
                None,
                source,
            )?);
        }
        let activities = sources.iter().flat_map(|source| {
            source.events.iter().map(|link| {
                (
                    (link.table, link.lineage),
                    (source.origin.journal_partition, source.position),
                )
            })
        });
        changes.extend(
            self.prepare_local_delivery_activities(attempt, activities)
                .await?,
        );
        Ok(changes)
    }

    pub async fn load_source_position<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        origin: &OriginId,
        position: OriginPosition,
    ) -> StorageResult<Option<SourcePosition>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.source_key(origin, position)?;
            let rows = self
                .load_immutable_waves(attempt, &[key], ReadProtection::Snapshot)
                .await?;
            let Some(record) = &rows[0] else {
                return Ok(None);
            };
            let source: SourcePosition = self.records.decode(record)?;
            source.validate()?;
            if &source.origin != origin || source.position != position {
                return Err(corrupt_record());
            }
            Ok(Some(source))
        }
        .await;
        settlement.complete(result)
    }
    /// Release each native wave before observing later source positions.
    pub(crate) async fn select_local_source_manifest<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &crate::source_frame::SourceFrameRequest,
        input: (
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
            &replication_provider::runtime_configuration::limits::WorkLimits,
        ),
    ) -> Result<
        Option<replication_types::event::interval::SourceIntervalManifest>,
        replication_provider::delivery::error::DeliveryError,
    > {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let keys = self.prepare_source_range_keys(&request.origin, request.range)?;
            let mut selection = crate::source_frame::SourceManifestSelection::new(request, input.1);
            for wave in self.records.read_waves(&keys, |key| key)? {
                let rows = self
                    .load_immutable_waves(
                        settlement.as_mut(),
                        &keys[wave.clone()],
                        ReadProtection::Snapshot,
                    )
                    .await?;
                for (index, row) in wave.zip(rows) {
                    let expected = (u64::from(request.range.after()) + index as u64 + 1).into();
                    let position = self.decode_sealed_source_position(
                        row.ok_or_else(corrupt_record)?,
                        (&request.origin, expected),
                    )?;
                    if !selection.append_local_position(position, (&request.tables, input.0))? {
                        return Ok(None);
                    }
                }
            }
            Ok(Some(selection.into_manifest()))
        }
        .await;
        settlement.complete(result)
    }

    fn prepare_source_range_keys(
        &self,
        origin: &OriginId,
        range: replication_types::event::interval::ClosedSourceInterval,
    ) -> StorageResult<Vec<infrastructure_provider::record::DurableRecordKey>> {
        let width = u64::from(range.through()) - u64::from(range.after());
        self.records
            .limits
            .check_batch(usize::try_from(width).map_err(|_| corrupt_record())?, 0)?;
        (u64::from(range.after()) + 1..=u64::from(range.through()))
            .map(|position| self.source_key(origin, position.into()))
            .collect()
    }

    fn decode_sealed_source_position(
        &self,
        record: infrastructure_provider::record::DurableRecord,
        identity: (&OriginId, OriginPosition),
    ) -> StorageResult<SealedSourcePosition> {
        let source: SourcePosition = self.records.decode(&record)?;
        source.validate()?;
        if &source.origin != identity.0 || source.position != identity.1 {
            return Err(corrupt_record());
        }
        Ok(SealedSourcePosition {
            source,
            receipt: replication_types::protocol::CanonicalDigest::of(&record.bytes),
        })
    }
}

impl SourcePosition {
    pub(in crate::journal) fn validate(&self) -> StorageResult<()> {
        if u64::from(self.position) == 0
            || self.events.windows(2).any(|w| {
                w[0].ordinal >= w[1].ordinal || w[0].publication_clock != w[1].publication_clock
            })
            || self
                .events
                .iter()
                .any(|link| link.position != self.position)
        {
            return Err(corrupt_record());
        }
        for link in &self.events {
            link.require_source(&self.origin)
                .map_err(|_| corrupt_record())?;
        }
        Ok(())
    }
}
