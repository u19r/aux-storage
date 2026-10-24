use std::collections::BTreeMap;

use replication_provider::{
    CANONICAL_EVENT_SEMANTIC_VERSION, canonical_event_codec::CanonicalJsonCodec,
    canonical_item_original::AuthoredItemOriginal,
    regional_application::error::RegionalApplicationError,
};
use replication_types::{
    event::{
        content::{AcceptedItemEvent, CanonicalEventCodec, SourceImagesRef},
        disposition::{CanonicalAcceptanceReceipt, EventDisposition},
        identity::{CanonicalEventId, OriginEventId, VersionIdentity},
        interval::CanonicalEventLink,
        order::{EffectRank, WinnerOrder, WinnerProvenance},
    },
    identity::{journal_partition::JournalPartition, resource::OperationIdentity},
};
use storage_types::{ItemProvenance, StorageResult};

use super::{
    super::{JournalStore, PartitionAllocation},
    OriginMutation,
    allocation::allocate_partitions,
    mutation::PreparedOriginMutation,
};
use crate::native_record::corrupt_record;

pub struct OriginCaptureInput<'accepted> {
    pub mutations: Vec<OriginMutation<'accepted>>,
    pub wall_ms: u64,
    pub transaction: Option<OperationIdentity>,
    pub observations: Vec<WinnerProvenance>,
}

pub struct CapturedOriginMutation {
    pub ordinal: replication_types::identity::origin::MutationOrdinal,
    pub provenance: ItemProvenance,
    pub event: CanonicalEventId,
}

/// Batch members can be left unprocessed before any publication or item effect.
pub(crate) fn select_batch_mutations(
    journal: &JournalStore,
    mutations: &[OriginMutation<'_>],
    codec: &CanonicalJsonCodec,
) -> StorageResult<Vec<replication_types::identity::origin::MutationOrdinal>> {
    let maximum = journal
        .source_byte_allowances
        .get()
        .ok_or_else(|| {
            storage_types::StorageError::internal("source original allowance is not bound")
        })?
        .maximum_events
        .get();
    let mut counts = BTreeMap::new();
    let mut selected = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        let key = codec
            .encode_key(mutation.view()?.key)
            .map_err(RegionalApplicationError::from_local_encoding)?;
        let partition = journal
            .partitions
            .select(mutation.table.state().configuration.lineage, &key);
        let count = counts.entry(partition).or_insert(0usize);
        if *count < maximum {
            *count += 1;
            selected.push(mutation.ordinal);
        }
    }
    Ok(selected)
}

/// Capture all selected global members before applying shared accepted effects.
/// The caller still owns native settlement and its customer response receipt.
pub async fn capture_origin_mutations<
    A: stream_provider::retained_image::RetainedOwnershipAttempt,
>(
    journal: &JournalStore,
    attempt: &mut A,
    input: OriginCaptureInput<'_>,
    codec: &CanonicalJsonCodec,
) -> StorageResult<Vec<CapturedOriginMutation>> {
    if input.mutations.is_empty() {
        return Ok(Vec::new());
    }
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let mutations = prepare_mutations(journal, attempt, (input.mutations, codec)).await?;
        let observations =
            collect_transaction_observations(&mutations, (input.transaction, input.observations))?;
        let observed_clock = observations
            .iter()
            .filter_map(|observation| match observation.order {
                WinnerOrder::Mutation { clock, .. } => Some(clock),
                _ => None,
            })
            .max();
        let allocations = allocate_partitions(
            journal,
            attempt,
            (
                mutations.iter().map(|mutation| {
                    let clock = match mutation
                        .predecessor
                        .as_ref()
                        .map(|previous| &previous.provenance.order)
                    {
                        Some(WinnerOrder::Mutation { clock, .. }) => Some(*clock),
                        _ => None,
                    };
                    (mutation.partition, clock)
                }),
                input.wall_ms,
                observed_clock,
            ),
        )
        .await?;
        let capture = OriginCaptureBatch {
            allocations,
            links: BTreeMap::new(),
            captured: Vec::with_capacity(mutations.len()),
        };
        capture
            .retain_mutations(
                journal,
                attempt,
                &mutations,
                (codec, input.transaction, &observations),
            )
            .await
    }
    .await;
    settlement.complete(result)
}

/// Bind allocated publications and encoded winner provenance to the exact
/// canonical events consumed by the guarded acceptance/publication operation.
struct OriginCaptureBatch {
    allocations: BTreeMap<JournalPartition, PartitionAllocation>,
    links: BTreeMap<JournalPartition, Vec<CanonicalEventLink>>,
    captured: Vec<CapturedOriginMutation>,
}

impl OriginCaptureBatch {
    async fn retain_mutations<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        mut self,
        journal: &JournalStore,
        attempt: &mut A,
        mutations: &[PreparedOriginMutation<'_>],
        encoding: (
            &CanonicalJsonCodec,
            Option<OperationIdentity>,
            &[WinnerProvenance],
        ),
    ) -> StorageResult<Vec<CapturedOriginMutation>> {
        let (codec, transaction, observations) = encoding;
        let mut accepted = super::super::source_acceptance::PreparedSourceAcceptance::default();
        for mutation in mutations {
            let allocation = self
                .allocations
                .get(&mutation.partition)
                .ok_or_else(corrupt_record)?;
            let event = encode_mutation(mutation, (allocation, transaction, observations), codec)?;
            let origin = allocation.origin().clone();
            let receipt = CanonicalAcceptanceReceipt {
                event: event.publication().identity().clone(),
                digest: event.publication().digest(),
                disposition: EventDisposition::WinnerApplied,
            };
            let link = self.record_mutation(mutation, (&event, receipt.clone()), codec)?;
            accepted.include(
                journal,
                (
                    (&event).into(),
                    &receipt,
                    Some(mutation.table.state().physical),
                ),
                (&origin, &link),
            )?;
        }
        let mut publications = Vec::with_capacity(self.links.len());
        for (partition, links) in self.links {
            publications.push((
                self.allocations
                    .remove(&partition)
                    .ok_or_else(corrupt_record)?,
                links,
            ));
        }
        if !self.allocations.is_empty() {
            return Err(corrupt_record());
        }
        journal
            .store_prepared_source_publications(attempt, publications, accepted)
            .await?;
        Ok(self.captured)
    }

    fn record_mutation(
        &mut self,
        mutation: &PreparedOriginMutation<'_>,
        input: (&AuthoredItemOriginal, CanonicalAcceptanceReceipt),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<CanonicalEventLink> {
        let (event, receipt) = input;
        let allocation = self
            .allocations
            .get(&mutation.partition)
            .ok_or_else(corrupt_record)?;
        let provenance = event.winner();
        self.captured.push(CapturedOriginMutation {
            ordinal: mutation.ordinal,
            event: receipt.event.clone(),
            provenance: codec
                .encode_winner_provenance(provenance)
                .map_err(RegionalApplicationError::from_local_encoding)?,
        });
        let link = CanonicalEventLink {
            position: allocation.position(),
            publication_clock: allocation.clock(),
            ordinal: mutation.ordinal,
            table: event.publication().source_table(),
            lineage: event.publication().lineage(),
            event: receipt.event,
            digest: receipt.digest,
        };
        self.links
            .entry(mutation.partition)
            .or_default()
            .push(link.clone());
        Ok(link)
    }
}

async fn prepare_mutations<
    'accepted,
    A: stream_provider::retained_image::RetainedOwnershipAttempt,
>(
    journal: &JournalStore,
    attempt: &mut A,
    input: (Vec<OriginMutation<'accepted>>, &CanonicalJsonCodec),
) -> StorageResult<Vec<PreparedOriginMutation<'accepted>>> {
    let (mutations, codec) = input;
    if mutations.windows(2).any(|w| w[0].ordinal >= w[1].ordinal) {
        return Err(corrupt_record());
    }
    let inputs = mutations
        .into_iter()
        .map(|mutation| OriginMutationInput::try_new(journal, mutation, codec))
        .collect::<StorageResult<Vec<_>>>()?;
    let tables = inputs
        .iter()
        .map(|input| input.mutation.table)
        .collect::<Vec<_>>();
    let parents = super::configuration::OriginConfigurationParents::try_new(&tables)?
        .load(journal, attempt)
        .await?;
    let predecessors = inputs
        .iter()
        .map(|input| (input.mutation.table, &input.view, &input.canonical_key))
        .collect::<Vec<_>>();
    let predecessors = journal
        .observe_origin_predecessors(attempt, &predecessors, codec)
        .await?;
    if parents.len() != inputs.len() || predecessors.len() != inputs.len() {
        return Err(corrupt_record());
    }
    Ok(inputs
        .into_iter()
        .zip(parents)
        .zip(predecessors)
        .map(|((input, configuration_parent), predecessor)| {
            let OriginMutationInput {
                mutation,
                view,
                canonical_key,
            } = input;
            let partition = journal
                .partitions
                .select(mutation.table.state().configuration.lineage, &canonical_key);
            PreparedOriginMutation {
                table: mutation.table,
                key: view.key,
                canonical_key,
                images: view.images,
                predecessor,
                partition,
                cause: mutation.cause,
                ordinal: mutation.ordinal,
                configuration_parent,
            }
        })
        .collect())
}

/// Pure validation and key encoding precede every native predecessor
/// observation.
struct OriginMutationInput<'accepted> {
    mutation: OriginMutation<'accepted>,
    view: super::mutation::MutationView<'accepted>,
    canonical_key: replication_types::event::identity::CanonicalItemKey,
}

impl<'accepted> OriginMutationInput<'accepted> {
    fn try_new(
        journal: &JournalStore,
        mutation: OriginMutation<'accepted>,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Self> {
        let view = mutation.view()?;
        mutation.table.require_origination(journal, view.physical)?;
        let canonical_key = codec
            .encode_key(view.key)
            .map_err(RegionalApplicationError::from_local_encoding)?;
        Ok(Self {
            mutation,
            view,
            canonical_key,
        })
    }
}

fn encode_mutation(
    mutation: &PreparedOriginMutation<'_>,
    input: (
        &PartitionAllocation,
        Option<OperationIdentity>,
        &[WinnerProvenance],
    ),
    codec: &CanonicalJsonCodec,
) -> StorageResult<AuthoredItemOriginal> {
    let (identity, item) = prepare_mutation_event(mutation, input)?;
    codec
        .author_item_event(
            CANONICAL_EVENT_SEMANTIC_VERSION.into(),
            &identity.into(),
            &item,
        )
        .map_err(RegionalApplicationError::from_local_encoding)
        .map_err(Into::into)
}

fn prepare_mutation_event<'image>(
    mutation: &'image PreparedOriginMutation<'_>,
    input: (
        &PartitionAllocation,
        Option<OperationIdentity>,
        &[WinnerProvenance],
    ),
) -> StorageResult<(OriginEventId, AcceptedItemEvent<SourceImagesRef<'image>>)> {
    let (allocation, transaction, observations) = input;
    let configuration = &mutation.table.state().configuration;
    let predecessor = mutation
        .predecessor
        .as_ref()
        .map(|previous| &previous.provenance);
    let retained = mutation
        .predecessor
        .as_ref()
        .map(|previous| previous.authority.images())
        .transpose()?;
    let images = SourceImagesRef {
        old: retained.as_ref().and_then(|images| images.new),
        old_indexers: retained.as_ref().and_then(|images| images.new_indexers),
        new: mutation.images.new,
        new_indexers: mutation.images.new_indexers,
    };
    let identity = OriginEventId {
        lineage: configuration.lineage,
        origin: allocation.origin().clone(),
        position: allocation.position(),
        ordinal: mutation.ordinal,
    };
    let item = AcceptedItemEvent {
        source_table: configuration.table,
        lineage: configuration.lineage,
        schema: configuration.schema,
        key: mutation.key.clone(),
        canonical_key: mutation.canonical_key.clone(),
        images,
        order: WinnerOrder::Mutation {
            clock: allocation.clock(),
            origin: allocation.origin().clone(),
            position: allocation.position(),
            ordinal: mutation.ordinal,
            rank: EffectRank::Mutation,
        },
        version: VersionIdentity::Mutation(identity.clone()),
        cause: mutation.cause,
        victim_order: None,
        parents: recorded_parents(mutation, observations),
        predecessor: predecessor.map(|previous| previous.version.clone()),
        transaction,
        observations: observations.to_vec(),
    };
    Ok((identity, item))
}

fn collect_transaction_observations(
    mutations: &[PreparedOriginMutation<'_>],
    input: (Option<OperationIdentity>, Vec<WinnerProvenance>),
) -> StorageResult<Vec<WinnerProvenance>> {
    let (transaction, mut observations) = input;
    if transaction.is_none() {
        return if observations.is_empty() {
            Ok(observations)
        } else {
            Err(corrupt_record())
        };
    }
    observations.extend(mutations.iter().filter_map(|mutation| {
        mutation
            .predecessor
            .as_ref()
            .map(|previous| previous.provenance.clone())
    }));
    let mut unique: Vec<WinnerProvenance> = Vec::new();
    for observation in observations {
        if let Some(previous) = unique
            .iter()
            .find(|previous| previous.version == observation.version)
        {
            if previous != &observation {
                return Err(corrupt_record());
            }
        } else {
            unique.push(observation);
        }
    }
    Ok(unique)
}

fn recorded_parents(
    mutation: &PreparedOriginMutation<'_>,
    observations: &[WinnerProvenance],
) -> Vec<CanonicalEventId> {
    let predecessor = mutation
        .predecessor
        .as_ref()
        .and_then(|previous| previous.provenance.event.as_ref());
    let mut parents = Vec::new();
    for parent in predecessor
        .into_iter()
        .chain(mutation.configuration_parent.iter())
        .chain(
            observations
                .iter()
                .filter_map(|observation| observation.event.as_ref()),
        )
    {
        if !parents.contains(parent) {
            parents.push(parent.clone());
        }
    }
    parents
}
