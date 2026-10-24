use std::collections::BTreeMap;

use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    ordering::winner::{WinnerDecision, WinnerEffectPlan},
};
use replication_types::{
    event::{
        disposition::{CanonicalAcceptanceReceipt, EventDisposition},
        interval::CanonicalEventLink,
        order::{HybridLogicalClock, WinnerOrder, WinnerProvenance},
    },
    identity::journal_partition::JournalPartition,
};
use storage_provider::AcceptedDeleteItem;
use storage_types::{ItemProvenance, StorageResult};

use super::{
    super::{JournalStore, TableStateObservation},
    allocation::allocate_partitions,
    expiration_victim::{encode_expiration_child, load_expiration_victim},
};
use crate::native_record::corrupt_record;

struct PreparedExpiration {
    winner: WinnerProvenance,
    partition: JournalPartition,
}

/// Candidate eligibility has already been observed by the shared TTL owner.
/// Original history, deterministic child disposition, source links and native
/// deletion effects remain in this caller-owned attempt.
pub(crate) async fn capture_expiration_deletions<
    A: stream_provider::retained_image::RetainedOwnershipAttempt,
>(
    journal: &JournalStore,
    attempt: &mut A,
    input: (&TableStateObservation, &[AcceptedDeleteItem], u64),
    codec: &CanonicalJsonCodec,
) -> StorageResult<Vec<ItemProvenance>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let (table, items, wall_ms) = input;
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut accepted = super::super::source_acceptance::PreparedSourceAcceptance::default();
        let mut prepared = Vec::with_capacity(items.len());
        for item in items {
            prepared.push(
                prepare_expiration(journal, attempt, (table, item, codec), &mut accepted).await?,
            );
        }
        accepted.require_unaccepted(journal, attempt).await?;
        let (publications, provenance) =
            allocate_expiration_publications(journal, attempt, (table, prepared, wall_ms), codec)
                .await?;
        journal
            .store_prepared_source_publications(attempt, publications, accepted)
            .await?;
        Ok(provenance)
    }
    .await;
    settlement.complete(result)
}

async fn allocate_expiration_publications<
    A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
>(
    journal: &JournalStore,
    attempt: &mut A,
    input: (&TableStateObservation, Vec<PreparedExpiration>, u64),
    codec: &CanonicalJsonCodec,
) -> StorageResult<(
    Vec<(super::super::PartitionAllocation, Vec<CanonicalEventLink>)>,
    Vec<ItemProvenance>,
)> {
    let (table, prepared, wall_ms) = input;
    let clocks = prepared
        .iter()
        .map(|item| (item.partition, victim_clock(&item.winner)));
    let mut allocations = allocate_partitions(journal, attempt, (clocks, wall_ms, None)).await?;
    let mut links: BTreeMap<JournalPartition, Vec<CanonicalEventLink>> = BTreeMap::new();
    let mut provenance = Vec::with_capacity(prepared.len());
    for (ordinal, item) in prepared.iter().enumerate() {
        let allocation = allocations
            .get(&item.partition)
            .ok_or_else(corrupt_record)?;
        links
            .entry(item.partition)
            .or_default()
            .push(CanonicalEventLink {
                position: allocation.position(),
                publication_clock: allocation.clock(),
                ordinal: u32::try_from(ordinal).map_err(|_| corrupt_record())?.into(),
                table: table.state().configuration.table,
                lineage: item.winner.lineage,
                event: item.winner.event.clone().ok_or_else(corrupt_record)?,
                digest: item.winner.digest,
            });
        provenance.push(
            codec
                .encode_winner_provenance(&item.winner)
                .map_err(|_| corrupt_record())?,
        );
    }
    let publications = links
        .into_iter()
        .map(|(partition, events)| {
            Ok((
                allocations.remove(&partition).ok_or_else(corrupt_record)?,
                events,
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    if !allocations.is_empty() {
        return Err(corrupt_record());
    }
    Ok((publications, provenance))
}

async fn prepare_expiration<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
    journal: &JournalStore,
    attempt: &mut A,
    input: (
        &TableStateObservation,
        &AcceptedDeleteItem,
        &CanonicalJsonCodec,
    ),
    accepted: &mut super::super::source_acceptance::PreparedSourceAcceptance,
) -> StorageResult<PreparedExpiration> {
    let (table, _, codec) = input;
    let victim = load_expiration_victim(journal, attempt, input).await?;
    let event = encode_expiration_child(&victim, codec)?;
    let winner = event.winner().clone();
    if (WinnerEffectPlan {
        incoming: &winner,
        observed: Some(&victim.provenance),
        known_identity: None,
    })
    .decide_winner()
    .map_err(|_| corrupt_record())?
        != WinnerDecision::Replace
    {
        return Err(corrupt_record());
    }
    let partition = journal
        .partitions
        .select(table.state().configuration.lineage, &winner.key);
    let receipt = CanonicalAcceptanceReceipt {
        event: event.publication().identity().clone(),
        digest: event.publication().digest(),
        disposition: EventDisposition::WinnerApplied,
    };
    accepted.include_unpublished(
        journal,
        ((&event).into(), &receipt, Some(table.state().physical)),
    )?;
    Ok(PreparedExpiration { winner, partition })
}

fn victim_clock(winner: &WinnerProvenance) -> Option<HybridLogicalClock> {
    match &winner.order {
        WinnerOrder::Mutation { clock, .. } => Some(*clock),
        WinnerOrder::Baseline { .. } => None,
    }
}
