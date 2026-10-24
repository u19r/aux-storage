use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::installed::{InstalledTableState, RegionalSourceBinding},
    identity::origin::OriginId,
};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult};

use super::{JournalStore, retirement::RetiredSourceAuthority};
use crate::{
    history::{
        discovery::{
            CanonicalHistoryDiscoveryRequest, CanonicalHistorySources, DiscoveredHistorySource,
            HistorySourceCursor, HistorySourceFrontier, HistorySourcePosition,
        },
        source_selection::{AvailableSourcePrefix, CanonicalSourceSelection},
    },
    native_record::corrupt_record,
};

pub(in crate::journal) struct SourceDiscoveryPage {
    pub(in crate::journal) source: Option<DiscoveredHistorySource>,
    pub(in crate::journal) available: Vec<HistorySourceFrontier>,
    pub(in crate::journal) next: Option<HistorySourcePosition>,
}

impl JournalStore {
    pub(crate) async fn load_history_sources<A: CatalogAttempt + PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &CanonicalHistoryDiscoveryRequest,
        budget: (
            &replication_provider::runtime_configuration::limits::WorkLimits,
            crate::transport::SourceReservation,
        ),
    ) -> StorageResult<CanonicalHistorySources> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let stamp = attempt
                .read_catalog_stamp(&request.table)
                .await?
                .ok_or_else(|| StorageError::table_not_found(&request.table))?;
            let physical = attempt.read_table_at_stamp(&request.table, stamp).await?;
            let observed = self
                .observe_history_table(attempt, &physical, storage_types::ReadProtection::Snapshot)
                .await?;
            let state = observed.state();
            if request
                .after
                .as_ref()
                .is_some_and(|cursor| cursor.trim != observed.authority.revision())
            {
                return Err(storage_types::StorageEnum::HistoryExpired.into());
            }
            if request.after.as_ref().is_some_and(|cursor| {
                cursor.physical != state.physical
                    || cursor.lineage != state.configuration.lineage
                    || cursor.membership != state.membership
            }) {
                return Err(StorageError::validation(
                    "History table revision changed; restart discovery",
                ));
            }
            let page = match request.after.as_ref().map(|cursor| &cursor.next) {
                Some(HistorySourcePosition::Historical { after }) => {
                    self.load_historical_discovery_page(
                        attempt,
                        state,
                        after.clone(),
                        ReadProtection::Snapshot,
                    )
                    .await?
                }
                Some(HistorySourcePosition::Retired { source, partition }) => {
                    let authority = self
                        .load_retired_source_authority(
                            attempt,
                            state,
                            source,
                            ReadProtection::Snapshot,
                        )
                        .await?;
                    self.load_retired_discovery_page(attempt, state, authority, *partition)
                        .await?
                }
                next => {
                    self.load_current_discovery_page(attempt, state, next)
                        .await?
                }
            };
            let continuation = page.next.map(|next| HistorySourceCursor {
                trim: observed.authority.revision(),
                physical: state.physical,
                lineage: state.configuration.lineage,
                membership: state.membership,
                next,
            });
            let response = CanonicalHistorySources {
                lineage: state.configuration.lineage,
                membership: state.membership,
                source: page.source,
                available: page.available,
                continuation,
                _source: budget.1,
            };
            crate::history::response_budget::encoded_length(&response, budget.0.frame_bytes.get())?;
            Ok(response)
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) fn is_current_history_source(
        &self,
        state: &InstalledTableState,
        source: &RegionalSourceBinding,
    ) -> bool {
        match &state.configuration.regional {
            replication_types::configuration::table::RegionalMode::Global { members, .. } => {
                members.contains(&source.region)
            }
            replication_types::configuration::table::RegionalMode::Local => {
                source.region == self.region
                    && source.database_group == self.group
                    && source.generation == self.generation
            }
        }
    }

    pub(in crate::journal) async fn load_authorized_source_prefix<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: (
            &InstalledTableState,
            &super::public_trim_authority::PublicTrimAuthority,
        ),
        selection: &CanonicalSourceSelection,
        protection: ReadProtection,
    ) -> StorageResult<AvailableSourcePrefix> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (state, authority) = scope;
            let source = &selection.source;
            if source.region != selection.origin.region
                || source.database_group != selection.origin.database_group
                || source.generation != selection.origin.journal_generation
            {
                return Err(StorageError::validation(
                    "History origin does not match its source",
                ));
            }
            source
                .partitions
                .require_partition(selection.origin.journal_partition)
                .map_err(|_| StorageError::validation("Invalid history source partition"))?;
            if state.sources.iter().any(|installed| {
                installed == source && self.is_current_history_source(state, installed)
            }) {
                let frontier = self
                    .load_history_frontiers(
                        attempt,
                        state,
                        (source, std::slice::from_ref(&selection.origin)),
                        protection,
                    )
                    .await?
                    .pop()
                    .ok_or_else(corrupt_record)?;
                return Ok(AvailableSourcePrefix::Current(
                    authority.source_bounds(selection, frontier.through)?,
                ));
            }
            let retired = self
                .load_retired_source_authority(attempt, state, source, protection)
                .await?;
            let cuts = self
                .load_retired_source_cuts(
                    attempt,
                    state,
                    (&retired, std::slice::from_ref(&selection.origin)),
                    protection,
                )
                .await?;
            Ok(AvailableSourcePrefix::Retired(
                authority.retired_source_bounds(
                    selection,
                    cuts.first().ok_or_else(corrupt_record)?.through,
                )?,
            ))
        }
        .await;
        settlement.complete(result)
    }

    async fn load_current_discovery_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        next: Option<&HistorySourcePosition>,
    ) -> StorageResult<SourceDiscoveryPage> {
        let sources = state
            .sources
            .iter()
            .filter(|source| self.is_current_history_source(state, source))
            .collect::<Vec<_>>();
        let Some((index, first)) = select_current_source(&sources, next)? else {
            return self.load_first_retired_page(attempt, state).await;
        };
        let binding = sources[index];
        let end = self.history_partition_end(binding, first)?;
        let origins = (first..end)
            .map(|partition| source_origin(binding, partition))
            .collect::<StorageResult<Vec<_>>>()?;
        let available = self
            .load_history_frontiers(
                attempt,
                state,
                (binding, &origins),
                ReadProtection::Snapshot,
            )
            .await?;
        let next = if end < u32::from(binding.partitions) {
            Some(HistorySourcePosition::Current {
                origin: source_origin(binding, end)?,
            })
        } else if let Some(source) = sources.get(index + 1) {
            Some(HistorySourcePosition::Current {
                origin: source_origin(source, 0)?,
            })
        } else {
            self.find_next_retired_source(attempt, state, None, ReadProtection::Snapshot)
                .await?
                .map(|authority| retired_start(&authority))
                .transpose()?
                .or(Some(HistorySourcePosition::Historical { after: None }))
        };
        Ok(SourceDiscoveryPage {
            source: Some(DiscoveredHistorySource::Current(binding.clone())),
            available,
            next,
        })
    }

    async fn load_first_retired_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
    ) -> StorageResult<SourceDiscoveryPage> {
        match self
            .find_next_retired_source(attempt, state, None, ReadProtection::Snapshot)
            .await?
        {
            Some(authority) => {
                self.load_retired_discovery_page(attempt, state, authority, 0)
                    .await
            }
            None => {
                self.load_historical_discovery_page(attempt, state, None, ReadProtection::Snapshot)
                    .await
            }
        }
    }

    async fn load_retired_discovery_page<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        authority: RetiredSourceAuthority<'static>,
        first: u32,
    ) -> StorageResult<SourceDiscoveryPage> {
        let binding = authority.source()?;
        let end = self.history_partition_end(binding, first)?;
        let origins = (first..end)
            .map(|partition| source_origin(binding, partition))
            .collect::<StorageResult<Vec<_>>>()?;
        let cuts = self
            .load_retired_source_cuts(
                attempt,
                state,
                (&authority, &origins),
                ReadProtection::Snapshot,
            )
            .await?;
        let available = cuts
            .into_iter()
            .map(|cut| HistorySourceFrontier {
                origin: cut.origin,
                through: cut.through,
            })
            .collect();
        let next = if end < u32::from(binding.partitions) {
            Some(HistorySourcePosition::Retired {
                source: binding.clone(),
                partition: end,
            })
        } else {
            self.find_next_retired_source(attempt, state, Some(binding), ReadProtection::Snapshot)
                .await?
                .map(|authority| retired_start(&authority))
                .transpose()?
                .or(Some(HistorySourcePosition::Historical { after: None }))
        };
        Ok(SourceDiscoveryPage {
            source: Some(DiscoveredHistorySource::Retired(binding.clone())),
            available,
            next,
        })
    }

    fn history_partition_end(
        &self,
        binding: &RegionalSourceBinding,
        first: u32,
    ) -> StorageResult<u32> {
        let count = u32::from(binding.partitions);
        if first >= count {
            return Err(StorageError::validation(
                "Invalid history discovery partition",
            ));
        }
        let width = u32::try_from(
            self.records
                .limits
                .max_records()
                .get()
                .min(crate::constants::HISTORY_CUT_PAGE_WIDTH),
        )
        .map_err(|_| corrupt_record())?;
        Ok(first.saturating_add(width).min(count))
    }

    async fn load_history_frontiers<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        scope: (&RegionalSourceBinding, &[OriginId]),
        protection: ReadProtection,
    ) -> StorageResult<Vec<HistorySourceFrontier>> {
        let (binding, origins) = scope;
        let positions = if origins
            .first()
            .is_some_and(|origin| self.is_local_origin(origin))
        {
            let partitions = origins
                .iter()
                .map(|origin| origin.journal_partition)
                .collect::<Vec<_>>();
            self.load_source_heads(attempt, &partitions, protection)
                .await?
        } else {
            self.load_received_coverages(
                attempt,
                (
                    state.configuration.table,
                    binding.table,
                    state.configuration.lineage,
                ),
                origins,
                protection,
            )
            .await?
            .into_iter()
            .map(|coverage| coverage.through())
            .collect()
        };
        Ok(origins
            .iter()
            .cloned()
            .zip(positions)
            .map(|(origin, through)| HistorySourceFrontier { origin, through })
            .collect())
    }
}

fn select_current_source(
    sources: &[&RegionalSourceBinding],
    next: Option<&HistorySourcePosition>,
) -> StorageResult<Option<(usize, u32)>> {
    let Some(HistorySourcePosition::Current { origin }) = next else {
        return Ok((!sources.is_empty()).then_some((0, 0)));
    };
    let index = sources
        .iter()
        .position(|source| {
            source.region == origin.region
                && source.database_group == origin.database_group
                && source.generation == origin.journal_generation
        })
        .ok_or_else(|| StorageError::validation("History discovery source is not installed"))?;
    sources[index]
        .partitions
        .require_partition(origin.journal_partition)
        .map_err(|_| StorageError::validation("Invalid history discovery partition"))?;
    Ok(Some((
        index,
        u32::from(u16::from(origin.journal_partition)),
    )))
}

fn source_origin(source: &RegionalSourceBinding, partition: u32) -> StorageResult<OriginId> {
    Ok(OriginId {
        region: source.region.clone(),
        database_group: source.database_group,
        journal_generation: source.generation,
        journal_partition: u16::try_from(partition)
            .map_err(|_| corrupt_record())?
            .into(),
    })
}

fn retired_start(authority: &RetiredSourceAuthority<'_>) -> StorageResult<HistorySourcePosition> {
    Ok(HistorySourcePosition::Retired {
        source: authority.source()?.clone(),
        partition: 0,
    })
}
