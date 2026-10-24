//! Complete table source vectors share discovery custody and source-frame
//! integrity.

use std::collections::{BTreeMap, BTreeSet};

use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::{
    configuration::{
        installed::{InstalledTableState, RegionalSourceBinding},
        membership::MembershipRevision,
    },
    identity::{origin::OriginId, resource::TableLineage},
};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult};

use super::JournalStore;
use crate::history::{
    response_budget::encoded_length,
    source_cut::CanonicalSourceCut,
    source_selection::CanonicalSourceSelection,
    table_cut::{CanonicalTableCut, CanonicalTableCutAction, CanonicalTableCutRequest},
};

struct SourceVector {
    cut: CanonicalTableCut,
    pending: Option<BTreeMap<SourceVectorKey, CanonicalSourceCut>>,
    seen: BTreeSet<SourceVectorKey>,
    remaining: usize,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct SourceVectorKey {
    origin: OriginId,
    table: replication_types::identity::resource::TableIdentity,
    config: replication_types::configuration::table::ConfigRevision,
}

impl SourceVectorKey {
    fn from_source(origin: &OriginId, source: &RegionalSourceBinding) -> Self {
        Self {
            origin: origin.clone(),
            table: source.table,
            config: source.config,
        }
    }
}

impl SourceVector {
    fn try_new(
        scope: (
            TableLineage,
            MembershipRevision,
            crate::history::trim_authority::TrimRevision,
        ),
        candidate: Option<&CanonicalTableCut>,
        maximum: usize,
    ) -> StorageResult<Self> {
        let cut = CanonicalTableCut {
            trim: scope.2,
            lineage: scope.0,
            membership: scope.1,
            sources: Vec::new(),
        };
        let remaining = maximum
            .checked_sub(encoded_length(&cut, maximum)?)
            .ok_or_else(vector_too_large)?;
        if candidate.is_some_and(|candidate| candidate.trim != scope.2) {
            return Err(storage_types::StorageEnum::HistoryExpired.into());
        }
        let pending = candidate
            .map(|candidate| qualify_candidate((scope.0, scope.1), candidate, remaining))
            .transpose()?;
        Ok(Self {
            cut,
            pending,
            seen: BTreeSet::new(),
            remaining,
        })
    }

    fn select_cut(
        &mut self,
        selection: &CanonicalSourceSelection,
    ) -> StorageResult<Option<CanonicalSourceCut>> {
        let Some(pending) = &mut self.pending else {
            return Ok(None);
        };
        let key = SourceVectorKey::from_source(&selection.origin, &selection.source);
        let cut = pending.remove(&key).ok_or_else(incomplete_vector)?;
        cut.require_selection(selection)?;
        Ok(Some(cut))
    }

    fn retain_cut(&mut self, cut: CanonicalSourceCut) -> StorageResult<()> {
        cut.require_source()?;
        if cut.trim != self.cut.trim {
            return Err(incomplete_vector());
        }
        self.remaining = self
            .remaining
            .checked_sub(
                encoded_length(&cut, self.remaining)?
                    .checked_add(1)
                    .ok_or_else(vector_too_large)?,
            )
            .ok_or_else(vector_too_large)?;
        if !self
            .seen
            .insert(SourceVectorKey::from_source(&cut.origin, &cut.source))
        {
            return Err(crate::native_record::corrupt_record());
        }
        self.cut.sources.push(cut);
        Ok(())
    }

    fn finish(self) -> StorageResult<CanonicalTableCut> {
        self.cut.require_inventory()?;
        if self.pending.is_some_and(|pending| !pending.is_empty()) {
            return Err(incomplete_vector());
        }
        Ok(self.cut)
    }
}

impl JournalStore {
    pub(crate) async fn observe_table_cut<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &CanonicalTableCutRequest,
            crate::transport::SourceReservation,
        ),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<crate::history::table_cut::CanonicalTableCutObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let cut = self
                .load_table_source_vector(attempt, input.0, encoding, ReadProtection::Snapshot)
                .await?;
            let observation = crate::history::table_cut::CanonicalTableCutObservation {
                cut,
                _source: input.1,
            };
            encoded_length(&observation, encoding.0.frame_bytes.get())?;
            Ok(observation)
        }
        .await;
        settlement.complete(crate::history::retention::settle_expiry(result))?
    }

    /// A native held receipt fixes its original inventory independently of
    /// today's membership. External cuts must use load_table_source_vector.
    pub(in crate::journal) async fn verify_held_source_vector<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&storage_types::TableName, &CanonicalTableCut),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<CanonicalTableCut> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (table, cut) = input;
            let mut vector = SourceVector::try_new(
                (cut.lineage, cut.membership, cut.trim),
                Some(cut),
                encoding.0.frame_bytes.get(),
            )?;
            for component in &cut.sources {
                let source = &component.source;
                let selection = CanonicalSourceSelection {
                    serving_region: self.region.clone(),
                    serving_group: self.group,
                    table: table.clone(),
                    lineage: cut.lineage,
                    membership: cut.membership,
                    source: source.clone(),
                    origin: component.origin.clone(),
                };
                vector
                    .select_cut(&selection)?
                    .ok_or_else(crate::native_record::corrupt_record)?;
                let manifest = self
                    .load_history_terminal_manifest(
                        attempt,
                        (&selection, component.through),
                        encoding,
                    )
                    .await?;
                if manifest != component.terminal_manifest {
                    return Err(crate::native_record::corrupt_record());
                }
                vector.retain_cut(component.clone())?;
            }
            vector.finish()
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_table_source_vector<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &CanonicalTableCutRequest,
        encoding: (&WorkLimits, &CanonicalJsonCodec),
        protection: ReadProtection,
    ) -> StorageResult<CanonicalTableCut> {
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
                .observe_history_table(attempt, &physical, protection)
                .await?;
            let state = observed.state();
            let candidate = match &request.table_cut {
                CanonicalTableCutAction::Capture
                | CanonicalTableCutAction::CaptureHistorical { .. } => None,
                CanonicalTableCutAction::Verify(cut) => Some(cut),
            };
            let scope = match &request.table_cut {
                CanonicalTableCutAction::Capture => (state.configuration.lineage, state.membership),
                CanonicalTableCutAction::CaptureHistorical {
                    lineage,
                    membership,
                } => (*lineage, *membership),
                CanonicalTableCutAction::Verify(cut) => (cut.lineage, cut.membership),
            };
            if scope.0 != state.configuration.lineage {
                return self
                    .load_historical_source_vector(
                        attempt,
                        state,
                        (request, candidate),
                        (encoding.0, encoding.1, protection),
                    )
                    .await;
            }
            if scope.1 != state.membership {
                return Err(StorageError::validation(
                    "Canonical source vector table revision changed",
                ));
            }
            let mut vector = SourceVector::try_new(
                (
                    state.configuration.lineage,
                    state.membership,
                    observed.authority.revision(),
                ),
                candidate,
                encoding.0.frame_bytes.get(),
            )?;
            for binding in state
                .sources
                .iter()
                .filter(|binding| self.is_current_history_source(state, binding))
            {
                self.append_source_vector(
                    attempt,
                    (state, binding, &observed.authority),
                    (request, encoding, protection),
                    &mut vector,
                )
                .await?;
            }
            let mut after = None;
            while let Some(authority) = self
                .find_next_retired_source(attempt, state, after.as_ref(), protection)
                .await?
            {
                let binding = authority.source()?;
                self.append_source_vector(
                    attempt,
                    (state, binding, &observed.authority),
                    (request, encoding, protection),
                    &mut vector,
                )
                .await?;
                after = Some(binding.clone());
            }
            vector.finish()
        }
        .await;
        settlement.complete(result)
    }

    async fn load_historical_source_vector<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        input: (&CanonicalTableCutRequest, Option<&CanonicalTableCut>),
        encoding: (&WorkLimits, &CanonicalJsonCodec, ReadProtection),
    ) -> StorageResult<CanonicalTableCut> {
        let scope = match &input.0.table_cut {
            CanonicalTableCutAction::CaptureHistorical {
                lineage,
                membership,
            } => (*lineage, *membership),
            CanonicalTableCutAction::Verify(cut) => (cut.lineage, cut.membership),
            CanonicalTableCutAction::Capture => return Err(crate::native_record::corrupt_record()),
        };
        let cuts = self
            .load_installed_history_vector(
                attempt,
                state,
                (scope, encoding.0.frame_bytes.get()),
                encoding.2,
            )
            .await?;
        let authority = self
            .load_public_trim_head(attempt, (state, scope.0), encoding.2)
            .await?;
        let mut vector = SourceVector::try_new(
            (scope.0, scope.1, authority.revision()),
            input.1,
            encoding.0.frame_bytes.get(),
        )?;
        for native in cuts {
            let selection = CanonicalSourceSelection {
                serving_region: input.0.serving_region.clone(),
                serving_group: input.0.serving_group,
                table: input.0.table.clone(),
                lineage: native.lineage,
                membership: native.membership,
                source: native.source.clone(),
                origin: native.origin.clone(),
            };
            let supplied = vector.select_cut(&selection)?;
            let bounds = authority.retired_source_bounds(&selection, native.through)?;
            let mut cut = match supplied {
                Some(cut) => {
                    bounds.require_cut(&cut)?;
                    cut
                }
                None => CanonicalSourceCut {
                    trim: bounds.trim,
                    after: bounds.after,
                    ..native.clone()
                },
            };
            crate::history::source_selection::AvailableSourcePrefix::Retired(bounds)
                .require_through(cut.through)?;
            let terminal = self
                .load_history_terminal_manifest(
                    attempt,
                    (&selection, cut.through),
                    (encoding.0, encoding.1),
                )
                .await?;
            if terminal != cut.terminal_manifest {
                return Err(StorageError::validation(
                    "Canonical source vector terminal manifest does not match native history",
                ));
            }
            cut.terminal_manifest = terminal;
            vector.retain_cut(cut)?;
        }
        vector.finish()
    }

    async fn append_source_vector<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        source: (
            &InstalledTableState,
            &RegionalSourceBinding,
            &super::public_trim_authority::PublicTrimAuthority,
        ),
        input: (
            &CanonicalTableCutRequest,
            (&WorkLimits, &CanonicalJsonCodec),
            ReadProtection,
        ),
        vector: &mut SourceVector,
    ) -> StorageResult<()> {
        let (state, binding, authority) = source;
        for partition in 0..u32::from(binding.partitions) {
            let origin = OriginId {
                region: binding.region.clone(),
                database_group: binding.database_group,
                journal_generation: binding.generation,
                journal_partition: u16::try_from(partition)
                    .map_err(|_| crate::native_record::corrupt_record())?
                    .into(),
            };
            let selection = CanonicalSourceSelection {
                serving_region: input.0.serving_region.clone(),
                serving_group: input.0.serving_group,
                table: input.0.table.clone(),
                lineage: state.configuration.lineage,
                membership: state.membership,
                source: binding.clone(),
                origin,
            };
            let available = self
                .load_authorized_source_prefix(attempt, (state, authority), &selection, input.2)
                .await?;
            let supplied = vector.select_cut(&selection)?;
            let through = supplied
                .as_ref()
                .map_or(available.through(), |cut| cut.through);
            available.require_through(through)?;
            if let Some(cut) = &supplied {
                available.bounds().require_cut(cut)?;
            }
            let terminal_manifest = self
                .load_history_terminal_manifest(attempt, (&selection, through), input.1)
                .await?;
            if supplied
                .as_ref()
                .is_some_and(|cut| cut.terminal_manifest != terminal_manifest)
            {
                return Err(StorageError::validation(
                    "Canonical source vector terminal manifest does not match native history",
                ));
            }
            vector.retain_cut(CanonicalSourceCut {
                trim: available.bounds().trim,
                after: available.bounds().after,
                lineage: selection.lineage,
                membership: selection.membership,
                source: selection.source,
                origin: selection.origin,
                through,
                terminal_manifest,
            })?;
        }
        Ok(())
    }
}

fn qualify_candidate(
    scope: (TableLineage, MembershipRevision),
    candidate: &CanonicalTableCut,
    maximum: usize,
) -> StorageResult<BTreeMap<SourceVectorKey, CanonicalSourceCut>> {
    candidate.require_inventory()?;
    if (candidate.lineage, candidate.membership) != scope {
        return Err(StorageError::validation(
            "Canonical source vector table revision changed",
        ));
    }
    let mut remaining = maximum;
    let mut pending = BTreeMap::new();
    for cut in &candidate.sources {
        remaining = remaining
            .checked_sub(
                encoded_length(cut, remaining)?
                    .checked_add(1)
                    .ok_or_else(vector_too_large)?,
            )
            .ok_or_else(vector_too_large)?;
        pending.insert(
            SourceVectorKey::from_source(&cut.origin, &cut.source),
            cut.clone(),
        );
    }
    Ok(pending)
}

fn vector_too_large() -> StorageError {
    StorageError::validation(
        "Canonical history response exceeds the configured source frame budget",
    )
}
fn incomplete_vector() -> StorageError {
    StorageError::validation(
        "Canonical source vector does not match the complete native source inventory",
    )
}
