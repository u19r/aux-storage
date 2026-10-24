//! One native lineage, source or original partition per inventory turn.
use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::DurableRecordKey,
};
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::{
    configuration::{installed::RegionalSourceBinding, proposal::ControlProposal},
    identity::origin::OriginId,
};
use storage_types::{ReadProtection, StorageResult};

use super::record::{InventoryPhase, InventoryRecord, RetainedCut, RetainedLineage};
use crate::{
    constants::REENABLE_INVENTORY_SUFFIX,
    history::{source_cut::CanonicalSourceCut, source_selection::CanonicalSourceSelection},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn advance_reenable_inventory<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        encoding: (&CanonicalJsonCodec, &WorkLimits),
    ) -> StorageResult<bool> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let Some((row, mut root)) = self
                .load_reenable_inventory(attempt, proposal.operation().id, ReadProtection::Conflict)
                .await?
            else {
                let before = proposal
                    .participant(&self.region)
                    .map_err(|_| corrupt_record())?
                    .before
                    .as_ref()
                    .ok_or_else(corrupt_record)?;
                return if u64::from(before.configuration.config) == 0 {
                    Ok(true)
                } else {
                    Err(corrupt_record())
                };
            };
            if root.proposal != *proposal {
                return Err(corrupt_record());
            }
            self.require_reenable_custody(attempt, &root, ReadProtection::Shared)
                .await?;
            match root.phase.clone() {
                InventoryPhase::Lineages { after } => {
                    self.select_inventory_lineage(attempt, &mut root, after, encoding.0)
                        .await?
                }
                InventoryPhase::Baselines { .. } => {
                    self.capture_retained_baselines(attempt, &mut root, encoding)
                        .await?
                }
                InventoryPhase::Survivor {
                    lineage,
                    partition,
                    next,
                } => {
                    self.capture_survivor_cut(
                        attempt,
                        &mut root,
                        (lineage, partition, next),
                        encoding,
                    )
                    .await?
                }
                InventoryPhase::Catalog {
                    lineage,
                    after,
                    next,
                } => {
                    self.select_inventory_retired(attempt, &mut root, (lineage, after, next))
                        .await?
                }
                InventoryPhase::Retired {
                    lineage,
                    source,
                    partition,
                    next,
                } => {
                    self.capture_retired_cut(
                        attempt,
                        &mut root,
                        (lineage, source, partition, next),
                        encoding,
                    )
                    .await?
                }
                InventoryPhase::Inherited { .. } => {
                    self.capture_inherited_inventory(attempt, &mut root).await?
                }
                InventoryPhase::Ready => return Ok(true),
                InventoryPhase::Released | InventoryPhase::Cancelled => {
                    return Err(corrupt_record());
                }
            }
            self.retain_history_epoch(attempt, proposal.operation().lineage)
                .await?;
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(
                        self.identity_key(REENABLE_INVENTORY_SUFFIX, &proposal.operation().id)?,
                        Some(row.revision),
                        &root,
                    )?],
                )
                .await?;
            Ok(matches!(root.phase, InventoryPhase::Ready))
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn select_inventory_lineage<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        root: &mut InventoryRecord,
        after: Option<DurableRecordKey>,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let first = self.retained_lineage_prefix(root.proposal.operation().table)?;
        let bounds = self.record_prefix_range(&first)?;
        let range = BoundedRecordRange::try_new(
            first,
            bounds.bounds().1.clone(),
            std::num::NonZeroUsize::MIN,
        )?;
        if after.as_ref().is_some_and(|after| !range.contains(after)) {
            return Err(corrupt_record());
        }
        let page = self
            .records
            .scan(
                attempt,
                RecordPageRequest {
                    range,
                    after,
                    max_bytes: self.records.limits.max_bytes(),
                    protection: ReadProtection::Shared,
                },
            )
            .await?;
        let Some(row) = page.records.into_iter().next() else {
            if root.cuts == 0 {
                return Err(corrupt_record());
            }
            self.begin_inherited_inventory(attempt, root).await?;
            return Ok(());
        };
        let lineage: RetainedLineage = self.records.decode(&row)?;
        self.require_retained_lineage(&root.proposal, &lineage)?;
        if row.key != self.retained_lineage_key(&lineage)? {
            return Err(corrupt_record());
        }
        let receipt = self
            .load_control(attempt, &lineage.disable, ReadProtection::Shared)
            .await?
            .ok_or_else(corrupt_record)?;
        let receipt = self.decode_control(&receipt, &lineage.disable)?;
        if receipt.decision != crate::control::ControlDecision::Completed {
            return Err(corrupt_record());
        }
        self.require_control_history(attempt, &receipt, (codec, ReadProtection::Shared))
            .await?;
        let initial = self
            .load_lineage_baseline_definition(
                attempt,
                &lineage.survivor,
                storage_types::ReadProtection::Shared,
            )
            .await?;
        let vector = crate::bootstrap::ReenableBaselineDefinition::initial_seed(&initial)?;
        root.phase = InventoryPhase::Baselines {
            lineage,
            initial: Box::new(initial),
            after: None,
            vector,
            count: 0,
            next: row.key,
        };
        Ok(())
    }

    async fn capture_survivor_cut<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        root: &mut InventoryRecord,
        selection: (RetainedLineage, u32, DurableRecordKey),
        encoding: (&CanonicalJsonCodec, &WorkLimits),
    ) -> StorageResult<()> {
        let (lineage, partition, next) = selection;
        let source = lineage
            .survivor
            .sources
            .iter()
            .find(|source| source.region == self.region)
            .ok_or_else(corrupt_record)?;
        let origin = self.inventory_origin(source, partition)?;
        let through = self
            .load_source_heads(
                attempt,
                &[origin.journal_partition],
                ReadProtection::Conflict,
            )
            .await?
            .pop()
            .ok_or_else(corrupt_record)?;
        self.store_inventory_cut(attempt, root, (&lineage, source, origin, through), encoding)
            .await?;
        let partition = partition.checked_add(1).ok_or_else(corrupt_record)?;
        root.phase = if partition == u32::from(source.partitions) {
            InventoryPhase::Catalog {
                lineage,
                after: None,
                next,
            }
        } else {
            InventoryPhase::Survivor {
                lineage,
                partition,
                next,
            }
        };
        Ok(())
    }

    async fn select_inventory_retired<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        root: &mut InventoryRecord,
        selection: (
            RetainedLineage,
            Option<RegionalSourceBinding>,
            DurableRecordKey,
        ),
    ) -> StorageResult<()> {
        let (lineage, after, next) = selection;
        root.phase = match self
            .find_next_retired_source(
                attempt,
                &lineage.survivor,
                after.as_ref(),
                ReadProtection::Shared,
            )
            .await?
        {
            Some(authority) => InventoryPhase::Retired {
                source: authority.source()?.clone(),
                lineage,
                partition: 0,
                next,
            },
            None => InventoryPhase::Lineages { after: Some(next) },
        };
        Ok(())
    }

    async fn capture_retired_cut<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        root: &mut InventoryRecord,
        selection: (
            RetainedLineage,
            RegionalSourceBinding,
            u32,
            DurableRecordKey,
        ),
        encoding: (&CanonicalJsonCodec, &WorkLimits),
    ) -> StorageResult<()> {
        let (lineage, source, partition, next) = selection;
        let authority = self
            .load_retired_source_authority(
                attempt,
                &lineage.survivor,
                &source,
                ReadProtection::Shared,
            )
            .await?;
        let origin = self.inventory_origin(&source, partition)?;
        let cut = self
            .load_retired_source_cuts(
                attempt,
                &lineage.survivor,
                (&authority, std::slice::from_ref(&origin)),
                ReadProtection::Shared,
            )
            .await?
            .pop()
            .ok_or_else(corrupt_record)?;
        self.store_inventory_cut(
            attempt,
            root,
            (&lineage, &source, origin, cut.through),
            encoding,
        )
        .await?;
        let partition = partition.checked_add(1).ok_or_else(corrupt_record)?;
        root.phase = if partition == u32::from(source.partitions) {
            InventoryPhase::Catalog {
                lineage,
                after: Some(source),
                next,
            }
        } else {
            InventoryPhase::Retired {
                lineage,
                source,
                partition,
                next,
            }
        };
        Ok(())
    }

    fn inventory_origin(
        &self,
        source: &RegionalSourceBinding,
        partition: u32,
    ) -> StorageResult<OriginId> {
        if partition >= u32::from(source.partitions) {
            return Err(corrupt_record());
        }
        Ok(OriginId {
            region: source.region.clone(),
            database_group: source.database_group,
            journal_generation: source.generation,
            journal_partition: u16::try_from(partition)
                .map_err(|_| corrupt_record())?
                .into(),
        })
    }

    async fn store_inventory_cut<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        &self,
        attempt: &mut A,
        root: &mut InventoryRecord,
        scope: (
            &RetainedLineage,
            &RegionalSourceBinding,
            OriginId,
            replication_types::identity::origin::OriginPosition,
        ),
        encoding: (&CanonicalJsonCodec, &WorkLimits),
    ) -> StorageResult<()> {
        let (lineage, source, origin, through) = scope;
        let state = &lineage.survivor;
        let authority = self
            .load_public_trim_head(
                attempt,
                (state, state.configuration.lineage),
                ReadProtection::Conflict,
            )
            .await?;
        let selection = CanonicalSourceSelection {
            serving_region: self.region.clone(),
            serving_group: self.group,
            table: state.name.clone(),
            lineage: state.configuration.lineage,
            membership: state.membership,
            source: source.clone(),
            origin: origin.clone(),
        };
        let terminal_manifest = self
            .load_history_terminal_manifest(
                attempt,
                (&selection, through),
                (encoding.1, encoding.0),
            )
            .await?;
        let bounds = if self.is_current_history_source(state, source) {
            authority.source_bounds(&selection, through)?
        } else {
            authority.retired_source_bounds(&selection, through)?
        };
        let cut = CanonicalSourceCut {
            trim: bounds.trim,
            after: bounds.after,
            lineage: selection.lineage,
            membership: selection.membership,
            source: source.clone(),
            origin,
            through,
            terminal_manifest,
        };
        let baseline = self
            .load_reenable_baseline_inventory(
                attempt,
                root.proposal.operation().id,
                &lineage.survivor,
                storage_types::ReadProtection::Shared,
            )
            .await?;
        let history = crate::bootstrap::ReenableHistoryCut {
            ordinal: root.cuts,
            visibility: authority.capture_historical_visibility()?,
            custody: None,
            disable: lineage.disable.clone(),
            survivor: lineage.survivor.clone(),
            cut,
            baseline,
        };
        let local = &root
            .proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?
            .proposed;
        self.store_retained_catalog_cut(attempt, local, (root.proposal.operation().id, &history))
            .await?;
        let key = self.reenable_cut_key(root.proposal.operation().id, &history)?;
        if self
            .records
            .load(
                attempt,
                std::slice::from_ref(&key),
                ReadProtection::Conflict,
            )
            .await?[0]
            .is_some()
        {
            return Err(corrupt_record());
        }
        self.retain_history_epoch(attempt, history.cut.lineage)
            .await?;
        let previous = root.vector;
        root.vector = history.fold_inventory(previous)?;
        root.cuts = root.cuts.checked_add(1).ok_or_else(corrupt_record)?;
        self.records
            .store(
                attempt,
                &[self.records.prepare(
                    key,
                    None,
                    &RetainedCut {
                        previous,
                        operation: root.proposal.operation().id,
                        history,
                    },
                )?],
            )
            .await
    }
}
