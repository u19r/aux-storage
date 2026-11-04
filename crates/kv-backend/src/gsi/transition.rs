//! Atomic KV GSI build effects on the caller's native write attempt.

use storage_backfill::GsiBuild;
use storage_provider::{GsiWriteSource, UpdateTimeToLiveAttempt, maintenance::GsiTransition};
use storage_types::{BoundTableInfo, StorageResult, maintenance::MaintenanceKind};

use super::{GsiWriteInput, catalog::build_mutations, prepare_gsi_writes, read::target_index};
use crate::{
    KvAttempt, catalog::KvCatalogAttempt, item_write::apply_ttl_index_writes, maintenance,
};

pub(crate) async fn apply_gsi_transition<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    table: &BoundTableInfo,
    transition: GsiTransition<'_, Vec<u8>>,
    observed_progress: &maintenance::ProgressFence,
) -> StorageResult<()> {
    let identity = catalog.identity_for(&table.info.table_name)?.clone();
    let index_id = target_index(&identity, &transition.expected.target)?;
    let GsiTransition {
        expected_revision,
        next_progress,
        expected,
        projections,
        next_state,
        ttl_activation,
    } = transition;
    // Candidate discovery protected the runnable entry in this attempt.
    // A concurrent build transition changes that entry and aborts this commit.
    let (projections, ttl_writes) = projections.separate_ttl_writes()?;
    prepare_gsi_writes(
        catalog,
        GsiWriteInput {
            identity: &identity,
            table,
            source: GsiWriteSource::Backfill,
            writes: projections,
            prepared: &mut Vec::new(),
        },
    )?;
    catalog
        .attempt
        .stage(std::mem::take(&mut catalog.projection_changes).into_mutations()?)?;
    apply_ttl_index_writes(catalog.attempt, identity.table_id, ttl_writes).await?;
    let next = GsiBuild {
        target: expected.target.clone(),
        state: next_state,
    };
    catalog
        .attempt
        .stage(build_mutations(identity.table_id, index_id, &next)?.into())?;
    if let Some(activation) = ttl_activation {
        catalog
            .write_ttl_config(table, activation.activated.clone())
            .await?;
    }
    maintenance::write_progress(
        catalog.attempt,
        MaintenanceKind::GsiBackfill,
        observed_progress,
        expected_revision,
        next_progress,
    )
}
