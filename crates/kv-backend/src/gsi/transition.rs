//! Atomic KV GSI build effects on the caller's native write attempt.

use storage_backfill::GsiBuild;
use storage_provider::{TtlPolicyAttempt, maintenance::GsiTransition};
use storage_types::{BoundTableInfo, StorageResult};

use super::{GsiWriteInput, catalog::build_mutations, prepare_gsi_writes, read::target_index};
use crate::{
    KvAttempt, catalog::KvCatalogAttempt, item_write::apply_ttl_index_writes, maintenance,
};

pub(crate) async fn apply_gsi_transition<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    table: &BoundTableInfo,
    transition: GsiTransition<'_, Vec<u8>>,
    progress: storage_provider::maintenance::ProgressWrite,
) -> StorageResult<()> {
    let identity = catalog.load_table_identity(table).await?;
    let index_id = target_index(&identity, &transition.expected.target)?;
    // Only a phase change that writers see changes the binding and the
    // stamp; a build page stores job state only.
    let binding = transition.next_binding(&catalog.current_binding(table))?;
    let GsiTransition {
        expected_revision: _,
        next_progress: _,
        expected,
        projections,
        primary_ttl_metadata: _,
        next_state,
        ttl_activation,
    } = transition;
    // Candidate discovery protected the runnable entry in this attempt.
    // A concurrent build transition changes that entry and aborts this commit.
    let (projections, ttl_writes) = projections.separate_ttl_writes()?;
    prepare_gsi_writes(
        catalog,
        GsiWriteInput {
            table,
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
    if let Some(binding) = binding {
        catalog
            .replace_table_layout(table, identity, table.info.as_ref().clone(), binding)
            .await?;
    }
    if let Some(activation) = ttl_activation {
        catalog
            .write_ttl_config(table, activation.activated.clone())
            .await?;
    }
    maintenance::store_progress(catalog.attempt, progress)
}
