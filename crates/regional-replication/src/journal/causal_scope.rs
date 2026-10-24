use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use storage_provider::CatalogAttempt;
use storage_types::{StorageError, StorageResult, TimestampMillis};

use super::{JournalStore, portable_checkpoint::PortableReadScope};
use crate::history::{
    ancestry_scope::{CanonicalAncestryScope, CanonicalAncestryScopes},
    table_cut::CanonicalTableCut,
};

pub(super) struct HeldCausalScope {
    pub declaration: CanonicalAncestryScope,
    pub cut: CanonicalTableCut,
}

impl JournalStore {
    pub(super) async fn load_held_causal_scopes<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &CanonicalAncestryScope,
            &CanonicalAncestryScopes,
            TimestampMillis,
        ),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<Vec<HeldCausalScope>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (root, additional, observed_at) = input;
            additional.require_root(root, encoding.0.frame_events.get())?;
            let mut remaining = encoding.0.frame_bytes.get();
            let mut scopes = Vec::with_capacity(1 + additional.as_ref().len());
            for declaration in std::iter::once(root).chain(additional.as_ref()) {
                let cut = self
                    .load_held_causal_cut(attempt, (declaration, observed_at), encoding)
                    .await?;
                let bytes = crate::history::response_budget::encoded_length(&cut, remaining)?;
                remaining = remaining.checked_sub(bytes).ok_or_else(|| {
                    StorageError::validation("Ancestry cuts exceed the source work budget")
                })?;
                scopes.push(HeldCausalScope {
                    declaration: declaration.clone(),
                    cut,
                });
            }
            Ok(scopes)
        }
        .await;
        settlement.complete(crate::history::retention::settle_expiry(result))?
    }

    async fn load_held_causal_cut<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&CanonicalAncestryScope, TimestampMillis),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<CanonicalTableCut> {
        let (scope, observed_at) = input;
        let held = PortableReadScope {
            table: &scope.table,
            lineage: scope.lineage,
            membership: scope.membership,
        };
        let cut = self
            .load_portable_checkpoint_cut(attempt, held, scope.checkpoint, observed_at)
            .await?;
        self.verify_held_source_vector(attempt, (&scope.table, &cut), encoding)
            .await
    }
}
