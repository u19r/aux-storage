use storage_common::TtlIndexWrite;
use storage_types::{
    BoundTableInfo, StorageEnum, StorageError, StorageResult, StoredTableInfo,
    SystemStreamPosition, SystemStreamScope, WireItem,
};

use super::{
    GsiActivationState, GsiBuild, GsiBuildStatus, GsiBuildTarget, GsiLifecycleError,
    GsiProjectionKey, GsiProjectionLifecycle, GsiProjectionWrite, GsiProjectionWriteBatch,
    GsiState, GsiWriteAction, projection::plan_gsi_write_actions_for,
};

impl GsiBuildTarget {
    pub fn validate_table(&self, table: &BoundTableInfo) -> StorageResult<()> {
        if table.info.table_name != self.table_name
            || !table
                .info
                .global_secondary_indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|index| index.index_name == self.index_name)
        {
            return Err(StorageError::internal("GSI target has changed"));
        }
        Ok(())
    }
}

impl GsiActivationState {
    pub fn try_from_build<Cursor>(
        build: GsiBuild<Cursor>,
        target: &GsiBuildTarget,
    ) -> StorageResult<Self> {
        if &build.target != target {
            return Err(Self::conflict_error());
        }
        let GsiState::CatchingUp(state) = build.state else {
            return Err(Self::conflict_error());
        };
        Ok(Self {
            target_incarnation: build.target.incarnation,
            projected_through: state.cursor,
        })
    }

    pub fn conflict_error() -> StorageError {
        StorageEnum::TransactionConflict {
            message: "GSI build changed before activation".to_owned(),
        }
        .into()
    }
}

impl GsiProjectionWriteBatch {
    pub fn validate_target(&self, target: &GsiBuildTarget) -> StorageResult<()> {
        if self
            .0
            .iter()
            .any(|write| write.key.index_name != target.index_name)
        {
            return Err(StorageError::internal(
                "GSI projection write has another target",
            ));
        }
        Ok(())
    }

    /// Keep the hidden expiration index out of ordinary GSI row encoders.
    pub fn separate_ttl_writes(self) -> StorageResult<(Self, Vec<TtlIndexWrite>)> {
        let mut projections = Vec::new();
        let mut ttl = Vec::new();
        for write in self {
            match (
                write.key.index_name.is_ttl_index(),
                write.key.ttl_index_key.as_ref(),
            ) {
                (true, Some(key)) => ttl.push(if write.row.is_some() {
                    TtlIndexWrite::Upsert(key.clone())
                } else {
                    TtlIndexWrite::Delete(key.clone())
                }),
                (false, None) => projections.push(write),
                _ => {
                    return Err(StorageError::internal(
                        "GSI projection key has an invalid TTL shape",
                    ));
                }
            }
        }
        Ok((Self(projections), ttl))
    }
}

impl<Build: Into<GsiBuildStatus>> TryFrom<Vec<Build>> for GsiProjectionLifecycle {
    type Error = GsiLifecycleError;

    fn try_from(builds: Vec<Build>) -> Result<Self, Self::Error> {
        let builds: Vec<GsiBuildStatus> = builds.into_iter().map(Into::into).collect();
        GsiBuildStatus::require_distinct_targets(&builds)?;
        let mut writable_targets = Vec::with_capacity(builds.len());
        for build in builds {
            if build.phase.permits_projection() {
                writable_targets.push(build.target);
            }
        }
        Ok(Self { writable_targets })
    }
}

impl<Cursor> GsiBuild<Cursor> {
    pub fn validate_candidate_holds(
        &self,
        observed: Option<&Self>,
        holds: &[(SystemStreamScope, SystemStreamPosition)],
    ) -> StorageResult<()>
    where
        Cursor: PartialEq,
    {
        if observed != Some(self) {
            return Err(StorageEnum::TransactionConflict {
                message: "GSI build changed during candidate validation".to_owned(),
            }
            .into());
        }
        let checkpoint = self.state.required_stream_history();
        if holds.len() != checkpoint.frontier().len() {
            return Err(StorageError::internal(
                "GSI private retention hold does not match its build state",
            ));
        }
        for (scope, position) in holds {
            self.validate_private_hold(scope, position)?;
        }
        for required in checkpoint.frontier() {
            if !holds.iter().any(|(_, position)| {
                position.shard_id() == required.shard_id()
                    && position.lineage_id() == required.lineage_id()
                    && position.offset().compare_order(required.offset())
                        == Ok(std::cmp::Ordering::Equal)
            }) {
                return Err(StorageError::internal(
                    "GSI private retention hold does not match its build state",
                ));
            }
        }
        Ok(())
    }

    /// A physical hold is authority only when the referenced build still
    /// requires this exact consumed position in the same stream generation.
    pub fn validate_private_hold(
        &self,
        scope: &SystemStreamScope,
        position: &SystemStreamPosition,
    ) -> StorageResult<()> {
        let checkpoint = self.state.required_stream_history();
        let _offset = position
            .offset()
            .order()
            .map_err(|_| StorageError::internal("GSI private retention hold offset is invalid"))?;
        if checkpoint.scope() != scope
            || !checkpoint.frontier().iter().any(|required| {
                required.shard_id() == position.shard_id()
                    && required.lineage_id() == position.lineage_id()
                    && required.offset().compare_order(position.offset())
                        == Ok(std::cmp::Ordering::Equal)
            })
        {
            return Err(StorageError::internal(
                "GSI private retention hold does not match its build state",
            ));
        }
        Ok(())
    }
}

impl GsiProjectionLifecycle {
    pub fn writable_targets(&self) -> &[GsiBuildTarget] {
        &self.writable_targets
    }

    pub fn plan_write_actions<'a>(
        &self,
        table: &'a StoredTableInfo,
        old_item: Option<&WireItem>,
        new_item: Option<&WireItem>,
    ) -> StorageResult<Vec<GsiWriteAction<'a>>> {
        let Some(indexes) = table.global_secondary_indexes.as_deref() else {
            return Ok(Vec::new());
        };
        let writable = indexes
            .iter()
            .filter(|index| {
                !index.index_name.is_ttl_index()
                    && self.writable_targets.iter().any(|target| {
                        target.table_name == table.table_name
                            && target.index_name == index.index_name
                    })
            })
            .collect::<Vec<_>>();
        plan_gsi_write_actions_for(table, &writable, old_item, new_item)
    }

    pub fn plan_immediate_writes(
        &self,
        table: &StoredTableInfo,
        old_item: Option<&storage_types::WireItem>,
        new_item: Option<&storage_types::WireItem>,
        indexers: &[String],
        source_version: storage_types::ItemRevision,
    ) -> StorageResult<GsiProjectionWriteBatch> {
        if let Some(new_item) = new_item {
            let indexers =
                storage_types::IndexerDeclaration::try_new(indexers.to_vec(), table.max_indexers)?;
            return super::PreparedGsiPut::try_new(table, new_item, &indexers)?.finish(
                self,
                table,
                old_item,
                source_version,
            );
        }
        Ok(GsiProjectionWriteBatch(self.plan_deletes(
            table,
            old_item,
            source_version,
        )?))
    }

    pub(super) fn plan_deletes(
        &self,
        table: &StoredTableInfo,
        old_item: Option<&storage_types::WireItem>,
        source_version: storage_types::ItemRevision,
    ) -> StorageResult<Vec<GsiProjectionWrite>> {
        self.plan_write_actions(table, old_item, None)?
            .into_iter()
            .map(|action| {
                let GsiWriteAction::Delete {
                    index,
                    gsi_key,
                    table_key,
                } = action
                else {
                    return Err(storage_types::StorageError::internal(
                        "old-only GSI plan contains a put",
                    ));
                };
                Ok(GsiProjectionWrite {
                    key: projection_key(index, gsi_key, table_key),
                    row: None,
                    item_version: source_version,
                })
            })
            .collect()
    }
}

pub(super) fn projection_key(
    index: &storage_types::GlobalSecondaryIndex,
    index_key: super::GsiKeyParts<'_>,
    table_item_key: super::GsiKeyParts<'_>,
) -> GsiProjectionKey {
    GsiProjectionKey {
        index_name: index.index_name.clone(),
        index_key: index_key
            .into_iter()
            .map(|part| (part.name.to_owned(), part.value))
            .collect(),
        table_item_key: table_item_key
            .into_iter()
            .map(|part| (part.name.to_owned(), part.value))
            .collect(),
        ttl_index_key: None,
    }
}
