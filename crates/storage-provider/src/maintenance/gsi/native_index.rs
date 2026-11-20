//! Native index build decisions; physical ports supply rows and index validity.

use storage_backfill::{
    GsiActive, GsiBuild, GsiBuildTarget, GsiNativeBuilding, GsiPageLimit, GsiScanning, GsiState,
};
use storage_types::{
    BoundGsi, BoundTableInfo, KeyAttributes, Revision, StorageError, StorageResult,
    maintenance::{MaintenanceKind, StepResult, TargetIncarnation, TaskProgress},
};

use super::{GsiBuildAdvance, GsiPort, GsiTargetAdmission, transaction::GsiAttempt};

pub struct NativeGsiRow {
    pub key: KeyAttributes,
    pub item: storage_types::WireItem,
    pub current: KeyAttributes,
    pub version: NativeGsiRowVersion,
}

pub enum NativeGsiRowVersion {
    Serialized,
    Postgres { ctid: String, xmin: String },
}

pub struct NativeGsiPage {
    pub rows: Vec<NativeGsiRow>,
    pub more: bool,
}

pub struct NativeGsiFill {
    pub key: KeyAttributes,
    pub values: Option<KeyAttributes>,
    pub version: NativeGsiRowVersion,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NativeIndexState {
    Missing,
    /// Not valid, and no session holds its build lock: the build failed.
    Invalid,
    /// Not valid while another session holds its build lock: a build runs.
    Building,
    Valid,
}

#[derive(Clone, Copy)]
pub enum NativeIndexAction {
    Create,
    Drop,
}

pub struct NativeIndexWork {
    pub table: BoundTableInfo,
    pub index: BoundGsi,
    pub action: NativeIndexAction,
}

pub struct NativeGsiProgress {
    pub expected_revision: Option<Revision>,
    pub next_progress: TaskProgress<Option<GsiBuildTarget>>,
    pub page_limit: GsiPageLimit,
}

pub struct NativeGsiTransition {
    pub expected: GsiBuild<Vec<u8>>,
    pub next: GsiBuild<Vec<u8>>,
    pub progress: NativeGsiProgress,
}

impl<P: GsiPort, S: GsiTargetAdmission<P>> GsiAttempt<'_, P, S> {
    pub(super) async fn advance_native_index(
        &mut self,
        build: GsiBuild<Vec<u8>>,
        progress: NativeGsiProgress,
    ) -> StorageResult<GsiBuildAdvance> {
        let table = self.load_table(&build.target.table_name).await?;
        build.target.validate_table(&table)?;
        let index = table
            .binding
            .gsi(&build.target.index_name)
            .filter(|index| index.build == build.target.incarnation)
            .ok_or_else(storage_backfill::GsiActivationState::conflict_error)?;
        let (next, replacement, work) = match &build.state {
            GsiState::Filling(scan) => (
                self.fill_native_page(&table, &build.target, scan, progress.page_limit)
                    .await?,
                None,
                None,
            ),
            GsiState::Building(state) => {
                match self.inspect_native_index(&table, index, state).await? {
                    Some(inspected) => inspected,
                    None => {
                        // Another node builds the index. Keep the state and
                        // wait at the normal cadence instead of retiring its
                        // build.
                        self.port
                            .store_task_progress(
                                MaintenanceKind::GsiBackfill,
                                &self.progress_fence,
                                progress.expected_revision,
                                progress.next_progress,
                            )
                            .await?;
                        return Ok(GsiBuildAdvance {
                            result: StepResult::Complete,
                            ttl_activation: None,
                        });
                    }
                }
            }
            _ => {
                return Err(StorageError::internal(
                    "Native GSI job has an invalid phase",
                ));
            }
        };
        let mut target = build.target.clone();
        if let Some(incarnation) = replacement {
            target.incarnation = incarnation;
        }
        let progress_write = crate::maintenance::ProgressWrite::new(
            MaintenanceKind::GsiBackfill,
            &self.progress_fence,
            progress.expected_revision,
            &progress.next_progress,
        )?;
        self.port
            .store_native_gsi_transition(
                &table,
                NativeGsiTransition {
                    expected: build,
                    next: GsiBuild {
                        target,
                        state: next,
                    },
                    progress,
                },
                progress_write,
            )
            .await?;
        if let Some(work) = work {
            self.port.stage_native_index(work)?;
        }
        Ok(GsiBuildAdvance {
            result: StepResult::Advanced(()),
            ttl_activation: None,
        })
    }

    async fn fill_native_page(
        &mut self,
        table: &BoundTableInfo,
        target: &GsiBuildTarget,
        scan: &GsiScanning<Vec<u8>>,
        limit: GsiPageLimit,
    ) -> StorageResult<GsiState<Vec<u8>>> {
        let page = self
            .port
            .load_native_gsi_page(table, target, scan.after.as_ref(), limit)
            .await?;
        let (keys, fills) = prepare_native_fills(table, target, page.rows)?;
        let changed = self
            .port
            .store_native_gsi_fills(table, target, fills)
            .await?;
        if changed.len() != keys.len() {
            return Err(StorageError::internal("Native GSI fill counts differ"));
        }
        // A phase change uses a separate, empty page. A fill that owns row
        // locks must not upgrade its shared catalog lock behind a writer.
        let complete = keys.is_empty() && !page.more;
        if complete {
            return Ok(GsiState::Building(GsiNativeBuilding {
                low: scan.low.clone(),
                retired: None,
            }));
        }
        let mut after = scan.after.clone();
        for (key, changed) in keys.into_iter().zip(changed) {
            if !changed {
                break;
            }
            after = Some(storage_types::storage_serde::to_bytes(&key)?);
        }
        Ok(GsiState::Filling(GsiScanning {
            low: scan.low.clone(),
            after,
        }))
    }

    async fn inspect_native_index(
        &mut self,
        table: &BoundTableInfo,
        index: &BoundGsi,
        state: &GsiNativeBuilding,
    ) -> StorageResult<
        Option<(
            GsiState<Vec<u8>>,
            Option<TargetIncarnation>,
            Option<NativeIndexWork>,
        )>,
    > {
        let mut physical = index.clone();
        if let Some(retired) = &state.retired {
            physical.build = retired.clone();
        }
        let observed = self.port.load_native_index_state(table, &physical).await?;
        if state.retired.is_some() {
            let next = GsiState::Building(GsiNativeBuilding {
                low: state.low.clone(),
                retired: (observed != NativeIndexState::Missing).then(|| physical.build.clone()),
            });
            return Ok(Some((
                next,
                None,
                (observed != NativeIndexState::Missing).then(|| NativeIndexWork {
                    table: table.clone(),
                    index: physical,
                    action: NativeIndexAction::Drop,
                }),
            )));
        }
        match observed {
            NativeIndexState::Valid => Ok(Some((
                GsiState::Active(GsiActive {
                    cursor: state.low.clone(),
                }),
                None,
                None,
            ))),
            NativeIndexState::Building => Ok(None),
            NativeIndexState::Missing => Ok(Some((
                GsiState::Building(GsiNativeBuilding {
                    low: state.low.clone(),
                    retired: None,
                }),
                None,
                Some(NativeIndexWork {
                    table: table.clone(),
                    index: physical,
                    action: NativeIndexAction::Create,
                }),
            ))),
            NativeIndexState::Invalid => {
                let replacement =
                    TargetIncarnation::from(*storage_types::ambient::uuid_v4().as_bytes());
                Ok(Some((
                    GsiState::Building(GsiNativeBuilding {
                        low: state.low.clone(),
                        retired: Some(index.build.clone()),
                    }),
                    Some(replacement),
                    Some(NativeIndexWork {
                        table: table.clone(),
                        index: physical,
                        action: NativeIndexAction::Drop,
                    }),
                )))
            }
        }
    }
}

fn prepare_native_fills(
    table: &BoundTableInfo,
    target: &GsiBuildTarget,
    rows: Vec<NativeGsiRow>,
) -> StorageResult<(Vec<KeyAttributes>, Vec<NativeGsiFill>)> {
    let mut keys = Vec::with_capacity(rows.len());
    let mut fills = Vec::with_capacity(rows.len());
    let index = table
        .binding
        .gsi(&target.index_name)
        .ok_or_else(|| StorageError::internal("Native GSI binding is absent"))?;
    for row in rows {
        let mut values = KeyAttributes::new();
        for column in &index.columns {
            if let Some(value) = row.item.attribute_value(&column.attribute)?
                && value.matches_key_type(&column.kind)
            {
                values.insert(column.attribute.clone(), value);
            }
        }
        keys.push(row.key.clone());
        fills.push(NativeGsiFill {
            key: row.key,
            values: (values != row.current).then_some(values),
            version: row.version,
        });
    }
    Ok((keys, fills))
}

impl NativeGsiTransition {
    /// `current` with the target GSI's phase and build incarnation set to
    /// `next`. Equal to `current` when neither changes.
    pub fn next_binding(
        &self,
        current: &storage_types::TableBinding,
    ) -> StorageResult<storage_types::TableBinding> {
        let mut binding = current.clone();
        let mut indexes = binding.gsis.to_vec();
        let index = indexes
            .iter_mut()
            .find(|index| index.name == self.next.target.index_name)
            .ok_or_else(|| StorageError::internal("Native GSI binding is absent"))?;
        index.phase = storage_backfill::GsiBuildPhase::from(&self.next.state).into();
        index.build = self.next.target.incarnation.clone();
        binding.gsis = indexes.into();
        Ok(binding)
    }
}
