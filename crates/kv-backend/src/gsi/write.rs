//! Apply shared projection decisions to KV GSI rows.

use foundationdb_tuple::{TupleDepth, TupleUnpack, pack_into, unpack};
use storage_backfill::{GsiProjectionKey, GsiProjectionWriteBatch};
use storage_provider::{GsiProjectionOwner, GsiWriteSource};
use storage_types::{BoundTableInfo, ItemRevision, StorageError, StorageResult, WireItem};

use super::key_layout::{ProjectionKeys, projection_suffix};
use crate::catalog::{KvIndexKind, KvTableIdentity};

pub(crate) struct GsiWriteInput<'a> {
    pub identity: &'a KvTableIdentity,
    pub table: &'a BoundTableInfo,
    pub source: GsiWriteSource<'a>,
    pub writes: GsiProjectionWriteBatch,
    pub prepared: &'a mut Vec<PreparedGsiWrite>,
}

/// Encode the writes already decided by the shared GSI executor. Its
/// destination observations were protected in this native attempt.
pub(crate) async fn prepare_gsi_writes<A: crate::KvAttempt>(
    catalog: &mut crate::catalog::KvCatalogAttempt<'_, A>,
    input: GsiWriteInput<'_>,
) -> StorageResult<()> {
    let mut operation =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(catalog);
    let catalog = operation.as_mut();
    let result = async {
        let table = input.table;
        let (rows, plans) = bind_gsi_writes(input)?;
        let mut transitions = Vec::new();
        let mut writes = Vec::new();
        transitions
            .try_reserve_exact(rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        writes
            .try_reserve_exact(rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let previous = BoundGsiWrite::load_descriptors(catalog, table, &rows).await?;
        let mut images =
            stream_provider::retained_image::AcceptedProjectionPlan::prepare_batch(catalog, plans)
                .await?
                .into_iter();
        for (row, previous) in rows.into_iter().zip(previous) {
            let image =
                match row.state {
                    ProjectionWriteState::Live => Some(images.next().ok_or_else(|| {
                        StorageError::internal("Missing GSI projection descriptor")
                    })?),
                    ProjectionWriteState::Deleted => None,
                };
            transitions.push(
                row.owner
                    .prepare_transition(previous.as_ref(), image.as_ref())?,
            );
            writes.push((row, image));
        }
        // Tree edits hydrate the old leaf under its current ownership receipt.
        // Stage them before replacing that receipt; the caller commits both
        // together.
        for (row, image) in writes {
            row.store(catalog, table, image).await?;
        }
        stream_provider::retained_image::RetainedOwnerTransition::prepare_batch(
            catalog,
            transitions,
        )
        .await?;
        if images.next().is_some() {
            return Err(StorageError::internal("Unused GSI projection descriptor"));
        }
        Ok(())
    }
    .await;
    operation.complete(result)
}

fn bind_gsi_writes(
    input: GsiWriteInput<'_>,
) -> StorageResult<(
    Vec<BoundGsiWrite>,
    Vec<stream_provider::retained_image::AcceptedProjectionPlan>,
)> {
    let mut rows = Vec::new();
    let mut plans = Vec::new();
    let writes = input.writes.into_iter();
    rows.try_reserve_exact(writes.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    plans
        .try_reserve_exact(writes.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for write in writes {
        input.source.validate(&write)?;
        let prepared = if write.row.is_some() {
            input
                .prepared
                .iter()
                .position(|put| put.history_id == input.table.history_id && put.key == write.key)
        } else {
            None
        };
        let prepared = match prepared {
            Some(position) => input.prepared.swap_remove(position),
            None => PreparedGsiWrite::try_new(input.table, &write.key, write.row.as_ref())?,
        };
        let (row, image) = prepared.bind(input.identity, input.table, write.item_version)?;
        if let Some(image) = image {
            plans.push(image);
        }
        rows.push(row);
    }
    Ok((rows, plans))
}

/// A new live image or an old-image tombstone, without its protected revision.
pub(crate) struct PreparedGsiWrite {
    pub(crate) history_id: storage_types::TableHistoryId,
    pub(crate) key: GsiProjectionKey,
    suffix: Vec<u8>,
    image: PreparedProjectionImage,
}

enum PreparedProjectionImage {
    NewItem(stream_provider::retained_image::AcceptedProjectionPlan),
    DeletedItem,
}

impl PreparedGsiWrite {
    pub(crate) fn try_new(
        table: &BoundTableInfo,
        key: &GsiProjectionKey,
        row: Option<&storage_backfill::GsiProjectedRow>,
    ) -> StorageResult<Self> {
        let image = match row {
            Some(row) => PreparedProjectionImage::NewItem(
                stream_provider::retained_image::AcceptedProjectionPlan::try_new(
                    table.history_id,
                    &row.source_item,
                    &row.projection,
                )?,
            ),
            None => PreparedProjectionImage::DeletedItem,
        };
        Ok(Self {
            history_id: table.history_id,
            key: key.clone(),
            suffix: projection_suffix(table, key)?,
            image,
        })
    }

    fn bind(
        self,
        identity: &KvTableIdentity,
        table: &BoundTableInfo,
        revision: ItemRevision,
    ) -> StorageResult<(
        BoundGsiWrite,
        Option<stream_provider::retained_image::AcceptedProjectionPlan>,
    )> {
        let index_id = identity
            .indexes
            .iter()
            .find(|index| {
                index.index_name == self.key.index_name && index.index_kind == KvIndexKind::UserGsi
            })
            .ok_or_else(|| StorageError::internal("GSI projection has no KV index identity"))?
            .index_id;
        let owner = GsiProjectionOwner::try_new(
            table,
            &self.key,
            &super::key_layout::index_incarnation(identity.table_id, index_id).into(),
        )?;
        let keys = ProjectionKeys::try_from_suffix(identity.table_id, index_id, self.suffix)?;
        let (image, state) = match self.image {
            PreparedProjectionImage::NewItem(image) => (Some(image), ProjectionWriteState::Live),
            PreparedProjectionImage::DeletedItem => (None, ProjectionWriteState::Deleted),
        };
        Ok((
            BoundGsiWrite {
                keys,
                index: self.key.index_name,
                revision,
                state,
                owner,
            },
            image,
        ))
    }
}

struct BoundGsiWrite {
    index: storage_types::IndexName,
    keys: ProjectionKeys,
    revision: ItemRevision,
    state: ProjectionWriteState,
    owner: GsiProjectionOwner,
}

enum ProjectionWriteState {
    Live,
    Deleted,
}

impl BoundGsiWrite {
    async fn load_descriptors<A: crate::KvAttempt>(
        catalog: &mut crate::catalog::KvCatalogAttempt<'_, A>,
        table: &BoundTableInfo,
        rows: &[Self],
    ) -> StorageResult<Vec<Option<stream_provider::retained_image::RetainedProjectionRecord>>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let count = rows
            .len()
            .checked_mul(2)
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let mut keys = Vec::new();
        let mut previous = Vec::new();
        keys.try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        previous
            .try_reserve_exact(rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for row in rows {
            keys.push(copy_descriptor_bytes(&row.keys.live)?);
            keys.push(copy_descriptor_bytes(&row.keys.tombstone)?);
        }
        let maximum =
            (stream_provider::retained_image::RetainedProjectionRecord::encoded_length_bound()?
                + 10
                + crate::constants::GSI_REVISION_BYTES)
                .checked_mul(rows.len())
                .ok_or_else(|| StorageError::internal("GSI descriptor read bound overflow"))?;
        let mut values = catalog
            .attempt
            .get_batch_bounded(
                keys,
                crate::ReadProtection::Conflict,
                std::num::NonZeroUsize::new(maximum)
                    .ok_or_else(|| StorageError::internal("Empty GSI descriptor budget"))?,
            )
            .await?;
        if values.len() != count {
            return Err(StorageError::internal("GSI descriptor read count differs"));
        }
        let keys = rows
            .iter()
            .flat_map(|row| [&row.keys.live, &row.keys.tombstone]);
        for (key, value) in keys.zip(&mut values) {
            if let Some(pending) = catalog.projection_tree.rows.get(key) {
                *value = pending.as_deref().map(copy_descriptor_bytes).transpose()?;
            }
        }
        for values in values.as_chunks::<2>().0 {
            let state = decode_projection_state(table, values[0].as_deref(), values[1].as_deref())?;
            match state {
                Some(PhysicalProjection::Live(_)) => {
                    let value = values[0].as_deref().ok_or_else(|| {
                        StorageError::internal("Missing live projection descriptor")
                    })?;
                    let envelope = ProjectionEnvelope::decode(value)?;
                    previous.push(Some(
                        stream_provider::retained_image::RetainedProjectionRecord::decode(
                            envelope.item,
                            table.history_id,
                            crate::constants::PAGE_READ_BYTES.get(),
                        )?,
                    ));
                }
                Some(PhysicalProjection::Tombstone(_)) | None => previous.push(None),
            }
        }
        Ok(previous)
    }

    async fn store<A: crate::KvAttempt>(
        self,
        catalog: &mut crate::catalog::KvCatalogAttempt<'_, A>,
        table: &BoundTableInfo,
        image: Option<stream_provider::retained_image::RetainedProjectionRecord>,
    ) -> StorageResult<()> {
        let (value, tombstone) = match self.state {
            ProjectionWriteState::Live => {
                let image = image
                    .ok_or_else(|| StorageError::internal("Missing GSI projection descriptor"))?
                    .encode()?;
                let bytes = 10usize
                    .checked_add(image.len())
                    .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                let mut value = Vec::new();
                value
                    .try_reserve_exact(bytes)
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                value.push(crate::constants::GSI_PROJECTION_FORMAT);
                pack_into(&self.revision.get(), &mut value);
                value.extend_from_slice(&image);
                (Some(value), None)
            }
            ProjectionWriteState::Deleted => (
                None,
                Some(copy_descriptor_bytes(&self.revision.get().to_be_bytes())?),
            ),
        };
        super::tree_write::store_tree(
            catalog,
            super::tree_write::TreeWrite {
                table,
                index: &self.index,
                scope: self.keys.scope,
                word: self.keys.word,
                value,
                tombstone,
            },
        )
        .await
    }
}

pub(super) fn copy_descriptor_bytes(bytes: &[u8]) -> StorageResult<Vec<u8>> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(bytes.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    copied.extend_from_slice(bytes);
    Ok(copied)
}

pub(super) enum PhysicalProjection {
    Live(ItemRevision),
    Tombstone(ItemRevision),
}

pub(super) fn decode_projection_state(
    table: &BoundTableInfo,
    live: Option<&[u8]>,
    tombstone: Option<&[u8]>,
) -> StorageResult<Option<PhysicalProjection>> {
    match (live, tombstone) {
        (Some(_), Some(_)) => Err(StorageError::internal("GSI has live and tombstone rows")),
        (Some(bytes), None) => {
            let projection = ProjectionEnvelope::decode(bytes)?;
            stream_provider::retained_image::RetainedProjectionRecord::decode(
                projection.item,
                table.history_id,
                crate::constants::PAGE_READ_BYTES.get(),
            )?;
            ItemRevision::try_from(projection.revision)
                .map(PhysicalProjection::Live)
                .map(Some)
        }
        (None, Some(bytes)) => {
            let revision: [u8; 8] = bytes
                .try_into()
                .map_err(|_| StorageError::internal("GSI tombstone revision is malformed"))?;
            ItemRevision::try_from(u64::from_be_bytes(revision))
                .map(PhysicalProjection::Tombstone)
                .map(Some)
        }
        (None, None) => Ok(None),
    }
}

// A tuple revision prefixes the codec-owned image, which is intentionally
// opaque.
struct ProjectionEnvelope<'a> {
    revision: u64,
    item: &'a [u8],
}

pub(crate) struct HydratedProjection {
    pub image: WireItem,
    pub revision: ItemRevision,
}

impl<'a> ProjectionEnvelope<'a> {
    fn decode(bytes: &'a [u8]) -> StorageResult<Self> {
        let (&format, bytes) = bytes
            .split_first()
            .ok_or_else(|| StorageError::internal("GSI envelope is empty"))?;
        if format != crate::constants::GSI_PROJECTION_FORMAT {
            return Err(StorageError::internal("GSI envelope format is invalid"));
        }
        unpack(bytes).map_err(|_| StorageError::internal("GSI live revision is malformed"))
    }
}

pub(super) fn projection_read_work(table: &BoundTableInfo, value: &[u8]) -> StorageResult<usize> {
    let maximum = crate::constants::PAGE_READ_BYTES.get();
    let envelope = ProjectionEnvelope::decode(value)?;
    ItemRevision::try_from(envelope.revision)?;
    stream_provider::retained_image::RetainedProjectionRecord::decode(
        envelope.item,
        table.history_id,
        maximum,
    )?
    .read_work_bytes(maximum)
}

pub(crate) async fn hydrate_projections<
    A: stream_provider::retained_image::RetainedOwnershipAttempt,
>(
    attempt: &mut A,
    table: &BoundTableInfo,
    values: &[crate::KvEntry],
    target: &storage_backfill::GsiBuildTarget,
) -> StorageResult<Vec<HydratedProjection>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let maximum = crate::constants::PAGE_READ_BYTES.get();
        let mut records = Vec::with_capacity(values.len());
        let mut revisions = Vec::with_capacity(values.len());
        for value in values {
            let envelope = ProjectionEnvelope::decode(&value.value)?;
            revisions.push(ItemRevision::try_from(envelope.revision)?);
            records.push(
                stream_provider::retained_image::RetainedProjectionRecord::decode(
                    envelope.item,
                    table.history_id,
                    maximum,
                )?,
            );
        }
        if records.is_empty() {
            return Ok(Vec::new());
        }
        if target.table_name != table.table_name {
            return Err(StorageError::internal(
                "GSI hydration target differs from table",
            ));
        }
        let proofs = records
            .iter()
            .map(stream_provider::retained_image::RetainedProjectionRecord::owner_proof)
            .collect::<StorageResult<Vec<_>>>()?;
        let images = stream_provider::retained_image::RetainedProjectionRecord::hydrate(
            attempt, records, maximum,
        )
        .await?;
        if images.len() != revisions.len() {
            return Err(StorageError::internal("GSI hydration count differs"));
        }
        let receipts = images
            .iter()
            .zip(proofs)
            .map(|(image, proof)| {
                storage_provider::GsiProjectionOwner::from_image(
                    table,
                    &target.index_name,
                    &target.incarnation,
                    image,
                )?
                .prepare_read_receipt(proof)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        stream_provider::retained_image::RetainedOwnerRecord::require_current_batch(
            attempt, &receipts,
        )
        .await?;
        Ok(images
            .into_iter()
            .zip(revisions)
            .map(|(stored, revision)| HydratedProjection {
                image: stored,
                revision,
            })
            .collect())
    }
    .await;
    settlement.complete(result)
}

impl<'a> TupleUnpack<'a> for ProjectionEnvelope<'a> {
    fn unpack(
        input: &'a [u8],
        depth: TupleDepth,
    ) -> foundationdb_tuple::PackResult<(&'a [u8], Self)> {
        let (item, revision) = u64::unpack(input, depth)?;
        Ok((&[], Self { revision, item }))
    }
}
