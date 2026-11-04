//! Apply shared projection decisions to KV GSI rows.

use std::collections::BTreeMap;

use foundationdb_tuple::{TupleDepth, TupleUnpack, pack_into, unpack};
use storage_backfill::{GsiProjectionKey, GsiProjectionWriteBatch};
use storage_provider::GsiWriteSource;
use storage_types::{BoundTableInfo, ItemRevision, StorageError, StorageResult, WireItem};

use super::key_layout::{ProjectionKeys, projection_suffix};
use crate::catalog::{KvIndexKind, KvTableIdentity};

#[derive(Default)]
pub(crate) struct ProjectionChanges {
    pub(super) rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl ProjectionChanges {
    pub(crate) fn into_mutations(self) -> StorageResult<Vec<crate::KvMutation>> {
        let mut mutations = Vec::new();
        mutations
            .try_reserve_exact(self.rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (key, value) in self.rows {
            let is_image = key.first() == Some(&crate::constants::GSI_PROJECTION_FAMILY);
            mutations.push(match (is_image, value) {
                (true, Some(value)) => crate::KvMutation::Put { key, value },
                (true, None) => crate::KvMutation::Delete { key },
                (false, Some(value)) => crate::KvMutation::PutUnchunked { key, value },
                (false, None) => crate::KvMutation::DeleteUnchunked { key },
            });
        }
        Ok(mutations)
    }
}

pub(crate) struct GsiWriteInput<'a> {
    pub identity: &'a KvTableIdentity,
    pub table: &'a BoundTableInfo,
    pub source: GsiWriteSource<'a>,
    pub writes: GsiProjectionWriteBatch,
    pub prepared: &'a mut Vec<PreparedGsiWrite>,
}

/// Encode the writes already decided by the shared GSI executor. Its
/// destination observations were protected in this native attempt.
pub(crate) fn prepare_gsi_writes<A: crate::KvAttempt>(
    catalog: &mut crate::catalog::KvCatalogAttempt<'_, A>,
    input: GsiWriteInput<'_>,
) -> StorageResult<()> {
    let mut operation =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(catalog);
    let catalog = operation.as_mut();
    let result = (|| {
        for row in bind_gsi_writes(input)? {
            row.store(&mut catalog.projection_changes)?;
        }
        Ok(())
    })();
    operation.complete(result)
}

fn bind_gsi_writes(input: GsiWriteInput<'_>) -> StorageResult<Vec<BoundGsiWrite>> {
    let mut rows = Vec::new();
    let writes = input.writes.into_iter();
    rows.try_reserve_exact(writes.len())
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
        rows.push(prepared.bind(input.identity, write.item_version)?);
    }
    Ok(rows)
}

/// A new live image or an old-image tombstone, without its protected revision.
pub(crate) struct PreparedGsiWrite {
    pub(crate) history_id: storage_types::TableHistoryId,
    pub(crate) key: GsiProjectionKey,
    suffix: Vec<u8>,
    image: Option<Vec<u8>>,
}

impl PreparedGsiWrite {
    pub(crate) fn try_new(
        table: &BoundTableInfo,
        key: &GsiProjectionKey,
        row: Option<&storage_backfill::GsiProjectedRow>,
    ) -> StorageResult<Self> {
        let image = row
            .map(|row| {
                let image = row.projected_image()?;
                let declaration = storage_types::IndexerDeclaration::try_new(
                    Vec::new(),
                    storage_types::MaxIndexers::ZERO,
                )?;
                storage_types::MaterializedItemRecord::encode(
                    table.history_id,
                    &image,
                    &declaration,
                )
            })
            .transpose()?;
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
        revision: ItemRevision,
    ) -> StorageResult<BoundGsiWrite> {
        let index_id = identity
            .indexes
            .iter()
            .find(|index| {
                index.index_name == self.key.index_name && index.index_kind == KvIndexKind::UserGsi
            })
            .ok_or_else(|| StorageError::internal("GSI projection has no KV index identity"))?
            .index_id;
        let keys = ProjectionKeys::try_from_suffix(identity.table_id, index_id, self.suffix)?;
        Ok(BoundGsiWrite {
            keys,
            revision,
            image: self.image,
        })
    }
}

struct BoundGsiWrite {
    keys: ProjectionKeys,
    revision: ItemRevision,
    image: Option<Vec<u8>>,
}

impl BoundGsiWrite {
    fn store(self, changes: &mut ProjectionChanges) -> StorageResult<()> {
        let (value, tombstone) = match self.image {
            Some(image) => {
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
            None => (
                None,
                Some(copy_projection_bytes(&self.revision.get().to_be_bytes())?),
            ),
        };
        changes.rows.insert(self.keys.live, value);
        changes.rows.insert(self.keys.tombstone, tombstone);
        Ok(())
    }
}

pub(super) fn copy_projection_bytes(bytes: &[u8]) -> StorageResult<Vec<u8>> {
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
            storage_types::MaterializedItemRecord::decode(
                projection.item,
                table.history_id,
                storage_types::MaterializedItemRecord::encoded_length_bound(),
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

pub(crate) struct DecodedProjection {
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

pub(super) fn decode_projection(
    table: &BoundTableInfo,
    value: &[u8],
) -> StorageResult<DecodedProjection> {
    let envelope = ProjectionEnvelope::decode(value)?;
    let revision = ItemRevision::try_from(envelope.revision)?;
    let image = storage_types::MaterializedItemRecord::decode(
        envelope.item,
        table.history_id,
        storage_types::MaterializedItemRecord::encoded_length_bound(),
    )?
    .into_stored(storage_types::MaxIndexers::ZERO)?
    .item;
    Ok(DecodedProjection { image, revision })
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
