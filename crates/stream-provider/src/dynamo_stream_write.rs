//! One logical Dynamo write becomes the same stream rows on SQL and KV.
//!
//! The backends own row keys, statements, and native transaction staging.
//! Image encoding, pointer embedding, record identity, and the trim receipt
//! are decided once here after the operation has allocated a stream position.

use serde::{Deserialize, Serialize};
use storage_types::{
    BoundTableInfo, ItemHistoryAddress, ItemKey, ItemRevision, ItemStreamVersion, KeyAttributes,
    StorageError, StorageResult, StreamItemId, StreamName, SystemStreamEffectDigest,
    SystemStreamPosition, TableHistoryId, TimestampMillis, WireItem, storage_serde,
};

use crate::{
    LogicalSystemStreamMutation, PointerOwner, StoredStreamPointer, StreamDataType, StreamItem,
    StreamPointerInput, StreamPointerPlan, StreamWriteKind, SystemStreamStoredRecord,
    SystemStreamTrimRecord, can_embed_new_image, system_stream::pointer_slot,
};

pub struct DynamoStreamWriteInput<'a> {
    pub table: &'a BoundTableInfo,
    pub mutation: LogicalSystemStreamMutation,
    pub position: SystemStreamPosition,
}

impl DynamoStreamWriteInput<'_> {
    fn require_bound_identity(&self) -> StorageResult<()> {
        let table = self.table;
        let mutation = &self.mutation;
        if mutation.history_id != table.history_id
            || mutation.source_table_name.as_str() != table.table_name.as_ref()
        {
            return Err(StorageError::internal(
                "stream mutation does not match its bound table",
            ));
        }
        let revision = ItemRevision::try_from(mutation.item_stream_version.get())?;
        if mutation.stream_item_id
            != DynamoStreamIds::for_write(revision, &self.position)?.system_pointer_id()
        {
            return Err(StorageError::internal(
                "Dynamo stream pointer differs from its position",
            ));
        }
        Ok(())
    }
}

/// Item-local image version and commit-ordered pointer identities for one
/// ordinary Dynamo write. A validated lineage slot and its offset uniquely
/// identify the system pointer without a shared per-write allocator.
pub struct DynamoStreamIds {
    item_stream_version: ItemStreamVersion,
    system_pointer_id: StreamItemId,
}

impl DynamoStreamIds {
    pub fn for_write(
        revision: ItemRevision,
        position: &SystemStreamPosition,
    ) -> StorageResult<Self> {
        if let Some(stamp) = position.offset().versionstamp() {
            return Ok(Self {
                item_stream_version: ItemStreamVersion::new(revision.get()),
                system_pointer_id: StreamItemId::from(stamp),
            });
        }
        let offset = position
            .offset()
            .order()
            .map_err(|_| StorageError::internal("Dynamo stream position has no numeric offset"))?;
        if offset == 0 {
            return Err(StorageError::internal(
                "Dynamo stream position starts at zero",
            ));
        }
        let mut bytes = [0_u8; 12];
        let slot = if position.offset().as_str().ends_with("-ns") {
            [u8::MAX; 4]
        } else {
            pointer_slot(position.lineage_id())
        };
        bytes[..4].copy_from_slice(&slot);
        bytes[4..].copy_from_slice(&offset.to_be_bytes());
        let pointer_id = StreamItemId::from(bytes);
        Ok(Self {
            item_stream_version: ItemStreamVersion::new(revision.get()),
            system_pointer_id: pointer_id,
        })
    }

    pub fn item_stream_version(&self) -> ItemStreamVersion {
        self.item_stream_version
    }

    pub fn system_pointer_id(&self) -> StreamItemId {
        self.system_pointer_id
    }
}

/// Stream rows, retained content descriptor and receipt for one native write
/// attempt.
///
/// The item row uses the history-qualified physical name. Both pointer rows
/// carry identical bytes. Both families stage these three rows, the record,
/// and the receipt in the same native attempt.
pub struct DynamoStreamWritePlan {
    /// Item image, table pointer, and system pointer in write order.
    pub rows: [StreamWriteKind<StreamItem>; 3],
    /// Both independently retained pointer references to the item image.
    pub pointer_references: [StreamPointerReference; 2],
    pub record: crate::retained_image::system_stream::RetainedSystemStreamRecord,
    pub receipt: DynamoStreamEffectRow,
}

/// One physical pointer keeps the referenced item image alive until that
/// pointer is released by its own retention or SystemStream trim operation.
pub struct StreamPointerReference {
    pub owner: PointerOwner,
    pub owner_stream: StreamName,
    pub owner_item_id: StreamItemId,
    pub item_stream: StreamName,
    /// Embedded images own a pointer receipt without holding an item-history
    /// row.
    pub item_item_id: Option<StreamItemId>,
}

/// Durable receipt of one logical stream effect. Both backend families store
/// this compact identity under the effect digest. Images live in the ordered
/// record descriptor, which trim hydrates and verifies through the shared image
/// owner.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamoStreamEffectRow {
    pub effect_id: SystemStreamEffectDigest,
    pub history_id: TableHistoryId,
    pub item_stream_version: ItemStreamVersion,
    pub pointer_item_id: StreamItemId,
    pub position: SystemStreamPosition,
    pub created_at: TimestampMillis,
    pub trimmed: bool,
}

impl DynamoStreamEffectRow {
    pub fn with_versionstamp(mut self, stamp: [u8; 12]) -> StorageResult<Self> {
        self.position = self
            .position
            .with_versionstamp(stamp)
            .map_err(|_| StorageError::internal("Invalid committed stream position"))?;
        self.pointer_item_id = StreamItemId::from(stamp);
        Ok(self)
    }

    pub fn for_write(
        mutation: LogicalSystemStreamMutation,
        position: SystemStreamPosition,
    ) -> StorageResult<(SystemStreamStoredRecord, Self)> {
        let record = SystemStreamStoredRecord {
            source_table_name: mutation.source_table_name,
            keys: mutation.key,
            old_image: mutation.old_item,
            new_image: (!mutation.is_deleted).then_some(mutation.item),
            position,
        };
        let receipt = Self {
            effect_id: record.effect_digest(mutation.history_id, mutation.item_stream_version)?,
            history_id: mutation.history_id,
            item_stream_version: mutation.item_stream_version,
            pointer_item_id: mutation.stream_item_id,
            position: record.position.clone(),
            created_at: mutation.created_at,
            trimmed: false,
        };
        Ok((record, receipt))
    }

    /// A physical record may release its pointer only when its durable effect
    /// receipt identifies the same record and has not already been trimmed.
    pub fn accept_trim(
        mut self,
        candidate: &SystemStreamTrimRecord,
        record: &crate::retained_image::verified_system_stream::VerifiedSystemStreamRecord,
        pointer: &StreamItem,
        reference: &StreamPointerReference,
    ) -> StorageResult<Self> {
        let verified = record;
        let record = verified.record();
        if self.trimmed
            || self.position != candidate.position
            || self.pointer_item_id != candidate.pointer_item_id
            || self.effect_id != candidate.effect_id
            || self.created_at != candidate.created_at
            || record.position != candidate.position
            || pointer.id != candidate.pointer_item_id
            || pointer.stream_name.as_ref() != Some(&StreamName::system_table_stream())
            || pointer.data_type != StreamDataType::StreamPointer
            || pointer.created_at != candidate.created_at
        {
            return Err(StorageError::internal(
                "SystemStream record differs from its effect receipt",
            ));
        }
        verified.require_effect(self.history_id, self.item_stream_version, &self.effect_id)?;
        let physical_pointer: StoredStreamPointer = storage_serde::from_bytes(&pointer.data)?;
        if physical_pointer.table_name().as_ref() != record.source_table_name.as_str()
            || physical_pointer.history_address().history_id != self.history_id
            || physical_pointer.target_item_stream_version() != self.item_stream_version
        {
            return Err(StorageError::internal(
                "SystemStream physical pointer differs from its record source",
            ));
        }
        if reference.owner != PointerOwner::System
            || reference.owner_stream != StreamName::system_table_stream()
            || reference.owner_item_id != candidate.pointer_item_id
            || reference.item_stream != physical_pointer.history_address().physical_stream_name()
            || reference.item_item_id
                != match &physical_pointer {
                    StoredStreamPointer::Embedded { .. } => None,
                    StoredStreamPointer::Pointer { .. } => Some(StreamItemId::from(
                        physical_pointer.target_item_stream_version(),
                    )),
                }
        {
            return Err(StorageError::internal(
                "SystemStream pointer reference differs from its target",
            ));
        }
        self.trimmed = true;
        Ok(self)
    }
}

impl DynamoStreamWritePlan {
    pub async fn prepare<A: crate::retained_image::RetainedOwnershipAttempt>(
        attempt: &mut A,
        mut input: DynamoStreamWriteInput<'_>,
    ) -> StorageResult<Self> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            input.require_bound_identity()?;
            input.mutation.restore_source_image(attempt).await?;
            PreparedDynamoStreamContent::try_from(input)?
                .retain(attempt)
                .await
        }
        .await;
        settlement.complete(result)
    }
}

struct PreparedDynamoStreamContent {
    rows: PreparedDynamoStreamRows,
    pointer_references: [StreamPointerReference; 2],
    item_image: crate::retained_image::stream_image::AcceptedStreamImagePlan,
    pointer_image: crate::retained_image::pointer::AcceptedPointerPlan,
    record: SystemStreamStoredRecord,
    receipt: DynamoStreamEffectRow,
    raw_images: StreamImageBytes,
}

struct PreparedDynamoStreamRows {
    item: StreamItem,
    table: StreamItem,
    system: StreamItem,
}

impl TryFrom<DynamoStreamWriteInput<'_>> for PreparedDynamoStreamContent {
    type Error = StorageError;

    fn try_from(mut input: DynamoStreamWriteInput<'_>) -> StorageResult<Self> {
        let table = input.table;
        let mutation = &input.mutation;
        let item_history = dynamo_item_history_address(table, &mutation.key)?;
        let raw_images = StreamImageBytes::try_from_mutation(&mut input.mutation)?;
        let images = EncodedImages::from(&raw_images);
        let pointer = prepare_pointer(&input, &item_history.stream_name, &images)?;
        let referenced_version = pointer.referenced_version;
        let pointer_metadata = storage_serde::from_bytes(&pointer.encoded)?;
        let rows = build_stream_rows(&input, item_history, images, pointer);
        let pointer_references = pointer_references(&rows, referenced_version)?;
        let item_image = crate::retained_image::stream_image::AcceptedStreamImagePlan::try_new(
            table.history_id,
            &rows.item,
            &input.mutation.item,
        )?;
        let (record, receipt) = DynamoStreamEffectRow::for_write(input.mutation, input.position)?;
        let pointer_image = crate::retained_image::pointer::AcceptedPointerPlan::try_new(
            pointer_metadata,
            receipt.created_at,
            receipt.effect_id.clone(),
            [Some(raw_images.item.as_slice()), raw_images.old.as_deref()],
        )?;
        Ok(Self {
            rows,
            pointer_references,
            item_image,
            pointer_image,
            record,
            receipt,
            raw_images,
        })
    }
}

impl PreparedDynamoStreamContent {
    async fn retain<A: crate::retained_image::RetainedOwnershipAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<DynamoStreamWritePlan> {
        let Self {
            rows,
            pointer_references,
            item_image,
            pointer_image,
            record,
            receipt,
            raw_images,
        } = self;
        let PreparedDynamoStreamRows {
            mut item,
            mut table,
            mut system,
        } = rows;
        let record = crate::retained_image::system_stream::RetainedSystemStreamRecord::prepare(
            attempt,
            record,
            &receipt,
            raw_images.as_refs(),
        )
        .await?;
        item.data = item_image.prepare(attempt).await?;
        let pointer_bytes = pointer_image.prepare(attempt).await?;
        table.data = pointer_bytes.clone();
        system.data = pointer_bytes;
        let rows = [
            StreamWriteKind::Item(item),
            StreamWriteKind::Table(table),
            StreamWriteKind::System(system),
        ];
        let mut owners = rows
            .iter()
            .map(crate::retained_image::stream_row::StoredStreamRow::planned_owner)
            .collect::<StorageResult<Vec<_>>>()?;
        owners.extend(record.prepare_owner()?);
        crate::retained_image::RetainedOwnerRecord::publish_stream_batch(attempt, owners).await?;
        Ok(DynamoStreamWritePlan {
            rows,
            pointer_references,
            record,
            receipt,
        })
    }
}

fn pointer_references(
    rows: &PreparedDynamoStreamRows,
    referenced_version: Option<ItemStreamVersion>,
) -> StorageResult<[StreamPointerReference; 2]> {
    let PreparedDynamoStreamRows {
        item,
        table,
        system,
    } = rows;
    let item_stream = item
        .stream_name
        .as_ref()
        .ok_or_else(|| StorageError::internal("Dynamo item image has no stream name"))?;
    let reference = |pointer: &StreamItem, owner| -> StorageResult<StreamPointerReference> {
        let owner_stream = pointer
            .stream_name
            .as_ref()
            .ok_or_else(|| StorageError::internal("Dynamo pointer has no stream name"))?;
        Ok(StreamPointerReference {
            owner,
            owner_stream: owner_stream.clone(),
            owner_item_id: pointer.id,
            item_stream: item_stream.clone(),
            item_item_id: referenced_version.map(StreamItemId::from),
        })
    };
    Ok([
        reference(table, PointerOwner::Table)?,
        reference(system, PointerOwner::System)?,
    ])
}

struct EncodedImages {
    item_data: Vec<u8>,
    old_data: Option<Vec<u8>>,
    data_type: StreamDataType,
}

struct StreamImageBytes {
    item: Vec<u8>,
    old: Option<Vec<u8>>,
    is_deleted: bool,
}

impl StreamImageBytes {
    fn try_from_mutation(mutation: &mut LogicalSystemStreamMutation) -> StorageResult<Self> {
        Ok(Self {
            item: std::mem::take(&mut mutation.item_json),
            old: mutation
                .old_item
                .as_ref()
                .map(WireItem::dynamo_json_bytes)
                .transpose()?
                .map(std::borrow::Cow::into_owned),
            is_deleted: mutation.is_deleted,
        })
    }

    fn as_refs(&self) -> [Option<&[u8]>; 2] {
        [
            self.old.as_deref(),
            (!self.is_deleted).then_some(self.item.as_slice()),
        ]
    }
}

impl From<&StreamImageBytes> for EncodedImages {
    fn from(images: &StreamImageBytes) -> Self {
        let item_data = storage_serde::compress_json_bytes(&images.item);
        let old_data = if can_embed_new_image(item_data.len()) {
            images
                .old
                .as_deref()
                .map(storage_serde::compress_json_bytes)
        } else {
            None
        };
        let data_type = if images.is_deleted {
            StreamDataType::DeleteMarker
        } else {
            StreamDataType::DynamoDbJson
        };
        Self {
            item_data,
            old_data,
            data_type,
        }
    }
}

pub fn dynamo_item_history_address(
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<ItemHistoryAddress> {
    let item_key = ItemKey::from_key_schema(table.table_name.clone(), &table.key_schema, key)
        .map_err(|error| StorageError::internal(&format!("stream item key error: {error}")))?;
    Ok(ItemHistoryAddress {
        history_id: table.history_id,
        stream_name: StreamName::table_item_stream(&table.table_name, &item_key)?,
    })
}

fn prepare_pointer(
    input: &DynamoStreamWriteInput<'_>,
    item_stream: &StreamName,
    images: &EncodedImages,
) -> StorageResult<StreamPointerPlan> {
    let mutation = &input.mutation;
    StreamPointerPlan::try_new(StreamPointerInput {
        history_id: input.table.history_id,
        item_stream,
        table_name: &input.table.table_name,
        item_stream_version: mutation.item_stream_version,
        item_revision: ItemRevision::try_from(mutation.item_stream_version.get())?,
        item_data: &images.item_data,
        old_item_data: images.old_data.as_deref(),
        data_type: images.data_type,
        indexers: &mutation.indexers,
        old_indexers: mutation.old_indexers.as_deref(),
    })
}

fn build_stream_rows(
    input: &DynamoStreamWriteInput<'_>,
    item_history: ItemHistoryAddress,
    images: EncodedImages,
    pointer: StreamPointerPlan,
) -> PreparedDynamoStreamRows {
    let mutation = &input.mutation;
    let physical_item_stream = item_history.physical_stream_name();
    let physical_table_stream = ItemHistoryAddress {
        history_id: input.table.history_id,
        stream_name: StreamName::table_stream(&input.table.table_name),
    }
    .physical_stream_name();
    PreparedDynamoStreamRows {
        item: StreamItem {
            id: StreamItemId::from(mutation.item_stream_version),
            stream_name: Some(physical_item_stream),
            data: images.item_data,
            data_type: images.data_type,
            created_at: mutation.created_at,
        },
        table: StreamItem {
            id: mutation.stream_item_id,
            stream_name: Some(physical_table_stream),
            data: pointer.encoded.clone(),
            data_type: StreamDataType::StreamPointer,
            created_at: mutation.created_at,
        },
        system: StreamItem {
            id: mutation.stream_item_id,
            stream_name: Some(StreamName::system_table_stream()),
            data: pointer.encoded,
            data_type: StreamDataType::StreamPointer,
            created_at: mutation.created_at,
        },
    }
}
