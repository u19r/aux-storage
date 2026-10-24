//! Pointer metadata and exact embedded images share the immutable pool.
use serde::Deserialize;
use storage_types::{
    StorageResult, SystemStreamEffectDigest, TableHistoryId, TimestampMillis, WireItem,
};

use super::{
    RetainedImageManifest,
    error::invalid_image,
    image_read_work::{ImageProjectionShape, ImageReadWork},
    json_projection::RetainedJsonProjection,
    read_work::RetainedReadWork,
};
use crate::{StoredStreamPointer, StreamDataType, StreamItem};

pub(crate) struct AcceptedPointerPlan {
    history: TableHistoryId,
    created_at: TimestampMillis,
    effect: SystemStreamEffectDigest,
    bytes: Vec<u8>,
}

pub(super) struct RetainedPointerRecord {
    history: TableHistoryId,
    created_at: TimestampMillis,
    effect: SystemStreamEffectDigest,
    manifest: Vec<u8>,
    work: RetainedReadWork,
}

#[derive(Deserialize)]
struct PointerContent(
    StoredStreamPointer,
    TimestampMillis,
    SystemStreamEffectDigest,
    #[serde(with = "storage_types::dynamo_json::optional")] Option<WireItem>,
    #[serde(with = "storage_types::dynamo_json::optional")] Option<WireItem>,
);

#[derive(Deserialize)]
struct PointerDeclarationSpans<'a> {
    #[serde(borrow)]
    indexers: &'a serde_json::value::RawValue,
    #[serde(borrow)]
    old_indexers: Option<&'a serde_json::value::RawValue>,
}

#[derive(Deserialize)]
struct PointerSpans<'a>(
    #[serde(borrow)] PointerDeclarationSpans<'a>,
    serde::de::IgnoredAny,
    serde::de::IgnoredAny,
    #[serde(borrow)] Option<&'a serde_json::value::RawValue>,
    #[serde(borrow)] Option<&'a serde_json::value::RawValue>,
);

impl AcceptedPointerPlan {
    pub(crate) fn try_new(
        mut pointer: StoredStreamPointer,
        created_at: TimestampMillis,
        effect: SystemStreamEffectDigest,
        images: [Option<&[u8]>; 2],
    ) -> StorageResult<Self> {
        let history = pointer.history_address().history_id;
        let images = match &mut pointer {
            StoredStreamPointer::Pointer { .. } => [None, None],
            StoredStreamPointer::Embedded { items, .. } => {
                if !(1..=2).contains(&items.len())
                    || images[0].is_none()
                    || images[1].is_some() != (items.len() == 2)
                {
                    return Err(invalid_image());
                }
                for item in items {
                    item.data.clear();
                }
                images
            }
        };
        require_pointer_shape(&pointer)?;
        let new: Option<&serde_json::value::RawValue> =
            images[0].map(serde_json::from_slice).transpose()?;
        let old: Option<&serde_json::value::RawValue> =
            images[1].map(serde_json::from_slice).transpose()?;
        let bytes = super::json_output::RetainedJsonOutput::encode(&(
            pointer, created_at, &effect, new, old,
        ))?;
        Ok(Self {
            history,
            created_at,
            effect,
            bytes,
        })
    }

    pub(crate) async fn prepare<A: super::RetainedOwnershipAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<Vec<u8>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let spans: PointerSpans<'_> = serde_json::from_slice(&self.bytes)?;
            let spans = [
                Some(spans.0.indexers),
                spans.0.old_indexers,
                spans.3,
                spans.4,
            ];
            let mut bodies = Vec::new();
            bodies
                .try_reserve_exact(spans.len())
                .map_err(|_| super::error::capacity_rejected())?;
            bodies.extend(
                spans
                    .into_iter()
                    .flatten()
                    .map(|body| body.get().as_bytes()),
            );
            let limits = RetainedImageManifest::image_work_limits(self.bytes.len())?;
            let manifest = RetainedImageManifest::split(&self.bytes, &bodies, Some(self.history))?;
            let mut prepared = manifest.prepare(attempt, &bodies, limits).await?;
            let bodies = std::mem::take(&mut prepared.bodies.work);
            let work = RetainedReadWork::prepare(&self.bytes, &prepared.manifest, bodies)?;
            work.qualify_images(usize::MAX, ImageProjectionShape::Pointer)?;
            let manifest = prepared.publish(attempt).await?;
            RetainedPointerRecord {
                history: self.history,
                created_at: self.created_at,
                effect: self.effect,
                manifest,
                work,
            }
            .encode()
        }
        .await;
        settlement_guard.complete(result)
    }
}

impl RetainedPointerRecord {
    fn encode(&self) -> StorageResult<Vec<u8>> {
        super::descriptor_codec::DescriptorInput {
            kind: super::descriptor_codec::DescriptorKind::Pointer,
            history: self.history,
            identity: &(self.created_at, &self.effect),
            manifest: &self.manifest,
            work: &self.work,
        }
        .encode(usize::MAX)
    }

    pub(super) fn decode(row: &StreamItem, maximum: usize) -> StorageResult<Self> {
        if row.data_type != StreamDataType::StreamPointer {
            return Err(invalid_image());
        }
        let descriptor = super::descriptor_codec::DescriptorDecoder::decode::<(
            TimestampMillis,
            SystemStreamEffectDigest,
        )>(
            &row.data,
            super::descriptor_codec::DescriptorKind::Pointer,
            maximum,
        )?;
        let (created_at, effect) = descriptor.identity;
        if created_at != row.created_at {
            return Err(invalid_image());
        }
        Ok(Self {
            history: descriptor.history,
            created_at,
            effect,
            manifest: descriptor.manifest,
            work: descriptor.work,
        })
    }

    pub(super) fn require_effect(&self, effect: &SystemStreamEffectDigest) -> StorageResult<()> {
        if &self.effect != effect {
            return Err(invalid_image());
        }
        Ok(())
    }

    pub(super) fn prepare_row_owner(
        &self,
        row: &StreamItem,
    ) -> StorageResult<super::RetainedOwnerRecord> {
        self.prepare_read(row)?.receipt.ok_or_else(invalid_image)
    }

    pub(super) fn prepare_read(
        &self,
        row: &StreamItem,
    ) -> StorageResult<super::stream_hydration::PreparedStreamImageRead<'_>> {
        let manifest = self
            .work
            .decode_manifest(&self.manifest, self.history, usize::MAX)?;
        let stream = row.stream_name.as_ref().ok_or_else(invalid_image)?;
        let kind = if stream == &storage_types::StreamName::system_table_stream() {
            super::RetainedOwnerKind::SystemPointer
        } else {
            super::RetainedOwnerKind::TablePointer
        };
        let locator = super::stream_ownership::effect_locator(self.history, &self.effect)?;
        let mut descriptor = self.encode()?;
        descriptor.extend_from_slice(
            &u64::try_from(stream.as_ref().len())
                .map_err(|_| invalid_image())?
                .to_be_bytes(),
        );
        descriptor.extend_from_slice(stream.as_ref());
        let receipt = manifest
            .prepare_owner_record(kind, locator, &descriptor)?
            .ok_or_else(invalid_image)?;
        Ok(super::stream_hydration::PreparedStreamImageRead {
            manifest,
            work: &self.work.bodies,
            receipt: Some(receipt),
        })
    }

    pub(super) fn qualify_read_work(&self, maximum: usize) -> StorageResult<ImageReadWork> {
        self.work
            .qualify_images(maximum, ImageProjectionShape::Pointer)
    }

    pub(super) fn project_pointer(
        &self,
        mut row: StreamItem,
        reader: &mut dyn std::io::Read,
    ) -> StorageResult<StreamItem> {
        let mut budget = self.work.projection_bytes;
        let projection =
            RetainedJsonProjection::try_for_images(reader, &mut budget, self.work.metadata_bytes)
                .map_err(|_| invalid_image())?;
        let mut content: PointerContent = serde_json::from_reader(projection)?;
        if budget != 0
            || content.1 != self.created_at
            || content.2 != self.effect
            || content.0.history_address().history_id != self.history
        {
            return Err(invalid_image());
        }
        require_pointer_address(&content.0, &row)?;
        restore_pointer_images(&mut content)?;
        row.data = storage_types::storage_serde::to_bytes(&content.0)?;
        Ok(row)
    }
}

fn require_pointer_shape(pointer: &StoredStreamPointer) -> StorageResult<()> {
    require_pointer_declarations(pointer)?;
    let history = pointer.history_address();
    if history.history_id.as_ref() == &[0; 16]
        || history
            .stream_name
            .item_table_name()
            .is_none_or(|table| table.sanitized_name() != pointer.table_name().sanitized_name())
        || pointer.target_item_stream_version().get() == 0
    {
        return Err(invalid_image());
    }
    if let StoredStreamPointer::Embedded { items, .. } = pointer
        && (!(1..=2).contains(&items.len())
            || items.iter().any(|item| !item.data.is_empty())
            || !matches!(
                items[0].data_type,
                StreamDataType::DynamoDbJson | StreamDataType::DeleteMarker
            )
            || items
                .get(1)
                .is_some_and(|item| item.data_type != StreamDataType::DynamoDbJson))
    {
        return Err(invalid_image());
    }
    Ok(())
}

fn require_pointer_declarations(pointer: &StoredStreamPointer) -> StorageResult<()> {
    let (StoredStreamPointer::Pointer {
        indexers,
        old_indexers,
        ..
    }
    | StoredStreamPointer::Embedded {
        indexers,
        old_indexers,
        ..
    }) = pointer;
    let capacity = storage_types::MaxIndexers::try_new(storage_types::MAX_INDEXERS_CAPACITY)?;
    for names in std::iter::once(indexers).chain(old_indexers) {
        storage_types::IndexerDeclaration::validate(names, capacity)
            .map_err(|_| invalid_image())?;
    }
    Ok(())
}

fn require_pointer_address(pointer: &StoredStreamPointer, row: &StreamItem) -> StorageResult<()> {
    require_pointer_shape(pointer)?;
    let table = storage_types::ItemHistoryAddress {
        history_id: pointer.history_address().history_id,
        stream_name: storage_types::StreamName::table_stream(pointer.table_name()),
    }
    .physical_stream_name();
    if row.stream_name.as_ref() != Some(&table)
        && row.stream_name.as_ref() != Some(&storage_types::StreamName::system_table_stream())
    {
        return Err(invalid_image());
    }
    Ok(())
}

fn restore_pointer_images(content: &mut PointerContent) -> StorageResult<()> {
    let images = [content.3.take(), content.4.take()];
    let StoredStreamPointer::Embedded { items, .. } = &mut content.0 else {
        if images.iter().any(Option::is_some) {
            return Err(invalid_image());
        }
        return Ok(());
    };
    if images[0].is_none() || images[1].is_some() != (items.len() == 2) {
        return Err(invalid_image());
    }
    for (item, image) in items.iter_mut().zip(images.into_iter().flatten()) {
        image.logical_size()?;
        item.data = storage_types::storage_serde::compress_json_bytes(&image.dynamo_json_bytes()?);
    }
    Ok(())
}
