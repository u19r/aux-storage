//! Item-history descriptors bind immutable images to their exact native row
//! identity.
use serde::{Deserialize, Serialize};
use storage_types::{
    StorageResult, StreamItemId, StreamName, TableHistoryId, TimestampMillis, WireItem,
};

use super::{
    RetainedImageManifest,
    error::invalid_image,
    image_read_work::{ImageProjectionShape, ImageReadWork},
    json_projection::RetainedJsonProjection,
    read_work::RetainedReadWork,
};
use crate::{StreamDataType, StreamItem};

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct StreamImageIdentity {
    stream: StreamName,
    version: StreamItemId,
    data_type: StreamDataType,
    created_at: TimestampMillis,
}

pub(crate) struct AcceptedStreamImagePlan {
    history: TableHistoryId,
    identity: StreamImageIdentity,
    bytes: Vec<u8>,
}

pub(super) struct RetainedStreamImageRecord {
    history: TableHistoryId,
    identity: StreamImageIdentity,
    manifest: Vec<u8>,
    work: RetainedReadWork,
}

#[derive(Deserialize)]
struct StreamImageContent(
    TableHistoryId,
    StreamImageIdentity,
    #[serde(with = "storage_types::dynamo_json::optional")] Option<WireItem>,
);

#[derive(Deserialize)]
struct StreamImageSpan<'a>(
    serde::de::IgnoredAny,
    serde::de::IgnoredAny,
    #[serde(borrow)] &'a serde_json::value::RawValue,
);

impl StreamImageIdentity {
    fn try_from_item(item: &StreamItem) -> StorageResult<Self> {
        let stream = item.stream_name.clone().ok_or_else(invalid_image)?;
        if !matches!(
            item.data_type,
            StreamDataType::DynamoDbJson | StreamDataType::DeleteMarker
        ) {
            return Err(invalid_image());
        }
        let version = storage_types::ItemStreamVersion::from(item.id);
        if version.get() == 0 || StreamItemId::from(version) != item.id {
            return Err(invalid_image());
        }
        Ok(Self {
            stream,
            version: item.id,
            data_type: item.data_type,
            created_at: item.created_at,
        })
    }
}

impl AcceptedStreamImagePlan {
    pub(crate) fn try_new(
        history: TableHistoryId,
        row: &StreamItem,
        image: &WireItem,
    ) -> StorageResult<Self> {
        let identity = StreamImageIdentity::try_from_item(row)?;
        if super::stream_row::decode_history_scope(&identity.stream)? != history {
            return Err(invalid_image());
        }
        let bytes = super::json_output::RetainedJsonOutput::encode(&(
            history,
            &identity,
            storage_types::dynamo_json::ItemRef(image),
        ))?;
        Ok(Self {
            history,
            identity,
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
            let StreamImageSpan(_, _, image) = serde_json::from_slice(&self.bytes)?;
            let image = image.get().as_bytes();
            let limits = RetainedImageManifest::image_work_limits(self.bytes.len())?;
            let manifest = RetainedImageManifest::split(&self.bytes, &[image], Some(self.history))?;
            let mut prepared = manifest.prepare(attempt, &[image], limits).await?;
            let bodies = std::mem::take(&mut prepared.bodies.work);
            let work = RetainedReadWork::prepare(&self.bytes, &prepared.manifest, bodies)?;
            work.qualify_images(usize::MAX, ImageProjectionShape::History)?;
            let manifest = prepared.publish(attempt).await?;
            RetainedStreamImageRecord {
                history: self.history,
                identity: self.identity,
                manifest,
                work,
            }
            .encode()
        }
        .await;
        settlement_guard.complete(result)
    }
}

impl RetainedStreamImageRecord {
    fn encode(&self) -> StorageResult<Vec<u8>> {
        super::descriptor_codec::DescriptorInput {
            kind: super::descriptor_codec::DescriptorKind::ItemHistory,
            history: self.history,
            identity: &self.identity,
            manifest: &self.manifest,
            work: &self.work,
        }
        .encode(usize::MAX)
    }

    pub(super) fn decode(row: &StreamItem, maximum: usize) -> StorageResult<Self> {
        let descriptor = super::descriptor_codec::DescriptorDecoder::decode::<StreamImageIdentity>(
            &row.data,
            super::descriptor_codec::DescriptorKind::ItemHistory,
            maximum,
        )?;
        if descriptor.identity != StreamImageIdentity::try_from_item(row)?
            || descriptor.history
                != super::stream_row::decode_history_scope(&descriptor.identity.stream)?
        {
            return Err(invalid_image());
        }
        Ok(Self {
            history: descriptor.history,
            identity: descriptor.identity,
            manifest: descriptor.manifest,
            work: descriptor.work,
        })
    }

    pub(super) fn prepare_owner(&self) -> StorageResult<super::RetainedOwnerRecord> {
        self.prepare_read()?.receipt.ok_or_else(invalid_image)
    }

    pub(super) fn prepare_read(
        &self,
    ) -> StorageResult<super::stream_hydration::PreparedStreamImageRead<'_>> {
        let manifest = self.content_manifest(usize::MAX)?;
        let locator = super::stream_ownership::history_locator(
            self.history,
            &self.identity.stream,
            self.identity.version,
        )?;
        let descriptor = self.encode()?;
        let receipt = manifest
            .prepare_owner_record(super::RetainedOwnerKind::ItemHistory, locator, &descriptor)?
            .ok_or_else(invalid_image)?;
        Ok(super::stream_hydration::PreparedStreamImageRead {
            manifest,
            work: &self.work.bodies,
            receipt: Some(receipt),
        })
    }

    pub(super) fn qualify_read_work(&self, maximum: usize) -> StorageResult<ImageReadWork> {
        self.work
            .qualify_images(maximum, ImageProjectionShape::History)
    }

    fn content_manifest(&self, maximum: usize) -> StorageResult<RetainedImageManifest> {
        let manifest = self
            .work
            .decode_manifest(&self.manifest, self.history, maximum)?;
        if manifest.segments.len() != 3 {
            return Err(invalid_image());
        }
        Ok(manifest)
    }

    pub(super) fn project_image(&self, reader: &mut dyn std::io::Read) -> StorageResult<WireItem> {
        let mut budget = self.work.projection_bytes;
        let projection =
            RetainedJsonProjection::try_for_images(reader, &mut budget, self.work.metadata_bytes)
                .map_err(|_| invalid_image())?;
        let content: StreamImageContent = serde_json::from_reader(projection)?;
        if budget != 0 || content.0 != self.history || content.1 != self.identity {
            return Err(invalid_image());
        }
        let image = content.2.ok_or_else(invalid_image)?;
        image.logical_size()?;
        Ok(image)
    }
}
