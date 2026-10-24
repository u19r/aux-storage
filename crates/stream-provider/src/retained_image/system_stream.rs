//! Native SystemStream content uses the shared immutable image owner.
use serde::Deserialize;
use storage_types::{
    ItemStreamVersion, KeyAttributes, StorageError, StorageResult, SystemStreamEffectDigest,
    SystemStreamPosition, SystemStreamSourceTableName, TableHistoryId, WireItem,
};

use super::{
    image_read_work::{ImageProjectionShape, ImageReadWork},
    page_budget::RetainedImagePageBudget,
    verified_system_stream::VerifiedSystemStreamRecord,
};
use crate::{SystemStreamStoredRecord, retained_image::RetainedImageManifest};

pub const RETAINED_SYSTEM_STREAM_RECORD_FORMAT: u8 = 6;

pub struct RetainedSystemStreamRecord {
    pub position: SystemStreamPosition,
    history: TableHistoryId,
    version: ItemStreamVersion,
    manifest: Vec<u8>,
    work: super::read_work::RetainedReadWork,
}

#[derive(Deserialize)]
struct SystemStreamContent(
    TableHistoryId,
    ItemStreamVersion,
    SystemStreamSourceTableName,
    KeyAttributes,
    #[serde(with = "storage_types::dynamo_json::optional")] Option<WireItem>,
    #[serde(with = "storage_types::dynamo_json::optional")] Option<WireItem>,
);

#[derive(Deserialize)]
struct ImageSpans<'a>(
    serde::de::IgnoredAny,
    serde::de::IgnoredAny,
    serde::de::IgnoredAny,
    serde::de::IgnoredAny,
    #[serde(borrow)] Option<&'a serde_json::value::RawValue>,
    #[serde(borrow)] Option<&'a serde_json::value::RawValue>,
);

impl RetainedSystemStreamRecord {
    /// Native metadata stores each compressed manifest and body identity once.
    pub fn encode(&self, maximum: usize) -> StorageResult<Vec<u8>> {
        super::descriptor_codec::DescriptorInput {
            kind: super::descriptor_codec::DescriptorKind::SystemRecord,
            history: self.history,
            identity: &(&self.position, self.version),
            manifest: &self.manifest,
            work: &self.work,
        }
        .encode(maximum)
    }

    pub fn decode(bytes: &[u8], maximum: usize) -> StorageResult<Self> {
        let descriptor = super::descriptor_codec::DescriptorDecoder::decode::<(
            SystemStreamPosition,
            ItemStreamVersion,
        )>(
            bytes,
            super::descriptor_codec::DescriptorKind::SystemRecord,
            maximum,
        )?;
        let (position, version) = descriptor.identity;
        if version.get() == 0 {
            return Err(invalid_record());
        }
        Ok(Self {
            position,
            history: descriptor.history,
            version,
            manifest: descriptor.manifest,
            work: descriptor.work,
        })
    }

    pub(crate) async fn prepare<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        record: SystemStreamStoredRecord,
        receipt: &crate::DynamoStreamEffectRow,
        images: [Option<&[u8]>; 2],
    ) -> StorageResult<Self> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let history = receipt.history_id;
            let version = receipt.item_stream_version;
            let bytes = encode_accepted_content(&record, receipt, images)?;
            let spans: ImageSpans<'_> = serde_json::from_slice(&bytes)?;
            let spans = [spans.4, spans.5];
            let mut images = Vec::new();
            images
                .try_reserve_exact(spans.len())
                .map_err(|_| super::error::capacity_rejected())?;
            images.extend(
                spans
                    .into_iter()
                    .flatten()
                    .map(|image| image.get().as_bytes()),
            );
            let limits = RetainedImageManifest::image_work_limits(bytes.len())?;
            let manifest = RetainedImageManifest::split(
                &bytes,
                &images,
                (!images.is_empty()).then_some(history),
            )?;
            let mut prepared = manifest.prepare(attempt, &images, limits).await?;
            let bodies = std::mem::take(&mut prepared.bodies.work);
            let work =
                super::read_work::RetainedReadWork::prepare(&bytes, &prepared.manifest, bodies)?;
            work.qualify_images(usize::MAX, ImageProjectionShape::System)?;
            let manifest = prepared.publish(attempt).await?;
            Ok(Self {
                position: record.position,
                history,
                version,
                manifest,
                work,
            })
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) fn prepare_owner(&self) -> StorageResult<Option<super::RetainedOwnerRecord>> {
        Ok(self.prepare_read()?.receipt)
    }

    pub(super) fn prepare_read(
        &self,
    ) -> StorageResult<super::stream_hydration::PreparedStreamImageRead<'_>> {
        let manifest = self.content_manifest(usize::MAX)?;
        let effect = SystemStreamEffectDigest::try_new(hex::encode(manifest.digest))?;
        let locator = super::stream_ownership::effect_locator(self.history, &effect)?;
        let descriptor = super::descriptor_codec::DescriptorInput {
            kind: super::descriptor_codec::DescriptorKind::SystemRecord,
            history: self.history,
            identity: &self.version,
            manifest: &self.manifest,
            work: &self.work,
        }
        .encode(usize::MAX)?;
        let receipt = manifest.prepare_owner_record(
            super::RetainedOwnerKind::SystemRecord,
            locator,
            &descriptor,
        )?;
        Ok(super::stream_hydration::PreparedStreamImageRead {
            manifest,
            work: &self.work.bodies,
            receipt,
        })
    }

    pub fn retirement_receipts(records: &[Self]) -> StorageResult<Vec<super::RetainedOwnerRecord>> {
        Ok(records
            .iter()
            .map(Self::prepare_owner)
            .collect::<StorageResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect())
    }

    pub fn history(&self) -> TableHistoryId {
        self.history
    }

    fn content_manifest(&self, maximum: usize) -> StorageResult<RetainedImageManifest> {
        if self.history.as_ref() == &[0; 16] || self.version.get() == 0 {
            return Err(invalid_record());
        }
        self.work
            .decode_manifest(&self.manifest, self.history, maximum)
    }

    pub fn read_work_bytes(&self, maximum: usize) -> StorageResult<usize> {
        Ok(self.qualify_read_work(maximum)?.page_bytes())
    }

    fn qualify_read_work(&self, maximum: usize) -> StorageResult<ImageReadWork> {
        self.work
            .qualify_images(maximum, ImageProjectionShape::System)
    }

    fn admit_read_work(records: &[Self], maximum: usize) -> StorageResult<ImageReadWork> {
        ImageReadWork::admit_batch(
            records
                .iter()
                .map(|record| record.qualify_read_work(maximum)),
            maximum,
        )
    }

    pub fn select_prefix(records: Vec<Self>, maximum: usize) -> StorageResult<(Vec<Self>, bool)> {
        let mut budget = RetainedImagePageBudget::try_new(maximum)?;
        let mut selected = Vec::new();
        for record in records {
            let length = record.read_work_bytes(maximum)?;
            if !budget.admit(length)? {
                return Ok((selected, true));
            }
            selected.push(record);
        }
        Ok((selected, false))
    }

    pub fn select_pointer_prefix(
        records: Vec<Self>,
        pointers: Vec<Option<super::stream_row::StoredStreamRow>>,
        maximum: usize,
    ) -> StorageResult<(
        Vec<Self>,
        Vec<Option<super::stream_row::StoredStreamRow>>,
        bool,
    )> {
        if records.len() != pointers.len() {
            return Err(invalid_record());
        }
        let mut record_budget = RetainedImagePageBudget::try_new(maximum)?;
        let mut pointer_budget = RetainedImagePageBudget::try_new(maximum)?;
        let mut selected = Vec::new();
        let mut retained = Vec::new();
        for (record, pointer) in records.into_iter().zip(pointers) {
            let pointer_bytes = pointer
                .as_ref()
                .map(|row| row.read_work_bytes(maximum))
                .transpose()?
                .unwrap_or(0);
            if !record_budget.admit(record.read_work_bytes(maximum)?)?
                || !pointer_budget.admit(pointer_bytes)?
            {
                return Ok((selected, retained, true));
            }
            selected.push(record);
            retained.push(pointer);
        }
        Ok((selected, retained, false))
    }

    pub fn admit_trim_work(
        records: &[Self],
        pointers: &[super::stream_row::StoredStreamRow],
        maximum: usize,
    ) -> StorageResult<()> {
        if records.len() != pointers.len() {
            return Err(invalid_record());
        }
        Self::admit_read_work(records, maximum)?;
        super::stream_row::StoredStreamRow::admit_read_work(pointers.iter(), maximum)?;
        Ok(())
    }

    pub fn select_trim_prefix(
        records: Vec<(usize, crate::SystemStreamTrimRecord)>,
        pointers: Vec<super::stream_row::StoredStreamRow>,
        maximum: usize,
    ) -> StorageResult<(Vec<crate::SystemStreamTrimRecord>, bool)> {
        if records.len() != pointers.len() {
            return Err(invalid_record());
        }
        let mut record_budget = RetainedImagePageBudget::try_new(maximum)?;
        let mut pointer_budget = RetainedImagePageBudget::try_new(maximum)?;
        let mut selected = Vec::with_capacity(records.len());
        for ((record_bytes, record), pointer) in records.into_iter().zip(pointers) {
            pointer.require_address(
                &storage_types::StreamName::system_table_stream(),
                record.pointer_item_id,
            )?;
            pointer.require_pointer_effect(&record.effect_id)?;
            if pointer.metadata().1 != record.created_at {
                return Err(invalid_record());
            }
            if !record_budget.admit(record_bytes)?
                || !pointer_budget.admit(pointer.read_work_bytes(maximum)?)?
            {
                return Ok((selected, true));
            }
            selected.push(record);
        }
        Ok((selected, false))
    }

    pub(super) fn admit_pointer_group(
        records: &[Self],
        group: &[Option<super::stream_row::StoredStreamRow>],
        maximum: usize,
    ) -> StorageResult<()> {
        if records.len() != group.len() {
            return Err(invalid_record());
        }
        let work = Self::admit_read_work(records, maximum)?;
        super::stream_row::StoredStreamRow::admit_read_work(group.iter().flatten(), maximum)?;
        for (record, pointer) in records.iter().zip(group.iter()) {
            if let Some(pointer) = pointer {
                let digest = record.content_manifest(work.page_bytes())?.digest;
                pointer.require_pointer_effect(&SystemStreamEffectDigest::try_new(
                    hex::encode(digest),
                )?)?;
            }
        }
        Ok(())
    }

    pub async fn hydrate_history_groups<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        records: Vec<Vec<Self>>,
        pointers: Vec<Vec<Option<super::stream_row::StoredStreamRow>>>,
        maximum: usize,
    ) -> StorageResult<
        Vec<(
            Vec<SystemStreamStoredRecord>,
            Vec<Option<crate::StreamItem>>,
        )>,
    > {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            super::system_history_hydration::PreparedSystemHistoryHydration::try_new(
                records, pointers, maximum,
            )?
            .load(settlement.as_mut())
            .await
        }
        .await;
        settlement.complete(result)
    }

    pub async fn hydrate_optional<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        records: Vec<Option<Self>>,
        maximum: usize,
    ) -> StorageResult<Vec<Option<VerifiedSystemStreamRecord>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let present = records.iter().map(Option::is_some).collect::<Vec<_>>();
            let mut hydrated =
                Self::hydrate(attempt, records.into_iter().flatten().collect(), maximum)
                    .await?
                    .into_iter();
            let result = present
                .into_iter()
                .map(|present| {
                    if present {
                        Ok(Some(hydrated.next().ok_or_else(invalid_record)?))
                    } else {
                        Ok(None)
                    }
                })
                .collect::<StorageResult<Vec<_>>>()?;
            if hydrated.next().is_some() {
                return Err(invalid_record());
            }
            Ok(result)
        }
        .await;
        settlement.complete(result)
    }

    pub async fn hydrate<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        records: Vec<Self>,
        maximum: usize,
    ) -> StorageResult<Vec<VerifiedSystemStreamRecord>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            Self::admit_read_work(&records, maximum)?;
            let mut prepared = super::stream_hydration::PreparedStreamHydration::default();
            for record in records {
                prepared.include_system(record)?;
            }
            prepared
                .load(attempt, maximum)
                .await?
                .into_iter()
                .map(VerifiedSystemStreamRecord::try_from)
                .collect()
        }
        .await;
        settlement.complete(result)
    }
    pub(super) fn project_record(
        self,
        reader: &mut dyn std::io::Read,
        digest: [u8; 32],
    ) -> StorageResult<VerifiedSystemStreamRecord> {
        let mut projection_remaining = self.work.projection_bytes;
        let projection = super::json_projection::RetainedJsonProjection::try_for_images(
            reader,
            &mut projection_remaining,
            self.work.metadata_bytes,
        )
        .map_err(|_| invalid_record())?;
        let content: SystemStreamContent = serde_json::from_reader(projection)?;
        if projection_remaining != 0 {
            return Err(invalid_record());
        }
        for image in content.4.iter().chain(content.5.iter()) {
            image.logical_size()?;
        }
        if content.0 != self.history || content.1 != self.version {
            return Err(invalid_record());
        }
        let effect = SystemStreamEffectDigest::try_new(hex::encode(digest))?;
        let logical = SystemStreamStoredRecord {
            position: self.position,
            source_table_name: content.2,
            keys: content.3,
            old_image: content.4,
            new_image: content.5,
        };
        Ok(VerifiedSystemStreamRecord::from_verified_content(
            logical, content.0, content.1, effect,
        ))
    }
}

fn invalid_record() -> StorageError {
    StorageError::internal("Invalid retained SystemStream record")
}

fn encode_accepted_content(
    record: &SystemStreamStoredRecord,
    receipt: &crate::DynamoStreamEffectRow,
    images: [Option<&[u8]>; 2],
) -> StorageResult<Vec<u8>> {
    use sha2::{Digest as _, Sha256};
    if record.position != receipt.position
        || images[0].is_some() != record.old_image.is_some()
        || images[1].is_some() != record.new_image.is_some()
    {
        return Err(invalid_record());
    }
    let old: Option<&serde_json::value::RawValue> =
        images[0].map(serde_json::from_slice).transpose()?;
    let new: Option<&serde_json::value::RawValue> =
        images[1].map(serde_json::from_slice).transpose()?;
    let bytes = super::json_output::RetainedJsonOutput::encode(&(
        receipt.history_id,
        receipt.item_stream_version,
        &record.source_table_name,
        &record.keys,
        old,
        new,
    ))?;
    let digest = SystemStreamEffectDigest::try_new(hex::encode(Sha256::digest(&bytes)))?;
    if digest != receipt.effect_id {
        return Err(invalid_record());
    }
    Ok(bytes)
}
