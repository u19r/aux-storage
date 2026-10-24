//! Primary images share exact immutable bodies with retained operation history.
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use storage_types::{
    ImageAddress, IndexerDeclaration, MaxIndexers, StorageError, StorageResult, StoredImageSource,
    StoredPrimaryDescriptor, StoredWireItem, TableHistoryId, WireItem,
};

use super::{
    ImageHydrationBatch, RetainedImageManifest, error::invalid_image,
    json_projection::RetainedJsonProjection, read_work::RetainedReadWork,
};

/// CPU-only accepted content; native preparation belongs to the caller's
/// attempt.
pub struct AcceptedImagePlan {
    history: TableHistoryId,
    bytes: Vec<u8>,
}

pub struct RetainedItemRecord {
    history: TableHistoryId,
    manifest: Vec<u8>,
    work: RetainedReadWork,
}

#[derive(Deserialize)]
struct ItemContent(
    TableHistoryId,
    Vec<String>,
    #[serde(with = "storage_types::dynamo_json::optional")] Option<WireItem>,
);

#[derive(Deserialize)]
struct ItemSpan<'a>(
    serde::de::IgnoredAny,
    #[serde(borrow)] &'a serde_json::value::RawValue,
    #[serde(borrow)] &'a serde_json::value::RawValue,
);

impl AcceptedImagePlan {
    pub fn try_new(
        history: TableHistoryId,
        item: &WireItem,
        declaration: &IndexerDeclaration,
    ) -> StorageResult<Self> {
        if history.as_ref() == &[0; 16] {
            return Err(invalid_image());
        }
        require_indexer_values(item, declaration.names())?;
        let bytes = super::json_output::RetainedJsonOutput::encode(&(
            history,
            declaration.names(),
            storage_types::dynamo_json::ItemRef(item),
        ))?;
        Ok(Self { history, bytes })
    }

    pub async fn prepare<A: super::RetainedOwnershipAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<RetainedItemRecord> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let ItemSpan(_, declaration, image) = serde_json::from_slice(&self.bytes)?;
            let bodies = [declaration.get().as_bytes(), image.get().as_bytes()];
            let limits = RetainedImageManifest::image_work_limits(self.bytes.len())?;
            let manifest = RetainedImageManifest::split(&self.bytes, &bodies, Some(self.history))?;
            let mut prepared = manifest.prepare(attempt, &bodies, limits).await?;
            let bodies = std::mem::take(&mut prepared.bodies.work);
            let work = RetainedReadWork::prepare(&self.bytes, &prepared.manifest, bodies)?;
            work.require_two_body_manifest(&prepared.manifest.encoded)?;
            let manifest = prepared.publish(attempt).await?;
            Ok(RetainedItemRecord {
                history: self.history,
                manifest,
                work,
            })
        }
        .await;
        settlement_guard.complete(result)
    }
}

impl RetainedItemRecord {
    /// Native metadata bound, independent of exact original body spelling.
    pub fn encoded_length_bound() -> StorageResult<usize> {
        super::descriptor_codec::encoded_length_bound(
            crate::constants::RETAINED_PRIMARY_IDENTITY_BYTES,
            2,
            super::RetainedPayload::encoded_length_bound(
                crate::constants::RETAINED_TWO_BODY_METADATA_BYTES,
            )?,
        )
    }

    pub fn encode(&self) -> StorageResult<Vec<u8>> {
        super::descriptor_codec::DescriptorInput {
            kind: super::descriptor_codec::DescriptorKind::Primary,
            history: self.history,
            identity: &(),
            manifest: &self.manifest,
            work: &self.work,
        }
        .encode(Self::encoded_length_bound()?)
    }

    pub fn decode(bytes: &[u8], history: TableHistoryId, maximum: usize) -> StorageResult<Self> {
        if bytes.len() > Self::encoded_length_bound()? {
            return Err(invalid_image());
        }
        let descriptor = super::descriptor_codec::DescriptorDecoder::decode::<()>(
            bytes,
            super::descriptor_codec::DescriptorKind::Primary,
            maximum,
        )?;
        if descriptor.history != history {
            return Err(invalid_image());
        }
        descriptor
            .work
            .require_two_body_manifest(&descriptor.manifest)?;
        Ok(Self {
            history,
            manifest: descriptor.manifest,
            work: descriptor.work,
        })
    }

    pub fn read_work_bytes(&self, maximum: usize) -> StorageResult<usize> {
        self.work.admit_item(maximum)
    }

    fn content_manifest(&self, maximum: usize) -> StorageResult<RetainedImageManifest> {
        let manifest = self
            .work
            .decode_manifest(&self.manifest, self.history, maximum)?;
        use super::manifest::ImageSegment::{Image, Inline};
        let [
            Inline(prefix),
            Image { .. },
            Inline(separator),
            Image { .. },
            Inline(suffix),
        ] = manifest.segments.as_slice()
        else {
            return Err(invalid_image());
        };
        let mut expected_prefix = super::json_output::RetainedJsonOutput::encode(&self.history)?;
        expected_prefix
            .try_reserve_exact(2)
            .map_err(|_| super::error::capacity_rejected())?;
        expected_prefix.insert(0, b'[');
        expected_prefix.push(b',');
        if *prefix != expected_prefix || separator != b"," || suffix != b"]" {
            return Err(invalid_image());
        }
        Ok(manifest)
    }

    pub(super) fn image_source(&self) -> StorageResult<StoredImageSource> {
        let manifest = self.content_manifest(usize::MAX)?;
        let super::manifest::ImageSegment::Image { identity, length } = &manifest.segments[3]
        else {
            return Err(invalid_image());
        };
        let work = self
            .work
            .bodies
            .iter()
            .find(|body| body.identity == *identity)
            .ok_or_else(invalid_image)?;
        let super::manifest::ImageSegment::Image {
            identity: declaration,
            length: declaration_bytes,
        } = &manifest.segments[1]
        else {
            return Err(invalid_image());
        };
        let payload = ImageAddress::try_new(self.history, *identity)?;
        let proof = StoredPrimaryDescriptor::try_new(
            Sha256::digest(&self.manifest).into(),
            (
                ImageAddress::try_new(self.history, *declaration)?,
                *declaration_bytes,
            ),
            (payload, *length),
        )?;
        StoredImageSource::try_new(payload, *length, work.encoded_bytes, proof)
    }

    /// Complete batch admission, content and primary ownership verification
    /// precede publication.
    pub async fn hydrate<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        records: Vec<(&storage_types::BoundTableInfo, Self, MaxIndexers)>,
        maximum: usize,
    ) -> StorageResult<Vec<StoredWireItem>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let work = RetainedReadWork::admit_item_batch(
                records.iter().map(|(_, record, _)| &record.work),
                maximum,
            )?;
            let mut manifests = Vec::new();
            manifests
                .try_reserve_exact(records.len())
                .map_err(|_| super::error::capacity_rejected())?;
            let mut tables = Vec::new();
            let mut receipts = Vec::new();
            tables
                .try_reserve_exact(records.len())
                .map_err(|_| super::error::capacity_rejected())?;
            receipts
                .try_reserve_exact(records.len())
                .map_err(|_| super::error::capacity_rejected())?;
            for (table, record, _) in &records {
                manifests.push((
                    record.content_manifest(work)?,
                    record.work.copy_body_work()?,
                ));
                tables.push(*table);
            }
            let mut records = records.into_iter();
            let items = ImageHydrationBatch::try_projected(manifests)?
                .load_projected(
                    attempt,
                    RetainedImageManifest::image_work_limits(maximum)?,
                    |reader, _| {
                        let (_, record, capacity) = records.next().ok_or_else(invalid_image)?;
                        let mut budget = record.work.projection_bytes;
                        let content: ItemContent = serde_json::from_reader(
                            RetainedJsonProjection::for_item(reader, &mut budget),
                        )?;
                        if budget != 0 || content.0 != record.history {
                            return Err(invalid_image());
                        }
                        IndexerDeclaration::validate(&content.1, capacity).map_err(|_| {
                            StorageError::internal("Corrupt retained indexer declaration")
                        })?;
                        let item = content.2.ok_or_else(invalid_image)?;
                        item.logical_size()?;
                        require_indexer_values(&item, &content.1)?;
                        Ok(StoredWireItem {
                            item,
                            indexers: content.1,
                            source: Some(record.image_source()?),
                        })
                    },
                )
                .await?;
            if records.next().is_some() || items.len() != tables.len() {
                return Err(invalid_image());
            }
            receipts.extend(tables.into_iter().zip(&items));
            super::PrimaryImageOwner::require_items(attempt, &receipts).await?;
            Ok(items)
        }
        .await;
        settlement.complete(result)
    }
}

fn require_indexer_values(item: &WireItem, names: &[String]) -> StorageResult<()> {
    let mut fields = Vec::new();
    fields
        .try_reserve_exact(names.len())
        .map_err(|_| super::error::capacity_rejected())?;
    fields.extend(names.iter().map(String::as_str));
    for value in item.select_attribute_values(&fields)?.into_iter().flatten() {
        if !matches!(value, storage_types::AttributeValue::S(value) if !value.is_empty()) {
            return Err(invalid_image());
        }
    }
    Ok(())
}
