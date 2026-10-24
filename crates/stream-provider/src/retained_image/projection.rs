//! Frozen GSI selections reuse the exact accepted source image body.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use serde::Deserialize;
use storage_types::{
    FrozenImageProjection, SourceOrdinalProjection, StorageResult, TableHistoryId, WireItem,
};

use super::{
    ImageHydrationBatch, RetainedImageManifest, RetainedPayload, error::invalid_image,
    json_projection::RetainedJsonProjection, read_work::RetainedReadWork,
};

pub struct AcceptedProjectionPlan {
    pub(super) history: TableHistoryId,
    pub(super) bytes: Vec<u8>,
}

pub struct RetainedProjectionRecord {
    pub(super) history: TableHistoryId,
    pub(super) manifest: Vec<u8>,
    pub(super) work: RetainedReadWork,
}

/// Exact frozen manifest evidence retained across hydration without copying
/// bodies.
pub struct RetainedProjectionProof {
    receipt: super::RetainedOwnerRecord,
}

impl RetainedProjectionProof {
    pub fn prepare_owner(
        mut self,
        locator: infrastructure_provider::record::DurableRecordKey,
    ) -> StorageResult<super::RetainedOwnerRecord> {
        if locator.as_ref().resource != self.receipt.history.as_ref() {
            return Err(invalid_image());
        }
        self.receipt.locator = locator;
        Ok(self.receipt)
    }
}

#[derive(Deserialize)]
struct ProjectionContent(
    TableHistoryId,
    SourceOrdinalProjection,
    #[serde(with = "storage_types::dynamo_json::optional")] Option<WireItem>,
);

impl AcceptedProjectionPlan {
    pub fn try_new(
        history: TableHistoryId,
        source: &WireItem,
        selection: &FrozenImageProjection,
    ) -> StorageResult<Self> {
        if history.as_ref() == &[0; 16] {
            return Err(invalid_image());
        }
        source.logical_size()?;
        let selection = selection.bind_ordinals(source)?;
        let bytes = super::json_output::RetainedJsonOutput::encode(&(
            history,
            selection,
            storage_types::dynamo_json::ItemRef(source),
        ))?;
        Ok(Self { history, bytes })
    }
}

impl RetainedProjectionRecord {
    pub fn prepare_owner(
        &self,
        locator: infrastructure_provider::record::DurableRecordKey,
    ) -> StorageResult<super::RetainedOwnerRecord> {
        self.owner_proof()?.prepare_owner(locator)
    }

    pub fn owner_proof(&self) -> StorageResult<RetainedProjectionProof> {
        let locator = infrastructure_provider::record::RecordAddress {
            namespace: crate::constants::GSI_IMAGE_LOCATOR_NAMESPACE.into(),
            resource: self.history.as_ref().to_vec(),
            generation: 0,
            suffix: Vec::new(),
        }
        .try_into()?;
        let receipt = self
            .content_manifest(usize::MAX)?
            .prepare_owner_record(
                super::RetainedOwnerKind::Projection,
                locator,
                &self.manifest,
            )?
            .ok_or_else(invalid_image)?;
        Ok(RetainedProjectionProof { receipt })
    }

    pub fn encode(&self) -> StorageResult<Vec<u8>> {
        self.encode_binary()
    }

    pub(super) fn try_new(
        history: TableHistoryId,
        manifest: Vec<u8>,
        work: RetainedReadWork,
        maximum: usize,
    ) -> StorageResult<Self> {
        let record = Self {
            history,
            manifest,
            work,
        };
        record.require_descriptor_bounds()?;
        record.work.admit_item(maximum)?;
        Ok(record)
    }

    pub fn encoded_length_bound() -> StorageResult<usize> {
        RetainedPayload::encoded_length_bound(crate::constants::RETAINED_TWO_BODY_METADATA_BYTES)?
            .checked_add(
                crate::constants::RETAINED_PROJECTION_HEADER_BYTES
                    + 2 * crate::constants::RETAINED_DESCRIPTOR_BODY_WORK_BYTES,
            )
            .ok_or_else(invalid_image)
    }

    fn require_descriptor_bounds(&self) -> StorageResult<()> {
        if self.history.as_ref() == &[0; 16] {
            return Err(invalid_image());
        }
        self.work.require_two_body_manifest(&self.manifest)
    }

    pub fn decode(bytes: &[u8], history: TableHistoryId, maximum: usize) -> StorageResult<Self> {
        if bytes.len() > maximum.min(Self::encoded_length_bound()?) {
            return Err(invalid_image());
        }
        super::projection_codec::ProjectionDecoder::decode(bytes, history, maximum)
    }

    pub fn read_work_bytes(&self, maximum: usize) -> StorageResult<usize> {
        self.work.admit_item(maximum)
    }

    fn content_manifest(&self, maximum: usize) -> StorageResult<RetainedImageManifest> {
        let manifest = self
            .work
            .decode_manifest(&self.manifest, self.history, maximum)?;
        let [
            super::manifest::ImageSegment::Inline(prefix),
            super::manifest::ImageSegment::Image {
                length: selection, ..
            },
            super::manifest::ImageSegment::Inline(separator),
            super::manifest::ImageSegment::Image { .. },
            super::manifest::ImageSegment::Inline(suffix),
        ] = manifest.segments.as_slice()
        else {
            return Err(invalid_image());
        };
        let mut expected_prefix = super::json_output::RetainedJsonOutput::encode(&(self.history,))?;
        // A one-element tuple ends with ']', while projection content continues
        // with ','.
        *expected_prefix.last_mut().ok_or_else(invalid_image)? = b',';
        if prefix != &expected_prefix
            || separator != b","
            || suffix != b"]"
            || *selection > SourceOrdinalProjection::encoded_length_bound()
        {
            return Err(invalid_image());
        }
        Ok(manifest)
    }

    pub async fn hydrate<A: PhysicalRecordAttempt>(
        attempt: &mut A,
        records: Vec<Self>,
        maximum: usize,
    ) -> StorageResult<Vec<WireItem>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let work = RetainedReadWork::admit_item_batch(
                records.iter().map(|record| &record.work),
                maximum,
            )?;
            let mut manifests = Vec::new();
            manifests
                .try_reserve_exact(records.len())
                .map_err(|_| super::error::capacity_rejected())?;
            for record in &records {
                manifests.push((
                    record.content_manifest(work)?,
                    record.work.copy_body_work()?,
                ));
            }
            let mut records = records.into_iter();
            let items = ImageHydrationBatch::try_projected(manifests)?
                .load_projected(
                    attempt,
                    RetainedImageManifest::image_work_limits(maximum)?,
                    |reader, _| {
                        let record = records.next().ok_or_else(invalid_image)?;
                        let mut budget = record.work.projection_bytes;
                        let content: ProjectionContent = serde_json::from_reader(
                            RetainedJsonProjection::for_item(reader, &mut budget),
                        )?;
                        if budget != 0 || content.0 != record.history {
                            return Err(invalid_image());
                        }
                        let source = content.2.ok_or_else(invalid_image)?;
                        source.logical_size()?;
                        let image = content.1.project(&source)?;
                        image.logical_size()?;
                        Ok(image)
                    },
                )
                .await?;
            if records.next().is_some() {
                return Err(invalid_image());
            }
            Ok(items)
        }
        .await;
        settlement.complete(result)
    }
}
