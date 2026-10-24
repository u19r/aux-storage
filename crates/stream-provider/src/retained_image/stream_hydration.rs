//! One complete ordered inventory shares body proof across history and pointer
//! rows.

use storage_types::StorageResult;

use super::{
    ImageHydrationBatch, RetainedImageManifest, RetainedOwnerRecord,
    error::{capacity_rejected, invalid_image},
    pointer::RetainedPointerRecord,
    preparation::RetainedImageBodyWork,
    stream_image::RetainedStreamImageRecord,
    stream_row::StreamRowContent,
};
use crate::StreamItem;

pub(super) struct PreparedStreamImageRead<'work> {
    pub(super) manifest: RetainedImageManifest,
    pub(super) work: &'work [RetainedImageBodyWork],
    pub(super) receipt: Option<RetainedOwnerRecord>,
}

#[derive(Default)]
pub(super) struct PreparedStreamHydration {
    rows: Vec<Option<HydratedStreamContent>>,
    projected: Vec<ProjectedStreamRow>,
    images: ImageHydrationBatch,
    receipts: Vec<RetainedOwnerRecord>,
}

enum ProjectedStreamRow {
    Image(RetainedStreamImageRecord, StreamItem),
    Pointer(RetainedPointerRecord, StreamItem),
    System(super::system_stream::RetainedSystemStreamRecord),
}

pub(super) enum HydratedStreamContent {
    Row(StreamItem),
    System(Box<super::verified_system_stream::VerifiedSystemStreamRecord>),
}

impl PreparedStreamHydration {
    pub(super) fn try_new(
        rows: impl Iterator<Item = (StreamItem, StreamRowContent)>,
    ) -> StorageResult<Self> {
        let mut prepared = Self::default();
        for (row, content) in rows {
            prepared.include_row(row, content)?;
        }
        Ok(prepared)
    }

    pub(super) fn include_row(
        &mut self,
        row: StreamItem,
        content: StreamRowContent,
    ) -> StorageResult<()> {
        self.rows.try_reserve(1).map_err(|_| capacity_rejected())?;
        let read = match &content {
            StreamRowContent::Opaque => {
                self.rows.push(Some(HydratedStreamContent::Row(row)));
                return Ok(());
            }
            StreamRowContent::Image(image) => image.prepare_read()?,
            StreamRowContent::Pointer(pointer) => pointer.prepare_read(&row)?,
        };
        self.projected
            .try_reserve(1)
            .map_err(|_| capacity_rejected())?;
        self.include_read(read)?;
        let projected = match content {
            StreamRowContent::Image(image) => ProjectedStreamRow::Image(image, row),
            StreamRowContent::Pointer(pointer) => ProjectedStreamRow::Pointer(pointer, row),
            StreamRowContent::Opaque => return Err(invalid_image()),
        };
        self.projected.push(projected);
        Ok(())
    }

    pub(super) fn include_system(
        &mut self,
        record: super::system_stream::RetainedSystemStreamRecord,
    ) -> StorageResult<()> {
        self.projected
            .try_reserve(1)
            .map_err(|_| capacity_rejected())?;
        self.include_read(record.prepare_read()?)?;
        self.projected.push(ProjectedStreamRow::System(record));
        Ok(())
    }

    fn include_read(&mut self, read: PreparedStreamImageRead<'_>) -> StorageResult<()> {
        self.rows.try_reserve(1).map_err(|_| capacity_rejected())?;
        self.receipts
            .try_reserve(usize::from(read.receipt.is_some()))
            .map_err(|_| capacity_rejected())?;
        self.images.include_projected(read.manifest, read.work)?;
        self.receipts.extend(read.receipt);
        self.rows.push(None);
        Ok(())
    }

    pub(super) async fn load<A: super::RetainedOwnershipAttempt>(
        self,
        attempt: &mut A,
        maximum: usize,
    ) -> StorageResult<Vec<HydratedStreamContent>> {
        let limits = RetainedImageManifest::image_work_limits(maximum)?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(self.rows.len())
            .map_err(|_| capacity_rejected())?;
        RetainedOwnerRecord::require_current_batch(attempt, &self.receipts).await?;
        let mut projected = self.projected.into_iter();
        let mut hydrated = self
            .images
            .load_projected(attempt, limits, |reader, digest| {
                projected
                    .next()
                    .ok_or_else(invalid_image)?
                    .project_row(reader, digest)
            })
            .await?
            .into_iter();
        if projected.next().is_some() {
            return Err(invalid_image());
        }
        for row in self.rows {
            rows.push(match row {
                Some(row) => row,
                None => hydrated.next().ok_or_else(invalid_image)?,
            });
        }
        if hydrated.next().is_some() {
            return Err(invalid_image());
        }
        Ok(rows)
    }
}

impl ProjectedStreamRow {
    fn project_row(
        self,
        reader: &mut dyn std::io::Read,
        digest: [u8; 32],
    ) -> StorageResult<HydratedStreamContent> {
        match self {
            Self::Image(image, mut row) => {
                let image = image.project_image(reader)?;
                row.data =
                    storage_types::storage_serde::compress_json_bytes(&image.dynamo_json_bytes()?);
                Ok(HydratedStreamContent::Row(row))
            }
            Self::Pointer(pointer, row) => pointer
                .project_pointer(row, reader)
                .map(HydratedStreamContent::Row),
            Self::System(record) => record
                .project_record(reader, digest)
                .map(|record| HydratedStreamContent::System(Box::new(record))),
        }
    }
}

impl TryFrom<HydratedStreamContent> for StreamItem {
    type Error = storage_types::StorageError;
    fn try_from(content: HydratedStreamContent) -> StorageResult<Self> {
        match content {
            HydratedStreamContent::Row(row) => Ok(row),
            _ => Err(invalid_image()),
        }
    }
}

impl TryFrom<HydratedStreamContent> for super::verified_system_stream::VerifiedSystemStreamRecord {
    type Error = storage_types::StorageError;
    fn try_from(content: HydratedStreamContent) -> StorageResult<Self> {
        match content {
            HydratedStreamContent::System(record) => Ok(*record),
            _ => Err(invalid_image()),
        }
    }
}
