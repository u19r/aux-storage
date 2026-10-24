//! Native image rows never expose their descriptors as public stream payloads.
use storage_types::{
    StorageError, StorageResult, StreamItemId, StreamName, TableHistoryId, TimestampMillis,
};

use super::{
    image_read_work::ImageReadWork, page_budget::RetainedImagePageBudget,
    pointer::RetainedPointerRecord, stream_image::RetainedStreamImageRecord,
};
use crate::{StreamItem, decode_stream_item_row};

pub struct StoredStreamRow {
    pub(super) item: StreamItem,
    pub(super) content: StreamRowContent,
}

pub(super) enum StreamRowContent {
    Opaque,
    Image(RetainedStreamImageRecord),
    Pointer(RetainedPointerRecord),
}

impl StoredStreamRow {
    pub fn decode(id: StreamItemId, bytes: &[u8], maximum: usize) -> StorageResult<Self> {
        if bytes.starts_with(crate::constants::STREAM_POINTER_ROW_PREFIX) {
            if bytes.len() > maximum {
                return Err(invalid_row());
            }
            let item = crate::stream_row::decode_stream_pointer_row(id, bytes)?;
            let pointer = RetainedPointerRecord::decode(&item, maximum)?;
            return Ok(Self {
                item,
                content: StreamRowContent::Pointer(pointer),
            });
        }
        if !bytes.starts_with(crate::constants::STREAM_IMAGE_ROW_PREFIX) {
            return Ok(Self {
                item: decode_stream_item_row(id, bytes)?,
                content: StreamRowContent::Opaque,
            });
        }
        if bytes.len() > maximum {
            return Err(invalid_row());
        }
        let item = crate::stream_row::decode_stream_image_row(id, bytes)?;
        let image = RetainedStreamImageRecord::decode(&item, maximum)?;
        Ok(Self {
            item,
            content: StreamRowContent::Image(image),
        })
    }

    pub(crate) fn planned_owner(
        row: &crate::StreamWriteKind<StreamItem>,
    ) -> StorageResult<super::RetainedOwnerRecord> {
        let maximum = usize::MAX;
        match row {
            crate::StreamWriteKind::Item(item) => {
                RetainedStreamImageRecord::decode(item, maximum)?.prepare_owner()
            }
            crate::StreamWriteKind::Table(item) => {
                RetainedPointerRecord::decode(item, maximum)?.prepare_row_owner(item)
            }
            crate::StreamWriteKind::System(item) => {
                RetainedPointerRecord::decode(item, maximum)?.prepare_row_owner(item)
            }
            _ => Err(invalid_row()),
        }
    }

    pub fn retirement_receipts(rows: &[Self]) -> StorageResult<Vec<super::RetainedOwnerRecord>> {
        Ok(rows
            .iter()
            .map(|row| match &row.content {
                StreamRowContent::Opaque => Ok(None),
                StreamRowContent::Image(image) => image.prepare_owner().map(Some),
                StreamRowContent::Pointer(pointer) => {
                    pointer.prepare_row_owner(&row.item).map(Some)
                }
            })
            .collect::<StorageResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect())
    }

    pub fn metadata(&self) -> (StreamItemId, TimestampMillis) {
        (self.item.id, self.item.created_at)
    }

    pub fn require_address(&self, stream: &StreamName, id: StreamItemId) -> StorageResult<()> {
        if self.item.id != id || self.item.stream_name.as_ref() != Some(stream) {
            return Err(invalid_row());
        }
        Ok(())
    }

    pub fn require_pointer_effect(
        &self,
        effect: &storage_types::SystemStreamEffectDigest,
    ) -> StorageResult<()> {
        let StreamRowContent::Pointer(pointer) = &self.content else {
            return Err(invalid_row());
        };
        pointer.require_effect(effect)
    }

    pub fn read_work_bytes(&self, maximum: usize) -> StorageResult<usize> {
        Ok(self.qualify_read_work(maximum)?.page_bytes())
    }

    fn qualify_read_work(&self, maximum: usize) -> StorageResult<ImageReadWork> {
        match &self.content {
            StreamRowContent::Image(image) => image.qualify_read_work(maximum),
            // Opaque payloads were admitted by the native read. This allowance
            // bounds additional immutable-body work, not a second opaque batch
            // cap.
            StreamRowContent::Pointer(pointer) => pointer.qualify_read_work(maximum),
            StreamRowContent::Opaque => Ok(ImageReadWork::default()),
        }
    }

    pub(super) fn admit_read_work<'row>(
        rows: impl Iterator<Item = &'row Self>,
        maximum: usize,
    ) -> StorageResult<()> {
        ImageReadWork::admit_batch(rows.map(|row| row.qualify_read_work(maximum)), maximum)?;
        Ok(())
    }

    pub fn select_prefix(rows: Vec<Self>, maximum: usize) -> StorageResult<(Vec<Self>, bool)> {
        let mut budget = RetainedImagePageBudget::try_new(maximum)?;
        let mut selected = Vec::new();
        for row in rows {
            if !budget.admit(row.read_work_bytes(maximum)?)? {
                return Ok((selected, true));
            }
            selected.push(row);
        }
        Ok((selected, false))
    }

    pub async fn hydrate<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        rows: Vec<Self>,
        maximum: usize,
    ) -> StorageResult<Vec<StreamItem>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            Self::admit_read_work(rows.iter(), maximum)?;
            super::stream_hydration::PreparedStreamHydration::try_new(
                rows.into_iter().map(|row| (row.item, row.content)),
            )?
            .load(attempt, maximum)
            .await?
            .into_iter()
            .map(StreamItem::try_from)
            .collect()
        }
        .await;
        settlement.complete(result)
    }

    pub async fn hydrate_groups<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        groups: Vec<Vec<Self>>,
        maximum: usize,
    ) -> StorageResult<Vec<Vec<StreamItem>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let bound = groups.len().checked_mul(maximum).ok_or_else(invalid_row)?;
            let mut counts = Vec::with_capacity(groups.len());
            let mut rows = Vec::new();
            for group in groups {
                Self::admit_read_work(group.iter(), maximum)?;
                counts.push(group.len());
                rows.extend(group);
            }
            let mut hydrated = Self::hydrate(attempt, rows, bound).await?.into_iter();
            let mut result = Vec::with_capacity(counts.len());
            for count in counts {
                let group = hydrated.by_ref().take(count).collect::<Vec<_>>();
                if group.len() != count {
                    return Err(invalid_row());
                }
                result.push(group);
            }
            if hydrated.next().is_some() {
                return Err(invalid_row());
            }
            Ok(result)
        }
        .await;
        settlement.complete(result)
    }

    pub async fn hydrate_optional<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        rows: Vec<Option<Self>>,
        maximum: usize,
    ) -> StorageResult<Vec<Option<StreamItem>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let present = rows.iter().map(Option::is_some).collect::<Vec<_>>();
            let mut items = Self::hydrate(attempt, rows.into_iter().flatten().collect(), maximum)
                .await?
                .into_iter();
            let result = present
                .into_iter()
                .map(|present| {
                    if present {
                        Ok(Some(items.next().ok_or_else(invalid_row)?))
                    } else {
                        Ok(None)
                    }
                })
                .collect::<StorageResult<Vec<_>>>()?;
            if items.next().is_some() {
                return Err(invalid_row());
            }
            Ok(result)
        }
        .await;
        settlement.complete(result)
    }
}

pub(super) fn decode_history_scope(stream: &StreamName) -> StorageResult<TableHistoryId> {
    let scope = stream
        .as_ref()
        .strip_prefix(b"history/")
        .ok_or_else(invalid_row)?;
    let (identity, name) = scope.split_at_checked(32).ok_or_else(invalid_row)?;
    if name.first() != Some(&b'/') || name.len() == 1 {
        return Err(invalid_row());
    }
    if StreamName::new(&name[1..]).item_table_name().is_none() {
        return Err(invalid_row());
    }
    let identity = std::str::from_utf8(identity).map_err(|_| invalid_row())?;
    let history = uuid::Uuid::parse_str(identity).map_err(|_| invalid_row())?;
    if history.is_nil() {
        return Err(invalid_row());
    }
    Ok(TableHistoryId::from(history))
}

fn invalid_row() -> StorageError {
    StorageError::internal("Invalid retained stream image row")
}
