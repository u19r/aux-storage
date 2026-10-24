//! Admit and hydrate source-backed projections before SQL range publication.
use storage_types::{BoundTableInfo, StorageError, StorageResult, StoredWireItem};
use stream_provider::retained_image::RetainedProjectionRecord;

use crate::{
    ColumnIndex, SqlAttempt, SqlLayout, SqlQueryPage, SqlValue, catalog::SqlCatalogAttempt,
};

pub(crate) struct ProjectionPageScope<'a> {
    pub table: &'a BoundTableInfo,
    pub index: &'a storage_types::IndexName,
    pub incarnation: &'a storage_types::maintenance::TargetIncarnation,
    pub image_column: usize,
}

pub(crate) async fn hydrate_projection_page<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut SqlCatalogAttempt<'_, A, L>,
    page: &mut SqlQueryPage,
    scope: ProjectionPageScope<'_>,
) -> StorageResult<Vec<Option<StoredWireItem>>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(catalog);
    let catalog = settlement.as_mut();
    let result = async {
        let table = scope.table;
        let offset = scope.image_column;
        let maximum = crate::constants::RANGE_PAGE_READ_BYTES.get();
        let mut budget =
            stream_provider::retained_image::RetainedImagePageBudget::try_new(maximum)?;
        let mut records = Vec::with_capacity(page.rows.len());
        for row in &page.rows {
            let SqlValue::Blob(bytes) = row.value(ColumnIndex::new(offset))? else {
                return Err(invalid_projection());
            };
            let record =
                RetainedProjectionRecord::decode(bytes.as_slice(), table.history_id, maximum)?;
            if !budget.admit(record.read_work_bytes(maximum)?)? {
                break;
            }
            records.push(record);
        }
        if records.len() < page.rows.len() {
            page.rows.truncate(records.len());
            page.more = true;
        }
        let proofs = records
            .iter()
            .map(RetainedProjectionRecord::owner_proof)
            .collect::<StorageResult<Vec<_>>>()?;
        let images = RetainedProjectionRecord::hydrate(catalog, records, maximum).await?;
        if images.len() != page.rows.len() {
            return Err(invalid_projection());
        }
        let receipts = images
            .iter()
            .zip(proofs)
            .map(|(image, proof)| {
                storage_provider::GsiProjectionOwner::from_image(
                    table,
                    scope.index,
                    scope.incarnation,
                    image,
                )?
                .prepare_read_receipt(proof)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        stream_provider::retained_image::RetainedOwnerRecord::require_current_batch(
            catalog, &receipts,
        )
        .await?;
        Ok(images
            .into_iter()
            .map(|item| {
                Some(StoredWireItem {
                    item,
                    indexers: Vec::new(),
                    source: None,
                })
            })
            .collect())
    }
    .await;
    settlement.complete(result)
}

fn invalid_projection() -> StorageError {
    StorageError::internal("Invalid retained SQL projection page")
}
