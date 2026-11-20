use storage_types::{BoundTableInfo, StorageResult};

use super::write::{DecodedProjection, decode_projection};
use crate::{KvAttempt, catalog::KvCatalogAttempt};

pub(crate) async fn read_projection_page<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    table: &BoundTableInfo,
    range: crate::KvRange,
) -> StorageResult<(crate::KvRangePage, Vec<DecodedProjection>)> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(catalog);
    let catalog = settlement.as_mut();
    let result = async {
        let page = catalog.attempt.scan(range, catalog.protection).await?;
        let images = page
            .entries
            .iter()
            .map(|entry| decode_projection(table, &entry.value))
            .collect::<StorageResult<Vec<_>>>()?;
        Ok((page, images))
    }
    .await;
    settlement.complete(result)
}
