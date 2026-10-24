use base64::{Engine as _, engine::general_purpose::STANDARD};
use storage_backfill::GsiProjectionKey;
use storage_provider::{
    DynamoRangeReadAttempt, GsiProjectionMetadataAttempt, GsiProjectionScan, GsiTombstonePage,
    GsiTombstoneRow, GsiTombstoneScan, NativeProjectionAddress, ProjectionMetadataCursor,
};
use storage_types::{
    AttributeValue, BoundTableInfo, ItemKey, KeyAttributes, StorageError, StorageResult,
};

use super::projection::GsiTombstone;
use crate::{SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt};

impl<A: SqlAttempt, L: SqlLayout> GsiProjectionMetadataAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn encode_gsi_projection_addresses(
        &mut self,
        table: &BoundTableInfo,
        keys: &[GsiProjectionKey],
    ) -> StorageResult<Vec<NativeProjectionAddress>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let mut addresses = Vec::with_capacity(keys.len());
            for key in keys {
                catalog
                    .read_gsi_readiness(table, Some(&key.index_name))
                    .await?
                    .require_queryable(&key.index_name)?;
                let incarnation = catalog
                    .gsi_target(&table.table_name, &key.index_name)?
                    .incarnation
                    .clone();
                addresses.push(NativeProjectionAddress::try_new(
                    table,
                    &key.index_name,
                    incarnation,
                    projection_address(table, key.item_key(&table.info)?)?,
                )?);
            }
            Ok(addresses)
        }
        .await;
        settlement.complete(result)
    }

    async fn scan_gsi_tombstones(
        &mut self,
        scan: &GsiTombstoneScan,
    ) -> StorageResult<GsiTombstonePage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            catalog
                .read_gsi_readiness(&scan.table, Some(&scan.index))
                .await?
                .require_queryable(&scan.index)?;
            let incarnation = catalog
                .gsi_target(&scan.table.table_name, &scan.index)?
                .incarnation
                .clone();
            let after = scan
                .after
                .as_ref()
                .map(|cursor| {
                    cursor.require_scope(&scan.table, &scan.index, &incarnation)?;
                    String::from_utf8(cursor.native_position().to_vec()).map_err(|_| {
                        StorageError::internal("invalid SQL projection metadata cursor")
                    })
                })
                .transpose()?;
            let request = GsiProjectionScan::try_new(
                scan.table.clone(),
                scan.index.clone(),
                after,
                Some(scan.limit.get()),
            )?;
            let bound = request.bind()?.mark_gsi_published()?;
            let page = request.page(&bound)?;
            let native = catalog
                .read_projected_range::<GsiTombstone>(&scan.table, &bound, &page)
                .await?;
            let rows = native
                .items
                .into_iter()
                .map(|(key, revision)| {
                    Ok(GsiTombstoneRow {
                        address: NativeProjectionAddress::try_new(
                            &scan.table,
                            &scan.index,
                            incarnation.clone(),
                            projection_address(&scan.table, key)?,
                        )?,
                        revision,
                    })
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let continuation = native
                .continuation
                .map(|token| {
                    let address = rows
                        .last()
                        .ok_or_else(|| {
                            StorageError::internal("SQL projection metadata cursor has no row")
                        })?
                        .address
                        .clone();
                    ProjectionMetadataCursor::try_new(address, token.into_bytes())
                })
                .transpose()?;
            Ok(GsiTombstonePage { rows, continuation })
        }
        .await;
        settlement.complete(result)
    }
}

fn projection_address(table: &BoundTableInfo, key: ItemKey) -> StorageResult<Vec<u8>> {
    let attributes = key.key_attributes(&table.info)?;
    let canonical = attributes
        .into_iter()
        .map(|attribute| {
            let value = match attribute.value {
                AttributeValue::B(value) => {
                    let bytes = storage_types::dynamodb_binary::decode_base64_string(&value)
                        .map_err(|_| StorageError::internal("invalid SQL projection binary key"))?;
                    AttributeValue::B(STANDARD.encode(bytes))
                }
                value => value,
            };
            Ok((attribute.name, value))
        })
        .collect::<StorageResult<KeyAttributes>>()?;
    canonical
        .canonical_dynamo_json()
        .map(String::into_bytes)
        .map_err(|_| StorageError::internal("invalid SQL projection key encoding"))
}
