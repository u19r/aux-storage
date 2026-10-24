use storage_backfill::GsiProjectionKey;
use storage_provider::{
    DynamoRangeReadAttempt, GsiProjectionMetadataAttempt, GsiTombstonePage, GsiTombstoneRow,
    GsiTombstoneScan, NativeProjectionAddress, ProjectionMetadataCursor,
};
use storage_types::{
    BoundTableInfo, ItemRevision, StorageError, StorageResult, maintenance::TargetIncarnation,
};

use crate::{
    KvAttempt, KvRange, ReadProtection,
    catalog::{KvCatalogAttempt, KvIndexKind},
    constants::{GSI_AUTHORITY_FAMILY, GSI_TOMBSTONE_FAMILY, PAGE_READ_BYTES},
    gsi::index_incarnation,
};

impl<A: KvAttempt> GsiProjectionMetadataAttempt for KvCatalogAttempt<'_, A> {
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
                let incarnation = catalog.projection_incarnation(table, &key.index_name)?;
                addresses.push(NativeProjectionAddress::try_new(
                    table,
                    &key.index_name,
                    incarnation,
                    crate::gsi::projection_address(&crate::gsi::ordered_projection_suffix(
                        &key.item_key(&table.info)?,
                    )?)
                    .to_vec(),
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
            let incarnation = catalog.projection_incarnation(&scan.table, &scan.index)?;
            let prefix = catalog.tombstone_prefix(&scan.table, &scan.index)?;
            let mut range = KvRange::try_for_prefix(prefix.clone(), scan.limit, PAGE_READ_BYTES)?;
            if let Some(cursor) = &scan.after {
                cursor.require_scope(&scan.table, &scan.index, &incarnation)?;
                let suffix = cursor.native_position().strip_prefix(prefix.as_slice());
                if suffix != Some(cursor.native_address().native_key()) {
                    return Err(StorageError::internal(
                        "KV projection cursor has another address",
                    ));
                }
                range.begin = cursor.native_position().to_vec();
                range.begin.push(0);
            }
            let page = catalog
                .attempt
                .scan_unchunked(
                    range.clone(),
                    ReadProtection::Conflict,
                    std::num::NonZeroUsize::MIN.saturating_add(7),
                )
                .await?;
            require_metadata_page(&range, &page)?;
            catalog.require_no_live_tombstones(&page.entries).await?;
            let mut rows = Vec::with_capacity(page.entries.len());
            let mut last = None;
            for entry in page.entries {
                let key = entry
                    .key
                    .strip_prefix(prefix.as_slice())
                    .filter(|key| key.len() == 32)
                    .ok_or_else(|| {
                        StorageError::internal("KV projection tombstone has another prefix")
                    })?;
                let revision: [u8; 8] = entry.value.as_slice().try_into().map_err(|_| {
                    StorageError::internal("invalid KV projection tombstone revision")
                })?;
                rows.push(GsiTombstoneRow {
                    address: NativeProjectionAddress::try_new(
                        &scan.table,
                        &scan.index,
                        incarnation.clone(),
                        key.to_vec(),
                    )?,
                    revision: ItemRevision::try_from(u64::from_be_bytes(revision))?,
                });
                last = Some(entry.key);
            }
            let continuation = if page.more {
                let address = rows
                    .last()
                    .ok_or_else(|| {
                        StorageError::internal("KV projection metadata page cannot advance")
                    })?
                    .address
                    .clone();
                Some(ProjectionMetadataCursor::try_new(
                    address,
                    last.ok_or_else(|| {
                        StorageError::internal("KV projection metadata page lacks position")
                    })?,
                )?)
            } else {
                None
            };
            Ok(GsiTombstonePage { rows, continuation })
        }
        .await;
        settlement.complete(result)
    }
}

impl<A: KvAttempt> KvCatalogAttempt<'_, A> {
    fn projection_incarnation(
        &self,
        table: &BoundTableInfo,
        index: &storage_types::IndexName,
    ) -> StorageResult<TargetIncarnation> {
        let identity = self.identity_for(&table.table_name)?;
        let index = identity
            .indexes
            .iter()
            .find(|entry| &entry.index_name == index && entry.index_kind == KvIndexKind::UserGsi)
            .ok_or_else(|| StorageError::internal("KV projection index is absent"))?;
        Ok(index_incarnation(identity.table_id, index.index_id).into())
    }
    fn tombstone_prefix(
        &self,
        table: &BoundTableInfo,
        index: &storage_types::IndexName,
    ) -> StorageResult<Vec<u8>> {
        let incarnation = self.projection_incarnation(table, index)?;
        let mut prefix = vec![GSI_TOMBSTONE_FAMILY];
        prefix.extend_from_slice(incarnation.as_ref());
        Ok(prefix)
    }
    async fn require_no_live_tombstones(
        &mut self,
        entries: &[crate::KvEntry],
    ) -> StorageResult<()> {
        let keys = entries
            .iter()
            .map(|entry| {
                let mut key = entry.key.clone();
                *key.first_mut()
                    .ok_or_else(|| StorageError::internal("empty KV projection metadata key"))? =
                    GSI_AUTHORITY_FAMILY;
                Ok(key)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let present = self
            .attempt
            .get_batch_presence(keys, ReadProtection::Conflict)
            .await?;
        if present.len() != entries.len() || present.into_iter().any(|value| value) {
            return Err(StorageError::internal(
                "KV projection has contradictory live and tombstone metadata",
            ));
        }
        Ok(())
    }
}

fn require_metadata_page(range: &KvRange, page: &crate::KvRangePage) -> StorageResult<()> {
    if page.entries.len() > range.limit.get() as usize || (page.more && page.entries.is_empty()) {
        return Err(StorageError::internal(
            "invalid KV projection metadata page size",
        ));
    }
    let mut previous: Option<&[u8]> = None;
    for entry in &page.entries {
        let key = entry.key.as_slice();
        if key < range.begin.as_slice()
            || key >= range.end.as_slice()
            || previous.is_some_and(|previous| previous >= key)
        {
            return Err(StorageError::internal(
                "invalid KV projection metadata page order",
            ));
        }
        previous = Some(key);
    }
    Ok(())
}
