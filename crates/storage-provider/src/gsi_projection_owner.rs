//! Projection custody addresses preserve incarnation scope and digest the
//! complete ordered key.

use infrastructure_provider::record::{DurableRecordKey, RecordAddress};
use sha2::{Digest as _, Sha256};
use storage_backfill::GsiProjectionKey;
use storage_types::{
    BoundTableInfo, StorageError, StorageResult, TableHistoryId, maintenance::TargetIncarnation,
};
use stream_provider::retained_image::{
    RetainedOwnerKind, RetainedOwnerTransition, RetainedProjectionRecord,
};

pub struct GsiProjectionOwner {
    locator: DurableRecordKey,
    history: TableHistoryId,
}

impl GsiProjectionOwner {
    pub fn try_new(
        table: &BoundTableInfo,
        key: &GsiProjectionKey,
        incarnation: &TargetIncarnation,
    ) -> StorageResult<Self> {
        if key.ttl_index_key.is_some() {
            return Err(StorageError::internal(
                "TTL has no retained projection descriptor",
            ));
        }
        Self::from_item_key(
            table,
            &key.index_name,
            incarnation,
            &key.item_key(&table.info)?,
        )
    }

    fn from_item_key(
        table: &BoundTableInfo,
        index: &storage_types::IndexName,
        incarnation: &TargetIncarnation,
        key: &storage_types::ItemKey,
    ) -> StorageResult<Self> {
        let mut suffix = Vec::new();
        suffix.extend_from_slice(
            &u16::try_from(incarnation.as_ref().len())
                .map_err(|_| {
                    StorageError::internal("Projection incarnation exceeds locator format")
                })?
                .to_be_bytes(),
        );
        suffix.extend_from_slice(incarnation.as_ref());
        let name = index.as_ref().as_bytes();
        if name.is_empty() || name.len() > storage_types::MAX_INDEX_NAME_BYTES {
            return Err(StorageError::internal(
                "Invalid projection owner index name",
            ));
        }
        suffix.extend_from_slice(
            &u16::try_from(name.len())
                .map_err(|_| {
                    StorageError::internal("Projection index name exceeds locator format")
                })?
                .to_be_bytes(),
        );
        suffix.extend_from_slice(name);
        let mut digest = Sha256::new();
        digest.update(crate::constants::GSI_PROJECTION_OWNER_IDENTITY_TAG);
        digest.update(key.sorted_storage_suffix()?);
        suffix.extend_from_slice(&digest.finalize());
        let locator = RecordAddress {
            namespace: stream_provider::retained_image::GSI_IMAGE_LOCATOR_NAMESPACE.into(),
            resource: table.history_id.as_ref().to_vec(),
            generation: 0,
            suffix,
        }
        .try_into()?;
        Ok(Self {
            locator,
            history: table.history_id,
        })
    }

    pub fn from_image(
        table: &BoundTableInfo,
        index: &storage_types::IndexName,
        incarnation: &TargetIncarnation,
        image: &storage_types::WireItem,
    ) -> StorageResult<Self> {
        let definition = table
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|definition| definition.index_name == *index)
            .ok_or_else(|| StorageError::internal("Projection read index is absent"))?;
        let names =
            storage_common::gsi_projection_fields(None, &table.key_schema, &definition.key_schema);
        let values = image.select_attribute_values(&names)?;
        let mut attributes = storage_types::KeyAttributes::with_capacity(names.len());
        for (name, value) in names.into_iter().zip(values) {
            attributes.insert(
                name,
                value.ok_or_else(|| StorageError::internal("Projection read key is absent"))?,
            );
        }
        let key = storage_types::ItemKey::from_key_schema_for_index(
            table.table_name.clone(),
            &table.key_schema,
            index,
            &definition.key_schema,
            &attributes,
        )?
        .ok_or_else(|| StorageError::internal("Projection read has no index key"))?;
        Self::from_item_key(table, index, incarnation, &key)
    }

    pub fn prepare_read_receipt(
        &self,
        proof: stream_provider::retained_image::RetainedProjectionProof,
    ) -> StorageResult<stream_provider::retained_image::RetainedOwnerRecord> {
        proof.prepare_owner(self.locator.clone())
    }

    pub fn prepare_transition(
        &self,
        previous: Option<&RetainedProjectionRecord>,
        next: Option<&RetainedProjectionRecord>,
    ) -> StorageResult<RetainedOwnerTransition> {
        let previous = previous
            .map(|record| record.prepare_owner(self.locator.clone()))
            .transpose()?;
        let next = next
            .map(|record| record.prepare_owner(self.locator.clone()))
            .transpose()?;
        RetainedOwnerTransition::try_new(
            RetainedOwnerKind::Projection,
            self.locator.clone(),
            self.history,
            (previous, next),
        )
    }
}
