use storage_backfill::{
    LogicalBackfillChunk, LogicalBackfillDomain, LogicalBackfillRecord, LogicalBackfillResult,
    LogicalBackfillTombstone, LogicalExportPage, LogicalExportRequest, LogicalImportApplyCase,
    LogicalImportApplyDecision, LogicalImportRecordKind, plan_logical_import_apply,
    validate_logical_chunk_for_manifest,
};
use storage_provider::{StorageProvider, split_item_into_key_and_attributes_sync};
use storage_types::{
    AttributeValue, DurableAbsenceProof, DurableItemRevision, DurablePointReadProof,
    DurablePointReadRequest, ItemStreamVersion, ItemVersionedWireItem, KeyAttributes,
    ScanTableRequest, SerializesToKey, StorageError, StorageResult, StoredTableInfo, TableName,
};

use super::{
    logical_backfill_domains::RawKvRecordPayload,
    logical_backfill_records::{
        RevisionRecordPayload, TtlRecordPayload, durable_revision_record, empty_page,
        table_metadata_record, ttl_record, unchecked_checksum,
    },
    provider_impl::kv_mutation_to_direct_with_literal_templates,
};
use crate::{
    SortedKvDbStorageProvider,
    backends::common::plan_table_write,
    helpers::increment_bytes,
    keys::{item_revision_key, item_revision_prefix},
    sorted_kv_store::{DirectWriteOperation, TransactWriteTableOperation},
};

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(crate) async fn export_logical_page_impl(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        match request.domain {
            LogicalBackfillDomain::TableMetadata => self.export_table_metadata(request).await,
            LogicalBackfillDomain::ItemRecords => self.export_item_records(request).await,
            LogicalBackfillDomain::DurableRevisions => self.export_durable_revisions(request).await,
            LogicalBackfillDomain::TtlRecords => self.export_ttl_records(request).await,
            LogicalBackfillDomain::StreamRecords => self.export_stream_records(request).await,
            LogicalBackfillDomain::GsiRecords => self.export_gsi_records(request).await,
            LogicalBackfillDomain::Tombstones
            | LogicalBackfillDomain::BackgroundJobs
            | LogicalBackfillDomain::StorageControlPlane
            | LogicalBackfillDomain::SyncControlPlane => Ok(empty_page(request)?),
        }
    }

    pub(crate) async fn import_logical_chunk_impl(
        &self,
        manifest: &storage_backfill::LogicalBackfillManifest,
        chunk: LogicalBackfillChunk,
    ) -> StorageResult<LogicalBackfillResult> {
        validate_logical_chunk_for_manifest(manifest, &chunk).map_err(|error| {
            StorageError::validation(format!("logical chunk rejected: {error}"))
        })?;
        for record in chunk.records {
            match record {
                LogicalBackfillRecord::PresentItem {
                    table_name,
                    item_json,
                    item_stream_version,
                    ..
                } => {
                    self.import_present_item(&table_name, &item_json, item_stream_version)
                        .await?;
                }
                LogicalBackfillRecord::Tombstone(tombstone) => {
                    self.import_tombstone(tombstone).await?;
                }
                LogicalBackfillRecord::DomainRecord {
                    domain,
                    payload_json,
                    ..
                } => match domain {
                    LogicalBackfillDomain::TableMetadata => {
                        self.import_table_metadata(&payload_json).await?;
                    }
                    LogicalBackfillDomain::DurableRevisions => {
                        self.import_durable_revision(&payload_json).await?;
                    }
                    LogicalBackfillDomain::TtlRecords => {
                        self.import_ttl_record(&payload_json).await?;
                    }
                    LogicalBackfillDomain::StreamRecords | LogicalBackfillDomain::GsiRecords => {
                        self.import_raw_kv_record(&payload_json).await?;
                    }
                    LogicalBackfillDomain::ItemRecords
                    | LogicalBackfillDomain::Tombstones
                    | LogicalBackfillDomain::BackgroundJobs
                    | LogicalBackfillDomain::StorageControlPlane
                    | LogicalBackfillDomain::SyncControlPlane => {
                        return Err(StorageError::validation(format!(
                            "kv logical import received unexpected domain record for {domain:?}"
                        )));
                    }
                },
                LogicalBackfillRecord::StreamRecord { .. } => {
                    return Err(StorageError::validation(
                        "kv logical import currently imports stream rows through domain records",
                    ));
                }
            }
        }
        Ok(LogicalBackfillResult::ChunkImported)
    }

    pub(crate) async fn scan_table_with_item_stream_versions_impl(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<ItemVersionedWireItem>, Option<String>)> {
        let (items, next_cursor) = <Self as StorageProvider>::scan_table(self, request).await?;
        let table_info = self.get_table_info(&request.table_name).await?;
        let mut versioned = Vec::with_capacity(items.len());
        for item in items {
            let item_map = item.to_attribute_map()?;
            let split = split_item_into_key_and_attributes_sync(item_map, &table_info)?;
            let item_stream_version = self
                .current_item_stream_version(&request.table_name, &split.key_attributes)
                .await?
                .unwrap_or_else(|| ItemStreamVersion::new(0));
            versioned.push(ItemVersionedWireItem {
                item,
                item_stream_version,
            });
        }
        Ok((versioned, next_cursor))
    }

    pub(crate) async fn get_item_with_durable_proof_impl(
        &self,
        request: DurablePointReadRequest,
    ) -> StorageResult<DurablePointReadProof> {
        let item = self
            .get_item(
                request.table_name.clone(),
                request.key.clone(),
                request.consistent_read,
            )
            .await?;
        let revision = self
            .current_item_stream_version(&request.table_name, &request.key)
            .await?
            .unwrap_or_else(|| ItemStreamVersion::new(0))
            .to_be_bytes()
            .to_vec();
        Ok(match item {
            Some(item) => DurablePointReadProof::Present {
                item: Box::new(item),
                revision: DurableItemRevision::new(revision),
            },
            None => DurablePointReadProof::Absent {
                proof: DurableAbsenceProof::new(revision),
            },
        })
    }

    async fn export_ttl_records(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let mut records = Vec::new();
        for (table_name, config) in self.list_ttl_configs().await? {
            if request
                .table_name
                .as_ref()
                .is_some_and(|filter| filter != table_name.as_ref())
            {
                continue;
            }
            records.push(ttl_record(table_name, config)?);
            if records.len() >= request.limit as usize {
                break;
            }
        }
        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::TtlRecords,
            records,
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    async fn export_table_metadata(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let records = if let Some(table_name) = request.table_name {
            vec![table_metadata_record(
                self.get_table_info(&TableName::new(&table_name)).await?,
            )?]
        } else {
            self.list_tables(request.limit, None)
                .await?
                .into_iter()
                .map(table_metadata_record)
                .collect::<StorageResult<Vec<_>>>()?
        };
        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::TableMetadata,
            records,
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    async fn export_item_records(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let table_name = request
            .table_name
            .as_deref()
            .map(TableName::new)
            .ok_or_else(|| StorageError::validation("item export requires table_name"))?;
        let (items, next_cursor) = self
            .scan_table_with_item_stream_versions_impl(&ScanTableRequest {
                table_name: table_name.clone(),
                index_name: None,
                limit: Some(request.limit),
                exclusive_start_key: request.cursor,
                consistent_read: true,
            })
            .await?;
        let table_info = self.get_table_info(&table_name).await?;
        let mut records = Vec::with_capacity(items.len());
        for versioned in items {
            let item = versioned.item.to_attribute_map()?;
            let key_attributes = self.get_key_attributes(&item, &table_info.key_schema)?;
            records.push(LogicalBackfillRecord::PresentItem {
                table_name: table_name.as_ref().to_string(),
                key_json: key_attributes.canonical_dynamo_json().map_err(|error| {
                    StorageError::validation(format!("logical export key encoding failed: {error}"))
                })?,
                item_json: serde_json::to_string(&item)?,
                item_stream_version: versioned.item_stream_version,
            });
        }
        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::ItemRecords,
            records,
            next_cursor,
            checksum: unchecked_checksum()?,
        })
    }

    async fn export_durable_revisions(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let prefix = item_revision_prefix();
        let range = self
            .kv_store
            .get_range(
                &prefix,
                &increment_bytes(prefix.clone()),
                Some(request.limit),
                None::<crate::newtypes::TablePageKey>,
                true,
            )
            .await?;
        let mut records = Vec::new();
        for (_, value) in range.items {
            let payload: RevisionRecordPayload = storage_types::storage_serde::from_bytes(&value)?;
            if request
                .table_name
                .as_ref()
                .is_some_and(|table_name| table_name != &payload.table_name)
            {
                continue;
            }
            records.push(durable_revision_record(payload)?);
        }
        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::DurableRevisions,
            records,
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    async fn import_present_item(
        &self,
        table_name: &str,
        item_json: &str,
        item_stream_version: ItemStreamVersion,
    ) -> StorageResult<()> {
        let table_name = TableName::new(&table_name);
        let table_info = self.get_table_info(&table_name).await?;
        let item =
            serde_json::from_str::<std::collections::HashMap<String, AttributeValue>>(item_json)?;
        let split = split_item_into_key_and_attributes_sync(item.clone(), &table_info)?;
        let current_version = self
            .current_item_stream_version(&table_name, &split.key_attributes)
            .await?;
        let decision = plan_logical_import_apply(LogicalImportApplyCase::new(
            current_version,
            item_stream_version,
            LogicalImportRecordKind::PresentItem,
        ));
        if !matches!(decision, LogicalImportApplyDecision::ApplyPresentItem) {
            return Ok(());
        }
        let item_key = storage_types::ItemKey::from_key_schema(
            table_name.clone(),
            &table_info.key_schema,
            &split.key_attributes,
        )?
        .serialize_to_bytes()?;
        let old_item = self.kv_store.get(&item_key, true).await?;
        let ttl_config = self.load_ttl_config(&table_name).await?;
        let plan = plan_table_write(
            &[TransactWriteTableOperation::Put {
                table_info,
                item,
                item_stream_ttl_hours: None,
                condition: None,
                return_values_on_condition_check_failure: None,
                replication: None,
                ttl_config,
            }],
            vec![old_item],
            &[Some(storage_types::StreamItemId::from(item_stream_version))],
            self.immediate_gsi_consistency,
        )?;
        let mut operations = plan
            .mutations
            .into_iter()
            .map(kv_mutation_to_direct_with_literal_templates)
            .collect::<Vec<_>>();
        operations.push(revision_put_operation(
            &table_name,
            &split.key_attributes,
            item_stream_version,
        )?);
        self.kv_store.transact_write_unchecked(operations).await
    }

    async fn import_tombstone(&self, tombstone: LogicalBackfillTombstone) -> StorageResult<()> {
        let table_name = TableName::new(&tombstone.table_name);
        let table_info = self.get_table_info(&table_name).await?;
        let key = serde_json::from_str::<KeyAttributes>(&tombstone.key_json)?;
        let current_version = self.current_item_stream_version(&table_name, &key).await?;
        let decision = plan_logical_import_apply(LogicalImportApplyCase::new(
            current_version,
            tombstone.item_stream_version,
            LogicalImportRecordKind::Tombstone,
        ));
        if !matches!(decision, LogicalImportApplyDecision::ApplyTombstone) {
            return Ok(());
        }
        let item_key = storage_types::ItemKey::from_key_schema(
            table_name.clone(),
            &table_info.key_schema,
            &key,
        )?
        .serialize_to_bytes()?;
        let old_item = self.kv_store.get(&item_key, true).await?;
        let ttl_config = self.load_ttl_config(&table_name).await?;
        let plan = plan_table_write(
            &[TransactWriteTableOperation::Delete {
                table_info,
                key: key.clone(),
                item_stream_ttl_hours: None,
                use_key_attributes_for_missing_item_condition: false,
                condition: None,
                return_values_on_condition_check_failure: None,
                replication: None,
                ttl_config,
            }],
            vec![old_item],
            &[Some(storage_types::StreamItemId::from(
                tombstone.item_stream_version,
            ))],
            self.immediate_gsi_consistency,
        )?;
        let mut operations = plan
            .mutations
            .into_iter()
            .map(kv_mutation_to_direct_with_literal_templates)
            .collect::<Vec<_>>();
        operations.push(revision_put_operation(
            &table_name,
            &key,
            tombstone.item_stream_version,
        )?);
        self.kv_store.transact_write_unchecked(operations).await
    }

    async fn import_table_metadata(&self, payload_json: &str) -> StorageResult<()> {
        let table_info = serde_json::from_str::<StoredTableInfo>(payload_json)?;
        if self.table_exists(&table_info.table_name).await? {
            return Ok(());
        }
        let key = crate::keys::table_metadata_key(&table_info.table_name);
        let value = storage_types::storage_serde::to_bytes(&table_info)?;
        self.kv_store.put(&key, &value, None).await
    }

    async fn import_durable_revision(&self, payload_json: &str) -> StorageResult<()> {
        let payload = serde_json::from_str::<RevisionRecordPayload>(payload_json)?;
        self.put_revision_payload(payload).await
    }

    async fn import_ttl_record(&self, payload_json: &str) -> StorageResult<()> {
        let payload = serde_json::from_str::<TtlRecordPayload>(payload_json)?;
        self.save_ttl_config(&TableName::new(&payload.table_name), &payload.config)
            .await
    }

    async fn import_raw_kv_record(&self, payload_json: &str) -> StorageResult<()> {
        let payload = serde_json::from_str::<RawKvRecordPayload>(payload_json)?;
        self.kv_store.put(&payload.key, &payload.value, None).await
    }

    async fn current_item_stream_version(
        &self,
        table_name: &TableName,
        key: &KeyAttributes,
    ) -> StorageResult<Option<ItemStreamVersion>> {
        let key_json = key.canonical_dynamo_json().map_err(|error| {
            StorageError::validation(format!("logical revision key encoding failed: {error}"))
        })?;
        let Some(bytes) = self
            .kv_store
            .get(&item_revision_key(table_name, &key_json), true)
            .await?
        else {
            return Ok(None);
        };
        let payload: RevisionRecordPayload = storage_types::storage_serde::from_bytes(&bytes)?;
        Ok(Some(ItemStreamVersion::new(payload.revision)))
    }

    async fn put_revision_payload(&self, payload: RevisionRecordPayload) -> StorageResult<()> {
        self.kv_store
            .put(
                &item_revision_key(&TableName::new(&payload.table_name), &payload.key_json),
                &storage_types::storage_serde::to_bytes(&payload)?,
                None,
            )
            .await
    }

    pub(super) async fn filtered_table_infos(
        &self,
        table_filter: Option<&str>,
    ) -> StorageResult<Vec<StoredTableInfo>> {
        if let Some(table_name) = table_filter {
            return Ok(vec![
                self.get_table_info(&TableName::new(&table_name)).await?,
            ]);
        }
        self.list_tables(u32::MAX, None).await
    }
}

pub(crate) fn revision_put_operation(
    table_name: &TableName,
    key: &KeyAttributes,
    version: ItemStreamVersion,
) -> StorageResult<DirectWriteOperation> {
    let key_json = key.canonical_dynamo_json().map_err(|error| {
        StorageError::validation(format!("logical revision key encoding failed: {error}"))
    })?;
    let payload = RevisionRecordPayload {
        table_name: table_name.as_ref().to_string(),
        key_json: key_json.clone(),
        revision: version.get(),
    };
    Ok(DirectWriteOperation::Put {
        key: item_revision_key(table_name, &key_json),
        value: storage_types::storage_serde::to_bytes(&payload)?,
    })
}
