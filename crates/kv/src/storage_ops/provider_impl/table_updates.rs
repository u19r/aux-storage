use std::sync::Arc;

use storage_types::{UpdateTableRequest, UpdateTableResponse};

use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn update_table_impl(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        let table_name = request.table_name.clone();
        let mut table_info = self.get_table_info(&table_name).await?;

        self.update_table_status_impl(&table_name, TableStatus::Updating)
            .await?;
        self.apply_direct_table_updates(&table_name, &mut table_info, &request)
            .await?;
        self.apply_stream_duration_update(&table_name, &mut table_info, &request)
            .await?;
        self.apply_gsi_updates(&table_name, &mut table_info, request)
            .await?;
        self.update_table_status_impl(&table_name, TableStatus::Active)
            .await?;

        Ok(update_table_response(table_info))
    }

    async fn apply_direct_table_updates(
        &self,
        table_name: &TableName,
        table_info: &mut StoredTableInfo,
        request: &UpdateTableRequest,
    ) -> StorageResult<()> {
        if let Some(spec) = request.stream_specification.clone() {
            table_info.stream_specification = Some(spec);
            self.save_table_info(table_name, table_info).await?;
        }

        if let Some(deletion_protection_enabled) = request.deletion_protection_enabled {
            table_info.deletion_protection_enabled = deletion_protection_enabled;
            self.save_table_info(table_name, table_info).await?;
        }

        Ok(())
    }

    async fn apply_stream_duration_update(
        &self,
        table_name: &TableName,
        table_info: &mut StoredTableInfo,
        request: &UpdateTableRequest,
    ) -> StorageResult<()> {
        if request.aux_stream_duration_hours.is_none()
            && request.aux_default_item_stream_duration_hours.is_none()
        {
            return Ok(());
        }

        if let Some(duration) = request.aux_stream_duration_hours {
            table_info.table_stream_duration = duration;
        }
        if let Some(duration) = request.aux_default_item_stream_duration_hours {
            table_info.default_item_stream_duration = duration;
        }

        let updated_metadata = self
            .updated_stream_duration_metadata(table_name, table_info)
            .await?;
        let plan = plan_table_stream_duration(
            table_name.clone(),
            kv_table_scope_id(table_name),
            crate::storage_ops::stream_duration::table_stream_policy_version(
                table_info.table_stream_duration,
                table_info.default_item_stream_duration,
            ),
            table_info.table_stream_duration,
            table_info.default_item_stream_duration,
            TimestampMillis::now(),
        );
        let key = compact::table_metadata_key(updated_metadata.identity.table_id);
        let value = storage_types::storage_serde::to_bytes(&updated_metadata)?;
        let mut operations = vec![DirectWriteOperation::Put { key, value }];
        operations.extend(stream_trim_state_write_ops_for_identity(
            &updated_metadata.identity,
            storage_provider::StreamTrimStateWrite {
                state: plan.trim_state,
                next_marker: plan.due_marker,
            },
        )?);
        self.kv_store.transact_write_unchecked(operations).await?;
        self.cache_table_identity(Arc::new(updated_metadata));
        Ok(())
    }

    async fn updated_stream_duration_metadata(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
    ) -> StorageResult<StoredTableMetadata> {
        let current_metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        let updated_identity = TableIdentity::user_indexes_for_table(
            current_metadata.identity.table_id,
            table_name,
            table_info.global_secondary_indexes.as_deref(),
        );
        Ok(StoredTableMetadata::active(
            updated_identity,
            table_info.clone(),
        ))
    }

    async fn apply_gsi_updates(
        &self,
        table_name: &TableName,
        table_info: &mut StoredTableInfo,
        request: UpdateTableRequest,
    ) -> StorageResult<()> {
        let Some(gsi_updates) = request.global_secondary_index_updates else {
            return Ok(());
        };

        for gsi_update in gsi_updates {
            if let Some(create) = gsi_update.create {
                self.create_gsi(table_name, table_info, create).await?;
            }
            if let Some(delete) = gsi_update.delete {
                self.delete_gsi(table_name, table_info, delete).await?;
            }
            if let Some(_update) = gsi_update.update {
                // Throughput-only in our model; no-op.
            }
        }

        Ok(())
    }

    async fn create_gsi(
        &self,
        table_name: &TableName,
        table_info: &mut StoredTableInfo,
        create: storage_types::CreateGlobalSecondaryIndex,
    ) -> StorageResult<()> {
        if table_info
            .global_secondary_indexes
            .as_ref()
            .is_some_and(|indexes| indexes.iter().any(|g| g.index_name == create.index_name))
        {
            return Err(StorageError::validation(format!(
                "Global secondary index already exists: {}",
                create.index_name
            )));
        }

        let mut indexes = table_info
            .global_secondary_indexes
            .clone()
            .unwrap_or_default();
        indexes.push(storage_types::GlobalSecondaryIndex {
            index_name: create.index_name.clone(),
            key_schema: create.key_schema.clone(),
            projection: create.projection.clone(),
        });
        table_info.global_secondary_indexes = Some(indexes);

        self.save_table_info(table_name, table_info).await?;

        let tail = self.capture_stream_tail().await?;
        self.initialize_backfill_record(table_name, &create.index_name, tail)
            .await
    }

    async fn delete_gsi(
        &self,
        table_name: &TableName,
        table_info: &mut StoredTableInfo,
        delete: storage_types::DeleteGlobalSecondaryIndexAction,
    ) -> StorageResult<()> {
        let existing_metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        let delete_ranges = table_keys::gsi_prefix(&existing_metadata.identity, &delete.index_name)
            .into_iter()
            .chain(table_keys::gsi_tombstone_prefix(
                &existing_metadata.identity,
                &delete.index_name,
            ))
            .map(delete_range)
            .collect::<Vec<_>>();
        let backfill_key =
            table_keys::gsi_backfill_key(&existing_metadata.identity, &delete.index_name);

        remove_gsi_from_table_info(table_info, &delete.index_name);
        self.save_table_info(table_name, table_info).await?;

        if !delete_ranges.is_empty() {
            self.kv_store
                .transact_write_unchecked(delete_ranges)
                .await?;
        }
        if let Some(backfill_key) = backfill_key {
            self.kv_store.delete(&backfill_key).await?;
        }
        Ok(())
    }
}

fn remove_gsi_from_table_info(table_info: &mut StoredTableInfo, index_name: &IndexName) {
    let Some(mut indexes) = table_info.global_secondary_indexes.clone() else {
        return;
    };
    indexes.retain(|g| g.index_name != *index_name);
    table_info.global_secondary_indexes = if indexes.is_empty() {
        None
    } else {
        Some(indexes)
    };
}

fn update_table_response(table_info: StoredTableInfo) -> UpdateTableResponse {
    UpdateTableResponse {
        table_description: storage_types::TableDescription {
            table_name: table_info.table_name.clone(),
            table_status: TableStatus::Active,
            created_at: table_info.created_at.into(),
            attribute_definitions: table_info.attribute_definitions.clone(),
            key_schema: table_info.key_schema.clone(),
            table_size_bytes: table_info.table_size_bytes,
            item_count: table_info.item_count,
            table_arn: format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{}",
                table_info.table_name
            ),
            replicas: None,
            multi_region_consistency: None,
            billing_mode_summary: Some(storage_types::BillingModeSummary {
                billing_mode: Some(storage_types::BillingMode::PayPerRequest),
                last_update_to_pay_per_request_date_time: None,
            }),
            global_secondary_indexes: table_info.global_secondary_indexes.map(|indexes| {
                indexes
                    .into_iter()
                    .map(|index| storage_types::GlobalSecondaryIndexDescription {
                        index_name: index.index_name,
                        key_schema: index.key_schema,
                        projection: index.projection,
                        index_status: None,
                        backfilling: None,
                        provisioned_throughput: None,
                        index_size_bytes: None,
                        item_count: None,
                        index_arn: None,
                    })
                    .collect()
            }),
            local_secondary_indexes: None,
            provisioned_throughput: None,
            stream_specification: table_info.stream_specification,
            latest_stream_arn: None,
            latest_stream_label: None,
            deletion_protection_enabled: table_info.deletion_protection_enabled,
        },
    }
}
