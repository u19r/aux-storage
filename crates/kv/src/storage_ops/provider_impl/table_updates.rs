use storage_types::{UpdateTableRequest, UpdateTableResponse};

use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn update_table_impl(
        &self,
        request: UpdateTableRequest,
    ) -> StorageResult<UpdateTableResponse> {
        let table_name = request.table_name.clone();
        let mut table_info = self.get_table_info(&table_name).await?;
        validate_max_indexers_update(table_info.max_indexers, request.max_indexers)?;

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
        let stream_specification = request.stream_specification.clone();
        let deletion_protection_enabled = request.deletion_protection_enabled;
        let max_indexers = request.max_indexers;
        if stream_specification.is_none()
            && deletion_protection_enabled.is_none()
            && max_indexers.is_none()
        {
            return Ok(());
        }

        let (updated_metadata, ()) = self
            .mutate_table_info(table_name, |current, _identity| {
                if let Some(spec) = &stream_specification {
                    current.stream_specification = Some(spec.clone());
                }
                if let Some(enabled) = deletion_protection_enabled {
                    current.deletion_protection_enabled = enabled;
                }
                if let Some(capacity) = max_indexers {
                    current.max_indexers = capacity;
                }
                Ok(())
            })
            .await?;
        *table_info = updated_metadata.table_info;
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

        let stream_duration = request.aux_stream_duration_hours;
        let default_item_stream_duration = request.aux_default_item_stream_duration_hours;
        let (updated_metadata, _) = self
            .mutate_table_info_with_operations(
                table_name,
                |current, _identity| {
                    if let Some(duration) = stream_duration {
                        current.table_stream_duration = duration;
                    }
                    if let Some(duration) = default_item_stream_duration {
                        current.default_item_stream_duration = duration;
                    }
                    Ok(plan_table_stream_duration(
                        table_name.clone(),
                        kv_table_scope_id(table_name),
                        crate::storage_ops::stream_duration::table_stream_policy_version(
                            current.table_stream_duration,
                            current.default_item_stream_duration,
                        ),
                        current.table_stream_duration,
                        current.default_item_stream_duration,
                        TimestampMillis::now(),
                    ))
                },
                |metadata, plan| {
                    stream_trim_state_write_ops_for_identity(
                        &metadata.identity,
                        storage_provider::StreamTrimStateWrite {
                            state: plan.trim_state.clone(),
                            next_marker: plan.due_marker.clone(),
                        },
                    )
                },
            )
            .await?;
        *table_info = updated_metadata.table_info;
        Ok(())
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
        let (updated_metadata, ()) = self
            .mutate_table_info(table_name, |current, _identity| {
                if current
                    .global_secondary_indexes
                    .as_ref()
                    .is_some_and(|indexes| {
                        indexes
                            .iter()
                            .any(|index| index.index_name == create.index_name)
                    })
                {
                    return Err(StorageError::validation(format!(
                        "Global secondary index already exists: {}",
                        create.index_name
                    )));
                }

                let mut indexes = current.global_secondary_indexes.clone().unwrap_or_default();
                indexes.push(storage_types::GlobalSecondaryIndex {
                    index_name: create.index_name.clone(),
                    key_schema: create.key_schema.clone(),
                    projection: create.projection.clone(),
                });
                current.global_secondary_indexes = Some(indexes);
                Ok(())
            })
            .await?;
        *table_info = updated_metadata.table_info;

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
        let (updated_metadata, (delete_ranges, backfill_key)) = self
            .mutate_table_info(table_name, |current, identity| {
                let delete_ranges = table_keys::gsi_prefix(identity, &delete.index_name)
                    .into_iter()
                    .chain(table_keys::gsi_tombstone_prefix(
                        identity,
                        &delete.index_name,
                    ))
                    .map(delete_range)
                    .collect::<Vec<_>>();
                let backfill_key = table_keys::gsi_backfill_key(identity, &delete.index_name);
                remove_gsi_from_table_info(current, &delete.index_name);
                Ok((delete_ranges, backfill_key))
            })
            .await?;
        *table_info = updated_metadata.table_info;

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

fn validate_max_indexers_update(
    current: storage_types::MaxIndexers,
    requested: Option<storage_types::MaxIndexers>,
) -> StorageResult<()> {
    if requested.is_some_and(|requested| requested < current) {
        return Err(StorageError::validation("MaxIndexers:cannot_decrease"));
    }
    Ok(())
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
            max_indexers: table_info.max_indexers,
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
