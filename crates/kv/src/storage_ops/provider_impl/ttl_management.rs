use storage_types::{TimeToLiveSpecification, UpdateTimeToLiveRequest, UpdateTimeToLiveResponse};

use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn update_time_to_live_impl(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        let UpdateTimeToLiveRequest {
            table_name,
            time_to_live_specification,
        } = request;

        let mut table_info = self.get_table_info(&table_name).await?;
        let existing_config = self.load_ttl_config(&table_name).await?;

        if time_to_live_specification.enabled {
            return self
                .enable_time_to_live(
                    &table_name,
                    &mut table_info,
                    time_to_live_specification,
                    existing_config.as_ref(),
                )
                .await;
        }

        self.disable_time_to_live(
            &table_name,
            &mut table_info,
            time_to_live_specification,
            existing_config,
        )
        .await
    }

    pub(super) async fn describe_time_to_live_impl(
        &self,
        table_name: &TableName,
    ) -> StorageResult<DescribeTimeToLiveResponse> {
        let _ = self.get_table_info(table_name).await?;

        let description = match self.load_ttl_config(table_name).await? {
            Some(config) => TimeToLiveDescription {
                attribute_name: Some(config.attribute_name),
                time_to_live_status: config.status,
            },
            None => TimeToLiveDescription {
                attribute_name: None,
                time_to_live_status: TimeToLiveStatus::Disabled,
            },
        };

        Ok(DescribeTimeToLiveResponse {
            time_to_live_description: Some(description),
        })
    }

    async fn enable_time_to_live(
        &self,
        table_name: &TableName,
        table_info: &mut StoredTableInfo,
        time_to_live_specification: TimeToLiveSpecification,
        existing_config: Option<&TtlConfigRecord>,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        let attribute_name = time_to_live_specification.attribute_name.clone();
        validate_ttl_enable_request(
            &attribute_name,
            existing_config,
            &time_to_live_specification,
        )?;
        if ttl_already_enabled_for_attribute(existing_config, &attribute_name) {
            return Ok(UpdateTimeToLiveResponse {
                time_to_live_specification,
            });
        }

        let gsi_name = ttl::ttl_gsi_name(table_name);
        let config = TtlConfigRecord::new(
            attribute_name.clone(),
            &gsi_name,
            TimeToLiveStatus::Enabling,
        );
        if !ttl_gsi_exists(table_info, &gsi_name) {
            add_ttl_gsi(table_info, gsi_name.clone(), attribute_name);
            self.save_table_info(table_name, table_info).await?;
        }
        self.save_ttl_config(table_name, &config).await?;

        let tail = self.capture_stream_tail().await?;
        self.initialize_backfill_record(table_name, &gsi_name, tail)
            .await?;

        Ok(UpdateTimeToLiveResponse {
            time_to_live_specification,
        })
    }

    async fn disable_time_to_live(
        &self,
        table_name: &TableName,
        table_info: &mut StoredTableInfo,
        mut time_to_live_specification: TimeToLiveSpecification,
        existing_config: Option<TtlConfigRecord>,
    ) -> StorageResult<UpdateTimeToLiveResponse> {
        if let Some(config) = existing_config {
            remove_ttl_gsi(table_info, &config);
            self.save_table_info(table_name, table_info).await?;
            self.delete_ttl_storage(table_name, &config).await?;
            self.delete_ttl_config(table_name).await?;
            time_to_live_specification.attribute_name = config.attribute_name;
        }

        time_to_live_specification.enabled = false;
        Ok(UpdateTimeToLiveResponse {
            time_to_live_specification,
        })
    }

    async fn delete_ttl_storage(
        &self,
        table_name: &TableName,
        config: &TtlConfigRecord,
    ) -> StorageResult<()> {
        let Ok(Some(metadata)) = self.get_table_identity_from_name(table_name).await else {
            return Ok(());
        };

        self.kv_store
            .transact_write_unchecked(vec![delete_range(ttl::compact_ttl_index_table_range(
                &metadata.identity,
            ))])
            .await?;
        if let Some(range) = table_keys::gsi_prefix(&metadata.identity, &config.gsi_name()) {
            self.kv_store
                .transact_write_unchecked(vec![delete_range(range)])
                .await?;
        }
        if let Some(range) =
            table_keys::gsi_tombstone_prefix(&metadata.identity, &config.gsi_name())
        {
            self.kv_store
                .transact_write_unchecked(vec![delete_range(range)])
                .await?;
        }
        if let Some(backfill_key) =
            table_keys::gsi_backfill_key(&metadata.identity, &config.gsi_name())
        {
            let _ = self.kv_store.delete(&backfill_key).await;
        }
        Ok(())
    }
}

fn validate_ttl_enable_request(
    attribute_name: &str,
    existing_config: Option<&TtlConfigRecord>,
    time_to_live_specification: &TimeToLiveSpecification,
) -> StorageResult<()> {
    if attribute_name.trim().is_empty() {
        return Err(StorageError::validation(
            "Time to live attribute name must not be empty",
        ));
    }

    let Some(config) = existing_config else {
        return Ok(());
    };

    if matches!(
        config.status,
        TimeToLiveStatus::Enabling | TimeToLiveStatus::Disabling
    ) {
        return Err(StorageError::validation(
            "Time to live configuration update in progress; retry later",
        ));
    }
    if config.status == TimeToLiveStatus::Enabled
        && config.attribute_name != time_to_live_specification.attribute_name
    {
        return Err(StorageError::validation(
            "Disable time to live before changing attribute name",
        ));
    }
    Ok(())
}

fn ttl_gsi_exists(table_info: &StoredTableInfo, gsi_name: &IndexName) -> bool {
    table_info
        .global_secondary_indexes
        .as_ref()
        .is_some_and(|indexes| indexes.iter().any(|index| index.index_name == *gsi_name))
}

fn ttl_already_enabled_for_attribute(
    existing_config: Option<&TtlConfigRecord>,
    attribute_name: &str,
) -> bool {
    existing_config.is_some_and(|config| {
        config.status == TimeToLiveStatus::Enabled && config.attribute_name == attribute_name
    })
}

fn add_ttl_gsi(table_info: &mut StoredTableInfo, gsi_name: IndexName, attribute_name: String) {
    let mut indexes = table_info
        .global_secondary_indexes
        .clone()
        .unwrap_or_default();
    indexes.push(storage_types::GlobalSecondaryIndex {
        index_name: gsi_name,
        key_schema: vec![
            KeySchemaElement {
                attribute_name: TTL_PARTITION_ATTRIBUTE.to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name,
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
    });
    table_info.global_secondary_indexes = Some(indexes);
}

fn remove_ttl_gsi(table_info: &mut StoredTableInfo, config: &TtlConfigRecord) {
    if let Some(ref mut indexes) = table_info.global_secondary_indexes {
        indexes.retain(|idx| idx.index_name != config.gsi_name());
        if indexes.is_empty() {
            table_info.global_secondary_indexes = None;
        }
    }
}
