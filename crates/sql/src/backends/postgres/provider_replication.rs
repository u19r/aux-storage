use storage_provider::split_item_into_key_and_attributes_sync;
use storage_types::{ReplicationMutation, StorageResult, WireItem};
use tokio_postgres::types::ToSql;

use crate::backends::postgres::{
    PostgresStorageProvider, physical_names, sql_statements,
    stream_helpers::PostgresWriteStreamEntriesInput,
};

const REPLICATION_APPLY_PARALLELISM_HINT: usize = 4;

impl PostgresStorageProvider {
    pub(super) async fn do_apply_replication_mutation(
        &self,
        mutation: ReplicationMutation,
    ) -> StorageResult<()> {
        self.retry_postgres_conflicts("apply_replication_mutation", || {
            let mutation = mutation.clone();
            async move {
                let table_name = mutation.table_name.clone();
                let metadata = mutation.metadata.clone();

                if let Some(new_image) = mutation.new_image {
                    let table_info = self.get_table_info_cached_arc(&table_name).await?;
                    let split_item =
                        split_item_into_key_and_attributes_sync(new_image, &table_info)?;
                    let key_bindings = Self::key_column_bindings_for_schema(
                        &table_info,
                        &table_info.key_schema,
                        &split_item.key_attributes,
                        None,
                    )?;
                    let mut client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    let transaction = client.transaction().await.map_err(|err| {
                        Self::map_postgres_write_error(
                            "start apply_replication_mutation put transaction",
                            err,
                        )
                    })?;
                    let old_item = self
                        .get_item_with_client(
                            &transaction,
                            &table_name,
                            &split_item.key_attributes,
                            &table_info,
                        )
                        .await?;

                    let attributes_blob = if split_item.non_key_attributes.is_empty() {
                        "{}".to_string()
                    } else {
                        serde_json::to_string(&split_item.non_key_attributes).map_err(|err| {
                            Self::map_postgres_error(
                                "serialize replication non-key attributes",
                                err,
                            )
                        })?
                    };

                    let physical_table_name = physical_names::physical_table_name(&table_name);
                    let key_columns = key_bindings
                        .iter()
                        .map(|binding| binding.column.clone())
                        .collect::<Vec<_>>();
                    let columns_sql = key_columns
                        .iter()
                        .cloned()
                        .chain(std::iter::once("attributes_blob".to_string()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let mut value_placeholders = key_bindings
                        .iter()
                        .enumerate()
                        .map(|(idx, binding)| {
                            Self::postgres_placeholder_for_type(idx + 1, &binding.attribute_type)
                        })
                        .collect::<Vec<_>>();
                    value_placeholders.push(format!("${}", key_bindings.len() + 1));
                    let values_placeholders = value_placeholders.join(", ");
                    let conflict_target = key_columns.join(", ");
                    let assignments = key_columns
                        .iter()
                        .cloned()
                        .chain(std::iter::once("attributes_blob".to_string()))
                        .map(|column| format!("{column} = EXCLUDED.{column}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let sql = sql_statements::upsert_main_row(
                        &physical_table_name,
                        &columns_sql,
                        &values_placeholders,
                        &conflict_target,
                        &assignments,
                    );

                    let mut bind_values = key_bindings
                        .iter()
                        .map(|binding| binding.value.clone())
                        .collect::<Vec<_>>();
                    bind_values.push(attributes_blob);
                    let params: Vec<&(dyn ToSql + Sync)> = bind_values
                        .iter()
                        .map(|value| value as &(dyn ToSql + Sync))
                        .collect();
                    transaction.execute(&sql, &params).await.map_err(|err| {
                        Self::map_postgres_write_error(
                            "apply_replication_mutation put execute",
                            err,
                        )
                    })?;
                    let item_stream_version = storage_types::ItemStreamVersion::try_from(
                        Self::bump_item_revision_with_client(
                            &transaction,
                            &table_name,
                            &split_item.key_attributes,
                        )
                        .await?,
                    )?;

                    let old_item_for_ttl = old_item
                        .as_ref()
                        .map(WireItem::to_attribute_map)
                        .transpose()?;
                    if self.immediate_gsi_consistency {
                        self.apply_gsi_entries_for_item_change_with_client(
                            &transaction,
                            &table_name,
                            &table_info,
                            old_item_for_ttl.as_ref(),
                            Some(&split_item.all_attributes),
                        )
                        .await?;
                    }
                    self.sync_ttl_index_entries_with_client(
                        &transaction,
                        &table_info,
                        old_item_for_ttl.as_ref(),
                        Some(&split_item.all_attributes),
                    )
                    .await?;
                    self.write_stream_entries_for_item_with_client(
                        &transaction,
                        &table_info,
                        &split_item.all_attributes,
                        PostgresWriteStreamEntriesInput {
                            old_item: old_item_for_ttl.as_ref(),
                            is_deleted: false,
                            item_stream_version,
                            replication: Some(&metadata),
                        },
                    )
                    .await?;
                    transaction.commit().await.map_err(|err| {
                        Self::map_postgres_write_error(
                            "commit apply_replication_mutation put transaction",
                            err,
                        )
                    })?;
                    return Ok(());
                }

                let table_info = self.get_table_info_cached_arc(&table_name).await?;
                let key_attributes = mutation.key.clone();
                let mut client = self
                    .pool
                    .get()
                    .await
                    .map_err(Self::map_postgres_client_acquire_error)?;
                let transaction = client.transaction().await.map_err(|err| {
                    Self::map_postgres_write_error(
                        "start apply_replication_mutation delete transaction",
                        err,
                    )
                })?;
                let old_item = self
                    .get_item_with_client(&transaction, &table_name, &key_attributes, &table_info)
                    .await?;

                let key_bindings = Self::key_column_bindings_for_schema(
                    &table_info,
                    &table_info.key_schema,
                    &key_attributes,
                    None,
                )?;
                let mut bind_values = Vec::with_capacity(key_bindings.len());
                let where_sql = Self::where_clause_for_bindings(&key_bindings, &mut bind_values);
                let physical_table_name = physical_names::physical_table_name(&table_name);
                let sql = sql_statements::delete_main_row(&physical_table_name, &where_sql);
                let params: Vec<&(dyn ToSql + Sync)> = bind_values
                    .iter()
                    .map(|value| value as &(dyn ToSql + Sync))
                    .collect();
                transaction.execute(&sql, &params).await.map_err(|err| {
                    Self::map_postgres_write_error("apply_replication_mutation delete execute", err)
                })?;
                let item_stream_version = storage_types::ItemStreamVersion::try_from(
                    Self::bump_item_revision_with_client(
                        &transaction,
                        &table_name,
                        &key_attributes,
                    )
                    .await?,
                )?;

                let old_map = old_item
                    .as_ref()
                    .map(WireItem::to_attribute_map)
                    .transpose()?;
                if self.immediate_gsi_consistency {
                    self.apply_gsi_entries_for_item_change_with_client(
                        &transaction,
                        &table_name,
                        &table_info,
                        old_map.as_ref(),
                        None,
                    )
                    .await?;
                }
                self.sync_ttl_index_entries_with_client(
                    &transaction,
                    &table_info,
                    old_map.as_ref(),
                    None,
                )
                .await?;
                self.write_stream_entries_for_item_with_client(
                    &transaction,
                    &table_info,
                    &mutation.key.to_attribute_map(),
                    PostgresWriteStreamEntriesInput {
                        old_item: old_map.as_ref(),
                        is_deleted: true,
                        item_stream_version,
                        replication: Some(&metadata),
                    },
                )
                .await?;
                transaction.commit().await.map_err(|err| {
                    Self::map_postgres_write_error(
                        "commit apply_replication_mutation delete transaction",
                        err,
                    )
                })?;
                Ok(())
            }
        })
        .await
    }

    pub(super) const fn do_replication_apply_parallelism_hint(&self) -> usize {
        REPLICATION_APPLY_PARALLELISM_HINT
    }
}
