use std::{collections::HashMap, time::Instant};

use deadpool_postgres::GenericClient;
#[cfg(test)]
use storage_common::provider_perf;
use storage_common::ttl::{
    TtlConfigRecord, normalize_ttl_seconds, ttl_index_key_map_from_token,
    ttl_index_key_token_for_item, ttl_value_from_item,
};
use storage_provider::StorageProvider;
use storage_types::{
    ScanTableRequest, StorageError, StorageResult, StoredTableInfo, TableName, TimeToLiveStatus,
};

use crate::{
    backends::postgres::{
        CachedTtlConfig, PostgresStorageProvider, physical_names, sql_statements,
    },
    helpers::MAX_SCAN_LIMIT,
};

impl PostgresStorageProvider {
    pub(super) async fn load_ttl_config(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        self.load_ttl_config_with_client(&client, table_name).await
    }

    pub(super) async fn load_ttl_config_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        if let Some(cached) = self.ttl_config_cache.read().await.get(table_name).cloned()
            && cached.is_fresh()
        {
            #[cfg(test)]
            provider_perf::record(
                "postgres",
                "ttl_config_cache_hit",
                std::time::Duration::ZERO,
            );
            return Ok(cached.config());
        }

        let started = Instant::now();
        let row = client
            .query_opt(sql_statements::get_ttl_config(), &[&table_name.as_ref()])
            .await
            .map_err(|err| Self::map_postgres_error("load ttl config", err))?;
        self.record_transaction_phase("batch_write_item", "ttl_config_query", started.elapsed());
        #[cfg(test)]
        provider_perf::record("postgres", "ttl_config_db_lookup", started.elapsed());
        let Some(row) = row else {
            self.ttl_config_cache
                .write()
                .await
                .insert(table_name.clone(), CachedTtlConfig::new(None));
            return Ok(None);
        };
        let blob: Vec<u8> = row
            .try_get("config_blob")
            .map_err(|err| Self::map_postgres_error("decode ttl config blob", err))?;
        let config: TtlConfigRecord = storage_types::storage_serde::from_bytes(&blob)?;
        self.ttl_config_cache.write().await.insert(
            table_name.clone(),
            CachedTtlConfig::new(Some(config.clone())),
        );
        Ok(Some(config))
    }

    pub(super) async fn list_ttl_configs(
        &self,
    ) -> StorageResult<Vec<(TableName, TtlConfigRecord)>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        let rows = client
            .query(sql_statements::list_ttl_configs(), &[])
            .await
            .map_err(|err| Self::map_postgres_error("list ttl configs", err))?;
        let mut configs = Vec::with_capacity(rows.len());
        for row in rows {
            let table_name: String = row
                .try_get("table_name")
                .map_err(|err| Self::map_postgres_error("decode ttl table_name", err))?;
            let blob: Vec<u8> = row
                .try_get("config_blob")
                .map_err(|err| Self::map_postgres_error("decode ttl config_blob", err))?;
            match storage_types::storage_serde::from_bytes::<TtlConfigRecord>(&blob) {
                Ok(record) => configs.push((TableName::new(&table_name), record)),
                Err(err) => {
                    tracing::warn!(table = %table_name, error = %err, "ttl.config.decode_failed");
                }
            }
        }
        Ok(configs)
    }

    pub(super) async fn save_ttl_config(
        &self,
        table_name: &TableName,
        config: &TtlConfigRecord,
    ) -> StorageResult<()> {
        let blob = storage_types::storage_serde::to_bytes(config)?;
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        client
            .execute(
                sql_statements::upsert_ttl_config(),
                &[&table_name.as_ref(), &blob],
            )
            .await
            .map_err(|err| Self::map_postgres_error("save ttl config", err))?;
        self.ttl_config_cache.write().await.insert(
            table_name.clone(),
            CachedTtlConfig::new(Some(config.clone())),
        );
        Ok(())
    }

    pub(super) async fn delete_ttl_config(&self, table_name: &TableName) -> StorageResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        client
            .execute(sql_statements::delete_ttl_config(), &[&table_name.as_ref()])
            .await
            .map_err(|err| Self::map_postgres_error("delete ttl config", err))?;
        self.ttl_config_cache
            .write()
            .await
            .insert(table_name.clone(), CachedTtlConfig::new(None));
        Ok(())
    }

    pub(super) async fn create_ttl_index_table(&self, table_name: &TableName) -> StorageResult<()> {
        let ttl_table_name = physical_names::physical_ttl_index_table_name(table_name);
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        client
            .batch_execute(&sql_statements::create_ttl_index_table(&ttl_table_name))
            .await
            .map_err(|err| Self::map_postgres_error("create ttl index table", err))?;
        Ok(())
    }

    pub(super) async fn drop_ttl_index_table(&self, table_name: &TableName) -> StorageResult<()> {
        let ttl_table_name = physical_names::physical_ttl_index_table_name(table_name);
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        client
            .batch_execute(&sql_statements::drop_ttl_index_table(&ttl_table_name))
            .await
            .map_err(|err| Self::map_postgres_error("drop ttl index table", err))?;
        Ok(())
    }

    pub(super) async fn backfill_ttl_index(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        ttl_attribute: &str,
    ) -> StorageResult<()> {
        let mut exclusive_start_key: Option<String> = None;
        let ttl_table_name = physical_names::physical_ttl_index_table_name(table_name);
        loop {
            let (items, lek) = self
                .scan_table(&ScanTableRequest {
                    table_name: table_name.clone(),
                    index_name: None,
                    limit: Some(MAX_SCAN_LIMIT),
                    exclusive_start_key: exclusive_start_key.clone(),
                    consistent_read: true,
                })
                .await?;
            if items.is_empty() {
                break;
            }

            let client = self
                .pool
                .get()
                .await
                .map_err(Self::map_postgres_client_acquire_error)?;
            for wire_item in items {
                let item = wire_item.into_attribute_map()?;
                let Some(ttl_value) = ttl_value_from_item(&item, ttl_attribute) else {
                    continue;
                };
                let normalized = i64::try_from(normalize_ttl_seconds(ttl_value))
                    .map_err(|_| StorageError::internal("postgres ttl normalize overflow"))?;
                let token = ttl_index_key_token_for_item(table_info, &item)?;
                client
                    .execute(
                        &sql_statements::insert_ttl_index_row(&ttl_table_name),
                        &[&normalized, &token],
                    )
                    .await
                    .map_err(|err| Self::map_postgres_error("insert ttl index row", err))?;
            }

            exclusive_start_key = lek;
            if exclusive_start_key.is_none() {
                break;
            }
        }
        Ok(())
    }

    pub(super) async fn sync_ttl_index_entries_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        table_info: &StoredTableInfo,
        old_item: Option<&HashMap<String, storage_provider::AttributeValue>>,
        new_item: Option<&HashMap<String, storage_provider::AttributeValue>>,
    ) -> StorageResult<()> {
        let Some(config) = self
            .load_ttl_config_with_client(client, &table_info.table_name)
            .await?
        else {
            return Ok(());
        };
        if !matches!(
            config.status,
            TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
        ) {
            return Ok(());
        }

        let old_entry = match old_item {
            Some(item) => match ttl_value_from_item(item, &config.attribute_name) {
                Some(ttl_value) => Some((
                    i64::try_from(normalize_ttl_seconds(ttl_value))
                        .map_err(|_| StorageError::internal("postgres ttl normalize overflow"))?,
                    ttl_index_key_token_for_item(table_info, item)?,
                )),
                None => None,
            },
            None => None,
        };
        let new_entry = match new_item {
            Some(item) => match ttl_value_from_item(item, &config.attribute_name) {
                Some(ttl_value) => Some((
                    i64::try_from(normalize_ttl_seconds(ttl_value))
                        .map_err(|_| StorageError::internal("postgres ttl normalize overflow"))?,
                    ttl_index_key_token_for_item(table_info, item)?,
                )),
                None => None,
            },
            None => None,
        };

        if old_entry.is_some() && old_entry == new_entry {
            return Ok(());
        }

        let ttl_table_name = physical_names::physical_ttl_index_table_name(&table_info.table_name);
        if let Some((ttl_value, key_token)) = old_entry {
            let started = Instant::now();
            client
                .execute(
                    &sql_statements::delete_ttl_index_row(&ttl_table_name),
                    &[&ttl_value, &key_token],
                )
                .await
                .map_err(|err| Self::map_postgres_error("delete ttl index row", err))?;
            self.record_transaction_phase("batch_write_item", "ttl_delete", started.elapsed());
        }
        if let Some((ttl_value, key_token)) = new_entry {
            let started = Instant::now();
            client
                .execute(
                    &sql_statements::insert_ttl_index_row(&ttl_table_name),
                    &[&ttl_value, &key_token],
                )
                .await
                .map_err(|err| Self::map_postgres_error("upsert ttl index row", err))?;
            self.record_transaction_phase("batch_write_item", "ttl_insert", started.elapsed());
        }
        Ok(())
    }

    pub(super) async fn run_ttl_sweep_once(&self) -> StorageResult<bool> {
        let configs = self.list_ttl_configs().await?;
        if configs.is_empty() {
            return Ok(false);
        }

        let now_seconds = chrono::Utc::now().timestamp();
        let mut did_work = false;

        for (table_name, config) in configs {
            if config.status != TimeToLiveStatus::Enabled {
                continue;
            }
            let table_info = match self.get_table_info(&table_name).await {
                Ok(table_info) => table_info,
                Err(err) => {
                    tracing::warn!(table = %table_name, error = %err, "ttl sweep skipped missing table");
                    continue;
                }
            };
            let ttl_table_name = physical_names::physical_ttl_index_table_name(&table_name);
            let client = self
                .pool
                .get()
                .await
                .map_err(Self::map_postgres_client_acquire_error)?;
            let rows = client
                .query(
                    &sql_statements::select_expired_ttl_rows(&ttl_table_name),
                    &[
                        &now_seconds,
                        &i64::try_from(crate::constants::TTL_SWEEP_DELETE_BATCH_SIZE)
                            .unwrap_or(i64::MAX),
                    ],
                )
                .await
                .map_err(|err| Self::map_postgres_error("read ttl sweep rows", err))?;

            if rows.is_empty() {
                continue;
            }

            for row in rows {
                let ttl_value: i64 = row
                    .try_get("ttl_value")
                    .map_err(|err| Self::map_postgres_error("decode ttl_value", err))?;
                let key_token: String = row
                    .try_get("key_token")
                    .map_err(|err| Self::map_postgres_error("decode key_token", err))?;
                let key_map = match ttl_index_key_map_from_token(&key_token, &table_info) {
                    Ok(key_map) => key_map,
                    Err(err) => {
                        tracing::warn!(
                            table = %table_name,
                            ttl_value,
                            key_token = %key_token,
                            error = %err,
                            "ttl sweep dropping malformed key token"
                        );
                        client
                            .execute(
                                &sql_statements::delete_ttl_index_row(&ttl_table_name),
                                &[&ttl_value, &key_token],
                            )
                            .await
                            .map_err(|e| Self::map_postgres_error("delete malformed ttl row", e))?;
                        did_work = true;
                        continue;
                    }
                };
                let _ = self
                    .delete_item(table_name.clone(), key_map.into(), None, None, None)
                    .await?;
                client
                    .execute(
                        &sql_statements::delete_ttl_index_row(&ttl_table_name),
                        &[&ttl_value, &key_token],
                    )
                    .await
                    .map_err(|err| Self::map_postgres_error("delete processed ttl row", err))?;
                did_work = true;
            }
        }

        Ok(did_work)
    }
}
