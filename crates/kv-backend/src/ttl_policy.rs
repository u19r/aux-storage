//! Compact KV record for the Dynamo TTL policy.

use storage_provider::{DescribeTimeToLiveAttempt, TtlConfigRecord, UpdateTimeToLiveAttempt};
use storage_types::{BoundTableInfo, StorageError, StorageResult, storage_serde};

use crate::{KvAttempt, KvMutation, catalog::KvCatalogAttempt, constants::TTL_CONFIG_FAMILY};

pub(crate) fn ttl_config_key(table_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(5);
    key.push(TTL_CONFIG_FAMILY);
    key.extend_from_slice(&table_id.to_be_bytes());
    key
}

impl<A: KvAttempt> DescribeTimeToLiveAttempt for KvCatalogAttempt<'_, A> {
    async fn read_ttl_configs(
        &mut self,
        tables: &[&BoundTableInfo],
    ) -> StorageResult<Vec<Option<TtlConfigRecord>>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        let keys = tables
            .iter()
            .map(|table| {
                self.identity_for(&table.info.table_name)
                    .map(|identity| ttl_config_key(identity.table_id))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let values = self.attempt.get_batch(keys, self.protection).await?;
        if values.len() != tables.len() {
            return Err(StorageError::internal(
                "KV TTL policy observation count differs",
            ));
        }
        values
            .into_iter()
            .map(|value| {
                value
                    .map(|bytes| storage_serde::from_bytes(&bytes))
                    .transpose()
            })
            .collect()
    }
}

impl<A: KvAttempt> UpdateTimeToLiveAttempt for KvCatalogAttempt<'_, A> {
    async fn write_ttl_config(
        &mut self,
        table: &BoundTableInfo,
        config: TtlConfigRecord,
    ) -> StorageResult<()> {
        let identity = self.identity_for(&table.info.table_name)?.clone();
        let key = ttl_config_key(identity.table_id);
        let value = storage_serde::to_bytes(&config)?;
        self.attempt.stage(vec![KvMutation::Put { key, value }])?;
        self.replace_table_layout(table, identity, table.info.as_ref().clone())
            .await?;
        Ok(())
    }
}
