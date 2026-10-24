//! Compact KV record for the Dynamo TTL policy.

use storage_provider::{DescribeTimeToLiveAttempt, TtlConfigRecord, UpdateTimeToLiveAttempt};
use storage_types::{BoundTableInfo, StorageError, StorageResult, storage_serde};

use crate::{
    KvAttempt, KvMutation,
    catalog::{KvCatalogAttempt, KvTableIdentity},
    constants::TTL_CONFIG_FAMILY,
};

pub(crate) fn ttl_config_key(identity: &KvTableIdentity) -> StorageResult<Vec<u8>> {
    let name = identity.table_name.as_ref().as_bytes();
    let name_len = u16::try_from(name.len())
        .map_err(|_| StorageError::internal("TTL table name exceeds KV key width"))?;
    let mut key = Vec::with_capacity(7 + name.len());
    key.push(TTL_CONFIG_FAMILY);
    key.extend_from_slice(&identity.table_id.to_be_bytes());
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name);
    Ok(key)
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
            .map(|table| ttl_config_key(self.identity_for(&table.info.table_name)?))
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
        let key = ttl_config_key(&identity)?;
        let value = storage_serde::to_bytes(&config)?;
        self.attempt.stage(vec![KvMutation::Put { key, value }])?;
        self.replace_table_layout(table, identity, table.info.as_ref().clone())
            .await?;
        Ok(())
    }
}
