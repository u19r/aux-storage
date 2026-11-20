//! KV record of the Dynamo TTL policy and its sweep checkpoint.
//!
//! The writer-visible part of the policy (attribute and status) is in the
//! catalog binding: a policy change stores this record and the binding in
//! one catalog change, which changes the stamp. The record under
//! `ttl_config_key` holds the full policy and the sweep checkpoint; no writer
//! reads it, and a checkpoint-only write changes no stamp (W6 A15).

use storage_provider::{TtlConfigRecord, TtlPolicyAttempt};
use storage_types::{BoundTableInfo, StorageResult, storage_serde};

use crate::{
    KvAttempt, KvMutation,
    catalog::{KvCatalogAttempt, table_id_from_history},
};

pub(crate) fn ttl_config_key(table_id: u32) -> Vec<u8> {
    crate::table_key_layout::table_prefix(
        table_id,
        crate::table_key_layout::TableSubFamily::TtlConfig,
    )
    .to_vec()
}

impl<A: KvAttempt> TtlPolicyAttempt for KvCatalogAttempt<'_, A> {
    async fn read_ttl_configs(
        &mut self,
        tables: &[&BoundTableInfo],
    ) -> StorageResult<storage_types::Exact<Option<TtlConfigRecord>>> {
        if tables.is_empty() {
            return Ok(storage_types::Exact::empty());
        }
        let keys = tables
            .iter()
            .map(|table| table_id_from_history(table.history_id).map(ttl_config_key))
            .collect::<StorageResult<Vec<_>>>()?;
        let values = self.attempt.get_batch_exact(keys, self.protection).await?;
        values.try_map(|value| {
            value
                .map(|bytes| storage_serde::from_bytes(&bytes))
                .transpose()
        })
    }

    async fn write_ttl_config(
        &mut self,
        table: &BoundTableInfo,
        config: TtlConfigRecord,
    ) -> StorageResult<()> {
        let identity = self.load_table_identity(table).await?;
        let key = ttl_config_key(identity.table_id);
        let value = storage_serde::to_bytes(&config)?;
        self.attempt.stage(vec![KvMutation::Put { key, value }])?;
        let binding = storage_types::TableBinding {
            ttl: Some(config.binding()),
            ..self.current_binding(table)
        };
        self.replace_table_layout(table, identity, table.info.as_ref().clone(), binding)
            .await?;
        Ok(())
    }

    async fn write_ttl_checkpoint(
        &mut self,
        table: &BoundTableInfo,
        change: storage_provider::TtlCheckpointChange,
    ) -> StorageResult<()> {
        let key = ttl_config_key(table_id_from_history(table.history_id)?);
        let value = storage_serde::to_bytes(&change.next)?;
        self.attempt.stage(vec![KvMutation::Put { key, value }])
    }
}
