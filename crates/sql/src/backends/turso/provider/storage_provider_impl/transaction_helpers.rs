use crate::backends::turso::provider::storage_provider_impl::*;

impl TursoStorageProvider {
    pub(crate) async fn preflight_transact_item_key(
        &self,
        item: &TransactWriteItem,
    ) -> StorageResult<TransactionKeyPreflight> {
        let Some(table_name) = transact_item_table_name(item) else {
            return Ok(TransactionKeyPreflight::default());
        };
        let table_info = self.get_table_info(table_name).await?;
        preflight_transact_item_key_with_table_info(item, &table_info)
    }

    pub(crate) async fn execute_prepared_batch_operations<C>(
        &self,
        conn: &C,
        prepared_ops: &[PreparedBatchOperation],
    ) -> StorageResult<()>
    where
        C: crate::backends::turso::provider::core::TursoSqlConnection + ?Sized,
    {
        for prepared_op in prepared_ops {
            match prepared_op {
                PreparedBatchOperation::Put {
                    table_info,
                    full_item,
                    aux_item_stream_ttl_hours,
                    ..
                } => {
                    let _ = self
                        .put_item_txn(
                            conn,
                            table_info,
                            full_item,
                            None,
                            false,
                            *aux_item_stream_ttl_hours,
                        )
                        .await?;
                }
                PreparedBatchOperation::Delete {
                    table_info,
                    key,
                    aux_item_stream_ttl_hours,
                    ..
                } => {
                    let _ = self
                        .delete_item_txn_with_replication(
                            conn,
                            TursoDeleteItemInput {
                                table_info,
                                key,
                                condition: None,
                                return_old_on_condition_failure: false,
                                replication: None,
                                item_stream_ttl_hours: *aux_item_stream_ttl_hours,
                            },
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }
}
