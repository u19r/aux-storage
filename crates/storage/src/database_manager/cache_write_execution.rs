use storage_types::StorageResult;

use crate::database_manager::DatabaseManager;

#[derive(Debug, Clone)]
pub(crate) enum PreparedCacheWrite {
    Effects(storage_cache::RuntimeWriteEffects),
    Update(Box<storage_cache::RuntimePreparedUpdateCacheWrite>),
}

impl DatabaseManager {
    async fn prepare_cache_write(&self, prepared: &PreparedCacheWrite) -> StorageResult<()> {
        match prepared {
            PreparedCacheWrite::Effects(effects) => {
                self.cache_services.prepare_write_intents(effects).await
            }
            PreparedCacheWrite::Update(update) => {
                self.cache_services
                    .prepare_update_write_intent(update)
                    .await
            }
        }
    }

    async fn release_cache_write(&self, prepared: &PreparedCacheWrite) -> StorageResult<()> {
        match prepared {
            PreparedCacheWrite::Effects(effects) => {
                self.cache_services.release_write_intents(effects).await
            }
            PreparedCacheWrite::Update(update) => {
                self.cache_services
                    .release_update_write_intent(update)
                    .await
            }
        }
    }

    pub(crate) async fn execute_with_cache_effects<T, DbF, DbFut, FinalizeF, FinalizeFut>(
        &self,
        prepared_cache: PreparedCacheWrite,
        db_op: DbF,
        finalize: FinalizeF,
    ) -> StorageResult<T>
    where
        DbF: FnOnce() -> DbFut,
        DbFut: std::future::Future<Output = StorageResult<T>>,
        FinalizeF: FnOnce(T) -> FinalizeFut,
        FinalizeFut:
            std::future::Future<Output = StorageResult<(T, storage_cache::RuntimeWriteEffects)>>,
    {
        self.prepare_cache_write(&prepared_cache).await?;

        let response = match db_op().await {
            Ok(response) => response,
            Err(error) => {
                self.release_cache_write(&prepared_cache).await?;
                return Err(error);
            }
        };

        let (response, cache_effects) = match finalize(response).await {
            Ok(result) => result,
            Err(error) => {
                self.release_cache_write(&prepared_cache).await?;
                return Err(error);
            }
        };

        self.cache_services
            .apply_write_effects(&cache_effects)
            .await?;
        Ok(response)
    }
}
