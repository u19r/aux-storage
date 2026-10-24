//! One protected expiration target bound for the lifetime of a native attempt.

use std::sync::Arc;

use storage_common::TtlPolicyGeneration;
use storage_types::{BoundTableInfo, StorageError, StorageResult};

use super::{ExpirationPageWrite, ExpirationProgressWrite, ExpirationScanRead, ExpirationTarget};

#[derive(Default)]
pub struct ExpirationAttemptState {
    table: Option<Arc<BoundTableInfo>>,
    target: Option<ExpirationTarget>,
}

impl ExpirationAttemptState {
    pub fn bind(
        &mut self,
        bound: Option<(ExpirationTarget, Arc<BoundTableInfo>)>,
    ) -> Option<ExpirationTarget> {
        self.target = bound.as_ref().map(|(target, _)| target.clone());
        self.table = bound.map(|(_, table)| table);
        self.target.clone()
    }

    pub fn table(&self) -> StorageResult<Arc<BoundTableInfo>> {
        self.table
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| StorageError::internal("expiration table was not bound in this attempt"))
    }

    pub fn validate_scan(&self, read: &ExpirationScanRead) -> StorageResult<()> {
        let target = self
            .target
            .as_ref()
            .ok_or_else(|| StorageError::internal("expiration target was not observed"))?;
        if target.table_name != read.table_name
            || target.table_incarnation != read.table_incarnation
            || target.policy_generation != read.policy_generation
            || target.ttl_attribute != read.ttl_attribute
        {
            return Err(StorageError::internal("expiration scan target changed"));
        }
        Ok(())
    }

    pub fn validate_policy(&self, generation: TtlPolicyGeneration) -> StorageResult<()> {
        if self
            .target
            .as_ref()
            .is_none_or(|target| target.policy_generation != generation)
        {
            return Err(StorageError::internal(
                "expiration policy changed in this attempt",
            ));
        }
        Ok(())
    }

    pub fn validate_page(&self, write: &ExpirationPageWrite) -> StorageResult<()> {
        let table = self.table()?;
        if table.info.table_name != write.table_name
            || write.table_incarnation.as_ref() != table.history_id.as_ref()
        {
            return Err(StorageError::internal("expiration page has another table"));
        }
        Ok(())
    }

    pub fn validate_progress(write: &ExpirationProgressWrite) -> StorageResult<()> {
        if write.next_progress.authority.scope.target != write.current_target {
            return Err(StorageError::internal(
                "expiration progress has another target",
            ));
        }
        Ok(())
    }
}
