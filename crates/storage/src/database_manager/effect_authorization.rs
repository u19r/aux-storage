use storage_types::{StorageResult, TransactConditionCheckRequest, TransactEncodeItem};

use super::DatabaseManager;

/// Publication requires every owning business claim. Scheduled publication
/// also requires the runtime's native job authority in the same transaction.
#[derive(Clone)]
pub struct DomainEffectAuthorization {
    job_authority: Option<scheduled_bg_jobs::JobEffectAuthority>,
    guards: Vec<TransactConditionCheckRequest>,
}

impl DomainEffectAuthorization {
    pub fn for_job(
        authority: scheduled_bg_jobs::JobEffectAuthority,
        guard: TransactConditionCheckRequest,
    ) -> Self {
        Self {
            job_authority: Some(authority),
            guards: vec![guard],
        }
    }

    pub fn for_claim(guard: TransactConditionCheckRequest) -> Self {
        Self {
            job_authority: None,
            guards: vec![guard],
        }
    }

    pub fn with_guard(mut self, guard: TransactConditionCheckRequest) -> Self {
        self.guards.push(guard);
        self
    }

    pub fn job_authority(&self) -> Option<&scheduled_bg_jobs::JobEffectAuthority> {
        self.job_authority.as_ref()
    }

    pub fn authorize_items(&self, items: &mut Vec<TransactEncodeItem>) -> StorageResult<()> {
        for guard in &self.guards {
            DatabaseManager::guard_job_effect_items(items, guard)?;
        }
        Ok(())
    }
}
