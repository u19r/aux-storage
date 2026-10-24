use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_backfill::GsiBuildTarget;
use storage_provider::maintenance::{GsiPort, GsiTargetAdmission};
use storage_types::StorageResult;

use crate::journal::JournalStore;

#[derive(Clone)]
pub(super) struct RegionalGsiAdmission {
    pub(super) journal: Arc<JournalStore>,
}

impl<A: GsiPort + PhysicalRecordAttempt> GsiTargetAdmission<A> for RegionalGsiAdmission {
    async fn admits_target(&self, attempt: &mut A, target: &GsiBuildTarget) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(stamp) = attempt.read_catalog_stamp(&target.table_name).await? else {
                return Ok(false);
            };
            let table = attempt
                .read_table_at_stamp(&target.table_name, stamp)
                .await?;
            target.validate_table(&table)?;
            self.journal.admits_gsi_build(attempt, &table).await
        }
        .await;
        settlement.complete(result)
    }
}
