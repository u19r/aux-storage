//! Closed effects selected by the ordinary expiration evaluator.

use storage_types::{StorageResult, maintenance::StepResult};

use super::{ExpirationPageWrite, ExpirationProgressWrite, ExpirationTransaction};

pub(super) enum ExpirationWrite {
    Progress(ExpirationProgressWrite),
    Page(ExpirationPageWrite),
}

impl ExpirationWrite {
    pub(super) async fn apply(
        self,
        transaction: &mut impl ExpirationTransaction,
    ) -> StorageResult<StepResult<()>> {
        match self {
            Self::Progress(write) => transaction.write_expiration_progress(write).await,
            Self::Page(write) => transaction.write_expiration_page(write).await,
        }
    }
}
