//! Optional native-call interception for provider-agnostic fault tests.

use futures::future::BoxFuture;
use storage_types::{StorageEnum, StorageError, StorageResult};

use crate::SqlQueryPage;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlFaultOperation {
    Query,
    Execute,
}

pub struct SqlFaultCall<'a> {
    pub operation: SqlFaultOperation,
    pub sql: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlCommitBoundary {
    BeforeCommit,
    AfterDurableAcknowledgement,
}

impl SqlCommitBoundary {
    pub(crate) async fn observe(self, probe: Option<&dyn SqlFaultProbe>) -> StorageResult<()> {
        let Some(probe) = probe else {
            return Ok(());
        };
        probe
            .at_commit_boundary(self)
            .await
            .map_err(|_| match self {
                Self::BeforeCommit => StorageEnum::TransactionConflict {
                    message: "SQL commit was rejected before native submission".to_owned(),
                }
                .into(),
                Self::AfterDurableAcknowledgement => StorageError::sync_submission_ambiguous(
                    None,
                    "SQL durable commit response was suppressed at its native boundary",
                ),
            })
    }
}

pub trait SqlFaultProbe: Send + Sync {
    fn fail(&self, call: SqlFaultCall<'_>) -> Option<StorageError>;

    /// Intercept a hydrated primary image without coupling fault fixtures to
    /// the native descriptor or immutable-body encoding.
    fn alter_item_image(
        &self,
        _protection: crate::ReadProtection,
        item: storage_types::WireItem,
    ) -> storage_types::WireItem {
        item
    }

    fn at_commit_boundary(&self, _boundary: SqlCommitBoundary) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn alter_query_page(&self, _call: SqlFaultCall<'_>, page: SqlQueryPage) -> SqlQueryPage {
        page
    }
}
