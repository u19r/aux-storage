use std::future::Future;

use storage_backfill::GsiBuildTarget;
use storage_types::StorageResult;

/// Admission belongs to the upper domain and joins the build's native attempt.
pub trait GsiTargetAdmission<A>: Send + Sync {
    fn admits_target(
        &self,
        attempt: &mut A,
        target: &GsiBuildTarget,
    ) -> impl Future<Output = StorageResult<bool>> + Send;
}

pub struct OrdinaryGsiBuild;

impl<A: Send> GsiTargetAdmission<A> for OrdinaryGsiBuild {
    async fn admits_target(
        &self,
        _attempt: &mut A,
        _target: &GsiBuildTarget,
    ) -> StorageResult<bool> {
        Ok(true)
    }
}
