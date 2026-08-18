use bg_jobs::{BackgroundJobName, DatabaseJobKind};

use crate::sorted_kv_store::TransactionPriority;

pub(crate) const fn uses_batch_priority(name: BackgroundJobName) -> bool {
    matches!(
        name,
        BackgroundJobName::Database {
            kind: DatabaseJobKind::GsiBackfill
                | DatabaseJobKind::QueuePayloadCleanup
                | DatabaseJobKind::StreamTtlCleanup
                | DatabaseJobKind::StreamTrim
                | DatabaseJobKind::TtlSweep,
        }
    )
}

pub(crate) const fn priority_for_job(name: BackgroundJobName) -> TransactionPriority {
    if uses_batch_priority(name) {
        TransactionPriority::Batch
    } else {
        TransactionPriority::Default
    }
}
