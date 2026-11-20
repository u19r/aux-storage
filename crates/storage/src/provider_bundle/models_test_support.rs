use super::models::{InitializedStore, MaintenanceRepositories, StoreKind};

impl InitializedStore {
    /// Test fixtures read capabilities before the store becomes a connection.
    pub(crate) fn native_store(&self) -> Option<&dyn super::native_store::NativeStore> {
        match &self.kind {
            StoreKind::Native(store) => Some(store.as_ref()),
            #[cfg(feature = "remote")]
            StoreKind::Remote { .. } => None,
        }
    }

    /// The old bundle fields that maintenance tests read, over one store value
    /// in the caller lane.
    pub(crate) fn caller_maintenance(&self) -> CallerMaintenance {
        let store = self.native_store().expect("native fixture store");
        CallerMaintenance {
            maintenance: Some(
                store.maintenance(storage_common::native_admission::AttemptAdmission::Caller),
            ),
            scheduled_job_store: store
                .scheduled_job_store(Default::default())
                .expect("fixture job store"),
        }
    }
}

pub(crate) struct CallerMaintenance {
    pub maintenance: Option<MaintenanceRepositories>,
    pub scheduled_job_store: bg_job_store::ScheduledJobStore,
}
