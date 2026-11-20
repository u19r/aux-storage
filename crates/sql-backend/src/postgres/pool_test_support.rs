//! Fault-injection setup retains duplicate-install assertions outside runtime
//! code.

use std::sync::Arc;

impl super::pool::Manager {
    pub(super) fn set_cleanup_probe(&self, probe: Arc<dyn crate::SqlFaultProbe>) {
        if self.cleanup_probe.set(probe).is_err() {
            panic!("Postgres cleanup probe installed twice");
        }
    }
}
