use std::sync::Arc;

use crate::{
    DatabaseManager,
    point_read::{PointReadSource, db_client::DatabasePointReadClient},
};

impl DatabaseManager {
    /// Shared physical strong/full-item source. It retains no result cache.
    /// Callers retain the database owner while using this source; the source
    /// uses a weak backreference so database shutdown cannot form a cycle.
    pub fn point_reads(self: &Arc<Self>) -> Arc<PointReadSource> {
        Arc::clone(self.point_reads.get_or_init(|| {
            Arc::new(PointReadSource::new(Arc::new(DatabasePointReadClient(
                Arc::downgrade(self),
            ))))
        }))
    }
}
