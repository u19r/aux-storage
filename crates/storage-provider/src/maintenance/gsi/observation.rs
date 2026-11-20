//! Protected source pages are the sole authority for backfill effects.
use storage_backfill::{
    GsiActive, GsiBuild, GsiChange, GsiPageLimit, GsiProjectionWriteBatch, GsiState,
};
use storage_types::{StorageError, StorageResult};

use super::GsiBuildTransaction;

pub(super) async fn evaluate<Transaction: GsiBuildTransaction>(
    transaction: &mut Transaction,
    build: &GsiBuild<Transaction::Cursor>,
    limit: GsiPageLimit,
) -> StorageResult<GsiChange<Transaction::Cursor>> {
    match &build.state {
        GsiState::Filling(scan) => {
            let page = transaction
                .read_gsi_scan(&build.target, scan.after.as_ref(), limit)
                .await?;
            build
                .advance_protected_page(page)
                .map_err(|error| StorageError::internal(&error.to_string()))
        }
        GsiState::Building(state) => Ok(GsiChange::Persist {
            projections: GsiProjectionWriteBatch::default(),
            next_state: GsiState::Active(GsiActive {
                cursor: state.low.clone(),
            }),
        }),
        GsiState::Active(_) => Err(StorageError::internal("Active GSI has no build work")),
    }
}
