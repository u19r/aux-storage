use scheduled_bg_jobs::store::{Check, Mutation};

use crate::RawOrderedJobStoreError;

/// Physical I/O inside one native conditional transaction. The shared executor
/// owns check evaluation; the caller owns finite replay and recovery.
pub trait AtomicCommit {
    type Error: From<RawOrderedJobStoreError>;

    /// Returns one optional raw value per check, in request order. The shared
    /// executor owns cardinality, size and condition validation.
    fn load_check_values(
        &mut self,
        checks: &[Check],
    ) -> impl std::future::Future<Output = Result<Vec<Option<Vec<u8>>>, Self::Error>> + Send;
    fn store_mutations(
        &mut self,
        mutations: &[Mutation],
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}
