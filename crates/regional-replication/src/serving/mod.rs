mod catalog_reads;
mod direct_expiration;
mod expiration;
mod expiration_admission;
mod listing;
mod metadata;
mod metadata_operations;
mod operations;
mod read_operations;
mod reads;
mod response;
mod runtime;
mod sequence;
mod ttl_update;

pub use runtime::RegionalDynamo;

mod gsi;
mod gsi_admission;

mod replica_description;
mod replica_update;

mod lifecycle_update;
pub(crate) mod replica_request;

mod replica_create_request;

mod replica_create_acceptance;

mod table_metadata;

mod retention;
mod table_reclaim;
