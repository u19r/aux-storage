mod capped_entity_create;
pub use capped_entity_create::{
    CappedEntityCreateOutcome, CappedEntityCreateRejection, PreparedCappedEntityCreate,
};
mod capped_entity_ops;
mod constants;
mod construction;
mod core;
mod effect_authorization;
mod entity_ops;
mod errors;
mod guarded_job_effect;
mod test_database;
pub use effect_authorization::DomainEffectAuthorization;
mod models;
mod native_admission;
mod operation_metrics;
mod query_ops;
mod read_batch;
mod read_ops;
mod read_sequence_ops;
#[cfg(feature = "regional")]
mod regional_startup;
mod runtime_options;
mod service_shutdown;
mod system_stream_trim;
mod table_ops;
mod transact_get_ops;
mod transaction_write_coordinator;
mod ttl_sweep;
mod wire_item_ops;
mod write_bulk_ops;
mod write_ops;

pub use runtime_options::DatabaseManagerRuntimeOptions;

pub(crate) use crate::database_manager::{
    constants::{
        CAPPED_ENTITY_COUNTER_CREATE_CONDITION, CAPPED_ENTITY_COUNTER_DELETE_CONDITION,
        CAPPED_ENTITY_COUNTER_UPDATE_EXPRESSION, CAPPED_ENTITY_COUNTER_VALUE_ATTR,
        ENTITY_ABSENT_CONDITION, ENTITY_EXISTS_CONDITION, ROUTED_DEFAULT_CONNECTION_ID,
    },
    operation_metrics::record_storage_operation,
};
pub use crate::database_manager::{
    core::{DatabaseManager, ResolvedGetItem},
    errors::CappedStorageError,
    models::{
        CreateCappedEntityInput, DeleteCappedEntityInput, DeleteItemInput,
        PutItemEntityEncodeInput, PutItemInput, QueryIndexInput, QueryTableInput, UpdateItemInput,
    },
};

#[cfg(test)]
mod admission_pressure_tests;
#[cfg(test)]
pub(crate) mod construction_tests;
#[cfg(test)]
mod models_tests;
#[cfg(test)]
mod operation_metrics_tests;
#[cfg(all(test, feature = "sqlite"))]
mod query_ops_tests;
#[cfg(test)]
mod read_ops_tests;
#[cfg(test)]
mod runtime_options_tests;
#[cfg(all(test, feature = "sqlite"))]
mod scheduled_job_store_tests;
#[cfg(test)]
mod transact_get_ops_tests;
#[cfg(test)]
mod transact_item_ops_tests;
#[cfg(test)]
mod ttl_sweep_tests;
#[cfg(test)]
mod write_ops_tests;

#[cfg(feature = "regional")]
mod history_read;
#[cfg(all(test, feature = "sqlite"))]
mod transaction_write_coordinator_tests;

mod history_response;
pub use history_response::CanonicalHistoryResponse;

#[cfg(all(test, feature = "sqlite"))]
mod read_sequence_ops_tests;

mod point_read_source;
