mod executor;
mod request;
mod status;

pub use executor::{advance_table_seal, cancel_table_seal, observe_table_seal};
pub use request::SealRequest;
pub use status::{SealStatus, SealedTableOrigin};

mod vector;
pub use executor::load_sealed_table_cuts;
pub use vector::{SealReceptionProgress, SealVectorPage, SealedOriginCut};

pub(crate) mod replica_request;

mod reception;
pub(crate) use reception::{
    observe_received_seal, receive_sealed_table_cuts, retire_removed_table,
};

pub(crate) mod deployment;
mod primary_transfer;
pub(crate) mod update;
