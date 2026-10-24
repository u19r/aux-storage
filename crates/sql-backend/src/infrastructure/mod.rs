//! Ordinary retained images and explicit record attachment share one physical
//! pool.

mod codec;
mod error;
pub(crate) mod layout;
mod physical_attempt;
mod read;
mod sql_statements;
mod write;

mod store;

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
mod record_shape;
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) use record_shape::RecordStatementShape;
