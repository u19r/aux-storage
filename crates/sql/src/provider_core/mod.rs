pub(crate) mod gsi_write;
#[cfg(test)]
mod gsi_write_alloc_tests;
#[cfg(test)]
mod gsi_write_tests;
pub(crate) mod read;
pub(crate) mod statements;
pub(crate) mod table_lifecycle;
pub(crate) mod transaction;
pub(crate) mod write;

#[cfg(test)]
mod table_lifecycle_tests;
