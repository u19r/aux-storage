pub mod compact;
pub(crate) mod stream_keys;
pub mod table_identity;
pub(crate) mod table_keys;
#[cfg(feature = "foundationdb-backend")]
pub(crate) mod tuple_keys;
#[cfg(all(test, feature = "foundationdb-backend"))]
mod tuple_keys_tests;

#[cfg(all(
    test,
    feature = "rocksdb-backend",
    not(feature = "foundationdb-backend")
))]
mod compact_tests;
#[cfg(test)]
mod table_identity_tests;
#[cfg(test)]
mod table_keys_tests;
