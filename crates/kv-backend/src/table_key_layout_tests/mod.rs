//! Table-first key shapes and the table data-range invariant.

#[cfg(any(
    feature = "rocksdb",
    all(feature = "foundationdb", feature = "test-support")
))]
mod invariant;
mod key_shapes;
