use storage_types::{IndexName, TableName};

/// Build the physical table name used in sqlite for a logical table.
#[inline]
pub fn physical_table_name(table: &TableName) -> String {
    format!("table_{}", table.sanitized_name())
}

/// Build the physical GSI table name used in sqlite for a logical GSI.
#[inline]
pub fn physical_gsi_table_name(table: &TableName, index: &IndexName) -> String {
    format!("gsi_{}_{}", table.sanitized_name(), index.sanitized_name())
}

/// Build the physical TTL index table name used in sqlite for a logical table.
#[inline]
pub fn physical_ttl_index_table_name(table: &TableName) -> String {
    format!("ttl_index_{}", table.sanitized_name())
}
