//! One persisted item relation (with GSI key columns and partial indexes) and
//! TTL partial-index layout for every SQL driver.

use storage_types::{
    AttributeName, BoundGsi, KeyColumn, KeySchemaElement, MAX_ITEM_SIZE_BYTES, MaxIndexers,
    PlannedKeyColumn, StorageError, StorageResult, StoredTableInfo, TableBinding, TableHistoryId,
    planned_key_columns,
};

use crate::SqlLayout;

// Customer key identifiers replace hyphens with underscores. A hyphen keeps
// this physical metadata column outside that namespace without rejecting keys.
pub(crate) const TTL_KEY_COLUMN: &str = "__aux-ttl-key";

pub(crate) fn ttl_key_identifier() -> String {
    format!("\"{TTL_KEY_COLUMN}\"")
}

/// Render the item relation: the table key, one nullable typed key column for
/// each GSI that writers maintain (sparse items stay NULL), and the payload.
#[must_use]
pub fn render_item_table(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    binding: &TableBinding,
    history_id: TableHistoryId,
) -> String {
    let mut columns = planned_key_columns(
        &table.key_schema,
        std::iter::empty(),
        &table.attribute_definitions,
    )
    .iter()
    .map(|column| key_definition(engine, column))
    .collect::<Vec<_>>();
    columns.extend(projecting_key_columns(binding).map(|column| {
        format!(
            "\"{}\" {}",
            key_column_identifier(column),
            engine.engine().key_column_type(column.kind)
        )
    }));
    columns.extend(item_metadata_columns(engine));
    columns.extend(indexer_columns(table.max_indexers));
    let primary = primary_key_columns(&table.key_schema);
    create_table(
        engine,
        &engine.item_table_name(history_id),
        columns,
        primary,
    )
}

/// The distinct key columns of the user GSIs that writers maintain. The
/// hidden TTL index uses the primary metadata columns.
pub(crate) fn projecting_key_columns(binding: &TableBinding) -> impl Iterator<Item = &KeyColumn> {
    let mut seen = Vec::<&KeyColumn>::new();
    binding
        .gsis
        .iter()
        .filter(|gsi| !gsi.name.is_ttl_index())
        .flat_map(|gsi| &gsi.columns)
        .filter(move |column| {
            if seen.iter().any(|known| {
                known.attribute == column.attribute
                    && known.kind == column.kind
                    && known.generation == column.generation
            }) {
                return false;
            }
            seen.push(column);
            true
        })
}

/// The quoted-identifier text of a key column (`"` is doubled).
#[must_use]
pub(crate) fn key_column_identifier(column: &KeyColumn) -> String {
    column.physical_name().replace('"', "\"\"")
}

/// The GSI key columns in key order: partition key, then sort key.
pub(crate) fn ordered_gsi_columns<'a>(
    table: &StoredTableInfo,
    gsi: &'a BoundGsi,
) -> StorageResult<Vec<&'a KeyColumn>> {
    let index = table
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|index| index.index_name == gsi.name)
        .ok_or_else(|| StorageError::internal("SQL GSI binding has no table index"))?;
    KeySchemaElement::ordered(&index.key_schema)
        .map(|element| {
            gsi.columns
                .iter()
                .find(|column| column.attribute == element.attribute_name)
                .ok_or_else(|| StorageError::internal("SQL GSI binding lacks a key column"))
        })
        .collect()
}

/// The partial index of one GSI on the item relation:
/// `(gsi pk[, gsi sk], pk[, sk]) WHERE gsi pk IS NOT NULL [AND gsi sk IS NOT
/// NULL]`. Items without the GSI key are not indexed.
pub fn render_gsi_index(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    history_id: TableHistoryId,
    gsi: &BoundGsi,
) -> StorageResult<String> {
    let keys = ordered_gsi_columns(table, gsi)?
        .into_iter()
        .map(|column| format!("\"{}\"", key_column_identifier(column)))
        .collect::<Vec<_>>();
    let mut columns = keys.clone();
    columns.extend(primary_key_columns(&table.key_schema));
    let predicate = keys
        .iter()
        .map(|key| format!("{key} IS NOT NULL"))
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(format!(
        "CREATE INDEX \"{}\" ON \"{}\" ({}) WHERE {predicate}",
        engine.gsi_index_name(history_id, gsi),
        engine.item_table_name(history_id),
        columns.join(", ")
    ))
}

#[must_use]
pub fn render_ttl_index(engine: impl SqlLayout, history_id: TableHistoryId) -> String {
    let ttl_key = ttl_key_identifier();
    let index_name = format!(
        "t_{}_ttl",
        uuid::Uuid::from_bytes(*history_id.as_ref()).simple()
    );
    format!(
        "CREATE INDEX \"{index_name}\" ON \"{}\" ({ttl_key}) WHERE {ttl_key} IS NOT NULL",
        engine.item_table_name(history_id),
    )
}

/// A GSI added by UpdateTable with a new key-column generation adds its
/// nullable column to the item relation.
#[must_use]
pub fn render_add_key_column(
    engine: impl SqlLayout,
    column: &KeyColumn,
    history_id: TableHistoryId,
) -> String {
    format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}",
        engine.item_table_name(history_id),
        key_column_identifier(column),
        engine.engine().key_column_type(column.kind)
    )
}

/// Grow an item relation's indexer capacity by one slot.
#[must_use]
pub fn render_add_indexer_column(physical: &str, ordinal: usize) -> String {
    format!("ALTER TABLE \"{physical}\" ADD COLUMN __aux_indexer_{ordinal} TEXT")
}

fn item_metadata_columns(engine: impl SqlLayout) -> Vec<String> {
    let int = engine.engine().integer_type();
    vec![
        format!("__aux_provenance {} NOT NULL", engine.engine().blob_type()),
        format!("__aux_ttl_epoch {int}"),
        format!("{} {}", ttl_key_identifier(), engine.engine().blob_type()),
        format!(
            "__aux_evaluated_bytes {int} NOT NULL CHECK (__aux_evaluated_bytes > 0 AND \
             __aux_evaluated_bytes <= {MAX_ITEM_SIZE_BYTES})"
        ),
        format!("item_value {} NOT NULL", engine.engine().blob_type()),
    ]
}

fn key_definition(engine: impl SqlLayout, column: &PlannedKeyColumn) -> String {
    let attribute = AttributeName::new(&column.attribute_name);
    let name = attribute.sanitized();
    let storage = engine.engine().key_column_type(column.attribute_type);
    format!("\"{name}\" {storage} NOT NULL")
}

fn primary_key_columns(schema: &[KeySchemaElement]) -> Vec<String> {
    KeySchemaElement::ordered(schema)
        .map(|element| {
            let attribute = AttributeName::new(&element.attribute_name);
            format!("\"{}\"", attribute.sanitized())
        })
        .collect()
}

fn indexer_columns(max_indexers: MaxIndexers) -> impl Iterator<Item = String> {
    (0..max_indexers.as_usize()).map(|ordinal| format!("__aux_indexer_{ordinal} TEXT"))
}

fn create_table(
    engine: impl SqlLayout,
    physical: &str,
    mut columns: Vec<String>,
    primary: Vec<String>,
) -> String {
    if !primary.is_empty() {
        columns.push(format!("PRIMARY KEY ({})", primary.join(", ")));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS \"{physical}\" ({}){}",
        columns.join(", "),
        engine.engine().table_suffix()
    )
}
