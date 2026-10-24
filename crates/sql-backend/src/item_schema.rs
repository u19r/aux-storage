//! One persisted item, GSI, and TTL relation layout for every SQL driver.

use std::collections::HashSet;

use storage_types::{
    AttributeName, GlobalSecondaryIndex, KeySchemaElement, MAX_ITEM_SIZE_BYTES, MaxIndexers,
    PlannedKeyColumn, StoredTableInfo, TableHistoryId, maintenance::TargetIncarnation,
    planned_key_columns,
};

use crate::SqlLayout;

/// Render the denormalized item relation, including the union of all GSI key
/// columns. GSI-only keys stay nullable because sparse items are valid.
#[must_use]
pub fn render_item_table(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    history_id: TableHistoryId,
) -> String {
    // Definitions retain previously indexed attributes. Keep their nullable
    // columns so deleting and recreating a GSI never repeats an ALTER ADD.
    let planned = table
        .attribute_definitions
        .iter()
        .map(PlannedKeyColumn::from);
    let table_keys: HashSet<&str> = table
        .key_schema
        .iter()
        .map(|element| element.attribute_name.as_str())
        .collect();
    let mut columns = planned
        .map(|column| {
            let required = table_keys.contains(column.attribute_name.as_str());
            key_definition(engine, &column, "", required)
        })
        .collect::<Vec<_>>();
    columns.extend(item_metadata_columns(engine));
    columns.extend(indexer_columns(table.max_indexers));
    let primary = primary_key_columns(&table.key_schema, "");
    create_table(
        engine,
        &engine.item_table_name(&table.table_name, history_id),
        columns,
        primary,
    )
}

/// GSI rows carry their own projection and table key so a query does not
/// point-read the base table to return the projected item.
#[must_use]
pub fn render_gsi_table(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
    incarnation: &TargetIncarnation,
) -> String {
    let gsi_keys = planned_key_columns(
        &index.key_schema,
        std::iter::empty(),
        &table.attribute_definitions,
    );
    let table_keys = planned_key_columns(
        &table.key_schema,
        std::iter::empty(),
        &table.attribute_definitions,
    );
    let mut columns = gsi_keys
        .iter()
        .map(|key| key_definition(engine, key, "", true))
        .chain(
            table_keys
                .iter()
                .map(|key| key_definition(engine, key, "table_", true)),
        )
        .collect::<Vec<_>>();
    columns.extend(gsi_metadata_columns(engine));
    columns.extend(indexer_columns(table.max_indexers));
    let mut primary = primary_key_columns(&index.key_schema, "");
    primary.extend(primary_key_columns(&table.key_schema, "table_"));
    create_table(
        engine,
        &engine.gsi_table_name(&table.table_name, &index.index_name, incarnation),
        columns,
        primary,
    )
}

/// The live-first index supports bounded GSI reads over a relation that also
/// contains tombstones.
#[must_use]
pub fn render_gsi_read_index(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
    incarnation: &TargetIncarnation,
) -> String {
    let physical = engine.gsi_table_name(&table.table_name, &index.index_name, incarnation);
    let name = engine.gsi_read_index_name(&table.table_name, &index.index_name, incarnation);
    let mut columns = vec!["__aux_tombstone".to_owned()];
    columns.extend(primary_key_columns(&index.key_schema, ""));
    columns.extend(primary_key_columns(&table.key_schema, "table_"));
    format!(
        "CREATE INDEX \"{name}\" ON \"{physical}\" ({})",
        columns.join(", ")
    )
}

#[must_use]
pub fn render_ttl_index_table(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    history_id: TableHistoryId,
) -> String {
    let columns = vec![
        format!("ttl_value {} NOT NULL", engine.engine().integer_type()),
        format!("key_token {} NOT NULL", engine.engine().text_type()),
    ];
    create_table(
        engine,
        &engine.ttl_table_name(&table.table_name, history_id),
        columns,
        vec!["ttl_value".to_owned(), "key_token".to_owned()],
    )
}

/// A newly added GSI can introduce a nullable attribute into the item-key
/// union. The owning table update decides which columns are genuinely new.
#[must_use]
pub fn render_add_union_key_column(
    engine: impl SqlLayout,
    table: &StoredTableInfo,
    column: &PlannedKeyColumn,
    history_id: TableHistoryId,
) -> String {
    let physical = engine.item_table_name(&table.table_name, history_id);
    let attribute = AttributeName::new(&column.attribute_name);
    let name = attribute.sanitized();
    let storage = engine.engine().key_column_type(column.attribute_type);
    format!("ALTER TABLE \"{physical}\" ADD COLUMN \"{name}\" {storage}")
}

/// Grow an item or GSI relation's indexer capacity by one slot.
#[must_use]
pub fn render_add_indexer_column(physical: &str, ordinal: usize) -> String {
    format!("ALTER TABLE \"{physical}\" ADD COLUMN __aux_indexer_{ordinal} TEXT")
}

fn item_metadata_columns(engine: impl SqlLayout) -> Vec<String> {
    let int = engine.engine().integer_type();
    vec![
        format!("__aux_revision {int} NOT NULL DEFAULT 0"),
        format!("__aux_provenance {} NOT NULL", engine.engine().blob_type()),
        format!("__aux_ttl_epoch {int}"),
        format!(
            "__aux_evaluated_bytes {int} NOT NULL CHECK (__aux_evaluated_bytes > 0 AND \
             __aux_evaluated_bytes <= {MAX_ITEM_SIZE_BYTES})"
        ),
        format!("item_descriptor {} NOT NULL", engine.engine().blob_type()),
        format!(
            "__aux_indexers {} NOT NULL DEFAULT '[]'",
            engine.engine().text_type()
        ),
    ]
}

fn gsi_metadata_columns(engine: impl SqlLayout) -> Vec<String> {
    let int = engine.engine().integer_type();
    vec![
        format!("__aux_tombstone {int} NOT NULL DEFAULT 0"),
        format!("__aux_item_version {int} NOT NULL DEFAULT 0"),
        format!(
            "__aux_evaluated_bytes {int} CHECK ((__aux_tombstone = 1 AND __aux_evaluated_bytes IS \
             NULL) OR (__aux_tombstone = 0 AND __aux_evaluated_bytes IS NOT NULL AND \
             __aux_evaluated_bytes > 0 AND __aux_evaluated_bytes <= {MAX_ITEM_SIZE_BYTES}))"
        ),
        format!("item_descriptor {} NOT NULL", engine.engine().blob_type()),
    ]
}

fn key_definition(
    engine: impl SqlLayout,
    column: &PlannedKeyColumn,
    prefix: &str,
    required: bool,
) -> String {
    let attribute = AttributeName::new(&column.attribute_name);
    let name = attribute.sanitized();
    let storage = engine.engine().key_column_type(column.attribute_type);
    let nullability = if required { " NOT NULL" } else { "" };
    format!("\"{prefix}{name}\" {storage}{nullability}")
}

fn primary_key_columns(schema: &[KeySchemaElement], prefix: &str) -> Vec<String> {
    KeySchemaElement::ordered(schema)
        .map(|element| {
            let attribute = AttributeName::new(&element.attribute_name);
            let name = attribute.sanitized();
            format!("\"{prefix}{name}\"")
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
