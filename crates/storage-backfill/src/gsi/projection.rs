use storage_types::{
    AttributeValue, GlobalSecondaryIndex, IndexName, IndexerDeclaration, KeyAttributes,
    KeySchemaElement, KeyType, ProjectionType, StorageError, StorageResult, StoredTableInfo,
    WireItem,
};

use super::{
    GsiIndexWriteActions, GsiKeyPart, GsiKeyParts, GsiProjectedRow, GsiProjectionKey,
    GsiProjectionWrite, GsiProjectionWriteBatch, GsiScannedSourceItem, GsiWriteAction,
};
impl GsiProjectionKey {
    /// The logical projection owns its combined secondary and primary shape;
    /// providers only encode the resulting native key.
    pub fn item_key(&self, table: &StoredTableInfo) -> StorageResult<storage_types::ItemKey> {
        let index = table
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|index| index.index_name == self.index_name)
            .ok_or_else(|| StorageError::internal("projection index is absent"))?;
        let secondary = storage_types::TableKey::from_key_schema(
            table.table_name.clone(),
            &index.key_schema,
            &self.index_key,
        )?;
        let primary = storage_types::TableKey::from_key_schema(
            table.table_name.clone(),
            &table.key_schema,
            &self.table_item_key,
        )?;
        Ok(storage_types::ItemKey::index_key(
            table.table_name.clone(),
            self.index_name.clone(),
            secondary.hash_key,
            secondary.range_key,
            primary,
        ))
    }
}

impl GsiProjectedRow {
    pub fn project_scanned_items(
        table: &StoredTableInfo,
        index_name: &IndexName,
        items: Vec<GsiScannedSourceItem>,
    ) -> StorageResult<GsiProjectionWriteBatch> {
        items
            .into_iter()
            .map(|source| Self::from_item(table, index_name, source.item, source.indexers))
            .collect::<StorageResult<Vec<_>>>()
            .map(|rows| {
                GsiProjectionWriteBatch(
                    rows.into_iter()
                        .flatten()
                        .map(|row| GsiProjectionWrite {
                            key: row.key.clone(),
                            row: Some(row),
                        })
                        .collect(),
                )
            })
    }

    pub fn from_item(
        table: &StoredTableInfo,
        index_name: &IndexName,
        item: WireItem,
        indexers: Vec<String>,
    ) -> StorageResult<Option<Self>> {
        let index = table
            .global_secondary_indexes
            .as_deref()
            .and_then(|indexes| indexes.iter().find(|index| &index.index_name == index_name))
            .ok_or_else(|| {
                StorageError::internal("GSI build target is missing its index schema")
            })?;
        let indexers = IndexerDeclaration::try_new(indexers, table.max_indexers)?;
        if index_name.is_ttl_index() {
            return project_ttl_item(table, index, item, indexers);
        }
        // An item whose index key attribute has another type than the index
        // declares is not indexed; it stays in the base table.
        if !index_key_types_match(table, index, &item)? {
            return Ok(None);
        }
        let mut actions = plan_gsi_write_actions_for(table, &[index], None, Some(&item))?;
        let Some(action) = actions.pop() else {
            return Ok(None);
        };
        let GsiWriteAction::Put {
            gsi_key,
            table_key,
            projection,
            ..
        } = action
        else {
            return Err(StorageError::internal(
                "GSI source projection produced an unexpected delete",
            ));
        };
        Ok(Some(GsiProjectedRow {
            key: GsiProjectionKey {
                index_name: index.index_name.clone(),
                index_key: key_attributes(gsi_key),
                table_item_key: key_attributes(table_key),
                ttl_index_key: None,
            },
            source_item: item,
            projection,
            indexers,
        }))
    }
}

fn index_key_types_match(
    table: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
    item: &WireItem,
) -> StorageResult<bool> {
    let fields = index
        .key_schema
        .iter()
        .map(|key| key.attribute_name.as_str())
        .collect::<Vec<_>>();
    let values = item.select_attribute_values(&fields)?;
    for (key, value) in index.key_schema.iter().zip(values) {
        let Some(value) = value else {
            continue;
        };
        let expected = table
            .attribute_definitions
            .iter()
            .find(|definition| definition.attribute_name == key.attribute_name)
            .ok_or_else(|| StorageError::internal("GSI key attribute has no table definition"))?;
        if !value.matches_key_type(&expected.attribute_type) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn project_ttl_item(
    table: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
    item: WireItem,
    indexers: IndexerDeclaration,
) -> StorageResult<Option<GsiProjectedRow>> {
    let ttl_attribute = index
        .key_schema
        .iter()
        .find(|element| element.key_type == KeyType::Range)
        .ok_or_else(|| StorageError::internal("TTL index range key is missing"))?;
    let Some(ttl_index_key) =
        storage_common::TtlIndexKey::for_wire_item(table, &ttl_attribute.attribute_name, &item)?
    else {
        return Ok(None);
    };
    Ok(Some(GsiProjectedRow {
        key: GsiProjectionKey {
            index_name: index.index_name.clone(),
            index_key: KeyAttributes::new(),
            table_item_key: KeyAttributes::new(),
            ttl_index_key: Some(ttl_index_key),
        },
        projection: storage_types::FrozenImageProjection::All,
        source_item: item,
        indexers,
    }))
}

fn key_attributes(parts: GsiKeyParts<'_>) -> KeyAttributes {
    parts
        .into_iter()
        .map(|part| (part.name.to_string(), part.value))
        .collect()
}

pub(super) fn plan_gsi_write_actions_for<'a>(
    table_info: &'a StoredTableInfo,
    gsis: &[&'a GlobalSecondaryIndex],
    old_item: Option<&WireItem>,
    new_item: Option<&WireItem>,
) -> StorageResult<Vec<GsiWriteAction<'a>>> {
    if gsis.is_empty() {
        return Ok(Vec::new());
    }
    let projection_fields = gsi_projection_fields(table_info, gsis);
    let old_attributes = old_item
        .map(|item| item.select_attribute_values(&projection_fields))
        .transpose()?;
    let new_attributes = new_item
        .map(|item| item.select_attribute_values(&projection_fields))
        .transpose()?;
    let mut actions = Vec::new();
    for gsi in gsis {
        let planned = plan_gsi_index_write_actions(
            table_info,
            gsi,
            &projection_fields,
            old_item.zip(old_attributes.as_deref()),
            new_item.zip(new_attributes.as_deref()),
        )?;
        if !planned.is_empty() && actions.is_empty() {
            actions.reserve(gsis.len().saturating_mul(2));
        }
        actions.extend(planned);
    }
    Ok(actions)
}

fn gsi_projection_fields<'a>(
    table_info: &'a StoredTableInfo,
    gsis: &[&'a GlobalSecondaryIndex],
) -> Vec<&'a str> {
    let mut fields = Vec::new();
    for gsi in gsis {
        for name in storage_common::gsi_projection_fields(
            Some(&gsi.projection),
            &table_info.key_schema,
            &gsi.key_schema,
        ) {
            push_unique(&mut fields, name);
        }
    }
    fields
}

fn push_unique<'a>(fields: &mut Vec<&'a str>, name: &'a str) {
    if !fields.contains(&name) {
        fields.push(name);
    }
}

pub(super) fn validate_gsi_key_values(
    table_info: &StoredTableInfo,
    item: &WireItem,
) -> StorageResult<()> {
    for index in table_info
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
    {
        if index.index_name.is_ttl_index() {
            continue;
        }
        let fields = index
            .key_schema
            .iter()
            .map(|key| key.attribute_name.as_str())
            .collect::<Vec<_>>();
        let values = item.select_attribute_values(&fields)?;
        for (key, value) in index.key_schema.iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            let expected = table_info
                .attribute_definitions
                .iter()
                .find(|definition| definition.attribute_name == key.attribute_name)
                .ok_or_else(|| {
                    StorageError::internal("GSI key attribute has no table definition")
                })?;
            if !value.matches_key_type(&expected.attribute_type) {
                return Err(StorageError::validation(format!(
                    "One or more parameter values were invalid: Type mismatch for Index Key {} \
                     Expected: {:?} Actual: {} IndexName: {}",
                    key.attribute_name,
                    expected.attribute_type,
                    value.variant_name(),
                    index.index_name,
                )));
            }
            storage_types::validate_key_attribute_value_for_schema(key, &value).map_err(
                |error| error.for_index_key_put(index.index_name.as_ref(), &key.attribute_name),
            )?;
        }
    }
    Ok(())
}

fn plan_gsi_index_write_actions<'a>(
    table_info: &'a StoredTableInfo,
    gsi: &'a GlobalSecondaryIndex,
    projection_fields: &[&str],
    old_item: Option<(&WireItem, &[Option<AttributeValue>])>,
    new_item: Option<(&WireItem, &[Option<AttributeValue>])>,
) -> StorageResult<GsiIndexWriteActions<'a>> {
    let mut old_table_key = key_parts(
        &table_info.key_schema,
        projection_fields,
        old_item.map(|(_, values)| values),
    );
    let new_table_key = key_parts(
        &table_info.key_schema,
        projection_fields,
        new_item.map(|(_, values)| values),
    );
    // Fail closed on corrupt primary keys: a present image without a
    // resolvable scalar table key cannot be keyed for deletes or puts, so
    // the page must be rejected instead of silently skipped. Absent GSI
    // keys legitimately yield no action below; absent table keys never do.
    if old_item.is_some() {
        require_resolvable_table_key(old_table_key.as_ref())?;
    }
    if new_item.is_some() {
        require_resolvable_table_key(new_table_key.as_ref())?;
    }
    let mut old_gsi_key = key_parts(
        &gsi.key_schema,
        projection_fields,
        old_item.map(|(_, values)| values),
    );
    let new_gsi_key = key_parts(
        &gsi.key_schema,
        projection_fields,
        new_item.map(|(_, values)| values),
    );
    let same_index_key = old_gsi_key == new_gsi_key;
    let mut actions = GsiIndexWriteActions::new();
    if !same_index_key
        && let (Some(gsi_key), Some(table_key)) = (old_gsi_key.take(), old_table_key.take())
    {
        actions.push(GsiWriteAction::Delete {
            index: gsi,
            gsi_key,
            table_key,
        });
    }
    let (Some((wire_item, values)), Some(gsi_key), Some(table_key)) =
        (new_item, new_gsi_key, new_table_key)
    else {
        return Ok(actions);
    };
    if projection_unchanged(
        table_info,
        gsi,
        projection_fields,
        old_item,
        (wire_item, values),
        same_index_key,
    )? {
        return Ok(actions);
    }
    actions.push(GsiWriteAction::Put {
        index: gsi,
        gsi_key,
        table_key,
        projection: freeze_projection(table_info, gsi)?,
    });
    Ok(actions)
}

fn projection_unchanged(
    table: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
    fields: &[&str],
    old_item: Option<(&WireItem, &[Option<AttributeValue>])>,
    new_item: (&WireItem, &[Option<AttributeValue>]),
    same_index_key: bool,
) -> StorageResult<bool> {
    let Some(old_item) = old_item else {
        return Ok(false);
    };
    if !same_index_key {
        return Ok(false);
    }
    Ok(!projected_item_changed(
        table, index, fields, old_item, new_item,
    )?)
}

fn projected_item_changed(
    table: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
    fields: &[&str],
    old_item: (&WireItem, &[Option<AttributeValue>]),
    new_item: (&WireItem, &[Option<AttributeValue>]),
) -> StorageResult<bool> {
    match index
        .projection
        .projection_type
        .as_ref()
        .unwrap_or(&ProjectionType::All)
    {
        ProjectionType::All => Ok(true),
        ProjectionType::KeysOnly => Ok(key_schema_attributes_changed(
            fields,
            old_item.1,
            new_item.1,
            table.key_schema.iter().chain(&index.key_schema),
        )),
        ProjectionType::Include => Ok(key_schema_attributes_changed(
            fields,
            old_item.1,
            new_item.1,
            table.key_schema.iter().chain(&index.key_schema),
        ) || index
            .projection
            .non_key_attributes
            .as_deref()
            .is_some_and(|attributes| {
                named_attributes_changed(fields, old_item.1, new_item.1, attributes)
            })),
    }
}

fn key_schema_attributes_changed<'a>(
    fields: &[&str],
    old_values: &[Option<AttributeValue>],
    new_values: &[Option<AttributeValue>],
    mut attributes: impl Iterator<Item = &'a KeySchemaElement>,
) -> bool {
    attributes.any(|attribute| {
        selected_attribute(fields, old_values, &attribute.attribute_name)
            != selected_attribute(fields, new_values, &attribute.attribute_name)
    })
}

fn named_attributes_changed(
    fields: &[&str],
    old_values: &[Option<AttributeValue>],
    new_values: &[Option<AttributeValue>],
    attributes: &[String],
) -> bool {
    attributes.iter().any(|attribute| {
        selected_attribute(fields, old_values, attribute)
            != selected_attribute(fields, new_values, attribute)
    })
}

fn selected_attribute<'a>(
    fields: &[&str],
    values: &'a [Option<AttributeValue>],
    name: &str,
) -> Option<&'a AttributeValue> {
    fields
        .iter()
        .position(|field| *field == name)
        .and_then(|index| values[index].as_ref())
}

fn freeze_projection(
    table: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
) -> StorageResult<storage_types::FrozenImageProjection> {
    if index
        .projection
        .projection_type
        .as_ref()
        .is_none_or(|kind| *kind == ProjectionType::All)
    {
        return Ok(storage_types::FrozenImageProjection::All);
    }
    let fields = storage_common::gsi_projection_fields(
        Some(&index.projection),
        &table.key_schema,
        &index.key_schema,
    )
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    Ok(storage_types::FrozenImageProjection::Fields(
        fields.try_into()?,
    ))
}

fn require_resolvable_table_key(table_key: Option<&GsiKeyParts<'_>>) -> StorageResult<()> {
    let Some(parts) = table_key else {
        return Err(StorageError::internal(
            "stored_item_corruption:gsi_source_primary_key",
        ));
    };
    for part in parts {
        if !matches!(
            part.value,
            AttributeValue::S(_) | AttributeValue::N(_) | AttributeValue::B(_)
        ) {
            return Err(StorageError::internal(
                "stored_item_corruption:gsi_source_primary_key",
            ));
        }
    }
    Ok(())
}

fn key_parts<'a>(
    key_schema: &'a [KeySchemaElement],
    fields: &[&str],
    values: Option<&[Option<AttributeValue>]>,
) -> Option<GsiKeyParts<'a>> {
    let values = values?;
    let mut parts = GsiKeyParts::new();
    for key in key_schema {
        parts.push(GsiKeyPart {
            name: &key.attribute_name,
            value: selected_attribute(fields, values, &key.attribute_name)?.clone(),
        });
    }
    Some(parts)
}
