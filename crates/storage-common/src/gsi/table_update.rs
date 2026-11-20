use storage_types::{
    CreateGlobalSecondaryIndex, GlobalSecondaryIndexUpdate, IndexName, KeyType, StorageError,
    StorageResult, StoredTableInfo, TableStatus, UpdateTableRequest,
};

use super::{GsiLifecycleAction, GsiTableUpdatePlan};

const MAX_GSI_COUNT: usize = 20;

impl GsiTableUpdatePlan {
    pub fn try_new(table: &StoredTableInfo, request: &UpdateTableRequest) -> StorageResult<Self> {
        let Some(updates) = request.global_secondary_index_updates.as_ref() else {
            return Ok(Self {
                next_table: table.clone(),
                lifecycle: None,
                attribute_definitions_changed: false,
            });
        };
        if updates.is_empty() {
            return Err(StorageError::validation(
                "GlobalSecondaryIndexUpdates cannot be empty",
            ));
        }
        if table.table_status != TableStatus::Active {
            return Err(StorageError::validation("table must be ACTIVE to update"));
        }
        let mut next_table = table.clone();
        let mut attribute_definitions_changed =
            Self::merge_attribute_definitions(&mut next_table, request)?;
        Self::validate_updates(&next_table, updates)?;
        let lifecycle = Self::lifecycle(updates);
        attribute_definitions_changed |= Self::apply_metadata(&mut next_table, lifecycle.as_ref())?;
        Ok(Self {
            next_table,
            lifecycle,
            attribute_definitions_changed,
        })
    }

    fn lifecycle(updates: &[GlobalSecondaryIndexUpdate]) -> Option<GsiLifecycleAction> {
        updates.iter().find_map(|update| {
            update
                .create
                .clone()
                .map(GsiLifecycleAction::Create)
                .or_else(|| update.delete.clone().map(GsiLifecycleAction::Delete))
        })
    }

    fn apply_metadata(
        table: &mut StoredTableInfo,
        lifecycle: Option<&GsiLifecycleAction>,
    ) -> StorageResult<bool> {
        match lifecycle {
            Some(GsiLifecycleAction::Create(create)) => table
                .global_secondary_indexes
                .get_or_insert_with(Vec::new)
                .push(create.clone().into()),
            Some(GsiLifecycleAction::Delete(delete)) => {
                let indexes = table
                    .global_secondary_indexes
                    .as_mut()
                    .ok_or_else(|| StorageError::validation("GSI does not exist"))?;
                indexes.retain(|index| index.index_name != delete.index_name);
                if indexes.is_empty() {
                    table.global_secondary_indexes = None;
                }
                // AWS keeps the deleted index's AttributeDefinitions (ddbpar5b
                // gsi_delete_deleting); aux has no later completion step to
                // prune them.
            }
            None => {}
        }
        Ok(false)
    }

    fn merge_attribute_definitions(
        table: &mut StoredTableInfo,
        request: &UpdateTableRequest,
    ) -> StorageResult<bool> {
        let Some(supplied) = request.attribute_definitions.as_ref() else {
            return Ok(false);
        };
        for definition in supplied {
            let conflicts = table.attribute_definitions.iter().any(|existing| {
                existing.attribute_name == definition.attribute_name
                    && std::mem::discriminant(&existing.attribute_type)
                        != std::mem::discriminant(&definition.attribute_type)
            });
            if conflicts {
                return Err(StorageError::validation(format!(
                    "attribute definition '{}' conflicts with the table",
                    definition.attribute_name
                )));
            }
        }
        let mut changed = false;
        for create in request
            .global_secondary_index_updates
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter_map(|update| update.create.as_ref())
        {
            changed |= Self::merge_create_attributes(table, supplied, create);
        }
        Ok(changed)
    }

    fn merge_create_attributes(
        table: &mut StoredTableInfo,
        supplied: &[storage_types::AttributeDefinition],
        create: &CreateGlobalSecondaryIndex,
    ) -> bool {
        let mut changed = false;
        for key in &create.key_schema {
            if table
                .attribute_definitions
                .iter()
                .any(|definition| definition.attribute_name == key.attribute_name)
            {
                continue;
            }
            if let Some(definition) = supplied
                .iter()
                .find(|definition| definition.attribute_name == key.attribute_name)
            {
                table.attribute_definitions.push(definition.clone());
                changed = true;
            }
        }
        changed
    }

    fn validate_updates(
        table: &StoredTableInfo,
        updates: &[GlobalSecondaryIndexUpdate],
    ) -> StorageResult<()> {
        for (position, update) in updates.iter().enumerate() {
            let name = Self::update_index_name(update);
            if let Some(name) = name
                && updates[..position]
                    .iter()
                    .any(|earlier| Self::update_index_name(earlier) == Some(name))
            {
                return Err(StorageError::validation(format!(
                    "One or more parameter values were invalid: Only one global secondary index \
                     update per index is allowed simultaneously. Index: {name}"
                )));
            }
        }
        let lifecycle_count = updates
            .iter()
            .filter(|update| update.create.is_some() || update.delete.is_some())
            .count();
        if lifecycle_count > 1 {
            return Err(StorageError::from(
                storage_types::StorageEnum::LimitExceeded {
                    message: storage_types::ONE_ONLINE_INDEX_MESSAGE.to_owned(),
                },
            ));
        }
        for update in updates {
            Self::validate_update(table, update)?;
        }
        Ok(())
    }

    fn update_index_name(update: &GlobalSecondaryIndexUpdate) -> Option<&IndexName> {
        update
            .create
            .as_ref()
            .map(|create| &create.index_name)
            .or_else(|| update.update.as_ref().map(|update| &update.index_name))
            .or_else(|| update.delete.as_ref().map(|delete| &delete.index_name))
    }

    fn validate_update(
        table: &StoredTableInfo,
        update: &GlobalSecondaryIndexUpdate,
    ) -> StorageResult<()> {
        match (&update.create, &update.update, &update.delete) {
            (Some(create), None, None) => Self::validate_create(table, create),
            (None, Some(update), None) => Self::require_existing(table, &update.index_name),
            (None, None, Some(delete)) => Self::require_existing(table, &delete.index_name),
            _ => Err(StorageError::validation(
                "GSI update must contain exactly one operation",
            )),
        }
    }

    fn validate_create(
        table: &StoredTableInfo,
        create: &CreateGlobalSecondaryIndex,
    ) -> StorageResult<()> {
        if create.index_name.is_ttl_index() {
            return Err(StorageError::validation("IndexName is reserved"));
        }
        storage_types::validate_index_name(&create.index_name, "IndexName")
            .map_err(StorageError::validation)?;
        if Self::contains(table, &create.index_name) {
            return Err(StorageError::validation(
                "Attempting to create an index which already exists",
            ));
        }
        if table
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|index| !index.index_name.is_ttl_index())
            .count()
            >= MAX_GSI_COUNT
        {
            return Err(StorageError::validation(
                "too many global secondary indexes",
            ));
        }
        Self::validate_key_schema(table, create)
    }

    fn validate_key_schema(
        table: &StoredTableInfo,
        create: &CreateGlobalSecondaryIndex,
    ) -> StorageResult<()> {
        if !(1..=2).contains(&create.key_schema.len()) {
            return Err(StorageError::validation(
                "GSI key schema must contain one or two elements",
            ));
        }
        let mut has_hash = false;
        for (position, key) in create.key_schema.iter().enumerate() {
            if create.key_schema[..position]
                .iter()
                .any(|earlier| earlier.attribute_name == key.attribute_name)
            {
                return Err(StorageError::validation(
                    "GSI key schema has duplicate attributes",
                ));
            }
            if !table
                .attribute_definitions
                .iter()
                .any(|definition| definition.attribute_name == key.attribute_name)
            {
                return Err(StorageError::validation(
                    storage_types::undefined_index_keys_message(
                        &create.key_schema,
                        &table.attribute_definitions,
                    ),
                ));
            }
            match key.key_type {
                KeyType::Hash if has_hash => {
                    return Err(StorageError::validation("GSI key schema has two HASH keys"));
                }
                KeyType::Hash => has_hash = true,
                KeyType::Range => {}
            }
        }
        if !has_hash {
            return Err(StorageError::validation(
                "GSI key schema requires a HASH key",
            ));
        }
        Ok(())
    }

    fn require_existing(table: &StoredTableInfo, index_name: &IndexName) -> StorageResult<()> {
        if Self::contains(table, index_name) {
            return Ok(());
        }
        Err(StorageError::index_not_found(index_name, &table.table_name))
    }

    fn contains(table: &StoredTableInfo, index_name: &IndexName) -> bool {
        table
            .global_secondary_indexes
            .as_ref()
            .is_some_and(|indexes| {
                indexes.iter().any(|index| {
                    !index.index_name.is_ttl_index() && &index.index_name == index_name
                })
            })
    }
}
