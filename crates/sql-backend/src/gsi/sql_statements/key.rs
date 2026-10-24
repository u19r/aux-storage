//! One SQL binding for projection keys used by reads and writes.

use storage_backfill::GsiProjectionKey;
use storage_types::{
    AttributeName, AttributeValue, GlobalSecondaryIndex, KeyAttributes, KeySchemaElement,
    StorageError, StorageResult, StoredTableInfo,
};

use crate::{SqlEngine, SqlLayout, SqlValue, item_read::encode_key_value};

pub(in crate::gsi) struct BoundProjectionKey {
    pub(in crate::gsi) columns: Vec<String>,
    pub(in crate::gsi) binds: Vec<SqlValue>,
    pub(in crate::gsi) numeric_keys: Vec<usize>,
}

impl BoundProjectionKey {
    pub(in crate::gsi) fn try_new(
        engine: impl SqlLayout,
        table: &StoredTableInfo,
        index: &GlobalSecondaryIndex,
        key: &GsiProjectionKey,
    ) -> StorageResult<Self> {
        if key.index_name != index.index_name {
            return Err(StorageError::internal(
                "SQL GSI projection has another index",
            ));
        }
        let mut bound = Self {
            columns: Vec::new(),
            binds: Vec::new(),
            numeric_keys: Vec::new(),
        };
        let count = index
            .key_schema
            .len()
            .checked_add(table.key_schema.len())
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        bound
            .columns
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        bound
            .binds
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        bound
            .numeric_keys
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        bound.append_schema(engine, &index.key_schema, &key.index_key, "")?;
        bound.append_schema(engine, &table.key_schema, &key.table_item_key, "table_")?;
        Ok(bound)
    }

    pub(super) fn append_request_row(
        self,
        engine: impl SqlLayout,
        ordinal: usize,
        binds: &mut Vec<SqlValue>,
        text: &mut crate::statement::SqlText,
    ) -> StorageResult<()> {
        let ordinal = i64::try_from(ordinal)
            .map_err(|_| StorageError::internal("GSI state ordinal exceeds SQL range"))?;
        binds.push(SqlValue::Integer(ordinal));
        text.append(format_args!("(CAST("))?;
        text.append_parameter(engine.engine(), binds.len(), false)?;
        text.append(format_args!(" AS BIGINT)"))?;
        for (position, value) in self.binds.into_iter().enumerate() {
            let is_binary =
                engine.engine() == SqlEngine::Postgres && matches!(value, SqlValue::Blob(_));
            binds.push(value);
            text.append(format_args!(", "))?;
            if is_binary {
                text.append(format_args!("CAST("))?;
            }
            text.append_parameter(
                engine.engine(),
                binds.len(),
                self.numeric_keys.contains(&(position + 1)),
            )?;
            if is_binary {
                text.append(format_args!(" AS BYTEA)"))?;
            }
        }
        text.append(format_args!(")"))
    }

    fn append_schema(
        &mut self,
        engine: impl SqlLayout,
        schema: &[KeySchemaElement],
        key: &KeyAttributes,
        prefix: &str,
    ) -> StorageResult<()> {
        for element in KeySchemaElement::ordered(schema) {
            let value = key
                .get(&element.attribute_name)
                .ok_or_else(|| StorageError::internal("GSI projection is missing a key"))?;
            self.columns
                .push(crate::statement::SqlText::render(format_args!(
                    "{prefix}{}",
                    AttributeName::new(&element.attribute_name).sanitized()
                ))?);
            if engine.engine() == SqlEngine::Postgres && matches!(value, AttributeValue::N(_)) {
                self.numeric_keys.push(self.binds.len() + 1);
            }
            self.binds.push(encode_key_value(engine, value)?);
        }
        Ok(())
    }
}
