//! Catalog metadata and its durable history identity from the same row.

use std::{ops::Deref, sync::Arc};

use crate::{StoredTableInfo, TableHistoryId, TableName, TimestampMillis};

/// Immutable-ID authority is independent of the reusable public name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableHistoryState {
    Active(Arc<TableHistorySchema>),
    Deleting,
}

/// The ID visibility observation supplies response keys without copying a
/// table name or key schema into every immutable pointer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableHistorySchema {
    pub source_table_name: crate::SystemStreamSourceTableName,
    pub key_schema: Vec<crate::KeySchemaElement>,
}

impl TryFrom<(TableName, Vec<crate::KeySchemaElement>)> for TableHistorySchema {
    type Error = crate::StorageError;

    fn try_from(
        (name, key_schema): (TableName, Vec<crate::KeySchemaElement>),
    ) -> Result<Self, Self::Error> {
        let source_table_name = crate::SystemStreamSourceTableName::try_from(&name)
            .map_err(|_| crate::StorageError::internal("Invalid history source table name"))?;
        Ok(Self {
            source_table_name,
            key_schema,
        })
    }
}

/// The public metadata Arc can be borrowed by shared request preparation
/// without copying its schemas. The enclosing cache entry retains the native
/// identity.
#[derive(Debug, Clone)]
pub struct BoundTableInfo {
    pub history_id: TableHistoryId,
    pub info: Arc<StoredTableInfo>,
}

impl Deref for BoundTableInfo {
    type Target = StoredTableInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

impl From<(StoredTableInfo, uuid::Uuid)> for BoundTableInfo {
    fn from((info, id): (StoredTableInfo, uuid::Uuid)) -> Self {
        Self {
            history_id: id.into(),
            info: Arc::new(info),
        }
    }
}

impl std::borrow::Borrow<StoredTableInfo> for BoundTableInfo {
    fn borrow(&self) -> &StoredTableInfo {
        &self.info
    }
}

impl From<BoundTableInfo> for StoredTableInfo {
    fn from(bound: BoundTableInfo) -> Self {
        Arc::unwrap_or_clone(bound.info)
    }
}

impl std::ops::DerefMut for BoundTableInfo {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.info)
    }
}

/// A timestamp is descriptive; only the immutable ID distinguishes
/// incarnations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableStreamIdentity {
    pub history_id: TableHistoryId,
    pub created_at: TimestampMillis,
}

impl BoundTableInfo {
    pub fn stream_identity(&self) -> TableStreamIdentity {
        TableStreamIdentity {
            history_id: self.history_id,
            created_at: self.info.created_at,
        }
    }
}

/// Pin a name-based request to one accepted immutable incarnation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "TableHistoryTargetInput")]
pub struct TableHistoryTarget {
    pub table_name: TableName,
    pub history_id: TableHistoryId,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TableHistoryTargetInput {
    table_name: TableName,
    history_id: TableHistoryId,
}

impl TryFrom<TableHistoryTargetInput> for TableHistoryTarget {
    type Error = crate::StorageError;

    fn try_from(input: TableHistoryTargetInput) -> Result<Self, Self::Error> {
        if input.table_name.as_ref().is_empty() || input.history_id.as_ref() == &[0; 16] {
            return Err(crate::StorageError::raw_validation(
                "Invalid table incarnation",
            ));
        }
        Ok(Self {
            table_name: input.table_name,
            history_id: input.history_id,
        })
    }
}

impl From<&BoundTableInfo> for TableHistoryTarget {
    fn from(table: &BoundTableInfo) -> Self {
        Self {
            table_name: table.info.table_name.clone(),
            history_id: table.history_id,
        }
    }
}

impl TryFrom<(TableName, &str)> for TableHistoryTarget {
    type Error = crate::StorageError;

    fn try_from((table_name, id): (TableName, &str)) -> Result<Self, Self::Error> {
        let invalid = || crate::StorageError::raw_validation("Invalid table incarnation");
        let id = uuid::Uuid::parse_str(id).map_err(|_| invalid())?;
        if table_name.as_ref().is_empty() || id.is_nil() {
            return Err(invalid());
        }
        Ok(Self {
            table_name,
            history_id: id.into(),
        })
    }
}

impl TryFrom<&crate::TableDescription> for TableHistoryTarget {
    type Error = crate::StorageError;

    fn try_from(description: &crate::TableDescription) -> Result<Self, Self::Error> {
        Self::try_from((
            description.table_name.clone(),
            description.table_id.as_str(),
        ))
    }
}

impl TryFrom<&str> for TableHistoryTarget {
    type Error = crate::StorageError;

    fn try_from(arn: &str) -> Result<Self, Self::Error> {
        let invalid = || {
            crate::StorageError::from(crate::StorageEnum::ResourceNotFound {
                resource_type: "stream",
                resource_id: String::new(),
            })
        };
        let (_, suffix) = arn.split_once(":table/").ok_or_else(invalid)?;
        let (table_name, label) = suffix.split_once("/stream/").ok_or_else(invalid)?;
        let (_, identity) = label.rsplit_once('-').ok_or_else(invalid)?;
        let id = uuid::Uuid::parse_str(identity).map_err(|_| invalid())?;
        if table_name.is_empty() || id.is_nil() {
            return Err(invalid());
        }
        Ok(Self {
            table_name: TableName::new(table_name),
            history_id: id.into(),
        })
    }
}
