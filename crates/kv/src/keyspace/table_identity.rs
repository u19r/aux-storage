use serde::{Deserialize, Serialize};
use storage_types::{GlobalSecondaryIndex, IndexName, StoredTableInfo, TableName};

use crate::keyspace::compact::{IndexStorageId, TableStorageId};

pub(crate) const TABLE_ID_ALLOCATOR_KEY: &[u8] = b"aT";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredTableMetadata {
    pub(crate) identity: TableIdentity,
    pub(crate) table_info: StoredTableInfo,
}

impl StoredTableMetadata {
    pub(crate) fn active(identity: TableIdentity, table_info: StoredTableInfo) -> Self {
        Self {
            identity,
            table_info,
        }
    }

    pub(crate) fn tombstone(identity: TableIdentity, table_info: StoredTableInfo) -> Self {
        Self {
            identity: identity.mark_deleted(),
            table_info,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableIdentity {
    pub(crate) table_id: TableStorageId,
    pub(crate) table_name: TableName,
    pub(crate) indexes: Vec<IndexIdentity>,
    /// The resolved tenant namespace is part of every FoundationDB Tuple key.
    /// It is persisted with table metadata so key construction cannot silently
    /// drift when a provider is reconfigured.
    pub(crate) tenant_keyspace: Vec<u8>,
    pub(crate) deleted: bool,
}

impl TableIdentity {
    pub(crate) fn new(
        table_id: TableStorageId,
        table_name: TableName,
        indexes: Vec<IndexIdentity>,
    ) -> Self {
        Self {
            table_id,
            table_name,
            indexes,
            tenant_keyspace: Vec::new(),
            deleted: false,
        }
    }

    pub(crate) fn mark_deleted(mut self) -> Self {
        self.deleted = true;
        self
    }

    pub(crate) fn user_indexes_for_table(
        table_id: TableStorageId,
        table_name: &TableName,
        indexes: Option<&[GlobalSecondaryIndex]>,
    ) -> Self {
        let indexes = indexes
            .unwrap_or_default()
            .iter()
            .enumerate()
            .map(|(position, index)| {
                let index_id = u16::try_from(position + 1).unwrap_or(u16::MAX);
                IndexIdentity {
                    index_id: IndexStorageId::new(index_id),
                    index_name: index.index_name.clone(),
                    index_kind: IndexIdentityKind::UserGsi,
                }
            })
            .collect();

        Self::new(table_id, table_name.clone(), indexes)
    }

    pub(crate) fn user_indexes_for_table_with_tenant(
        table_id: TableStorageId,
        table_name: &TableName,
        indexes: Option<&[GlobalSecondaryIndex]>,
        tenant_keyspace: Vec<u8>,
    ) -> Self {
        let mut identity = Self::user_indexes_for_table(table_id, table_name, indexes);
        identity.tenant_keyspace = tenant_keyspace;
        identity
    }

    #[cfg(test)]
    pub(crate) fn next_user_index_id(&self) -> IndexStorageId {
        let next = self
            .indexes
            .iter()
            .map(|index| index.index_id.get())
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        IndexStorageId::new(next)
    }

    #[cfg(test)]
    pub(crate) fn index_id_for_name(&self, index_name: &IndexName) -> Option<IndexStorageId> {
        self.indexes
            .iter()
            .find(|index| &index.index_name == index_name)
            .map(|index| index.index_id)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexIdentity {
    pub(crate) index_id: IndexStorageId,
    pub(crate) index_name: IndexName,
    pub(crate) index_kind: IndexIdentityKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum IndexIdentityKind {
    UserGsi,
    HiddenTtl,
}
