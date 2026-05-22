use storage_types::{IndexName, StorageError, TableNamespace};

#[derive(Debug, Clone, PartialEq)]
pub struct MigrationIndexKeyCodec {
    index_name: IndexName,
    partition_key_attribute: String,
    sort_key_attribute: String,
}

impl MigrationIndexKeyCodec {
    #[must_use]
    pub fn new(index_name: IndexName) -> Self {
        let partition_key_attribute = format!("{}pk", index_name.as_ref());
        let sort_key_attribute = format!("{}sk", index_name.as_ref());
        Self {
            index_name,
            partition_key_attribute,
            sort_key_attribute,
        }
    }

    #[must_use]
    pub fn index_name(&self) -> &IndexName {
        &self.index_name
    }

    #[must_use]
    pub fn partition_key_attribute(&self) -> &str {
        &self.partition_key_attribute
    }

    #[must_use]
    pub fn sort_key_attribute(&self) -> &str {
        &self.sort_key_attribute
    }

    #[must_use]
    pub fn key_condition_expression(&self) -> String {
        format!("{} = :pk", self.partition_key_attribute)
    }

    #[must_use]
    pub fn partition_key(&self, namespace: &TableNamespace, entity_type: &str) -> String {
        format!("{}#{entity_type}", namespace.storage_key())
    }

    #[must_use]
    pub fn encode_sort_key(&self, pk: &str, sk: &str) -> String {
        format!("{}|{}{}", pk.len(), pk, sk)
    }

    pub fn parse_sort_key(&self, raw: &str) -> Result<(String, String), StorageError> {
        parse_migration_index_sort_key(raw, self.sort_key_attribute())
    }
}

#[must_use]
pub fn migration_index_pk(namespace: &TableNamespace, entity_type: &str) -> String {
    format!("{}#{entity_type}", namespace.storage_key())
}

#[must_use]
pub fn migration_index_sk(pk: &str, sk: &str) -> String {
    format!("{}|{}{}", pk.len(), pk, sk)
}

pub fn parse_migration_index_sort_key(
    raw: &str,
    attribute_name: &str,
) -> Result<(String, String), StorageError> {
    let Some((pk_len_str, joined)) = raw.split_once('|') else {
        return Err(StorageError::validation(format!(
            "invalid {attribute_name}: missing length separator"
        )));
    };
    let pk_len = pk_len_str.parse::<usize>().map_err(|_| {
        StorageError::validation(format!("invalid {attribute_name}: length is not numeric"))
    })?;
    if joined.len() < pk_len {
        return Err(StorageError::validation(format!(
            "invalid {attribute_name}: encoded pk length exceeds payload"
        )));
    }
    let (pk, sk) = joined.split_at(pk_len);
    Ok((pk.to_string(), sk.to_string()))
}
