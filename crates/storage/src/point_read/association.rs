use std::collections::HashMap;

use super::{key_fingerprint, source::SourceKey};
use crate::types::{KeyAttributes, StorageError, StorageResult, TableName, WireItem};

/// A table has one primary-key shape. Decode those exact fields once per image,
/// then use the same native canonical identity used at request admission.
pub(super) struct ResponseSources<'a> {
    fields: HashMap<&'a TableName, Vec<&'a str>>,
    sources: HashMap<&'a str, &'a SourceKey>,
}

impl<'a> ResponseSources<'a> {
    pub(super) fn try_new(sources: &[&'a SourceKey]) -> StorageResult<Self> {
        let mut fields = HashMap::new();
        let mut indexed = HashMap::new();
        for source in sources {
            let request = &source.request;
            let mut names: Vec<_> = request.key.iter().map(|(name, _)| name).collect();
            names.sort_unstable();
            if let Some(existing) = fields.get(&request.table_name)
                && *existing != names
            {
                return Err(StorageError::invalid_or_missing_key());
            }
            fields.entry(&request.table_name).or_insert(names);
            indexed.insert(request.fingerprint.as_str(), *source);
        }
        Ok(Self {
            fields,
            sources: indexed,
        })
    }

    pub(super) fn find_source(
        &self,
        table: &TableName,
        item: &WireItem,
    ) -> StorageResult<Option<&'a SourceKey>> {
        let Some(fields) = self.fields.get(table) else {
            return Ok(None);
        };
        let values = item.select_attribute_values(fields)?;
        let mut key = KeyAttributes::with_capacity(fields.len());
        for (name, value) in fields.iter().zip(values) {
            let Some(value) = value else {
                return Ok(None);
            };
            key.insert(*name, value);
        }
        let fingerprint = key_fingerprint(table, &key)?;
        Ok(self.sources.get(fingerprint.as_str()).copied())
    }
}
