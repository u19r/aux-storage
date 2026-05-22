use storage_cache::{
    decode_query_start_key_sort_repr, hash_key_name, parse_partition_key_condition,
    parse_runtime_sort_condition, primary_key_from_schema, query_sort_clause,
    query_space_key_schema, range_key_name,
    runtime_query_proof::{RuntimeParsedQueryShape, RuntimeQueryBounds, RuntimeQueryDirection},
    sort_key_order_repr_for_schema_value, stable_query_space_schema_fingerprint,
};
use storage_types::{
    AttributeValue, QueryTableRequest, StorageError, StorageResult, StoredTableInfo, TableName,
    WireItem,
};

use crate::{
    dynamo_json::{canonical_dynamo_json, canonical_dynamo_map_json},
    query_proof_types::{QueryCoverageRange, QueryManifestEntry, QueryManifestKey},
};

type QueryPageWitness = storage_cache::RuntimePageWitness<String>;

#[derive(Debug, Clone)]
pub(crate) struct DerivedQueryManifestEntry {
    pub(crate) manifest_key: QueryManifestKey,
    pub(crate) schema_fingerprint: u64,
    pub(crate) entry: QueryManifestEntry,
}

impl DerivedQueryManifestEntry {
    fn from_item_map(
        table_name: &TableName,
        table_info: &StoredTableInfo,
        index_name: Option<&str>,
        item: &std::collections::HashMap<String, AttributeValue>,
    ) -> StorageResult<Self> {
        let key_schema = query_space_key_schema(table_info, index_name)?;
        let hash_key_name = hash_key_name(key_schema)?;
        let hash_value = item.get(hash_key_name).ok_or_else(|| {
            StorageError::internal(&format!(
                "missing hash key '{hash_key_name}' while deriving query manifest state"
            ))
        })?;
        let query_space_key = primary_key_from_schema(key_schema, item)?;
        let primary_key = primary_key_from_schema(&table_info.key_schema, item)?;
        let primary_key_json = canonical_key_json(&primary_key)?;
        let sort_key_order_repr = range_key_name(key_schema)
            .map(|name| sort_key_order_repr_for_schema_value(table_info, name, item))
            .transpose()?
            .flatten();

        Ok(Self {
            manifest_key: QueryManifestKey {
                table_name: table_name.clone(),
                index_name: index_name.map(str::to_string),
                partition_key_json: canonical_attribute_value_json(hash_value)?,
            },
            schema_fingerprint: stable_query_space_schema_fingerprint(table_info, index_name)?,
            entry: QueryManifestEntry {
                primary_key: primary_key.into(),
                query_space_key: query_space_key.into(),
                primary_key_json,
                sort_key_order_repr,
            },
        })
    }

    fn from_item(
        table_name: &TableName,
        table_info: &StoredTableInfo,
        index_name: Option<&str>,
        item: &WireItem,
    ) -> StorageResult<Self> {
        let item = item.to_attribute_map()?;
        Self::from_item_map(table_name, table_info, index_name, &item)
    }

    pub(crate) fn for_put(
        table_name: &TableName,
        table_info: &StoredTableInfo,
        index_name: Option<&str>,
        item: &WireItem,
    ) -> StorageResult<Self> {
        Self::from_item(table_name, table_info, index_name, item)
    }

    pub(crate) fn for_sparse_item(
        table_name: &TableName,
        table_info: &StoredTableInfo,
        index_name: Option<&str>,
        item: Option<&WireItem>,
    ) -> StorageResult<Option<Self>> {
        let Some(item) = item else {
            return Ok(None);
        };
        let item = item.to_attribute_map()?;
        let key_schema = query_space_key_schema(table_info, index_name)?;
        if key_schema
            .iter()
            .any(|element| !item.contains_key(&element.attribute_name))
        {
            return Ok(None);
        }
        Self::from_item_map(table_name, table_info, index_name, &item).map(Some)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DerivedQueryPage {
    pub(crate) manifest_key: QueryManifestKey,
    pub(crate) schema_fingerprint: u64,
    pub(crate) entries: Vec<QueryManifestEntry>,
    pub(crate) coverage_range: Option<QueryCoverageRange>,
    pub(crate) page_witness: Option<QueryPageWitness>,
}

impl DerivedQueryPage {
    pub(crate) fn for_query_page(
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
        items: &[WireItem],
        has_more: bool,
    ) -> StorageResult<Option<Self>> {
        let index_name = request.index_name.as_ref().map(|index| index.as_ref());
        let schema_fingerprint = stable_query_space_schema_fingerprint(table_info, index_name)?;
        let parsed_request = ParsedQueryRequest::from_request(table_name, table_info, request)?;

        let mut entries = Vec::with_capacity(items.len());
        let mut manifest_key = parsed_request
            .as_ref()
            .map(|parsed| parsed.manifest_key.clone());

        for item in items {
            let item_map = item.to_attribute_map()?;
            let derived = DerivedQueryManifestEntry::from_item_map(
                table_name, table_info, index_name, &item_map,
            )?;
            match manifest_key.as_ref() {
                Some(expected) if expected != &derived.manifest_key => {
                    return Err(StorageError::internal(
                        "query page crossed cached query spaces while recording query proof",
                    ));
                }
                Some(_) => {}
                None => {
                    manifest_key = Some(derived.manifest_key.clone());
                }
            }
            entries.push(derived.entry);
        }

        let Some(manifest_key) = manifest_key else {
            return Ok(None);
        };

        let coverage_range = match parsed_request {
            Some(ref parsed) => parsed.coverage_range_for_page(table_info, items, has_more)?,
            None => None,
        };

        Ok(Some(Self {
            manifest_key,
            schema_fingerprint,
            entries,
            coverage_range,
            page_witness: parsed_request.as_ref().and_then(|parsed| {
                storage_cache::runtime_page_witness(
                    parsed.runtime_bounds(),
                    parsed.shape.limit_option(),
                    items.len(),
                    has_more,
                )
            }),
        }))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedQueryRequest {
    pub(crate) manifest_key: QueryManifestKey,
    pub(crate) schema_fingerprint: u64,
    pub(crate) shape: RuntimeParsedQueryShape<String>,
}

impl ParsedQueryRequest {
    pub(crate) fn from_request(
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
    ) -> StorageResult<Option<Self>> {
        let index_name = request.index_name.as_ref().map(|index| index.as_ref());
        let query_key_schema = query_space_key_schema(table_info, index_name)?;
        let hash_key = hash_key_name(query_key_schema)?;
        let expression_values = request
            .expression_attribute_values
            .as_ref()
            .ok_or_else(|| {
                StorageError::validation(
                    "query proof cache requires expression values to derive partition equality",
                )
            })?;
        let (partition_field, partition_placeholder) = parse_partition_key_condition(
            &request.key_condition_expression,
            request.expression_attribute_names.as_ref(),
        )?;
        if partition_field != hash_key {
            return Ok(None);
        }

        let Some(partition_value) = expression_values.get(&partition_placeholder) else {
            return Err(StorageError::validation(format!(
                "query proof cache could not resolve partition value '{partition_placeholder}'",
            )));
        };
        let partition_key_json = canonical_attribute_value_json(partition_value)?;
        let sort_key_name = range_key_name(query_key_schema).map(str::to_string);
        let sort_condition = parse_runtime_sort_condition(
            query_sort_clause(&request.key_condition_expression),
            request.expression_attribute_names.as_ref(),
            expression_values,
            sort_key_name.as_deref(),
            canonical_attribute_value_json,
        )?;
        let start_exclusive_sort_key = decode_query_start_key_sort_repr(
            table_info,
            index_name,
            request.exclusive_start_key.as_deref(),
        )?;
        let sort_condition = sort_condition.as_ref();

        Ok(Some(Self {
            manifest_key: QueryManifestKey {
                table_name: table_name.clone(),
                index_name: request.index_name.as_ref().map(ToString::to_string),
                partition_key_json,
            },
            schema_fingerprint: stable_query_space_schema_fingerprint(table_info, index_name)?,
            shape: storage_cache::prepare_runtime_query_shape(
                sort_key_name.is_some(),
                start_exclusive_sort_key,
                sort_condition,
                if request.scan_index_forward.unwrap_or(true) {
                    RuntimeQueryDirection::Forward
                } else {
                    RuntimeQueryDirection::Reverse
                },
                request.limit.map(|limit| limit as usize),
            ),
        }))
    }

    pub(crate) fn coverage_range_for_page(
        &self,
        table_info: &StoredTableInfo,
        items: &[WireItem],
        has_more: bool,
    ) -> StorageResult<Option<QueryCoverageRange>> {
        let page_sort_keys = items
            .iter()
            .map(|item| item.to_attribute_map())
            .collect::<StorageResult<Vec<_>>>()?
            .iter()
            .map(|item| {
                DerivedQueryManifestEntry::for_put(
                    &self.manifest_key.table_name,
                    table_info,
                    self.manifest_key.index_name.as_deref(),
                    &WireItem::from_attribute_map(item)?,
                )
                .map(|derived| derived.entry.sort_key_order_repr)
            })
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(storage_cache::derive_runtime_page_coverage_range(
            &self.shape,
            &page_sort_keys,
            has_more,
        ))
    }

    pub(crate) fn limit(&self) -> usize {
        self.shape.limit()
    }

    pub(crate) fn runtime_bounds(&self) -> RuntimeQueryBounds<&str> {
        self.shape.runtime_bounds()
    }
}

fn canonical_key_json(
    key: &std::collections::HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    canonical_dynamo_map_json(key)
        .map_err(|err| StorageError::internal(&format!("encode query manifest key: {err}")))
}

fn canonical_attribute_value_json(value: &AttributeValue) -> StorageResult<String> {
    canonical_dynamo_json(value)
        .map_err(|err| StorageError::internal(&format!("encode query partition key: {err}")))
}
