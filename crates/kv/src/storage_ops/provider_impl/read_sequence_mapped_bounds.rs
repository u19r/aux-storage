use storage_types::{
    AttributeValue, GetItemRequest, IndexKeyPrefix, ItemKey, QueryRequest, StorageResult, TableKey,
};

use crate::keyspace::table_identity::StoredTableMetadata;

pub(super) struct MappedQueryRange {
    pub(super) begin: Vec<u8>,
    pub(super) end: Vec<u8>,
    pub(super) exclusive_start: Option<Vec<u8>>,
    pub(super) reverse: bool,
}

#[derive(Clone, Copy)]
enum Bound<'a> {
    Included(&'a AttributeValue),
    Excluded(&'a AttributeValue),
}

enum RangeConstraint<'a> {
    Bounds {
        lower: Option<Bound<'a>>,
        upper: Option<Bound<'a>>,
    },
    Prefix(&'a AttributeValue),
}

struct QueryConstraint<'a> {
    hash: &'a AttributeValue,
    range: RangeConstraint<'a>,
}

pub(super) fn mapped_query_bounds(
    metadata: &StoredTableMetadata,
    request: &QueryRequest,
) -> StorageResult<Option<MappedQueryRange>> {
    let Some(constraint) = query_constraint(request)? else {
        return Ok(None);
    };
    let (begin, end) = encode_bounds(metadata, request, constraint)?;
    let exclusive_start = exclusive_start(metadata, request, &begin, &end)?;
    Ok(Some(MappedQueryRange {
        begin,
        end,
        exclusive_start,
        reverse: request.scan_index_forward == Some(false),
    }))
}

pub(super) fn mapped_get_bounds(
    metadata: &StoredTableMetadata,
    request: &GetItemRequest,
) -> StorageResult<Option<MappedQueryRange>> {
    if request.key.iter().any(|(_, value)| {
        storage_types::read_sequence_input_marker_name(value).is_some()
            || storage_types::read_sequence_string_template_name(value).is_some()
            || storage_types::read_sequence_input_literal_name(value).is_some()
    }) {
        return Ok(None);
    }
    let item_key = ItemKey::from_key_schema(
        metadata.table_info.table_name.clone(),
        &metadata.table_info.key_schema,
        &request.key,
    )?;
    let begin = crate::keyspace::table_keys::item_key(&metadata.identity, &item_key)?;
    let end = crate::keyspace::table_keys::item_key_increment(&metadata.identity, &item_key)?;
    Ok(Some(MappedQueryRange {
        begin,
        end,
        exclusive_start: None,
        reverse: false,
    }))
}

fn query_constraint(request: &QueryRequest) -> StorageResult<Option<QueryConstraint<'_>>> {
    let expression = &request.key_condition_expression;
    let values = &request.expression_attribute_values;
    let constraint = if let Some((hash, range)) =
        crate::helpers::parse_hash_range_key_query(expression, values)
    {
        QueryConstraint::bounds(
            hash,
            Some(Bound::Included(range)),
            Some(Bound::Included(range)),
        )
    } else if let Some((hash, lower, upper)) =
        crate::helpers::parse_hash_between_query(expression, values)
    {
        QueryConstraint::bounds(
            hash,
            Some(Bound::Included(lower)),
            Some(Bound::Included(upper)),
        )
    } else if let Some((hash, lower_op, lower, upper_op, upper)) =
        crate::helpers::parse_hash_bounded_comparison_query(expression, values)
    {
        QueryConstraint::bounds(
            hash,
            Some(lower_bound(lower_op, lower)),
            Some(upper_bound(upper_op, upper)),
        )
    } else if let Some((hash, operator, value)) =
        crate::helpers::parse_hash_comparison_query(expression, values)
    {
        comparison_constraint(hash, operator, value)
    } else if let Some((hash, prefix)) =
        crate::helpers::parse_hash_begins_with_query(expression, values)?
    {
        QueryConstraint {
            hash,
            range: RangeConstraint::Prefix(prefix),
        }
    } else if let Some(hash) = crate::helpers::parse_hash_key_query(expression, values) {
        QueryConstraint::bounds(hash, None, None)
    } else {
        return Ok(None);
    };
    Ok(Some(constraint))
}

impl<'a> QueryConstraint<'a> {
    fn bounds(
        hash: &'a AttributeValue,
        lower: Option<Bound<'a>>,
        upper: Option<Bound<'a>>,
    ) -> Self {
        Self {
            hash,
            range: RangeConstraint::Bounds { lower, upper },
        }
    }
}

fn comparison_constraint<'a>(
    hash: &'a AttributeValue,
    operator: &str,
    value: &'a AttributeValue,
) -> QueryConstraint<'a> {
    match operator {
        "<" => QueryConstraint::bounds(hash, None, Some(Bound::Excluded(value))),
        "<=" => QueryConstraint::bounds(hash, None, Some(Bound::Included(value))),
        ">" => QueryConstraint::bounds(hash, Some(Bound::Excluded(value)), None),
        ">=" => QueryConstraint::bounds(hash, Some(Bound::Included(value)), None),
        _ => QueryConstraint::bounds(hash, None, None),
    }
}

fn lower_bound<'a>(operator: &str, value: &'a AttributeValue) -> Bound<'a> {
    if operator == ">" {
        Bound::Excluded(value)
    } else {
        Bound::Included(value)
    }
}

fn upper_bound<'a>(operator: &str, value: &'a AttributeValue) -> Bound<'a> {
    if operator == "<" {
        Bound::Excluded(value)
    } else {
        Bound::Included(value)
    }
}

fn encode_bounds(
    metadata: &StoredTableMetadata,
    request: &QueryRequest,
    constraint: QueryConstraint<'_>,
) -> StorageResult<(Vec<u8>, Vec<u8>)> {
    let hash = query_key(request, constraint.hash, None);
    let partition = || crate::keyspace::table_keys::item_key_prefix(&metadata.identity, &hash);
    let partition_end =
        || crate::keyspace::table_keys::item_key_prefix_end(&metadata.identity, &hash);
    match constraint.range {
        RangeConstraint::Prefix(value) => {
            let key = query_key(request, constraint.hash, Some(value));
            Ok((
                crate::keyspace::table_keys::item_key_prefix(&metadata.identity, &key)?,
                crate::keyspace::table_keys::item_key_prefix_end(&metadata.identity, &key)?,
            ))
        }
        RangeConstraint::Bounds { lower, upper } => Ok((
            encode_lower(metadata, request, constraint.hash, lower)?.unwrap_or(partition()?),
            encode_upper(metadata, request, constraint.hash, upper)?.unwrap_or(partition_end()?),
        )),
    }
}

fn encode_lower(
    metadata: &StoredTableMetadata,
    request: &QueryRequest,
    hash: &AttributeValue,
    bound: Option<Bound<'_>>,
) -> StorageResult<Option<Vec<u8>>> {
    let Some(bound) = bound else { return Ok(None) };
    let (value, included) = match bound {
        Bound::Included(value) => (value, true),
        Bound::Excluded(value) => (value, false),
    };
    let key = query_key(request, hash, Some(value));
    let bytes = if included {
        crate::keyspace::table_keys::item_key_prefix(&metadata.identity, &key)?
    } else {
        crate::keyspace::table_keys::item_key_increment(&metadata.identity, &key)?
    };
    Ok(Some(bytes))
}

fn encode_upper(
    metadata: &StoredTableMetadata,
    request: &QueryRequest,
    hash: &AttributeValue,
    bound: Option<Bound<'_>>,
) -> StorageResult<Option<Vec<u8>>> {
    let Some(bound) = bound else { return Ok(None) };
    let (value, included) = match bound {
        Bound::Included(value) => (value, true),
        Bound::Excluded(value) => (value, false),
    };
    let key = query_key(request, hash, Some(value));
    let bytes = if included {
        crate::keyspace::table_keys::item_key_increment(&metadata.identity, &key)?
    } else {
        crate::keyspace::table_keys::item_key_prefix(&metadata.identity, &key)?
    };
    Ok(Some(bytes))
}

fn query_key(
    request: &QueryRequest,
    hash: &AttributeValue,
    range: Option<&AttributeValue>,
) -> ItemKey {
    match request.index_name.as_ref() {
        Some(index) => ItemKey::IndexPrefix(IndexKeyPrefix::new(
            request.table_name.clone(),
            index.clone(),
            hash.clone(),
            range.cloned(),
        )),
        None => ItemKey::Table(TableKey::new(
            request.table_name.clone(),
            hash.clone(),
            range.cloned(),
        )),
    }
}

fn exclusive_start(
    metadata: &StoredTableMetadata,
    request: &QueryRequest,
    begin: &[u8],
    end: &[u8],
) -> StorageResult<Option<Vec<u8>>> {
    let Some(exclusive_start) = request.exclusive_start_key.as_ref() else {
        return Ok(None);
    };
    let token = exclusive_start.to_page_token(&metadata.table_info, request.index_name.as_ref())?;
    let Some(key) =
        ItemKey::item_key_from_next_page_token(&token, &metadata.table_info, &request.index_name)?
    else {
        return Ok(None);
    };
    let physical = crate::keyspace::table_keys::item_key(&metadata.identity, &key)?;
    Ok((physical.as_slice() >= begin && physical.as_slice() < end).then_some(physical))
}
