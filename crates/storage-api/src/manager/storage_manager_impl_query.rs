use std::collections::HashMap;

use http_error::HttpApiError;
use storage::{QueryIndexInput, QueryTableInput};
use storage_types::{
    AttributeValue, IndexName, QueryRequest, QueryResponse, StoredTableInfo, TableName, WireItem,
    subset_expression_attribute_names_for_expression,
    subset_expression_attribute_values_for_expression, validate_expression_attribute_usage,
};

use crate::{
    manager::{
        StorageApiManagerImpl,
        storage_manager_impl_consumed_capacity::calculate_consumed_capacity_from_inputs,
        storage_manager_impl_read_pagination::{
            page_token_to_key_attributes, paginate_items_by_response_bytes,
            paginate_wire_items_by_response_bytes, resolve_exclusive_start_key,
        },
    },
    query_wire_response::QueryWireResponse,
    types::Response,
};

/// Handles a `DynamoDB` Query operation
///
/// # Errors
/// Returns `HttpApiError` if:
/// - The table does not exist
/// - The query operation fails
/// - Filter or projection expressions are invalid
impl StorageApiManagerImpl {
    pub(super) async fn query_internal(
        &self,
        request: QueryRequest,
    ) -> Result<Response, HttpApiError> {
        let mut query_expressions = vec![request.key_condition_expression.as_str()];
        if let Some(filter_expr) = request.filter_expression.as_deref() {
            query_expressions.push(filter_expr);
        }
        if let Some(projection_expr) = request.projection_expression.as_deref() {
            query_expressions.push(projection_expr);
        }
        validate_expression_attribute_usage(
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
            query_expressions,
        )
        .map_err(HttpApiError::from)?;

        let key_expression_attribute_names = subset_expression_attribute_names_for_expression(
            &request.key_condition_expression,
            request.expression_attribute_names.as_ref(),
        );
        let key_expression_attribute_values = subset_expression_attribute_values_for_expression(
            &request.key_condition_expression,
            request.expression_attribute_values.as_ref(),
        );

        let table_info = self.db().get_table_info(&request.table_name).await?;
        let exclusive_start_key = resolve_exclusive_start_key(
            request.exclusive_start_key.as_ref(),
            &table_info,
            request.index_name.as_ref(),
            "The provided starting key is invalid",
        )
        .map_err(HttpApiError::from)?;
        let prepared_query = PreparedQuery::from_request(
            &request,
            exclusive_start_key,
            key_expression_attribute_names,
            key_expression_attribute_values,
        );

        if query_wire_fast_path_enabled(&request) {
            return self
                .query_wire_internal(request, table_info, prepared_query)
                .await;
        }

        let (items, last_evaluated_key) = self.query_map_items(&prepared_query).await?;

        // Apply filtering if FilterExpression is provided
        #[expect(clippy::cast_possible_truncation)]
        let (filtered_items, count, scanned_count) =
            if let Some(filter_expr) = &request.filter_expression {
                let filtered = apply_filter_expression_refs(
                    &items,
                    filter_expr,
                    request.expression_attribute_names.as_ref(),
                    request.expression_attribute_values.as_ref(),
                )?;
                let filtered_len = filtered.len() as u32;
                (filtered, filtered_len, items.len() as u32)
            } else {
                let count = items.len() as u32;
                (items.iter().collect::<Vec<_>>(), count, count)
            };

        // Handle COUNT-only response
        let (response_items, count, last_evaluated_key) =
            if request.select.as_deref() == Some("COUNT") {
                (None, count, last_evaluated_key)
            } else {
                let final_items = if let Some(projection_expr) = &request.projection_expression {
                    apply_projection_expression_refs(
                        &filtered_items,
                        projection_expr,
                        request.expression_attribute_names.as_ref(),
                    )
                } else {
                    let mut out = Vec::with_capacity(filtered_items.len());
                    for item in &filtered_items {
                        out.push((*item).clone());
                    }
                    out
                };
                let (page_items, page_lek) = paginate_items_by_response_bytes(
                    &request.table_name,
                    &table_info,
                    request.index_name.as_ref(),
                    &filtered_items,
                    final_items,
                    last_evaluated_key,
                )
                .map_err(HttpApiError::from)?;
                #[expect(clippy::cast_possible_truncation)]
                let page_count = page_items.len() as u32;
                (Some(page_items), page_count, page_lek)
            };
        let last_evaluated_key = page_token_to_key_attributes(
            last_evaluated_key.as_deref(),
            &table_info,
            request.index_name.as_ref(),
        )
        .map_err(HttpApiError::from)?;

        let response = QueryResponse {
            items: response_items,
            count,
            scanned_count,
            last_evaluated_key,
            consumed_capacity: calculate_consumed_capacity_from_inputs(
                request.return_consumed_capacity.as_deref(),
                &request.table_name,
                request.index_name.as_ref(),
                scanned_count,
            ),
        };

        Ok(Response::Query(response))
    }

    async fn query_wire_internal(
        &self,
        request: QueryRequest,
        table_info: StoredTableInfo,
        prepared_query: PreparedQuery,
    ) -> Result<Response, HttpApiError> {
        let (items, last_evaluated_key) = self.query_wire_items(&prepared_query).await?;

        #[expect(clippy::cast_possible_truncation)]
        let scanned_count = items.len() as u32;
        let (page_items, last_evaluated_key) = paginate_wire_items_by_response_bytes(
            &request.table_name,
            &table_info,
            request.index_name.as_ref(),
            items,
            last_evaluated_key,
        )
        .map_err(HttpApiError::from)?;
        #[expect(clippy::cast_possible_truncation)]
        let count = page_items.len() as u32;
        let last_evaluated_key = page_token_to_key_attributes(
            last_evaluated_key.as_deref(),
            &table_info,
            request.index_name.as_ref(),
        )
        .map_err(HttpApiError::from)?;

        Ok(Response::QueryWire(QueryWireResponse {
            items: Some(page_items),
            count,
            scanned_count,
            last_evaluated_key,
            consumed_capacity: calculate_consumed_capacity_from_inputs(
                request.return_consumed_capacity.as_deref(),
                &request.table_name,
                request.index_name.as_ref(),
                scanned_count,
            ),
        }))
    }

    async fn query_map_items(
        &self,
        query: &PreparedQuery,
    ) -> Result<(Vec<HashMap<String, AttributeValue>>, Option<String>), HttpApiError> {
        if let Some(index_name) = query.index_name.clone() {
            return Ok(self
                .db()
                .query_index_map(query.index_input(index_name)?)
                .await?);
        }

        self.ensure_sync_read_barrier(query.consistent_read).await?;
        Ok(self.db().query_table_map(query.table_input()).await?)
    }

    async fn query_wire_items(
        &self,
        query: &PreparedQuery,
    ) -> Result<(Vec<WireItem>, Option<String>), HttpApiError> {
        if let Some(index_name) = query.index_name.clone() {
            return Ok(self
                .db()
                .query_index(query.index_input(index_name)?)
                .await?);
        }

        self.ensure_sync_read_barrier(query.consistent_read).await?;
        Ok(self.db().query_table(query.table_input()).await?)
    }
}

fn query_wire_fast_path_enabled(request: &QueryRequest) -> bool {
    request.filter_expression.is_none()
        && request.projection_expression.is_none()
        && request.select.as_deref() != Some("COUNT")
}

struct PreparedQuery {
    table_name: TableName,
    index_name: Option<IndexName>,
    key_condition_expression: String,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    limit: Option<u32>,
    exclusive_start_key: Option<String>,
    scan_index_forward: Option<bool>,
    consistent_read: bool,
}

impl PreparedQuery {
    fn from_request(
        request: &QueryRequest,
        exclusive_start_key: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> Self {
        Self {
            table_name: request.table_name.clone(),
            index_name: request.index_name.clone(),
            key_condition_expression: request.key_condition_expression.clone(),
            expression_attribute_names,
            expression_attribute_values,
            limit: request.limit,
            exclusive_start_key,
            scan_index_forward: request.scan_index_forward,
            consistent_read: request.consistent_read.unwrap_or(false),
        }
    }

    fn index_input(&self, index_name: IndexName) -> Result<QueryIndexInput, HttpApiError> {
        if self.consistent_read {
            return Err(HttpApiError::validation_error(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }

        Ok(QueryIndexInput {
            table_name: self.table_name.clone(),
            index_name,
            key_condition_expression: self.key_condition_expression.clone(),
            expression_attribute_names: self.expression_attribute_names.clone(),
            expression_attribute_values: self.expression_attribute_values.clone(),
            limit: self.limit,
            exclusive_start_key: self.exclusive_start_key.clone(),
            scan_index_forward: self.scan_index_forward,
        })
    }

    fn table_input(&self) -> QueryTableInput {
        QueryTableInput {
            table_name: self.table_name.clone(),
            key_condition_expression: self.key_condition_expression.clone(),
            expression_attribute_names: self.expression_attribute_names.clone(),
            expression_attribute_values: self.expression_attribute_values.clone(),
            limit: self.limit,
            exclusive_start_key: self.exclusive_start_key.clone(),
            scan_index_forward: self.scan_index_forward,
            consistent_read: self.consistent_read,
        }
    }
}

pub(crate) fn apply_filter_expression_refs<'a>(
    items: &'a [std::collections::HashMap<String, AttributeValue>],
    filter_expr: &str,
    attribute_names: Option<&std::collections::HashMap<String, String>>,
    attribute_values: Option<&std::collections::HashMap<String, AttributeValue>>,
) -> Result<Vec<&'a std::collections::HashMap<String, AttributeValue>>, HttpApiError> {
    let mut filtered = Vec::with_capacity(items.len());
    for item in items {
        if evaluate_filter_condition(item, filter_expr, attribute_names, attribute_values)? {
            filtered.push(item);
        }
    }
    Ok(filtered)
}

#[expect(clippy::string_slice)]
fn evaluate_filter_condition(
    item: &std::collections::HashMap<String, AttributeValue>,
    filter_expr: &str,
    attribute_names: Option<&std::collections::HashMap<String, String>>,
    attribute_values: Option<&std::collections::HashMap<String, AttributeValue>>,
) -> Result<bool, HttpApiError> {
    // Simple expression evaluator
    let expr = filter_expr.trim();

    // Handle expressions like "#status = :statusVal"
    if let Some(equals_pos) = expr.find(" = ") {
        let left = expr[..equals_pos].trim();
        let right = expr[equals_pos + 3..].trim();

        let attr_name = resolve_attribute_name(left, attribute_names);
        let target_value = resolve_attribute_value(right, attribute_values)?;

        if let Some(item_value) = item.get(&attr_name) {
            return Ok(attribute_values_equal(item_value, &target_value));
        }
        return Ok(false);
    }

    // Handle expressions like "age > :minAge"
    if let Some(gt_pos) = expr.find(" > ") {
        let left = expr[..gt_pos].trim();
        let right = expr[gt_pos + 3..].trim();

        let attr_name = resolve_attribute_name(left, attribute_names);
        let target_value = resolve_attribute_value(right, attribute_values)?;

        if let Some(item_value) = item.get(&attr_name) {
            return Ok(
                compare_attribute_values(item_value, &target_value) == std::cmp::Ordering::Greater
            );
        }
        return Ok(false);
    }

    // Handle expressions like "age < :maxAge"
    if let Some(lt_pos) = expr.find(" < ") {
        let left = expr[..lt_pos].trim();
        let right = expr[lt_pos + 3..].trim();

        let attr_name = resolve_attribute_name(left, attribute_names);
        let target_value = resolve_attribute_value(right, attribute_values)?;

        if let Some(item_value) = item.get(&attr_name) {
            return Ok(
                compare_attribute_values(item_value, &target_value) == std::cmp::Ordering::Less
            );
        }
        return Ok(false);
    }

    // Handle expressions like "age >= :minAge"
    if let Some(gte_pos) = expr.find(" >= ") {
        let left = expr[..gte_pos].trim();
        let right = expr[gte_pos + 4..].trim();

        let attr_name = resolve_attribute_name(left, attribute_names);
        let target_value = resolve_attribute_value(right, attribute_values)?;

        if let Some(item_value) = item.get(&attr_name) {
            let cmp = compare_attribute_values(item_value, &target_value);
            return Ok(cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal);
        }
        return Ok(false);
    }

    // Handle expressions like "age <= :maxAge"
    if let Some(lte_pos) = expr.find(" <= ") {
        let left = expr[..lte_pos].trim();
        let right = expr[lte_pos + 4..].trim();

        let attr_name = resolve_attribute_name(left, attribute_names);
        let target_value = resolve_attribute_value(right, attribute_values)?;

        if let Some(item_value) = item.get(&attr_name) {
            let cmp = compare_attribute_values(item_value, &target_value);
            return Ok(cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal);
        }
        return Ok(false);
    }

    // Handle expressions like "status <> :inactiveVal"
    if let Some(ne_pos) = expr.find(" <> ") {
        let left = expr[..ne_pos].trim();
        let right = expr[ne_pos + 4..].trim();

        let attr_name = resolve_attribute_name(left, attribute_names);
        let target_value = resolve_attribute_value(right, attribute_values)?;

        if let Some(item_value) = item.get(&attr_name) {
            return Ok(!attribute_values_equal(item_value, &target_value));
        }
        return Ok(true); // If attribute doesn't exist, it's not equal to target value
    }

    Err(unsupported_filter_expression())
}

fn resolve_attribute_name(
    name: &str,
    attribute_names: Option<&std::collections::HashMap<String, String>>,
) -> String {
    if let Some(stripped) = name.strip_prefix('#') {
        if let Some(names_map) = attribute_names
            && let Some(resolved) = names_map.get(name)
        {
            return resolved.clone();
        }
        // If not found in map, use the name without the #
        return stripped.to_string();
    }
    name.to_string()
}

fn resolve_attribute_value(
    value: &str,
    attribute_values: Option<&std::collections::HashMap<String, AttributeValue>>,
) -> Result<AttributeValue, HttpApiError> {
    if value.starts_with(':') {
        if let Some(values_map) = attribute_values
            && let Some(resolved) = values_map.get(value)
        {
            return Ok(resolved.clone());
        }
        return Err(missing_expression_value_error(value));
    }
    // For literal values, create a string attribute value
    Ok(AttributeValue::S(value.to_string()))
}

#[cold]
#[inline(never)]
fn unsupported_filter_expression() -> HttpApiError {
    HttpApiError::validation_error("Unsupported filter expression".to_string())
}

#[cold]
#[inline(never)]
fn missing_expression_value_error(value: &str) -> HttpApiError {
    HttpApiError::validation_error(format!("ExpressionAttributeValues missing key {value}"))
}

fn attribute_values_equal(a: &AttributeValue, b: &AttributeValue) -> bool {
    match (a, b) {
        (AttributeValue::S(a_str), AttributeValue::S(b_str)) => a_str == b_str,
        (AttributeValue::N(a_num), AttributeValue::N(b_num)) => a_num == b_num,
        (AttributeValue::B(a_bin), AttributeValue::B(b_bin)) => a_bin == b_bin,
        (AttributeValue::BOOL(a_bool), AttributeValue::BOOL(b_bool)) => a_bool == b_bool,
        (AttributeValue::NULL(_), AttributeValue::NULL(_)) => true,
        _ => false,
    }
}

fn compare_attribute_values(a: &AttributeValue, b: &AttributeValue) -> std::cmp::Ordering {
    match (a, b) {
        (AttributeValue::S(a_str), AttributeValue::S(b_str)) => a_str.cmp(b_str),
        (AttributeValue::N(a_num), AttributeValue::N(b_num)) => {
            // Parse as f64 for numeric comparison
            let a_val: f64 = a_num.parse().unwrap_or(0.0);
            let b_val: f64 = b_num.parse().unwrap_or(0.0);
            a_val
                .partial_cmp(&b_val)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        (AttributeValue::B(a_bin), AttributeValue::B(b_bin)) => a_bin.cmp(b_bin),
        (AttributeValue::BOOL(a_bool), AttributeValue::BOOL(b_bool)) => a_bool.cmp(b_bool),
        _ => std::cmp::Ordering::Equal,
    }
}

pub(crate) fn apply_projection_expression_refs(
    items: &[&std::collections::HashMap<String, AttributeValue>],
    projection_expr: &str,
    attribute_names: Option<&std::collections::HashMap<String, String>>,
) -> Vec<std::collections::HashMap<String, AttributeValue>> {
    let attributes: Vec<String> = projection_expr
        .split(',')
        .map(str::trim)
        .map(|attr| resolve_attribute_name(attr, attribute_names))
        .collect();
    let mut projected_items = Vec::with_capacity(items.len());

    for item in items {
        let mut projected_item = std::collections::HashMap::with_capacity(attributes.len());
        for attr_name in &attributes {
            if let Some(value) = item.get(attr_name) {
                projected_item.insert(attr_name.clone(), value.clone());
            }
        }
        projected_items.push(projected_item);
    }

    projected_items
}
