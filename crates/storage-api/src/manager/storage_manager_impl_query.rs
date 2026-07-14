use std::collections::HashMap;

use http_error::HttpApiError;
use storage::{QueryIndexInput, QueryTableInput};
use storage_provider::StorageProviderReadContext;
use storage_types::{
    AttributeValue, IndexName, KeySchemaElement, QueryRequest, QueryResponse, StorageEnum,
    StorageError, StoredTableInfo, TableName, WireItem, context::WrappedError,
    subset_expression_attribute_names_for_expression,
    subset_expression_attribute_values_for_expression, validate_expression_attribute_usage,
    validate_key_attribute_value_for_schema,
};

use crate::{
    manager::{
        StorageApiManagerImpl,
        storage_manager_impl_consumed_capacity::calculate_consumed_capacity_from_inputs,
        storage_manager_impl_expression::{
            apply_filter_expression_refs, apply_projection_expression_refs,
        },
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
        self.query_internal_with_context(request, QueryReadContext::Manager)
            .await
    }

    pub(super) async fn query_internal_with_read_context(
        &self,
        request: QueryRequest,
        read_context: &dyn StorageProviderReadContext,
    ) -> Result<Response, HttpApiError> {
        self.query_internal_with_context(request, QueryReadContext::Provider(read_context))
            .await
    }

    async fn query_internal_with_context(
        &self,
        request: QueryRequest,
        read_context: QueryReadContext<'_>,
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
        validate_query_key_condition_values(&request, &table_info)?;
        let exclusive_start_key = resolve_exclusive_start_key(
            request.exclusive_start_key.as_ref(),
            &table_info,
            request.index_name.as_ref(),
            "The provided starting key is invalid",
        )
        .map_err(HttpApiError::from)?;
        let prepared_query = PreparedQuery::from_request(
            &request,
            exclusive_start_key.as_deref(),
            key_expression_attribute_names.as_ref(),
            key_expression_attribute_values.as_ref(),
        );

        if query_wire_fast_path_enabled(&request) {
            return self
                .query_wire_internal(&request, table_info, prepared_query, read_context)
                .await;
        }

        let (items, last_evaluated_key) =
            self.query_map_items(&prepared_query, read_context).await?;

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
        request: &QueryRequest,
        table_info: StoredTableInfo,
        prepared_query: PreparedQuery<'_>,
        read_context: QueryReadContext<'_>,
    ) -> Result<Response, HttpApiError> {
        let (items, last_evaluated_key) =
            self.query_wire_items(&prepared_query, read_context).await?;

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
        query: &PreparedQuery<'_>,
        read_context: QueryReadContext<'_>,
    ) -> Result<(Vec<HashMap<String, AttributeValue>>, Option<String>), HttpApiError> {
        if let QueryReadContext::Provider(provider_context) = read_context {
            let (items, last_evaluated_key) =
                provider_context.query_table(&query.request()?).await?;
            let mut decoded = Vec::with_capacity(items.len());
            for item in items {
                decoded.push(item.into_attribute_map()?);
            }
            return Ok((decoded, last_evaluated_key));
        }

        if let Some(index_name) = query.index_name {
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
        query: &PreparedQuery<'_>,
        read_context: QueryReadContext<'_>,
    ) -> Result<(Vec<WireItem>, Option<String>), HttpApiError> {
        if let QueryReadContext::Provider(provider_context) = read_context {
            return Ok(provider_context.query_table(&query.request()?).await?);
        }

        if let Some(index_name) = query.index_name {
            return Ok(self
                .db()
                .query_index(query.index_input(index_name)?)
                .await?);
        }

        self.ensure_sync_read_barrier(query.consistent_read).await?;
        Ok(self.db().query_table(query.table_input()).await?)
    }
}

#[derive(Clone, Copy)]
pub(super) enum QueryReadContext<'a> {
    Manager,
    Provider(&'a dyn StorageProviderReadContext),
}

pub(super) fn validate_query_key_condition_values(
    request: &QueryRequest,
    table_info: &StoredTableInfo,
) -> Result<(), HttpApiError> {
    let key_schema = query_key_schema(table_info, request.index_name.as_ref())?;
    let Some(values) = request.expression_attribute_values.as_ref() else {
        return Ok(());
    };
    let tokens = tokenize_key_condition_expression(&request.key_condition_expression);

    for (schema, value_token) in key_condition_value_tokens(
        &tokens,
        request.expression_attribute_names.as_ref(),
        key_schema,
    ) {
        let Some(value) = values.get(value_token) else {
            continue;
        };
        validate_key_attribute_value_for_schema(schema, value)
            .map_err(query_key_validation_error)?;
    }

    Ok(())
}

fn query_key_schema<'a>(
    table_info: &'a StoredTableInfo,
    index_name: Option<&IndexName>,
) -> Result<&'a [KeySchemaElement], HttpApiError> {
    let Some(index_name) = index_name else {
        return Ok(&table_info.key_schema);
    };
    let Some(index) = table_info
        .global_secondary_indexes
        .as_ref()
        .and_then(|indexes| indexes.iter().find(|index| index.index_name == *index_name))
    else {
        return Ok(&table_info.key_schema);
    };
    Ok(&index.key_schema)
}

fn query_key_validation_error(error: StorageError) -> HttpApiError {
    let StorageEnum::Validation { message } = error.to_enum() else {
        return HttpApiError::from(error);
    };
    if message == "The parameter cannot be converted to a numeric value" {
        return HttpApiError::from(StorageError::validation(
            "The parameter cannot be converted to a numeric value: ",
        ));
    }
    if message == "Attempting to store more than 38 significant digits in a Number"
        || message
            == "Number underflow. Attempting to store a number with magnitude smaller than \
                supported range"
    {
        return HttpApiError::from(StorageError::raw_validation(message.clone()));
    }
    HttpApiError::from(StorageError::validation(message.clone()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyConditionToken<'a> {
    Identifier(&'a str),
    Value(&'a str),
    Function(&'a str),
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
    Between,
    And,
    LeftParen,
    RightParen,
    Comma,
}

pub(super) fn tokenize_key_condition_expression(expression: &str) -> Vec<KeyConditionToken<'_>> {
    let mut tokens = Vec::new();
    let mut chars = expression.char_indices().peekable();

    while let Some((start, ch)) = chars.next() {
        match ch {
            c if c.is_whitespace() => {}
            '(' => tokens.push(KeyConditionToken::LeftParen),
            ')' => tokens.push(KeyConditionToken::RightParen),
            ',' => tokens.push(KeyConditionToken::Comma),
            '=' => tokens.push(KeyConditionToken::Eq),
            '<' => {
                if matches!(chars.peek(), Some((_, '='))) {
                    chars.next();
                    tokens.push(KeyConditionToken::Le);
                } else {
                    tokens.push(KeyConditionToken::Lt);
                }
            }
            '>' => {
                if matches!(chars.peek(), Some((_, '='))) {
                    chars.next();
                    tokens.push(KeyConditionToken::Ge);
                } else {
                    tokens.push(KeyConditionToken::Gt);
                }
            }
            ':' => {
                let end = read_expression_token_end(expression, &mut chars);
                if let Some(token) = expression.get(start..end) {
                    tokens.push(KeyConditionToken::Value(token));
                }
            }
            '#' => {
                let end = read_expression_token_end(expression, &mut chars);
                if let Some(token) = expression.get(start..end) {
                    tokens.push(KeyConditionToken::Identifier(token));
                }
            }
            c if is_identifier_start(c) => {
                let end = read_expression_token_end(expression, &mut chars);
                if let Some(word) = expression.get(start..end) {
                    match word {
                        "AND" | "and" => tokens.push(KeyConditionToken::And),
                        "BETWEEN" | "between" => tokens.push(KeyConditionToken::Between),
                        "begins_with" => tokens.push(KeyConditionToken::Function(word)),
                        _ => tokens.push(KeyConditionToken::Identifier(word)),
                    }
                }
            }
            _ => {}
        }
    }

    tokens
}

fn read_expression_token_end(
    expression: &str,
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
) -> usize {
    while let Some((index, ch)) = chars.peek() {
        if !is_identifier_continue(*ch) {
            return *index;
        }
        chars.next();
    }
    expression.len()
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_identifier_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '#')
}

fn key_condition_value_tokens<'a>(
    tokens: &'a [KeyConditionToken<'a>],
    names: Option<&'a HashMap<String, String>>,
    key_schema: &'a [KeySchemaElement],
) -> Vec<(&'a KeySchemaElement, &'a str)> {
    let mut value_tokens = Vec::with_capacity(2);

    for index in 0..tokens.len() {
        collect_comparison_key_value(tokens, index, names, key_schema, &mut value_tokens);
        collect_between_key_values(tokens, index, names, key_schema, &mut value_tokens);
        collect_begins_with_key_value(tokens, index, names, key_schema, &mut value_tokens);
    }

    value_tokens
}

fn collect_comparison_key_value<'a>(
    tokens: &'a [KeyConditionToken<'a>],
    index: usize,
    names: Option<&'a HashMap<String, String>>,
    key_schema: &'a [KeySchemaElement],
    value_tokens: &mut Vec<(&'a KeySchemaElement, &'a str)>,
) {
    let Some(operator) = tokens.get(index + 1) else {
        return;
    };
    if !matches!(
        operator,
        KeyConditionToken::Eq
            | KeyConditionToken::Lt
            | KeyConditionToken::Le
            | KeyConditionToken::Gt
            | KeyConditionToken::Ge
    ) {
        return;
    }

    match (tokens.get(index), tokens.get(index + 2)) {
        (
            Some(KeyConditionToken::Identifier(identifier)),
            Some(KeyConditionToken::Value(value_token)),
        ) => push_key_value_token(identifier, value_token, names, key_schema, value_tokens),
        (
            Some(KeyConditionToken::Value(value_token)),
            Some(KeyConditionToken::Identifier(identifier)),
        ) => push_key_value_token(identifier, value_token, names, key_schema, value_tokens),
        _ => {}
    }
}

fn collect_between_key_values<'a>(
    tokens: &'a [KeyConditionToken<'a>],
    index: usize,
    names: Option<&'a HashMap<String, String>>,
    key_schema: &'a [KeySchemaElement],
    value_tokens: &mut Vec<(&'a KeySchemaElement, &'a str)>,
) {
    let (
        Some(KeyConditionToken::Identifier(identifier)),
        Some(KeyConditionToken::Between),
        Some(KeyConditionToken::Value(lower)),
        Some(KeyConditionToken::And),
        Some(KeyConditionToken::Value(upper)),
    ) = (
        tokens.get(index),
        tokens.get(index + 1),
        tokens.get(index + 2),
        tokens.get(index + 3),
        tokens.get(index + 4),
    )
    else {
        return;
    };

    push_key_value_token(identifier, lower, names, key_schema, value_tokens);
    push_key_value_token(identifier, upper, names, key_schema, value_tokens);
}

fn collect_begins_with_key_value<'a>(
    tokens: &'a [KeyConditionToken<'a>],
    index: usize,
    names: Option<&'a HashMap<String, String>>,
    key_schema: &'a [KeySchemaElement],
    value_tokens: &mut Vec<(&'a KeySchemaElement, &'a str)>,
) {
    let (
        Some(KeyConditionToken::Function(function)),
        Some(KeyConditionToken::LeftParen),
        Some(KeyConditionToken::Identifier(identifier)),
        Some(KeyConditionToken::Comma),
        Some(KeyConditionToken::Value(value_token)),
    ) = (
        tokens.get(index),
        tokens.get(index + 1),
        tokens.get(index + 2),
        tokens.get(index + 3),
        tokens.get(index + 4),
    )
    else {
        return;
    };
    if *function != "begins_with" {
        return;
    }

    push_key_value_token(identifier, value_token, names, key_schema, value_tokens);
}

fn push_key_value_token<'a>(
    identifier: &'a str,
    value_token: &'a str,
    names: Option<&'a HashMap<String, String>>,
    key_schema: &'a [KeySchemaElement],
    value_tokens: &mut Vec<(&'a KeySchemaElement, &'a str)>,
) {
    let attribute_name = resolve_expression_attribute_name(identifier, names);
    if let Some(schema) = key_schema
        .iter()
        .find(|element| element.attribute_name == attribute_name)
    {
        value_tokens.push((schema, value_token));
    }
}

fn resolve_expression_attribute_name<'a>(
    identifier: &'a str,
    names: Option<&'a HashMap<String, String>>,
) -> &'a str {
    if identifier.starts_with('#')
        && let Some(name) = names.and_then(|names| names.get(identifier))
    {
        return name;
    }
    identifier
}

fn query_wire_fast_path_enabled(request: &QueryRequest) -> bool {
    request.filter_expression.is_none()
        && request.projection_expression.is_none()
        && request.select.as_deref() != Some("COUNT")
}

pub(super) struct PreparedQuery<'a> {
    table_name: &'a TableName,
    index_name: Option<&'a IndexName>,
    key_condition_expression: &'a str,
    expression_attribute_names: Option<&'a HashMap<String, String>>,
    expression_attribute_values: Option<&'a HashMap<String, AttributeValue>>,
    limit: Option<u32>,
    exclusive_start_key: Option<&'a str>,
    scan_index_forward: Option<bool>,
    consistent_read: bool,
}

impl<'a> PreparedQuery<'a> {
    pub(super) fn from_request(
        request: &'a QueryRequest,
        exclusive_start_key: Option<&'a str>,
        expression_attribute_names: Option<&'a HashMap<String, String>>,
        expression_attribute_values: Option<&'a HashMap<String, AttributeValue>>,
    ) -> Self {
        Self {
            table_name: &request.table_name,
            index_name: request.index_name.as_ref(),
            key_condition_expression: &request.key_condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            limit: request.limit,
            exclusive_start_key,
            scan_index_forward: request.scan_index_forward,
            consistent_read: request.consistent_read.unwrap_or(false),
        }
    }

    fn index_input(&self, index_name: &IndexName) -> Result<QueryIndexInput, HttpApiError> {
        if self.consistent_read {
            return Err(HttpApiError::dynamodb_protocol_error(
                "ValidationException",
                "Consistent reads are not supported on global secondary indexes",
                400,
            ));
        }

        Ok(QueryIndexInput {
            table_name: self.table_name.clone(),
            index_name: index_name.clone(),
            key_condition_expression: self.key_condition_expression.to_string(),
            expression_attribute_names: self.expression_attribute_names.cloned(),
            expression_attribute_values: self.expression_attribute_values.cloned(),
            projection_expression: None,
            limit: self.limit,
            exclusive_start_key: self.exclusive_start_key.map(str::to_string),
            scan_index_forward: self.scan_index_forward,
        })
    }

    pub(super) fn table_input(&self) -> QueryTableInput {
        QueryTableInput {
            table_name: self.table_name.clone(),
            key_condition_expression: self.key_condition_expression.to_string(),
            expression_attribute_names: self.expression_attribute_names.cloned(),
            expression_attribute_values: self.expression_attribute_values.cloned(),
            limit: self.limit,
            exclusive_start_key: self.exclusive_start_key.map(str::to_string),
            scan_index_forward: self.scan_index_forward,
            consistent_read: self.consistent_read,
        }
    }

    fn request(&self) -> Result<storage_types::QueryTableRequest, HttpApiError> {
        if let Some(index_name) = self.index_name {
            return Ok(self.index_input(index_name)?.into());
        }
        Ok(self.table_input().into())
    }
}
