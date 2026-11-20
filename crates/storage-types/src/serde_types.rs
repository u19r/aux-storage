use serde::de::DeserializeOwned;

use crate::{
    BatchGetItemRequest, BatchWriteItemRequest, CreateTableRequest,
    DYNAMODB_STREAM_RECORDS_LIMIT_MAX, DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE,
    DYNAMODB_STREAM_RECORDS_LIMIT_MIN, DeleteItemRequest, DeleteTableRequest,
    DescribeStreamRequest, DescribeSystemStreamRequest, DescribeTableRequest, GetItemRequest,
    GetRecordsRequest, GetShardIteratorRequest, GetStreamRecordsRequest, KeyAttributes, KeyType,
    ListStreamsRequest, ListTablesRequest, PutItemRequest, QueryRequest, ReadSequenceRequest,
    ScanRequest, TableName, TransactGetItemsRequest, TransactWriteItemsRequest, UpdateItemRequest,
    UpdateTableRequest,
    constants::{
        DESCRIBE_STREAM_FIELDS, DESCRIBE_SYSTEM_STREAM_FIELDS,
        DYNAMODB_CLIENT_REQUEST_TOKEN_MAX_CHARS, GET_RECORDS_FIELDS, GET_SHARD_ITERATOR_FIELDS,
        GET_STREAM_RECORDS_FIELDS, LIST_STREAMS_FIELDS, LIST_TABLES_FIELDS, MAX_LIST_TABLES_LIMIT,
    },
    dynamodb_limits::{
        validate_attribute_name, validate_index_name, validate_index_name_lazy,
        validate_projected_attribute_limit, validate_transaction_request_size,
    },
    request_expression_validation::{
        expression_kind::ExpressionKind, validate_expression_attribute_name_keys,
        validate_expression_attribute_value_keys, validate_expression_set,
        validate_expression_syntax, validate_expression_value_operands,
        validate_transact_expression_attribute_value_keys, validation_message,
    },
};

fn deserialize_request_with_return_consumed_capacity<T: DeserializeOwned>(
    value: serde_json::Value,
    prefixed_capacity_error: bool,
) -> Result<T, String> {
    let (request, invalid_capacity) =
        crate::return_consumed_capacity::deserialize_request_value(value)
            .map_err(|error| format!("Invalid request format: {error}"))?;
    if let Some(value) = invalid_capacity {
        validate_return_consumed_capacity(Some(&value), prefixed_capacity_error)?;
    }
    Ok(request)
}

/// `ReturnValues` of PutItem and DeleteItem accepts only `NONE` and `ALL_OLD`.
/// Another member of the shared enum gets the semantic text; anything else
/// gets the enum-constraint text.
fn deserialize_item_write_request<T: DeserializeOwned>(
    mut value: serde_json::Value,
) -> Result<T, String> {
    let refused = value
        .get("ReturnValues")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !matches!(*value, "NONE" | "ALL_OLD"))
        .map(str::to_owned);
    if refused.is_some() {
        value["ReturnValues"] = serde_json::Value::String("NONE".to_owned());
    }
    let request = deserialize_request_with_return_consumed_capacity::<T>(value, true)?;
    match refused.as_deref() {
        None => Ok(request),
        Some("ALL_NEW" | "UPDATED_OLD" | "UPDATED_NEW") => {
            Err("1 validation error detected: ReturnValues can only be ALL_OLD or NONE".to_owned())
        }
        Some(_) => Err(
            "1 validation error detected: Failed to satisfy constraint: Member must satisfy enum \
             value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]"
                .to_owned(),
        ),
    }
}

pub(crate) fn reject_unknown_fields(
    value: &serde_json::Value,
    allowed: &[&str],
) -> Result<(), String> {
    if let Some(obj) = value.as_object() {
        for key in obj.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(format!("Invalid request format: unknown field '{key}'"));
            }
        }
    }
    Ok(())
}

pub trait DynamoRequestValidate {
    fn validate_for_dynamodb(&self) -> Result<(), String>;
}

/// Names of the expression parameters a request carries, in request order.
fn present_expression_parameters<const N: usize>(
    parameters: [(bool, &str); N],
) -> smallvec::SmallVec<[&str; 3]> {
    parameters
        .into_iter()
        .filter_map(|(present, name)| present.then_some(name))
        .collect()
}

/// AWS text (probe P11): no comma before `Expression parameters`; the
/// "1 validation error detected: " prefix follows the operation.
fn mixed_parameters_error(legacy: &str, expressions: &[&str], prefixed: bool) -> String {
    validation_message(
        format!(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {{{legacy}}} Expression parameters: {{{}}}",
            expressions.join(", ")
        ),
        prefixed,
    )
}

/// Legacy and expression parameters in one Scan or Query: AWS lists every
/// present parameter of each side, in its own order.
fn reject_legacy_expression_mixing(
    legacy: &[(bool, &str)],
    expressions: &[&str],
) -> Result<(), String> {
    let legacy = legacy
        .iter()
        .filter_map(|(present, name)| present.then_some(*name))
        .collect::<smallvec::SmallVec<[&str; 4]>>();
    if legacy.is_empty() || expressions.is_empty() {
        return Ok(());
    }
    Err(mixed_parameters_error(
        &legacy.join(", "),
        expressions,
        false,
    ))
}

/// A legacy parameter is rejected: next to an expression parameter it gets the
/// AWS mixed-parameter text, alone the `alone` text.
fn reject_legacy_parameter(
    present: bool,
    legacy: &str,
    expressions: &[&str],
    prefixed: bool,
    alone: impl FnOnce() -> String,
) -> Result<(), String> {
    if !present {
        return Ok(());
    }
    if expressions.is_empty() {
        return Err(alone());
    }
    Err(mixed_parameters_error(legacy, expressions, prefixed))
}

/// `ConditionalOperator` alone: AWS text (probe P11, PutItem).
fn conditional_operator_alone(prefixed: bool) -> String {
    validation_message(
        "ConditionalOperator cannot be used without Filter or Expected".to_owned(),
        prefixed,
    )
}

/// Legacy condition parameters of a write: `Expected` and
/// `ConditionalOperator`.
fn validate_condition_sources(
    expected: bool,
    conditional_operator: bool,
    expressions: &[&str],
) -> Result<(), String> {
    reject_legacy_parameter(expected, "Expected", expressions, true, || {
        "Expected is not supported; use ConditionExpression".to_owned()
    })?;
    reject_legacy_parameter(
        conditional_operator,
        "ConditionalOperator",
        expressions,
        true,
        || conditional_operator_alone(true),
    )
}

fn validate_attributes_to_get(attributes: Option<&[String]>) -> Result<(), String> {
    let Some(attributes) = attributes else {
        return Ok(());
    };
    if attributes.is_empty() {
        return Err(
            "1 validation error detected: Value '[]' at 'attributesToGet' failed to satisfy \
             constraint: Member must have length greater than or equal to 1"
                .to_owned(),
        );
    }
    for (index, attribute) in attributes.iter().enumerate() {
        if attributes[..index].contains(attribute) {
            return Err(format!(
                "One or more parameter values were invalid: Duplicate value in attribute name: \
                 {attribute}"
            ));
        }
    }
    Ok(())
}

/// `AttributesToGet` stays supported; only next to `ProjectionExpression` it
/// is the AWS mixed-parameter error (probe P11, GetItem).
fn validate_attributes_to_get_source(
    attributes: Option<&[String]>,
    projection_expression: bool,
) -> Result<(), String> {
    if attributes.is_some() && projection_expression {
        return Err(mixed_parameters_error(
            "AttributesToGet",
            &["ProjectionExpression"],
            false,
        ));
    }
    validate_attributes_to_get(attributes)
}

/// Query and Scan name the member in the model's casing and omit the value
/// from the empty-list constraint text.
fn validate_scan_attributes_to_get_source(
    attributes: Option<&[String]>,
    projection_expression: bool,
) -> Result<(), String> {
    if !projection_expression && attributes.is_some_and(<[String]>::is_empty) {
        return Err(
            "1 validation error detected: Value at 'AttributesToGet' failed to satisfy \
             constraint: Member must have length greater than or equal to 1"
                .to_owned(),
        );
    }
    validate_attributes_to_get_source(attributes, projection_expression)
}

fn validate_select(
    select: Option<&str>,
    has_index: bool,
    projection: Option<&str>,
    operation: &str,
) -> Result<(), String> {
    match select {
        None => Ok(()),
        Some(select @ ("ALL_ATTRIBUTES" | "COUNT" | "ALL_PROJECTED_ATTRIBUTES")) => {
            if let Some(projection) = projection {
                let chosen = match select {
                    "COUNT" => "only the Count",
                    other => other,
                };
                return Err(format!(
                    "1 validation error detected: Cannot specify the {projection} when choosing \
                     to get {chosen}"
                ));
            }
            if select == "ALL_PROJECTED_ATTRIBUTES" && !has_index {
                return Err(format!(
                    "1 validation error detected: ALL_PROJECTED_ATTRIBUTES can be used only when \
                     {operation} using an IndexName"
                ));
            }
            Ok(())
        }
        Some("SPECIFIC_ATTRIBUTES") => {
            if projection.is_none() {
                return Err(
                    "1 validation error detected: Must specify the AttributesToGet or \
                     ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"
                        .to_string(),
                );
            }
            Ok(())
        }
        Some(value) => Err(format!("Invalid Select value: {value}")),
    }
}

pub(super) fn validate_index_key_schema(
    key_schema: &[crate::KeySchemaElement],
    definitions: &[crate::AttributeDefinition],
) -> Result<(), String> {
    validate_key_schema_shape(key_schema, "GSI KeySchema")?;
    if key_schema.iter().any(|key| !is_defined(definitions, key)) {
        return Err(undefined_index_keys_message(key_schema, definitions));
    }
    Ok(())
}

/// The table `KeySchema` must name defined attributes (AWS text copied from the
/// behavioural suite).
fn validate_table_key_schema(
    key_schema: &[crate::KeySchemaElement],
    definitions: &[crate::AttributeDefinition],
) -> Result<(), String> {
    validate_key_schema_shape(key_schema, "KeySchema")?;
    if key_schema.iter().any(|key| !is_defined(definitions, key)) {
        return Err(INVALID_KEY_SCHEMA_UNDEFINED.to_string());
    }
    Ok(())
}

const INVALID_KEY_SCHEMA_UNDEFINED: &str =
    "Invalid KeySchema: Some index key attribute have no definition";

fn is_defined(definitions: &[crate::AttributeDefinition], key: &crate::KeySchemaElement) -> bool {
    definitions
        .iter()
        .any(|definition| definition.attribute_name == key.attribute_name)
}

/// AWS text for an index key schema naming attributes missing from
/// `AttributeDefinitions`.
pub fn undefined_index_keys_message(
    key_schema: &[crate::KeySchemaElement],
    definitions: &[crate::AttributeDefinition],
) -> String {
    let join = |names: Vec<&str>| names.join(", ");
    format!(
        "One or more parameter values were invalid: Some index key attributes are not defined in \
         AttributeDefinitions. Keys: [{}], AttributeDefinitions: [{}]",
        join(
            key_schema
                .iter()
                .map(|key| key.attribute_name.as_str())
                .collect()
        ),
        join(
            definitions
                .iter()
                .map(|definition| definition.attribute_name.as_str())
                .collect()
        ),
    )
}

fn validate_key_schema_shape(
    key_schema: &[crate::KeySchemaElement],
    label: &str,
) -> Result<(), String> {
    if key_schema.is_empty() {
        return Err(format!("{label} cannot be empty"));
    }
    if key_schema.len() > 2 {
        return Err(format!("{label} cannot have more than 2 elements"));
    }
    match key_schema
        .iter()
        .filter(|key| key.key_type == KeyType::Hash)
        .count()
    {
        0 => return Err(format!("{label} must have a HASH key")),
        1 => {}
        _ => return Err(format!("{label} can only have one HASH key")),
    }
    if let [first, second] = key_schema
        && first.attribute_name == second.attribute_name
    {
        if label == "KeySchema" {
            return Err(INVALID_KEY_SCHEMA_UNDEFINED.to_string());
        }
        return Err(format!("{label} has duplicate attributes"));
    }
    Ok(())
}

pub(super) fn validate_attribute_definitions(
    definitions: &[crate::AttributeDefinition],
) -> Result<(), String> {
    for (index, definition) in definitions.iter().enumerate() {
        if definitions[..index]
            .iter()
            .any(|previous| previous.attribute_name == definition.attribute_name)
        {
            return Err(format!(
                "Duplicate AttributeDefinitions entry for '{}'",
                definition.attribute_name
            ));
        }
    }
    Ok(())
}

impl DynamoRequestValidate for PutItemRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        validate_table_name(&self.table_name)?;
        // AWS looks the table up before sizing a PutItem item, so the size
        // refusal is left to the operation, which answers it after the lookup.
        if let Err(message) = self.item.validate_for_write("Item")
            && message != crate::logical_item_size::ITEM_TOO_LARGE_MESSAGE
        {
            return Err(prefixed_item_validation_message(message));
        }
        self.validate_write_options()
    }
}

impl PutItemRequest {
    /// Validate and normalize a write together, inspecting the item only once.
    pub fn normalized_for_dynamodb(
        mut self,
    ) -> crate::StorageResult<(Self, crate::LogicalItemSize)> {
        validate_table_name(&self.table_name).map_err(crate::StorageError::raw_validation)?;
        let (item, size) = self.item.normalized_for_write()?;
        self.item = item;
        self.validate_write_options()
            .map_err(crate::StorageError::raw_validation)?;
        Ok((self, size))
    }

    fn validate_write_options(&self) -> Result<(), String> {
        validate_return_item_collection_metrics(
            self.return_item_collection_metrics.as_deref(),
            true,
        )?;
        validate_return_values_on_condition_check_failure(
            self.return_values_on_condition_check_failure.as_deref(),
            "returnValuesOnConditionCheckFailure",
            true,
        )?;
        validate_expression_size_for_label(
            self.condition_expression.as_deref(),
            "ConditionExpression",
            true,
            false,
        )?;
        validate_expression_attribute_name_keys(self.expression_attribute_names.as_ref(), true)?;
        validate_expression_attribute_value_keys(self.expression_attribute_values.as_ref(), true)?;
        validate_condition_sources(
            self.expected.is_some(),
            self.conditional_operator.is_some(),
            &present_expression_parameters([(
                self.condition_expression.is_some(),
                "ConditionExpression",
            )]),
        )?;
        validate_expression_set(
            [(
                self.condition_expression.as_deref(),
                ExpressionKind::Condition,
            )],
            self.expression_attribute_names.as_ref(),
            self.expression_attribute_values.as_ref(),
            true,
        )?;
        Ok(())
    }
}

impl DynamoRequestValidate for GetItemRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        validate_table_name(&self.table_name)?;
        if self.key.is_empty() {
            return Err("The provided key element does not match the schema".to_string());
        }
        validate_key_set_operands(&self.key, false)?;
        validate_attributes_to_get_source(
            self.attributes_to_get.as_deref(),
            self.projection_expression.is_some(),
        )?;
        validate_expression_attribute_name_keys(self.expression_attribute_names.as_ref(), false)?;
        validate_expression_set(
            [(
                self.projection_expression.as_deref(),
                ExpressionKind::Projection,
            )],
            self.expression_attribute_names.as_ref(),
            None,
            false,
        )
    }
}

impl DynamoRequestValidate for DeleteItemRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        validate_table_name(&self.table_name)?;
        validate_key_not_empty(&self.key)?;
        validate_key_set_operands(&self.key, true)?;
        validate_return_item_collection_metrics(
            self.return_item_collection_metrics.as_deref(),
            true,
        )?;
        validate_return_values_on_condition_check_failure(
            self.return_values_on_condition_check_failure.as_deref(),
            "returnValuesOnConditionCheckFailure",
            true,
        )?;
        validate_expression_size_for_label(
            self.condition_expression.as_deref(),
            "ConditionExpression",
            true,
            false,
        )?;
        validate_expression_attribute_name_keys(self.expression_attribute_names.as_ref(), true)?;
        validate_expression_attribute_value_keys(self.expression_attribute_values.as_ref(), true)?;
        validate_condition_sources(
            self.expected.is_some(),
            self.conditional_operator.is_some(),
            &present_expression_parameters([(
                self.condition_expression.is_some(),
                "ConditionExpression",
            )]),
        )?;
        validate_expression_set(
            [(
                self.condition_expression.as_deref(),
                ExpressionKind::Condition,
            )],
            self.expression_attribute_names.as_ref(),
            self.expression_attribute_values.as_ref(),
            true,
        )
    }
}

impl DynamoRequestValidate for UpdateItemRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        validate_table_name(&self.table_name)?;
        validate_key_not_empty(&self.key)?;
        let expressions = present_expression_parameters([
            (self.update_expression.is_some(), "UpdateExpression"),
            (self.condition_expression.is_some(), "ConditionExpression"),
        ]);
        if self.attribute_updates.is_some() && !expressions.is_empty() {
            return Err(mixed_parameters_error(
                "AttributeUpdates",
                &expressions,
                true,
            ));
        }
        // Empty and blank expressions are answered before the ReturnValues and
        // size checks (empty text, or `<EOF>` syntax error for blank).
        if let Some(expression) = self.update_expression.as_deref()
            && expression.trim().is_empty()
        {
            validate_expression_syntax(expression, ExpressionKind::Update, true)?;
        }
        validate_condition_sources(
            self.expected.is_some(),
            self.conditional_operator.is_some(),
            &expressions,
        )?;
        validate_return_item_collection_metrics(
            self.return_item_collection_metrics.as_deref(),
            true,
        )?;
        validate_return_values_on_condition_check_failure(
            self.return_values_on_condition_check_failure.as_deref(),
            "returnValuesOnConditionCheckFailure",
            false,
        )?;
        validate_expression_size_for_label(
            self.update_expression.as_deref(),
            "UpdateExpression",
            true,
            false,
        )?;
        validate_expression_size_for_label(
            self.condition_expression.as_deref(),
            "ConditionExpression",
            true,
            false,
        )?;
        validate_expression_attribute_name_keys(self.expression_attribute_names.as_ref(), true)?;
        validate_expression_attribute_value_keys(self.expression_attribute_values.as_ref(), true)?;
        validate_expression_set(
            [
                (
                    self.condition_expression.as_deref(),
                    ExpressionKind::Condition,
                ),
                (self.update_expression.as_deref(), ExpressionKind::Update),
            ],
            self.expression_attribute_names.as_ref(),
            self.expression_attribute_values.as_ref(),
            true,
        )?;
        Ok(())
    }
}

/// AWS text for a Query that carries neither `KeyConditions` nor
/// `KeyConditionExpression`.
const MISSING_KEY_CONDITION_MESSAGE: &str = "Either the KeyConditions or KeyConditionExpression \
                                             parameter must be specified in the request.";

pub(crate) fn validate_query_fields(
    table_name: &TableName,
    key_condition_expression: Option<&str>,
    filter_expression: Option<&str>,
    projection_expression: Option<&str>,
    expression_attribute_names: Option<&crate::VecMap<String, String>>,
    expression_attribute_values: Option<&crate::AttributeMap>,
) -> Result<(), String> {
    validate_table_name(table_name)?;
    if key_condition_expression.is_none() {
        return Err(MISSING_KEY_CONDITION_MESSAGE.to_owned());
    }
    validate_expression_size_for_label(
        key_condition_expression,
        "KeyConditionExpression",
        false,
        false,
    )?;
    validate_expression_size_for_label(filter_expression, "FilterExpression", false, false)?;
    validate_expression_size_for_label(
        projection_expression,
        "ProjectionExpression",
        false,
        false,
    )?;
    validate_expression_attribute_name_keys(expression_attribute_names, false)?;
    validate_expression_attribute_value_keys(expression_attribute_values, false)?;
    validate_expression_set(
        [
            (key_condition_expression, ExpressionKind::KeyCondition),
            (filter_expression, ExpressionKind::Filter),
            (projection_expression, ExpressionKind::Projection),
        ],
        expression_attribute_names,
        expression_attribute_values,
        false,
    )?;
    Ok(())
}

impl QueryRequest {
    /// ReadSequence input binding changes values after the graph's structural
    /// validation. Recheck only value-dependent rules, not names or syntax.
    pub fn validate_bound_values(&self) -> Result<(), String> {
        for (expression, kind) in [
            (
                self.key_condition_expression.as_deref(),
                ExpressionKind::KeyCondition,
            ),
            (self.filter_expression.as_deref(), ExpressionKind::Filter),
        ] {
            if let Some(expression) = expression {
                validate_expression_value_operands(
                    expression,
                    kind,
                    self.expression_attribute_values.as_ref(),
                    false,
                )?;
            }
        }
        Ok(())
    }
}

impl DynamoRequestValidate for QueryRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        // AWS names KeyConditionExpression for a QueryFilter or
        // ConditionalOperator next to it (probe P11).
        let expressions = present_expression_parameters([
            (self.projection_expression.is_some(), "ProjectionExpression"),
            (self.filter_expression.is_some(), "FilterExpression"),
            (
                self.key_condition_expression.is_some(),
                "KeyConditionExpression",
            ),
        ]);
        reject_legacy_expression_mixing(
            &[
                (self.attributes_to_get.is_some(), "AttributesToGet"),
                (self.query_filter.is_some(), "QueryFilter"),
                (self.conditional_operator.is_some(), "ConditionalOperator"),
                (
                    self.key_conditions.is_some() && self.key_condition_expression.is_some(),
                    "KeyConditions",
                ),
            ],
            &expressions,
        )?;
        reject_legacy_parameter(
            self.key_conditions.is_some(),
            "KeyConditions",
            &expressions,
            false,
            || "KeyConditions is not supported; use KeyConditionExpression".to_owned(),
        )?;
        reject_legacy_parameter(
            self.conditional_operator.is_some(),
            "ConditionalOperator",
            &expressions,
            false,
            || conditional_operator_alone(false),
        )?;
        reject_legacy_parameter(
            self.query_filter.is_some(),
            "QueryFilter",
            &expressions,
            false,
            || "QueryFilter is not supported; use FilterExpression".to_owned(),
        )?;
        validate_scan_attributes_to_get_source(
            self.attributes_to_get.as_deref(),
            self.projection_expression.is_some(),
        )?;
        validate_query_fields(
            &self.table_name,
            self.key_condition_expression.as_deref(),
            self.filter_expression.as_deref(),
            self.projection_expression.as_deref(),
            self.expression_attribute_names.as_ref(),
            self.expression_attribute_values.as_ref(),
        )?;
        validate_select(
            self.select.as_deref(),
            self.index_name.is_some(),
            self.projection_expression
                .is_some()
                .then_some("ProjectionExpression")
                .or_else(|| {
                    self.attributes_to_get
                        .is_some()
                        .then_some("AttributesToGet")
                }),
            "Querying",
        )
    }
}

impl DynamoRequestValidate for BatchGetItemRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        if self.request_items.is_empty() {
            return Err("The requestItems parameter is required for BatchGetItem".to_string());
        }

        let mut total_requests = 0;
        for (table_name, keys_and_attributes) in &self.request_items {
            validate_table_name_not_empty(table_name)?;
            if keys_and_attributes.keys.is_empty() {
                return Err(format!(
                    "1 validation error detected: Value at \
                     'RequestItems.{table_name}.member.Keys' failed to satisfy constraint: Member \
                     must have length greater than or equal to 1"
                ));
            }

            if keys_and_attributes.keys.len() > 100 {
                return Err(format!(
                    "1 validation error detected: Value at \
                     'RequestItems.{table_name}.member.Keys' failed to satisfy constraint: Member \
                     must have length less than or equal to 100"
                ));
            }

            total_requests += keys_and_attributes.keys.len();
            for key in &keys_and_attributes.keys {
                if key.is_empty() {
                    return Err("The provided key element does not match the schema".to_string());
                }
                validate_key_set_operands(key, false)?;
            }
            validate_batch_get_keys_distinct(&keys_and_attributes.keys)?;
            validate_attributes_to_get_source(
                keys_and_attributes.attributes_to_get.as_deref(),
                keys_and_attributes.projection_expression.is_some(),
            )?;
            validate_projection_expression_size(
                keys_and_attributes.projection_expression.as_deref(),
            )?;
            validate_expression_attribute_name_keys(
                keys_and_attributes.expression_attribute_names.as_ref(),
                false,
            )?;
            validate_expression_set(
                [(
                    keys_and_attributes.projection_expression.as_deref(),
                    ExpressionKind::Projection,
                )],
                keys_and_attributes.expression_attribute_names.as_ref(),
                None,
                false,
            )?;
        }

        // The request as a whole may not mix legacy and expression parameters
        // across its tables.
        if self
            .request_items
            .values()
            .any(|keys| keys.attributes_to_get.is_some())
            && self
                .request_items
                .values()
                .any(|keys| keys.projection_expression.is_some())
        {
            return Err(mixed_parameters_error(
                "AttributesToGet",
                &["ProjectionExpression"],
                false,
            ));
        }

        if total_requests > 100 {
            return Err("Too many items requested. Maximum allowed: 100".to_string());
        }

        Ok(())
    }
}

/// Duplicate keys fail the request before any table is looked up. Numeric
/// key values compare by their canonical number.
fn validate_batch_get_keys_distinct(keys: &[KeyAttributes]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::with_capacity(keys.len());
    for key in keys {
        let mut identity = Vec::with_capacity(key.len());
        for (name, value) in key.iter() {
            let rendered = match value {
                crate::AttributeValue::S(text) => format!("S{text}"),
                crate::AttributeValue::B(text) => format!("B{text}"),
                crate::AttributeValue::N(number) => format!(
                    "N{}",
                    crate::canonical_dynamodb_number(number).unwrap_or_else(|_| number.clone())
                ),
                _ => return Ok(()),
            };
            identity.push((name.to_owned(), rendered));
        }
        identity.sort();
        if !seen.insert(identity) {
            return Err("Provided list of item keys contains duplicates".to_string());
        }
    }
    Ok(())
}

fn validate_projection_expression_size(expression: Option<&str>) -> Result<(), String> {
    if let Some(expression) = expression
        && expression.len() > crate::MAX_EXPRESSION_BYTES
    {
        return Err(
            "Invalid ProjectionExpression: Expression size has exceeded the maximum allowed size;"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_expression_size_for_label(
    expression: Option<&str>,
    label: &str,
    prefixed: bool,
    include_actual_size: bool,
) -> Result<(), String> {
    let Some(expression) = expression else {
        return Ok(());
    };
    if expression.len() <= crate::MAX_EXPRESSION_BYTES {
        return Ok(());
    }

    let mut message =
        format!("Invalid {label}: Expression size has exceeded the maximum allowed size;");
    if include_actual_size {
        message.push_str(&format!(" expression size: {}", expression.len()));
    }
    if prefixed {
        Err(format!("1 validation error detected: {message}"))
    } else {
        Err(message)
    }
}

/// PutItem reports item defects with the validation-count prefix; the batch
/// and transactional writes carry the bare text.
fn prefixed_item_validation_message(message: String) -> String {
    if message == "The parameter cannot be converted to a numeric value" {
        return "1 validation error detected: The parameter cannot be converted to a numeric \
                value: "
            .to_string();
    }
    if message.starts_with("The parameter cannot be converted to a numeric value: ") {
        return format!("1 validation error detected: {message}");
    }
    if message == "Attempting to store more than 38 significant digits in a Number"
        || message
            == "Number underflow. Attempting to store a number with magnitude smaller than \
                supported range"
        || message
            == "Number overflow. Attempting to store a number with magnitude larger than supported \
                range"
        || message == crate::dynamodb_limits::NESTING_LEVELS_MESSAGE
        || message == crate::dynamodb_limits::ATTRIBUTE_NAME_TOO_LARGE_MESSAGE
        || message.starts_with("One or more parameter values were invalid:")
    {
        return format!("1 validation error detected: {message}");
    }
    message
}

impl DynamoRequestValidate for TransactWriteItemsRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        validate_transact_write_items_request(self)
    }
}

impl DynamoRequestValidate for TransactGetItemsRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        if self.transact_items.is_empty() {
            return Err("TransactItems cannot be empty".to_string());
        }
        if self.transact_items.len() > crate::constants::MAX_TRANSACTION_MEMBERS {
            return Err(
                "1 validation error detected: Value at 'TransactItems' failed to satisfy \
                 constraint: Member must have length less than or equal to 100"
                    .to_string(),
            );
        }
        for (item_index, item) in self.transact_items.iter().enumerate() {
            validate_table_name_at(
                &item.get.table_name,
                &format!("transactItems.{}.member.get.tableName", item_index + 1),
            )?;
            validate_key_not_empty(&item.get.key)?;
            validate_key_set_operands(&item.get.key, false)?;
            validate_projection_expression_size(item.get.projection_expression.as_deref())?;
            validate_expression_attribute_name_keys(
                item.get.expression_attribute_names.as_ref(),
                false,
            )?;
            validate_expression_set(
                [(
                    item.get.projection_expression.as_deref(),
                    ExpressionKind::Projection,
                )],
                item.get.expression_attribute_names.as_ref(),
                None,
                false,
            )?;
        }
        Ok(())
    }
}

impl DynamoRequestValidate for ReadSequenceRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        self.validate().map_err(|error| error.to_string())
    }
}

fn validate_table_name_not_empty(table_name: &TableName) -> Result<(), String> {
    if table_name.as_ref().is_empty() {
        return Err("TableName cannot be empty".to_string());
    }
    Ok(())
}

fn validate_table_name(table_name: &TableName) -> Result<(), String> {
    validate_table_name_at(table_name, "tableName")
}

/// A table name of the wrong length or alphabet is a shape violation of the
/// member at `path`; every violated constraint is listed.
fn validate_table_name_at(table_name: &TableName, path: &str) -> Result<(), String> {
    validate_table_name_not_empty(table_name)?;
    let name = table_name.as_ref();
    let resource = table_name.dynamodb_resource_name();
    let mut constraints = smallvec::SmallVec::<[&str; 2]>::new();
    if resource.len() < 3 {
        constraints.push("Member must have length greater than or equal to 3");
    } else if resource.len() > 255 {
        constraints.push("Member must have length less than or equal to 255");
    }
    if !resource
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        constraints.push("Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+");
    }
    if constraints.is_empty() {
        return Ok(());
    }
    let count = constraints.len();
    let plural = if count == 1 { "" } else { "s" };
    let violations = constraints
        .iter()
        .map(|constraint| {
            format!("Value '{name}' at '{path}' failed to satisfy constraint: {constraint}")
        })
        .collect::<Vec<_>>()
        .join("; ");
    Err(format!(
        "{count} validation error{plural} detected: {violations}"
    ))
}

/// AWS shape-checks a set-typed key operand before matching the key schema,
/// so its empty or repeated members are reported first.
fn validate_key_set_operands(key: &KeyAttributes, prefixed: bool) -> Result<(), String> {
    for (_, value) in key.iter() {
        if matches!(
            value,
            crate::AttributeValue::SS(_)
                | crate::AttributeValue::NS(_)
                | crate::AttributeValue::BS(_)
        ) {
            crate::validate_attribute_value_for_write(value, "Key")
                .map_err(|message| validation_message(message, prefixed))?;
        }
    }
    Ok(())
}

fn validate_key_not_empty(key: &KeyAttributes) -> Result<(), String> {
    if key.is_empty() {
        return Err("Key cannot be empty".to_string());
    }
    Ok(())
}

/// AWS names every too-short global and local index name of a CreateTable in
/// one message, global indexes first. Only two or more aggregate; a single one
/// keeps its own text.
fn create_table_index_name_violations(value: &serde_json::Value) -> Option<String> {
    let mut violations = Vec::new();
    for (member, path) in [
        ("GlobalSecondaryIndexes", "globalSecondaryIndexes"),
        ("LocalSecondaryIndexes", "localSecondaryIndexes"),
    ] {
        let Some(indexes) = value.get(member).and_then(serde_json::Value::as_array) else {
            continue;
        };
        for (position, index) in indexes.iter().enumerate() {
            let name = index.get("IndexName").and_then(serde_json::Value::as_str);
            if let Some(name) =
                name.filter(|name| name.len() < crate::constants::MIN_INDEX_NAME_BYTES)
            {
                violations.push(format!(
                    "Value '{name}' at '{path}.{}.member.indexName' failed to satisfy constraint: \
                     Member must have length greater than or equal to {}",
                    position + 1,
                    crate::constants::MIN_INDEX_NAME_BYTES
                ));
            }
        }
    }
    (violations.len() > 1).then(|| {
        format!(
            "{} validation errors detected: {}",
            violations.len(),
            violations.join("; ")
        )
    })
}

impl TryFrom<serde_json::Value> for CreateTableRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        if let Some(message) = create_table_index_name_violations(&value) {
            return Err(message);
        }
        validate_attribute_definition_types(&value)?;
        let request: CreateTableRequest =
            serde_json::from_value(value).map_err(|e| format!("Invalid request format: {e}"))?;

        if request.table_name.as_ref().is_empty() || request.table_name.as_ref().trim().is_empty() {
            return Err("TableName cannot be empty".to_string());
        }

        if request.table_name.as_ref().len() < 3 || request.table_name.as_ref().len() > 255 {
            return Err(format!(
                "1 validation error detected: Value '{}' at 'tableName' failed to satisfy \
                 constraint: Member must have length greater than or equal to 3",
                request.table_name
            ));
        }

        if !request
            .table_name
            .as_ref()
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
        {
            return Err(format!(
                "1 validation error detected: Value '{}' at 'tableName' failed to satisfy \
                 constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
                request.table_name
            ));
        }

        if request.attribute_definitions.is_empty() {
            return Err("AttributeDefinitions cannot be empty".to_string());
        }
        validate_attribute_definitions(&request.attribute_definitions)?;

        for ad in &request.attribute_definitions {
            validate_attribute_name(&ad.attribute_name, || {
                "AttributeName in AttributeDefinitions".into()
            })?;

            if !ad
                .attribute_name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
            {
                return Err(
                    "AttributeName in AttributeDefinitions contains invalid characters".to_string(),
                );
            }
        }

        validate_table_key_schema(&request.key_schema, &request.attribute_definitions)?;

        if let Some(gsi) = &request.global_secondary_indexes
            && gsi.len() > 20
        {
            return Err("Cannot have more than 20 Global Secondary Indexes".to_string());
        }
        if let Some(gsis) = &request.global_secondary_indexes {
            for gsi in gsis {
                validate_index_name(&gsi.index_name, "IndexName")?;
                validate_index_key_schema(&gsi.key_schema, &request.attribute_definitions)?;
                for key in &gsi.key_schema {
                    validate_attribute_name(&key.attribute_name, || {
                        "GSI KeySchema AttributeName".into()
                    })?;
                }
                validate_projection_attributes(&gsi.projection)?;
            }
        }
        if let Some(lsis) = &request.local_secondary_indexes {
            for lsi in lsis {
                validate_index_name(&lsi.index_name, "IndexName")?;
                for key in &lsi.key_schema {
                    validate_attribute_name(&key.attribute_name, || {
                        "LSI KeySchema AttributeName".into()
                    })?;
                }
                validate_projection_attributes(&lsi.projection)?;
            }
        }
        validate_projected_attribute_limit(
            request.global_secondary_indexes.as_deref(),
            request.local_secondary_indexes.as_deref(),
        )?;
        validate_stream_specification(request.stream_specification.as_ref())?;
        validate_attribute_definitions_all_used(&request)?;

        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for ListTablesRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        reject_unknown_fields(&value, LIST_TABLES_FIELDS)?;

        let request: ListTablesRequest =
            serde_json::from_value(value).map_err(|e| format!("Invalid request format: {e}"))?;
        if let Some(limit) = request.limit
            && (limit == 0 || limit > MAX_LIST_TABLES_LIMIT)
        {
            return Err(format!(
                "Limit must be between 1 and {MAX_LIST_TABLES_LIMIT}"
            ));
        }

        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for DeleteTableRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request: DeleteTableRequest =
            serde_json::from_value(value).map_err(|e| format!("Invalid request format: {e}"))?;

        if request.table_name.as_ref().is_empty() {
            return Err("TableName cannot be empty".to_string());
        }

        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for DescribeTableRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request: DescribeTableRequest =
            serde_json::from_value(value).map_err(|e| format!("Invalid request format: {e}"))?;

        if request.table_name.as_ref().is_empty() {
            return Err("TableName cannot be empty".to_string());
        }

        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for PutItemRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request = deserialize_item_write_request::<PutItemRequest>(value)?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for GetItemRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request =
            deserialize_request_with_return_consumed_capacity::<GetItemRequest>(value, true)?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for DeleteItemRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request = deserialize_item_write_request::<DeleteItemRequest>(value)?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
}

/// AWS names every violated constraint of an UpdateItem in one message, in this
/// fixed member order. Only two or more violations aggregate; a single one
/// keeps its own text.
fn update_item_constraint_violations(value: &serde_json::Value) -> Option<String> {
    const RETURN_VALUES: [&str; 5] = ["ALL_NEW", "UPDATED_OLD", "ALL_OLD", "NONE", "UPDATED_NEW"];
    let text = |name: &str| value.get(name).and_then(serde_json::Value::as_str);
    let mut violations = Vec::new();
    if let Some(capacity) = text("ReturnConsumedCapacity")
        .filter(|capacity| !crate::ReturnConsumedCapacity::VALID_WIRE_VALUES.contains(capacity))
    {
        violations.push(format!(
            "Value '{capacity}' at 'returnConsumedCapacity' failed to satisfy constraint: Member \
             must satisfy enum value set: [{}]",
            crate::ReturnConsumedCapacity::VALID_WIRE_VALUES.join(", ")
        ));
    }
    if text("TableName") == Some("") {
        violations.push(
            "Value '' at 'tableName' failed to satisfy constraint: Member must have length \
             greater than or equal to 1"
                .to_owned(),
        );
    }
    if let Some(returned) =
        text("ReturnValues").filter(|returned| !RETURN_VALUES.contains(returned))
    {
        violations.push(format!(
            "Value '{returned}' at 'returnValues' failed to satisfy constraint: Member must \
             satisfy enum value set: [{}]",
            RETURN_VALUES.join(", ")
        ));
    }
    if value.get("Key").is_none_or(serde_json::Value::is_null) {
        violations.push(
            "Value null at 'key' failed to satisfy constraint: Member must not be null".to_owned(),
        );
    }
    (violations.len() > 1).then(|| {
        format!(
            "{} validation errors detected: {}",
            violations.len(),
            violations.join("; ")
        )
    })
}

impl TryFrom<serde_json::Value> for UpdateItemRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        if let Some(message) = update_item_constraint_violations(&value) {
            return Err(message);
        }
        let request =
            deserialize_request_with_return_consumed_capacity::<UpdateItemRequest>(value, false)?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for ScanRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request =
            deserialize_request_with_return_consumed_capacity::<ScanRequest>(value, true)?;

        request.validate_for_dynamodb()?;

        Ok(request)
    }
}

impl DynamoRequestValidate for ScanRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        validate_table_name(&self.table_name)?;
        let expressions = present_expression_parameters([
            (self.projection_expression.is_some(), "ProjectionExpression"),
            (self.filter_expression.is_some(), "FilterExpression"),
        ]);
        reject_legacy_expression_mixing(
            &[
                (self.attributes_to_get.is_some(), "AttributesToGet"),
                (self.scan_filter.is_some(), "ScanFilter"),
                (self.conditional_operator.is_some(), "ConditionalOperator"),
            ],
            &expressions,
        )?;
        reject_legacy_parameter(
            self.conditional_operator.is_some(),
            "ConditionalOperator",
            &expressions,
            false,
            || conditional_operator_alone(false),
        )?;
        reject_legacy_parameter(
            self.scan_filter.is_some(),
            "ScanFilter",
            &expressions,
            false,
            || "ScanFilter is not supported; use FilterExpression".to_owned(),
        )?;
        validate_scan_attributes_to_get_source(
            self.attributes_to_get.as_deref(),
            self.projection_expression.is_some(),
        )?;
        if self.segment.is_some() || self.total_segments.is_some() {
            return Err("Parallel Scan is not supported".to_string());
        }
        validate_select(
            self.select.as_deref(),
            self.index_name.is_some(),
            self.projection_expression
                .is_some()
                .then_some("ProjectionExpression")
                .or_else(|| {
                    self.attributes_to_get
                        .is_some()
                        .then_some("AttributesToGet")
                }),
            "Scanning",
        )?;

        for (expression, label) in [
            (self.filter_expression.as_deref(), "FilterExpression"),
            (
                self.projection_expression.as_deref(),
                "ProjectionExpression",
            ),
        ] {
            validate_expression_size_for_label(expression, label, false, false)?;
        }
        validate_expression_attribute_name_keys(self.expression_attribute_names.as_ref(), false)?;
        validate_expression_attribute_value_keys(self.expression_attribute_values.as_ref(), false)?;
        validate_expression_set(
            [
                (self.filter_expression.as_deref(), ExpressionKind::Filter),
                (
                    self.projection_expression.as_deref(),
                    ExpressionKind::Projection,
                ),
            ],
            self.expression_attribute_names.as_ref(),
            self.expression_attribute_values.as_ref(),
            false,
        )?;

        Ok(())
    }
}

impl TryFrom<serde_json::Value> for QueryRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request =
            deserialize_request_with_return_consumed_capacity::<QueryRequest>(value, true)?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for GetStreamRecordsRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        reject_unknown_fields(&value, GET_STREAM_RECORDS_FIELDS)?;
        let request = GetStreamRecordsRequest::from_json_value(value)?;

        match (request.table_name(), request.is_system_stream()) {
            (Some(table_name), false) if table_name.as_ref().is_empty() => {
                return Err("TableName cannot be empty".to_string());
            }
            (Some(_), false) | (None, true) => {}
            (Some(_), true) => {
                return Err("TableName and SystemStream cannot be used together".to_string());
            }
            (None, false) => {
                return Err("TableName or SystemStream is required".to_string());
            }
        }

        if let Some(limit) = request.limit() {
            let maximum = if request.is_system_stream() {
                crate::SYSTEM_STREAM_RECORDS_LIMIT_MAX
            } else {
                DYNAMODB_STREAM_RECORDS_LIMIT_MAX
            };
            if !(DYNAMODB_STREAM_RECORDS_LIMIT_MIN..=maximum).contains(&limit) {
                return Err(format!("Limit must be between 1 and {maximum}"));
            }
        }
        request.validate().map_err(|error| error.to_string())?;
        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for ListStreamsRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        reject_unknown_fields(&value, LIST_STREAMS_FIELDS)?;
        let request: ListStreamsRequest =
            serde_json::from_value(value).map_err(|e| format!("Invalid request format: {e}"))?;

        if let Some(table_name) = &request.table_name {
            validate_table_name_not_empty(table_name)?;
        }
        if let Some(limit) = request.limit
            && limit > 100
        {
            return Err(
                "One or more parameter values were invalid: Limit cannot be greater than the \
                 maximum allowed size: 100"
                    .to_string(),
            );
        }
        if request.limit == Some(0) {
            return Err("Limit must be between 1 and 100".to_string());
        }
        if request
            .exclusive_start_stream_arn
            .as_ref()
            .is_some_and(|arn| arn.is_empty())
        {
            return Err("ExclusiveStartStreamArn cannot be empty".to_string());
        }

        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for DescribeStreamRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        reject_unknown_fields(&value, DESCRIBE_STREAM_FIELDS)?;
        let request: DescribeStreamRequest =
            serde_json::from_value(value).map_err(|e| format!("Invalid request format: {e}"))?;

        if request.stream_arn.is_empty() {
            return Err("StreamArn cannot be empty".to_string());
        }
        if let Some(limit) = request.limit
            && !(1..=100).contains(&limit)
        {
            return Err("Limit must be between 1 and 100".to_string());
        }
        if request
            .exclusive_start_shard_id
            .as_ref()
            .is_some_and(|shard_id| shard_id.is_empty())
        {
            return Err("ExclusiveStartShardId cannot be empty".to_string());
        }

        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for DescribeSystemStreamRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        if !value.is_object() {
            return Err("Invalid request format: expected a JSON object".to_string());
        }
        reject_unknown_fields(&value, DESCRIBE_SYSTEM_STREAM_FIELDS)?;
        serde_json::from_value(value).map_err(|error| format!("Invalid request format: {error}"))
    }
}

impl TryFrom<serde_json::Value> for GetShardIteratorRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        reject_unknown_fields(&value, GET_SHARD_ITERATOR_FIELDS)?;
        let request: GetShardIteratorRequest =
            serde_json::from_value(value).map_err(|e| format!("Invalid request format: {e}"))?;

        if request.stream_arn.is_empty() {
            return Err("StreamArn cannot be empty".to_string());
        }
        if request.shard_id.is_empty() {
            return Err("ShardId cannot be empty".to_string());
        }
        match request.shard_iterator_type.as_str() {
            "TRIM_HORIZON" | "LATEST" => {
                if request.sequence_number.is_some() {
                    return Err("SequenceNumber is only valid with AT_SEQUENCE_NUMBER or \
                                AFTER_SEQUENCE_NUMBER"
                        .to_string());
                }
            }
            "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER" => {
                if request
                    .sequence_number
                    .as_ref()
                    .is_none_or(String::is_empty)
                {
                    return Err("One or more parameter values were invalid: SequenceNumber \
                                cannot be null"
                        .to_string());
                }
            }
            _ => {
                return Err(format!(
                    "Value '{}' at 'shardIteratorType' failed to satisfy constraint: Member must \
                     satisfy enum value set: [TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, \
                     AFTER_SEQUENCE_NUMBER]",
                    request.shard_iterator_type
                ));
            }
        }

        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for GetRecordsRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        reject_unknown_fields(&value, GET_RECORDS_FIELDS)?;
        let request: GetRecordsRequest =
            serde_json::from_value(value).map_err(|e| format!("Invalid request format: {e}"))?;

        if request.shard_iterator.is_empty() {
            return Err("ShardIterator cannot be empty".to_string());
        }
        if let Some(limit) = request.limit
            && !(DYNAMODB_STREAM_RECORDS_LIMIT_MIN..=DYNAMODB_STREAM_RECORDS_LIMIT_MAX)
                .contains(&limit)
        {
            return Err(DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE.to_string());
        }

        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for BatchWriteItemRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request = deserialize_request_with_return_consumed_capacity::<BatchWriteItemRequest>(
            value, true,
        )?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
}

impl DynamoRequestValidate for BatchWriteItemRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        validate_enum_value(
            self.return_item_collection_metrics.as_deref(),
            "returnItemCollectionMetrics",
            &["SIZE", "NONE"],
            true,
        )?;
        if self.request_items.is_empty() {
            return Err("RequestItems cannot be empty".to_string());
        }
        let mut total_requests = 0;
        for (table_name, write_requests) in &self.request_items {
            if table_name.as_ref().is_empty() {
                return Err("TableName cannot be empty".to_string());
            }
            if write_requests.is_empty() {
                return Err(format!(
                    "RequestItems for table '{table_name}' cannot be empty"
                ));
            }
            total_requests += write_requests.len();
            for request in write_requests {
                validate_batch_write_member(request)?;
            }
        }
        if total_requests > 25 {
            return Err("Too many items requested. Maximum allowed: 25".to_string());
        }
        Ok(())
    }
}

fn validate_batch_write_member(request: &crate::WriteRequest) -> Result<(), String> {
    if request.put_request.is_some() == request.delete_request.is_some() {
        return Err(
            "WriteRequest must contain exactly one of PutRequest or DeleteRequest".to_string(),
        );
    }
    if let Some(put) = &request.put_request {
        put.item
            .validate_for_write("PutRequest Item")
            .map_err(|message| {
                // A map key of "" inside a batch put is reported by concept.
                if message.ends_with("nested attribute name cannot be empty") {
                    "Empty attribute name".to_string()
                } else {
                    message
                }
            })?;
    }
    if let Some(delete) = &request.delete_request
        && delete.key.is_empty()
    {
        return Err("DeleteRequest Key cannot be empty".to_string());
    }
    Ok(())
}

impl TryFrom<serde_json::Value> for BatchGetItemRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request =
            deserialize_request_with_return_consumed_capacity::<BatchGetItemRequest>(value, true)?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for TransactWriteItemsRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        if let Some(message) = transact_write_shape_violations(&value) {
            return Err(message);
        }
        let request = deserialize_request_with_return_consumed_capacity::<TransactWriteItemsRequest>(
            value, true,
        )?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
}

/// Top-level shape violations that AWS reports before any member is looked at:
/// an overlength token hides every other violation, and an action naming no
/// operation is listed together with an invalid ReturnConsumedCapacity.
fn transact_write_shape_violations(value: &serde_json::Value) -> Option<String> {
    let token_too_long = value
        .get("ClientRequestToken")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|token| {
            token
                .chars()
                .take(DYNAMODB_CLIENT_REQUEST_TOKEN_MAX_CHARS + 1)
                .count()
                > DYNAMODB_CLIENT_REQUEST_TOKEN_MAX_CHARS
        });
    if token_too_long {
        return Some(
            "1 validation error detected: Value at 'ClientRequestToken' failed to satisfy \
             constraint: Member must have length less than or equal to 36"
                .to_owned(),
        );
    }
    let empty_action = value
        .get("TransactItems")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.as_object().is_some_and(|members| {
                    !["Put", "Update", "Delete", "ConditionCheck"]
                        .iter()
                        .any(|operation| members.contains_key(*operation))
                })
            })
        });
    let invalid_capacity = value
        .get("ReturnConsumedCapacity")
        .and_then(serde_json::Value::as_str)
        .filter(|capacity| !crate::ReturnConsumedCapacity::VALID_WIRE_VALUES.contains(capacity));
    let (true, Some(capacity)) = (empty_action, invalid_capacity) else {
        return None;
    };
    Some(format!(
        "2 validation errors detected: Value '' at 'transactItems' failed to satisfy constraint: \
         TransactWriteRequest should contain Delete or Put or Update or ConditionCheck; Value \
         '{capacity}' at 'returnConsumedCapacity' failed to satisfy constraint: Member must \
         satisfy enum value set: [{}]",
        crate::ReturnConsumedCapacity::VALID_WIRE_VALUES.join(", ")
    ))
}

impl TryFrom<serde_json::Value> for TransactGetItemsRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        validate_transaction_request_size(&value, "TransactGetItems request")?;
        let request = deserialize_request_with_return_consumed_capacity::<TransactGetItemsRequest>(
            value, true,
        )?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
}

pub fn validate_client_request_token(token: Option<&str>) -> Result<(), String> {
    let Some(token) = token else {
        return Ok(());
    };
    // Smithy string length counts Unicode scalar values, not UTF-8 bytes.
    let length = token
        .chars()
        .take(DYNAMODB_CLIENT_REQUEST_TOKEN_MAX_CHARS + 1)
        .count();
    if !(1..=DYNAMODB_CLIENT_REQUEST_TOKEN_MAX_CHARS).contains(&length) {
        return Err("ClientRequestToken must contain between 1 and 36 characters".to_string());
    }
    Ok(())
}

fn validate_transact_write_items_request(
    request: &TransactWriteItemsRequest,
) -> Result<(), String> {
    validate_client_request_token(request.client_request_token.as_deref())?;
    validate_enum_value(
        request.return_item_collection_metrics.as_deref(),
        "returnItemCollectionMetrics",
        &["SIZE", "NONE"],
        true,
    )?;

    if request.transact_items.is_empty() {
        return Err("TransactItems cannot be empty".to_string());
    }

    if request.transact_items.len() > crate::constants::MAX_TRANSACTION_MEMBERS {
        return Err("TransactItems cannot contain more than 100 operations".to_string());
    }

    let mut payload_bytes = 0_u64;
    for (item_index, item) in request.transact_items.iter().enumerate() {
        let operation_count = u8::from(item.put.is_some())
            + u8::from(item.update.is_some())
            + u8::from(item.delete.is_some())
            + u8::from(item.condition_check.is_some());

        if operation_count == 0 {
            return Err(
                "Invalid Request: TransactWriteRequest should contain Delete or Put or Update \
                 request"
                    .to_string(),
            );
        }
        if operation_count > 1 {
            return Err(
                "TransactItems can only contain one of Check, Put, Update or Delete".to_string(),
            );
        }
        if let Some(put) = &item.put {
            validate_transact_write_return_values_on_condition_check_failure(
                put.return_values_on_condition_check_failure.as_deref(),
                "put",
                item_index,
            )?;
            validate_table_name_at(
                &put.table_name,
                &format!("transactItems.{}.member.put.tableName", item_index + 1),
            )?;
            payload_bytes += u64::from(put.item.validate_for_write("TransactItems Put Item")?);
            // A Put member reports the PutItem text; the recorded
            // ConditionCheck text carries the size.
            validate_expression_size_for_label(
                put.condition_expression.as_deref(),
                "ConditionExpression",
                false,
                false,
            )?;
            validate_expression_attribute_name_keys(
                put.expression_attribute_names.as_ref(),
                false,
            )?;
            validate_transact_expression_attribute_value_keys(
                put.expression_attribute_values.as_ref(),
            )?;
            validate_transact_expression_attribute_value_payloads(
                put.expression_attribute_values.as_ref(),
            )?;
            // Unlike PutItem, a transactional member accepts placeholders
            // without an expression.
            if put.condition_expression.is_some() {
                validate_expression_set(
                    [(
                        put.condition_expression.as_deref(),
                        ExpressionKind::Condition,
                    )],
                    put.expression_attribute_names.as_ref(),
                    put.expression_attribute_values.as_ref(),
                    false,
                )?;
            }
        }
        if let Some(update) = &item.update {
            validate_transact_write_return_values_on_condition_check_failure(
                update.return_values_on_condition_check_failure.as_deref(),
                "update",
                item_index,
            )?;
            validate_table_name_at(
                &update.table_name,
                &format!("transactItems.{}.member.update.tableName", item_index + 1),
            )?;
            validate_key_not_empty(&update.key)?;
            validate_expression_size_for_label(
                Some(&update.update_expression),
                "UpdateExpression",
                false,
                false,
            )?;
            validate_expression_size_for_label(
                update.condition_expression.as_deref(),
                "ConditionExpression",
                false,
                true,
            )?;
            validate_expression_attribute_name_keys(
                update.expression_attribute_names.as_ref(),
                false,
            )?;
            validate_transact_expression_attribute_value_keys(
                update.expression_attribute_values.as_ref(),
            )?;
            validate_transact_expression_attribute_value_payloads(
                update.expression_attribute_values.as_ref(),
            )?;
            validate_expression_set(
                [
                    (
                        Some(update.update_expression.as_str()),
                        ExpressionKind::Update,
                    ),
                    (
                        update.condition_expression.as_deref(),
                        ExpressionKind::Condition,
                    ),
                ],
                update.expression_attribute_names.as_ref(),
                update.expression_attribute_values.as_ref(),
                false,
            )?;
        }
        if let Some(delete) = &item.delete {
            validate_transact_write_return_values_on_condition_check_failure(
                delete.return_values_on_condition_check_failure.as_deref(),
                "delete",
                item_index,
            )?;
            validate_table_name_at(
                &delete.table_name,
                &format!("transactItems.{}.member.delete.tableName", item_index + 1),
            )?;
            validate_key_not_empty(&delete.key)?;
            validate_expression_size_for_label(
                delete.condition_expression.as_deref(),
                "ConditionExpression",
                false,
                true,
            )?;
            validate_expression_attribute_name_keys(
                delete.expression_attribute_names.as_ref(),
                false,
            )?;
            validate_transact_expression_attribute_value_keys(
                delete.expression_attribute_values.as_ref(),
            )?;
            validate_transact_expression_attribute_value_payloads(
                delete.expression_attribute_values.as_ref(),
            )?;
            // Unlike PutItem, a transactional member accepts placeholders
            // without an expression.
            if delete.condition_expression.is_some() {
                validate_expression_set(
                    [(
                        delete.condition_expression.as_deref(),
                        ExpressionKind::Condition,
                    )],
                    delete.expression_attribute_names.as_ref(),
                    delete.expression_attribute_values.as_ref(),
                    false,
                )?;
            }
        }
        if let Some(check) = &item.condition_check {
            validate_transact_write_return_values_on_condition_check_failure(
                check.return_values_on_condition_check_failure.as_deref(),
                "conditionCheck",
                item_index,
            )?;
            validate_table_name_at(
                &check.table_name,
                &format!(
                    "transactItems.{}.member.conditionCheck.tableName",
                    item_index + 1
                ),
            )?;
            validate_key_not_empty(&check.key)?;
            validate_expression_size_for_label(
                Some(&check.condition_expression),
                "ConditionExpression",
                false,
                true,
            )?;
            validate_expression_attribute_name_keys(
                check.expression_attribute_names.as_ref(),
                false,
            )?;
            validate_transact_expression_attribute_value_keys(
                check.expression_attribute_values.as_ref(),
            )?;
            validate_transact_expression_attribute_value_payloads(
                check.expression_attribute_values.as_ref(),
            )?;
            validate_expression_set(
                [(
                    Some(check.condition_expression.as_str()),
                    ExpressionKind::Condition,
                )],
                check.expression_attribute_names.as_ref(),
                check.expression_attribute_values.as_ref(),
                false,
            )?;
        }
    }

    if payload_bytes > crate::MAX_TRANSACTION_ITEM_BYTES {
        return Err(format!(
            "Transaction payload size cannot exceed 4MB. Payload Size: {payload_bytes}"
        ));
    }
    Ok(())
}

fn validate_transact_expression_attribute_value_payloads(
    values: Option<&crate::AttributeMap>,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    for (key, value) in values.iter() {
        if let Err(message) =
            crate::validate_attribute_value_for_write(value, "ExpressionAttributeValues")
        {
            return Err(transact_expression_attribute_value_error(
                key, value, &message,
            ));
        }
    }
    Ok(())
}

fn transact_expression_attribute_value_error(
    key: &str,
    value: &crate::AttributeValue,
    message: &str,
) -> String {
    if message == crate::dynamodb_limits::NESTING_LEVELS_MESSAGE {
        return message.to_owned();
    }
    // AWS reports a duplicate-bearing binary set unwrapped.
    if matches!(value, crate::AttributeValue::BS(_)) && message.contains("contains duplicates") {
        return message.to_owned();
    }
    let message = match value {
        crate::AttributeValue::NS(_)
            if message.ends_with("Input collection contains duplicates.") =>
        {
            "Input collection contains duplicates".to_string()
        }
        crate::AttributeValue::SS(values) if message.contains("contains duplicates") => {
            format!(
                "One or more parameter values were invalid: Input collection [{}] contains \
                 duplicates.",
                values.join(", ")
            )
        }
        _ => message.to_string(),
    };
    format!("ExpressionAttributeValues contains invalid value: {message} for key {key}")
}

fn validate_return_consumed_capacity(value: Option<&str>, prefixed: bool) -> Result<(), String> {
    validate_enum_value(
        value,
        "returnConsumedCapacity",
        &crate::ReturnConsumedCapacity::VALID_WIRE_VALUES,
        prefixed,
    )
}

fn validate_return_item_collection_metrics(
    value: Option<&str>,
    prefixed: bool,
) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if matches!(value, "SIZE" | "NONE") {
        return Ok(());
    }
    let message = "ReturnItemCollectionMetrics can only be SIZE or NONE";
    if prefixed {
        Err(format!("1 validation error detected: {message}"))
    } else {
        Err(message.to_string())
    }
}

fn validate_transact_write_return_values_on_condition_check_failure(
    value: Option<&str>,
    member_name: &str,
    item_index: usize,
) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if matches!(value, "ALL_OLD" | "NONE") {
        return Ok(());
    }
    validate_return_values_on_condition_check_failure(
        Some(value),
        &format!(
            "transactItems.{}.member.{member_name}.returnValuesOnConditionCheckFailure",
            item_index + 1
        ),
        true,
    )
}

fn validate_return_values_on_condition_check_failure(
    value: Option<&str>,
    field_path: &str,
    prefixed: bool,
) -> Result<(), String> {
    validate_enum_value(value, field_path, &["ALL_OLD", "NONE"], prefixed)
}

fn validate_enum_value(
    value: Option<&str>,
    field_path: &str,
    allowed_values: &[&str],
    prefixed: bool,
) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if allowed_values.contains(&value) {
        return Ok(());
    }
    let message = format!(
        "Value '{value}' at '{field_path}' failed to satisfy constraint: Member must satisfy enum \
         value set: [{}]",
        allowed_values.join(", ")
    );
    if prefixed {
        Err(format!("1 validation error detected: {message}"))
    } else {
        Err(message)
    }
}

/// A scalar key type is the only `AttributeType` AWS accepts; the position is
/// one-based.
fn validate_attribute_definition_types(value: &serde_json::Value) -> Result<(), String> {
    let Some(definitions) = value
        .get("AttributeDefinitions")
        .and_then(serde_json::Value::as_array)
    else {
        return Ok(());
    };
    for (position, definition) in definitions.iter().enumerate() {
        let Some(attribute_type) = definition
            .get("AttributeType")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        validate_enum_value(
            Some(attribute_type),
            &format!("attributeDefinitions.{}.member.attributeType", position + 1),
            &["B", "N", "S"],
            true,
        )?;
    }
    Ok(())
}

fn validate_stream_specification(
    specification: Option<&crate::StreamSpecification>,
) -> Result<(), String> {
    if let Some(specification) = specification
        && !specification.stream_enabled
        && specification.stream_view_type.is_some()
    {
        return Err(
            "One or more parameter values were invalid: Table is being created with a stream \
             disabled, UpdateViewType should not be specified"
                .to_string(),
        );
    }
    Ok(())
}

/// Every attribute definition must be a key of the table or of one of its
/// indexes.
fn validate_attribute_definitions_all_used(request: &CreateTableRequest) -> Result<(), String> {
    let gsi_keys = request
        .global_secondary_indexes
        .iter()
        .flatten()
        .flat_map(|index| &index.key_schema);
    let lsi_keys = request
        .local_secondary_indexes
        .iter()
        .flatten()
        .flat_map(|index| &index.key_schema);
    let used = |definition: &crate::AttributeDefinition| {
        request
            .key_schema
            .iter()
            .chain(gsi_keys.clone())
            .chain(lsi_keys.clone())
            .any(|key| key.attribute_name == definition.attribute_name)
    };
    if request.attribute_definitions.iter().all(used) {
        return Ok(());
    }
    Err(
        "One or more parameter values were invalid: Number of attributes in KeySchema does not \
         exactly match number of attributes defined in AttributeDefinitions"
            .to_string(),
    )
}

fn validate_projection_attributes(projection: &crate::Projection) -> Result<(), String> {
    let has_non_key = projection.non_key_attributes.is_some();
    match projection.projection_type {
        Some(crate::ProjectionType::KeysOnly) if has_non_key => {
            return Err(
                "One or more parameter values were invalid: ProjectionType is KEYS_ONLY, but \
                 NonKeyAttributes is specified"
                    .to_string(),
            );
        }
        Some(crate::ProjectionType::Include) if !has_non_key => {
            return Err(
                "One or more parameter values were invalid: ProjectionType is INCLUDE, but \
                 NonKeyAttributes is not specified"
                    .to_string(),
            );
        }
        _ => {}
    }
    if let Some(attributes) = &projection.non_key_attributes {
        for attribute in attributes {
            validate_attribute_name(attribute, || {
                "Projection NonKeyAttributes attribute name".into()
            })?;
        }
    }
    Ok(())
}

impl TryFrom<serde_json::Value> for UpdateTableRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        let request: UpdateTableRequest =
            serde_json::from_value(value).map_err(|e| format!("Invalid request format: {e}"))?;

        if request.table_name.as_ref().trim().is_empty() {
            return Err("TableName cannot be empty".to_string());
        }

        if let Some(updates) = &request.global_secondary_index_updates {
            if updates.is_empty() {
                return Err("GlobalSecondaryIndexUpdates cannot be empty".to_string());
            }
            if updates.len() > 1 {
                // DynamoDB allows multiple, but we can start with single for
                // simplicity and expand later; enforce at least
                // our minimum validation here
            }

            for (i, u) in updates.iter().enumerate() {
                let count = u8::from(u.create.is_some())
                    + u8::from(u.update.is_some())
                    + u8::from(u.delete.is_some());
                if count != 1 {
                    return Err(format!(
                        "GlobalSecondaryIndexUpdates[{i}] must contain exactly one of Create, \
                         Update, or Delete"
                    ));
                }
                if let Some(create) = &u.create {
                    if create.index_name.as_ref().trim().is_empty() {
                        return Err(format!(
                            "GlobalSecondaryIndexUpdates[{i}].Create.IndexName cannot be empty"
                        ));
                    }
                    if create.key_schema.is_empty() {
                        return Err(format!(
                            "GlobalSecondaryIndexUpdates[{i}].Create.KeySchema cannot be empty"
                        ));
                    }
                    validate_index_name_lazy(&create.index_name, || {
                        format!("GlobalSecondaryIndexUpdates[{i}].Create.IndexName")
                    })?;
                    validate_projection_attributes(&create.projection)?;
                }
                if let Some(update) = &u.update
                    && update.index_name.as_ref().trim().is_empty()
                {
                    return Err(format!(
                        "GlobalSecondaryIndexUpdates[{i}].Update.IndexName cannot be empty"
                    ));
                }
                if let Some(update) = &u.update {
                    validate_index_name_lazy(&update.index_name, || {
                        format!("GlobalSecondaryIndexUpdates[{i}].Update.IndexName")
                    })?;
                }
                if let Some(delete) = &u.delete
                    && delete.index_name.as_ref().trim().is_empty()
                {
                    return Err(format!(
                        "GlobalSecondaryIndexUpdates[{i}].Delete.IndexName cannot be empty"
                    ));
                }
                if let Some(delete) = &u.delete {
                    validate_index_name_lazy(&delete.index_name, || {
                        format!("GlobalSecondaryIndexUpdates[{i}].Delete.IndexName")
                    })?;
                }
            }
        }

        Ok(request)
    }
}
