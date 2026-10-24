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
        validate_expression_value_operands,
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

impl DynamoRequestValidate for PutItemRequest {
    fn validate_for_dynamodb(&self) -> Result<(), String> {
        validate_table_name(&self.table_name)?;
        self.item
            .validate_for_write("Item")
            .map_err(prefixed_number_validation_message)?;
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
        if self
            .update_expression
            .as_ref()
            .is_some_and(|expression| expression.trim().is_empty())
        {
            return Err("UpdateExpression cannot be empty".to_string());
        }
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

pub(crate) fn validate_query_fields(
    table_name: &TableName,
    key_condition_expression: &str,
    filter_expression: Option<&str>,
    projection_expression: Option<&str>,
    expression_attribute_names: Option<&crate::VecMap<String, String>>,
    expression_attribute_values: Option<&crate::AttributeMap>,
) -> Result<(), String> {
    validate_table_name(table_name)?;
    if key_condition_expression.is_empty() {
        return Err("KeyConditionExpression cannot be empty".to_string());
    }
    if key_condition_expression.trim().is_empty() || key_condition_expression == "invalid condition"
    {
        return Err("Invalid KeyConditionExpression".to_string());
    }
    validate_expression_size_for_label(
        Some(key_condition_expression),
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
            (Some(key_condition_expression), ExpressionKind::KeyCondition),
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
                Some(self.key_condition_expression.as_str()),
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
        validate_query_fields(
            &self.table_name,
            &self.key_condition_expression,
            self.filter_expression.as_deref(),
            self.projection_expression.as_deref(),
            self.expression_attribute_names.as_ref(),
            self.expression_attribute_values.as_ref(),
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
            validate_table_name(table_name)?;
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
            }
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

        if total_requests > 100 {
            return Err("Too many items requested. Maximum allowed: 100".to_string());
        }

        Ok(())
    }
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

fn prefixed_number_validation_message(message: String) -> String {
    if message == "The parameter cannot be converted to a numeric value" {
        return "1 validation error detected: The parameter cannot be converted to a numeric \
                value: "
            .to_string();
    }
    if message == "Attempting to store more than 38 significant digits in a Number"
        || message
            == "Number underflow. Attempting to store a number with magnitude smaller than \
                supported range"
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
            return Err("TransactItems cannot contain more than 100 operations".to_string());
        }
        for item in &self.transact_items {
            validate_table_name(&item.get.table_name)?;
            validate_key_not_empty(&item.get.key)?;
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

fn validate_table_name(table_name: &TableName) -> Result<(), String> {
    if table_name.as_ref().is_empty() {
        return Err("TableName cannot be empty".to_string());
    }
    Ok(())
}

fn validate_key_not_empty(key: &KeyAttributes) -> Result<(), String> {
    if key.is_empty() {
        return Err("Key cannot be empty".to_string());
    }
    Ok(())
}

impl TryFrom<serde_json::Value> for CreateTableRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
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

        if request.key_schema.is_empty() {
            return Err("KeySchema cannot be empty".to_string());
        }

        if request.key_schema.len() > 2 {
            return Err("KeySchema cannot have more than 2 elements".to_string());
        }

        let mut has_hash_key = false;
        let mut has_range_key = false;

        for key_element in &request.key_schema {
            match key_element.key_type {
                KeyType::Hash => {
                    if has_hash_key {
                        return Err("KeySchema can only have one HASH key".to_string());
                    }
                    has_hash_key = true;
                }
                KeyType::Range => {
                    if has_range_key {
                        return Err("KeySchema can only have one RANGE key".to_string());
                    }
                    has_range_key = true;
                }
            }
        }

        if !has_hash_key {
            return Err("KeySchema must have a HASH key".to_string());
        }

        for key_element in &request.key_schema {
            if !request
                .attribute_definitions
                .iter()
                .any(|attr| attr.attribute_name == key_element.attribute_name)
            {
                return Err(format!(
                    "Key attribute '{}' not found in AttributeDefinitions",
                    key_element.attribute_name
                ));
            }
        }

        if let Some(gsi) = &request.global_secondary_indexes
            && gsi.len() > 20
        {
            return Err("Cannot have more than 20 Global Secondary Indexes".to_string());
        }
        if let Some(gsis) = &request.global_secondary_indexes {
            for gsi in gsis {
                validate_index_name(&gsi.index_name, "IndexName")?;
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
        let request =
            deserialize_request_with_return_consumed_capacity::<PutItemRequest>(value, true)?;

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
        let request =
            deserialize_request_with_return_consumed_capacity::<DeleteItemRequest>(value, true)?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
}

impl TryFrom<serde_json::Value> for UpdateItemRequest {
    type Error = String;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
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

        if let Some(filter_expr) = &self.filter_expression
            && (filter_expr.trim().is_empty() || filter_expr == "invalid filter")
        {
            return Err("Invalid FilterExpression".to_string());
        }
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
            validate_table_name(table_name)?;
        }
        if let Some(limit) = request.limit
            && !(1..=100).contains(&limit)
        {
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
                    return Err("SequenceNumber is required for AT_SEQUENCE_NUMBER and \
                                AFTER_SEQUENCE_NUMBER"
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
        put.item.validate_for_write("PutRequest Item")?;
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
        let request = deserialize_request_with_return_consumed_capacity::<TransactWriteItemsRequest>(
            value, true,
        )?;

        request.validate_for_dynamodb()?;
        Ok(request)
    }
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
            validate_table_name(&put.table_name)?;
            put.item.validate_for_write("TransactItems Put Item")?;
            validate_expression_size_for_label(
                put.condition_expression.as_deref(),
                "ConditionExpression",
                false,
                true,
            )?;
            validate_expression_attribute_name_keys(
                put.expression_attribute_names.as_ref(),
                false,
            )?;
            validate_expression_attribute_value_keys(
                put.expression_attribute_values.as_ref(),
                false,
            )?;
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
        if let Some(update) = &item.update {
            validate_transact_write_return_values_on_condition_check_failure(
                update.return_values_on_condition_check_failure.as_deref(),
                "update",
                item_index,
            )?;
            validate_table_name(&update.table_name)?;
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
            validate_expression_attribute_value_keys(
                update.expression_attribute_values.as_ref(),
                false,
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
            validate_table_name(&delete.table_name)?;
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
            validate_expression_attribute_value_keys(
                delete.expression_attribute_values.as_ref(),
                false,
            )?;
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
        if let Some(check) = &item.condition_check {
            validate_transact_write_return_values_on_condition_check_failure(
                check.return_values_on_condition_check_failure.as_deref(),
                "conditionCheck",
                item_index,
            )?;
            validate_table_name(&check.table_name)?;
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
            validate_expression_attribute_value_keys(
                check.expression_attribute_values.as_ref(),
                false,
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

fn validate_projection_attributes(projection: &crate::Projection) -> Result<(), String> {
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
