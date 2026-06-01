use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
};

use storage_condition::{Condition, parse_condition_expression};
use storage_types::{
    AttributeValue, StorageError, StorageResult, TimestampMillis,
    subset_expression_attribute_values_for_expression,
};

#[allow(unused_imports)]
pub use crate::update_logic::value::{ArithmeticOperator, UpdateOperand};
use crate::update_logic::{
    path::{
        get_attribute_value, remove_attribute_value, resolve_document_path, set_attribute_value,
    },
    value::{
        BoundUpdateOperand, SetFunctionPlan, UpdateValuePlan, add_add_expression_numbers,
        bind_borrowed_scalar_update_value_plan, bind_borrowed_update_value_plan,
        bind_scalar_update_value_plan, bind_update_value_plan, evaluate_bound_set_arithmetic,
        evaluate_bound_update_operand, evaluate_update_operand, list_append_values,
        resolve_attribute_value_plan, validate_add_number,
    },
};

/// Represents an update operation parsed from an update expression
#[derive(Debug, Clone)]
pub enum UpdateOperation {
    Set {
        field: Arc<str>,
        value: AttributeValue,
    },
    SetExpression {
        field: Arc<str>,
        value: UpdateOperand,
    },
    SetIfNotExists {
        field: Arc<str>,
        path: Arc<str>,
        operand: AttributeValue,
    },
    SetListAppend {
        field: Arc<str>,
        operand1: AttributeValue,
        operand2: AttributeValue,
    },
    Add {
        field: Arc<str>,
        value: AttributeValue,
    },
    Remove {
        field: Arc<str>,
    },
    Delete {
        field: Arc<str>,
        value: AttributeValue,
    },
}

impl UpdateOperation {
    #[must_use]
    pub fn field_name(&self) -> &str {
        match self {
            UpdateOperation::Add { field, .. }
            | UpdateOperation::Delete { field, .. }
            | UpdateOperation::Set { field, .. }
            | UpdateOperation::SetExpression { field, .. }
            | UpdateOperation::SetIfNotExists { field, .. }
            | UpdateOperation::SetListAppend { field, .. }
            | UpdateOperation::Remove { field } => field,
        }
    }
}

#[derive(Debug, Clone)]
pub enum BoundUpdateOperation<'a> {
    Set {
        field: Arc<str>,
        value: Cow<'a, AttributeValue>,
    },
    SetExpression {
        field: Arc<str>,
        value: BoundUpdateOperand<'a>,
    },
    SetArithmetic {
        field: Arc<str>,
        lhs: BoundUpdateOperand<'a>,
        rhs: BoundUpdateOperand<'a>,
        operator: ArithmeticOperator,
    },
    SetIfNotExists {
        field: Arc<str>,
        path: Arc<str>,
        operand: Cow<'a, AttributeValue>,
    },
    SetListAppend {
        field: Arc<str>,
        operand1: Cow<'a, AttributeValue>,
        operand2: Cow<'a, AttributeValue>,
    },
    Add {
        field: Arc<str>,
        value: Cow<'a, AttributeValue>,
    },
    Remove {
        field: Arc<str>,
    },
    Delete {
        field: Arc<str>,
        value: Cow<'a, AttributeValue>,
    },
}

impl BoundUpdateOperation<'_> {
    #[must_use]
    pub fn field_name(&self) -> &str {
        match self {
            BoundUpdateOperation::Add { field, .. }
            | BoundUpdateOperation::Delete { field, .. }
            | BoundUpdateOperation::Set { field, .. }
            | BoundUpdateOperation::SetArithmetic { field, .. }
            | BoundUpdateOperation::SetExpression { field, .. }
            | BoundUpdateOperation::SetIfNotExists { field, .. }
            | BoundUpdateOperation::SetListAppend { field, .. }
            | BoundUpdateOperation::Remove { field } => field,
        }
    }

    #[must_use]
    pub fn field_name_arc(&self) -> Arc<str> {
        match self {
            BoundUpdateOperation::Add { field, .. }
            | BoundUpdateOperation::Delete { field, .. }
            | BoundUpdateOperation::Set { field, .. }
            | BoundUpdateOperation::SetArithmetic { field, .. }
            | BoundUpdateOperation::SetExpression { field, .. }
            | BoundUpdateOperation::SetIfNotExists { field, .. }
            | BoundUpdateOperation::SetListAppend { field, .. }
            | BoundUpdateOperation::Remove { field } => Arc::clone(field),
        }
    }
}

/// Split update expression by commas, but preserve commas inside function calls
#[must_use]
pub fn split_operations_preserving_functions(expression: &str) -> Vec<String> {
    let normalized = normalize_update_section_keywords(expression);
    split_operation_parts_preserving_functions(&normalized)
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn split_operation_parts_preserving_functions(expression: &str) -> Vec<&str> {
    let mut parts = Vec::with_capacity(4);
    let mut current_start = 0;
    let mut paren_depth = 0;
    let bytes = expression.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let ch = bytes[i] as char;
        match ch {
            '(' => {
                paren_depth += 1;
                i += 1;
            }
            ')' => {
                paren_depth -= 1;
                i += 1;
            }
            ',' => {
                if paren_depth == 0 {
                    if let Some(part) = expression.get(current_start..i).map(str::trim)
                        && !part.is_empty()
                    {
                        parts.push(part);
                    }
                    current_start = i + 1;
                }
                i += 1;
            }
            _ => {
                if paren_depth == 0
                    && ch == ' '
                    && let Some((keyword, advance)) = split_keyword(&bytes[i..])
                {
                    if let Some(part) = expression.get(current_start..i).map(str::trim)
                        && !part.is_empty()
                    {
                        parts.push(part);
                    }
                    current_start = i + advance - keyword.len();
                    i += advance;
                    continue;
                }
                i += 1;
            }
        }
    }

    let part = expression.get(current_start..).unwrap_or("").trim();
    if !part.is_empty() {
        parts.push(part);
    }

    parts
}

/// Parse an update expression into individual operations
pub fn parse_update_expression(
    update_expression: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<Vec<UpdateOperation>> {
    let plan = cached_update_expression_plan(update_expression, expression_attribute_names)?;
    bind_update_expression_plan(&plan, expression_attribute_values)
}

#[derive(Debug, Clone)]
struct CachedUpdateExpressionPlan {
    attribute_names: Vec<(String, String)>,
    plan: Arc<Vec<UpdateOperationPlan>>,
}

#[derive(Debug, Clone)]
enum UpdateOperationPlan {
    Set {
        field: Arc<str>,
        value: UpdateValuePlan,
    },
    Add {
        field: Arc<str>,
        value: UpdateValuePlan,
    },
    Remove {
        field: Arc<str>,
    },
    Delete {
        field: Arc<str>,
        value: UpdateValuePlan,
    },
}

fn cached_update_expression_plan(
    update_expression: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<Arc<Vec<UpdateOperationPlan>>> {
    static UPDATE_EXPRESSION_PLAN_CACHE: OnceLock<
        RwLock<HashMap<String, Vec<CachedUpdateExpressionPlan>>>,
    > = OnceLock::new();

    let cache = UPDATE_EXPRESSION_PLAN_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
    if let Ok(guard) = cache.read()
        && let Some(plans) = guard.get(update_expression)
        && let Some(cached) = plans.iter().find(|cached| {
            attribute_names_match(expression_attribute_names, &cached.attribute_names)
        })
    {
        return Ok(Arc::clone(&cached.plan));
    }

    let plan = Arc::new(parse_update_expression_plan(
        update_expression,
        expression_attribute_names,
    )?);
    if let Ok(mut guard) = cache.write() {
        let plans = guard.entry(update_expression.to_string()).or_default();
        if let Some(cached) = plans.iter().find(|cached| {
            attribute_names_match(expression_attribute_names, &cached.attribute_names)
        }) {
            return Ok(Arc::clone(&cached.plan));
        }
        plans.push(CachedUpdateExpressionPlan {
            attribute_names: sorted_attribute_names(expression_attribute_names),
            plan: Arc::clone(&plan),
        });
    }
    Ok(plan)
}

fn attribute_names_match(
    expression_attribute_names: Option<&HashMap<String, String>>,
    cached_names: &[(String, String)],
) -> bool {
    let Some(names) = expression_attribute_names else {
        return cached_names.is_empty();
    };
    names.len() == cached_names.len()
        && cached_names
            .iter()
            .all(|(key, value)| names.get(key) == Some(value))
}

fn parse_update_expression_plan(
    update_expression: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<Vec<UpdateOperationPlan>> {
    let normalized_expression = normalize_update_section_keywords(update_expression);
    let parts = split_operation_parts_preserving_functions(&normalized_expression);
    let mut operations = Vec::with_capacity(parts.len());

    for part in parts {
        // Parse SET operations
        if part == "SET" {
            return Err(StorageError::validation(
                "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"SET\"",
            ));
        } else if let Some(set_part) = part.strip_prefix("SET ") {
            let assignment = set_part.trim();
            if let Some((field, value_expr)) = split_assignment(assignment) {
                push_set_operation_plan(
                    &mut operations,
                    field,
                    value_expr,
                    expression_attribute_names,
                )?;
            } else {
                return Err(StorageError::validation(format!(
                    "Invalid SET operation: {assignment}"
                )));
            }
        }
        // Parse ADD operations
        else if let Some(add_part) = part.strip_prefix("ADD ") {
            let assignment = add_part.trim();
            if let Some((field, value_expr)) = assignment.split_once(' ') {
                let field = Arc::from(resolve_attribute_name(
                    field.trim(),
                    expression_attribute_names,
                )?);
                match resolve_attribute_value_plan(value_expr.trim(), expression_attribute_names)? {
                    Ok(value) => {
                        operations.push(UpdateOperationPlan::Add { field, value });
                    }
                    Err(_) => {
                        return Err(StorageError::validation(format!(
                            "ADD operation does not support function calls: {assignment}"
                        )));
                    }
                }
            } else {
                return Err(StorageError::validation(format!(
                    "Invalid ADD operation: {assignment}"
                )));
            }
        }
        // Parse REMOVE operations
        else if let Some(remove_part) = part.strip_prefix("REMOVE ") {
            let field = Arc::from(resolve_attribute_name(
                remove_part.trim(),
                expression_attribute_names,
            )?);
            operations.push(UpdateOperationPlan::Remove { field });
        }
        // Parse DELETE operations
        else if let Some(delete_part) = part.strip_prefix("DELETE ") {
            let assignment = delete_part.trim();
            if let Some((field, value_expr)) = assignment.split_once(' ') {
                let field = Arc::from(resolve_attribute_name(
                    field.trim(),
                    expression_attribute_names,
                )?);
                match resolve_attribute_value_plan(value_expr.trim(), expression_attribute_names)? {
                    Ok(value) => {
                        operations.push(UpdateOperationPlan::Delete { field, value });
                    }
                    Err(_) => {
                        return Err(StorageError::validation(format!(
                            "DELETE operation does not support function calls: {assignment}"
                        )));
                    }
                }
            } else {
                return Err(StorageError::validation(format!(
                    "Invalid DELETE operation: {assignment}"
                )));
            }
        }
        // Check if this looks like a valid operation continuation or an unknown operation
        else {
            // If it doesn't start with a known operation keyword, check if it could be a
            // continuation
            let could_be_continuation = (part.contains('=') && split_assignment(part).is_some()) || // SET-like
                                           (part.contains(' ') && !part.contains('=') && part.split_once(' ').is_some()) || // ADD-like
                                           (!part.contains(' ') && !part.contains('=')); // REMOVE-like

            if could_be_continuation && operations.is_empty() {
                // If we have no previous operations, this is likely an unknown operation
                return Err(StorageError::validation(format!(
                    "Unknown update operation: {part}"
                )));
            } else if could_be_continuation {
                // This could be a continuation of a previous operation
                if part.contains('=') {
                    if let Some((field, value_expr)) = split_assignment(part) {
                        push_set_operation_plan(
                            &mut operations,
                            field,
                            value_expr,
                            expression_attribute_names,
                        )?;
                    } else {
                        return Err(StorageError::validation(format!(
                            "Invalid SET operation continuation: {part}"
                        )));
                    }
                }
                // Try ADD operation continuation if it contains " " but not "="
                else if part.contains(' ') && !part.contains('=') {
                    if let Some((field, value_expr)) = part.split_once(' ') {
                        let field = Arc::from(resolve_attribute_name(
                            field.trim(),
                            expression_attribute_names,
                        )?);
                        match resolve_attribute_value_plan(
                            value_expr.trim(),
                            expression_attribute_names,
                        )? {
                            Ok(value) => {
                                operations.push(UpdateOperationPlan::Add { field, value });
                            }
                            Err(_) => {
                                return Err(StorageError::validation(format!(
                                    "ADD operation continuation does not support function calls: \
                                     {part}"
                                )));
                            }
                        }
                    } else {
                        return Err(StorageError::validation(format!(
                            "Invalid ADD operation continuation: {part}"
                        )));
                    }
                }
                // Otherwise, assume it's a REMOVE operation continuation
                else {
                    let field = Arc::from(resolve_attribute_name(
                        part.trim(),
                        expression_attribute_names,
                    )?);
                    operations.push(UpdateOperationPlan::Remove { field });
                }
            } else {
                return Err(StorageError::validation(format!(
                    "Unknown update operation: {part}"
                )));
            }
        }
    }

    Ok(operations)
}

fn bind_update_expression_plan(
    plan: &[UpdateOperationPlan],
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<Vec<UpdateOperation>> {
    let mut operations = Vec::with_capacity(plan.len());
    for operation in plan {
        let operation = match operation {
            UpdateOperationPlan::Set { field, value } => {
                match bind_update_value_plan(value, expression_attribute_values)? {
                    UpdateOperand::Value(value) => UpdateOperation::Set {
                        field: Arc::clone(field),
                        value,
                    },
                    UpdateOperand::IfNotExists { path, operand } => {
                        if let UpdateOperand::Value(operand) = *operand {
                            UpdateOperation::SetIfNotExists {
                                field: Arc::clone(field),
                                path,
                                operand,
                            }
                        } else {
                            UpdateOperation::SetExpression {
                                field: Arc::clone(field),
                                value: UpdateOperand::IfNotExists { path, operand },
                            }
                        }
                    }
                    UpdateOperand::ListAppend { operand1, operand2 } => {
                        if let (UpdateOperand::Value(operand1), UpdateOperand::Value(operand2)) =
                            (&*operand1, &*operand2)
                        {
                            UpdateOperation::SetListAppend {
                                field: Arc::clone(field),
                                operand1: operand1.clone(),
                                operand2: operand2.clone(),
                            }
                        } else {
                            UpdateOperation::SetExpression {
                                field: Arc::clone(field),
                                value: UpdateOperand::ListAppend { operand1, operand2 },
                            }
                        }
                    }
                    value => UpdateOperation::SetExpression {
                        field: Arc::clone(field),
                        value,
                    },
                }
            }
            UpdateOperationPlan::Add { field, value } => UpdateOperation::Add {
                field: Arc::clone(field),
                value: bind_scalar_update_value_plan(value, expression_attribute_values)?,
            },
            UpdateOperationPlan::Remove { field } => UpdateOperation::Remove {
                field: Arc::clone(field),
            },
            UpdateOperationPlan::Delete { field, value } => UpdateOperation::Delete {
                field: Arc::clone(field),
                value: bind_scalar_update_value_plan(value, expression_attribute_values)?,
            },
        };
        operations.push(operation);
    }
    Ok(operations)
}

fn bind_borrowed_update_expression_plan<'a>(
    plan: &[UpdateOperationPlan],
    expression_attribute_values: Option<&'a HashMap<String, AttributeValue>>,
) -> StorageResult<Vec<BoundUpdateOperation<'a>>> {
    let mut operations = Vec::with_capacity(plan.len());
    for operation in plan {
        let operation = match operation {
            UpdateOperationPlan::Set { field, value } => match value {
                UpdateValuePlan::Arithmetic { lhs, operator, rhs } => {
                    let lhs = bind_borrowed_update_value_plan(lhs, expression_attribute_values)?;
                    let rhs = bind_borrowed_update_value_plan(rhs, expression_attribute_values)?;
                    BoundUpdateOperation::SetArithmetic {
                        field: Arc::clone(field),
                        lhs,
                        rhs,
                        operator: *operator,
                    }
                }
                UpdateValuePlan::IfNotExists { path, operand }
                    if matches!(
                        operand.as_ref(),
                        UpdateValuePlan::Placeholder(_) | UpdateValuePlan::Literal(_)
                    ) =>
                {
                    BoundUpdateOperation::SetIfNotExists {
                        field: Arc::clone(field),
                        path: Arc::clone(path),
                        operand: bind_borrowed_scalar_update_value_plan(
                            operand,
                            expression_attribute_values,
                        )?,
                    }
                }
                UpdateValuePlan::ListAppend { operand1, operand2 }
                    if matches!(
                        operand1.as_ref(),
                        UpdateValuePlan::Placeholder(_) | UpdateValuePlan::Literal(_)
                    ) && matches!(
                        operand2.as_ref(),
                        UpdateValuePlan::Placeholder(_) | UpdateValuePlan::Literal(_)
                    ) =>
                {
                    BoundUpdateOperation::SetListAppend {
                        field: Arc::clone(field),
                        operand1: bind_borrowed_scalar_update_value_plan(
                            operand1,
                            expression_attribute_values,
                        )?,
                        operand2: bind_borrowed_scalar_update_value_plan(
                            operand2,
                            expression_attribute_values,
                        )?,
                    }
                }
                _ => match bind_borrowed_update_value_plan(value, expression_attribute_values)? {
                    BoundUpdateOperand::Value(value) => BoundUpdateOperation::Set {
                        field: Arc::clone(field),
                        value,
                    },
                    BoundUpdateOperand::IfNotExists { path, operand } => {
                        if let BoundUpdateOperand::Value(operand) = *operand {
                            BoundUpdateOperation::SetIfNotExists {
                                field: Arc::clone(field),
                                path,
                                operand,
                            }
                        } else {
                            BoundUpdateOperation::SetExpression {
                                field: Arc::clone(field),
                                value: BoundUpdateOperand::IfNotExists { path, operand },
                            }
                        }
                    }
                    BoundUpdateOperand::ListAppend { operand1, operand2 } => {
                        if let (
                            BoundUpdateOperand::Value(operand1),
                            BoundUpdateOperand::Value(operand2),
                        ) = (&*operand1, &*operand2)
                        {
                            BoundUpdateOperation::SetListAppend {
                                field: Arc::clone(field),
                                operand1: operand1.clone(),
                                operand2: operand2.clone(),
                            }
                        } else {
                            BoundUpdateOperation::SetExpression {
                                field: Arc::clone(field),
                                value: BoundUpdateOperand::ListAppend { operand1, operand2 },
                            }
                        }
                    }
                    value => BoundUpdateOperation::SetExpression {
                        field: Arc::clone(field),
                        value,
                    },
                },
            },
            UpdateOperationPlan::Add { field, value } => BoundUpdateOperation::Add {
                field: Arc::clone(field),
                value: bind_borrowed_scalar_update_value_plan(value, expression_attribute_values)?,
            },
            UpdateOperationPlan::Remove { field } => BoundUpdateOperation::Remove {
                field: Arc::clone(field),
            },
            UpdateOperationPlan::Delete { field, value } => BoundUpdateOperation::Delete {
                field: Arc::clone(field),
                value: bind_borrowed_scalar_update_value_plan(value, expression_attribute_values)?,
            },
        };
        operations.push(operation);
    }
    Ok(operations)
}

fn push_set_operation_plan(
    operations: &mut Vec<UpdateOperationPlan>,
    field: &str,
    value_expr: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<()> {
    let field = Arc::from(resolve_attribute_name(field, expression_attribute_names)?);
    match resolve_attribute_value_plan(value_expr, expression_attribute_names)? {
        Ok(value) => {
            operations.push(UpdateOperationPlan::Set { field, value });
        }
        Err(SetFunctionPlan::IfNotExists { path, operand }) => {
            operations.push(UpdateOperationPlan::Set {
                field,
                value: UpdateValuePlan::IfNotExists { path, operand },
            });
        }
        Err(SetFunctionPlan::ListAppend { operand1, operand2 }) => {
            operations.push(UpdateOperationPlan::Set {
                field,
                value: UpdateValuePlan::ListAppend { operand1, operand2 },
            });
        }
    }
    Ok(())
}

fn split_assignment(assignment: &str) -> Option<(&str, &str)> {
    let (field, value_expr) = assignment.split_once('=')?;
    Some((field.trim(), value_expr.trim()))
}

fn normalize_update_section_keywords(expression: &str) -> String {
    let mut normalized = String::with_capacity(expression.len());
    let mut index = 0;
    let mut paren_depth = 0usize;

    while index < expression.len() {
        let Some(ch) = expression.get(index..).and_then(|tail| tail.chars().next()) else {
            break;
        };
        match ch {
            '(' => {
                paren_depth += 1;
                normalized.push(ch);
                index += ch.len_utf8();
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                normalized.push(ch);
                index += ch.len_utf8();
            }
            _ if paren_depth == 0 => {
                if let Some((keyword, consumed)) = update_section_keyword_at(expression, index) {
                    while normalized.ends_with(char::is_whitespace) {
                        normalized.pop();
                    }
                    if !normalized.is_empty() && !normalized.ends_with(',') {
                        normalized.push(' ');
                    }
                    normalized.push_str(keyword);
                    normalized.push(' ');
                    index += consumed;
                    while expression
                        .get(index..)
                        .and_then(|tail| tail.chars().next())
                        .is_some_and(char::is_whitespace)
                    {
                        index += expression
                            .get(index..)
                            .and_then(|tail| tail.chars().next())
                            .map_or(0, char::len_utf8);
                    }
                    continue;
                }
                normalized.push(ch);
                index += ch.len_utf8();
            }
            _ => {
                normalized.push(ch);
                index += ch.len_utf8();
            }
        }
    }

    normalized
}

fn update_section_keyword_at(expression: &str, index: usize) -> Option<(&'static str, usize)> {
    if index > 0 {
        let previous = expression.get(..index)?.chars().next_back()?;
        if !previous.is_whitespace() && previous != ',' {
            return None;
        }
    }
    let remaining = expression.get(index..)?;
    for keyword in ["SET", "REMOVE", "ADD", "DELETE"] {
        let candidate = remaining.get(..keyword.len())?;
        if !candidate.eq_ignore_ascii_case(keyword) {
            continue;
        }
        let after_keyword = remaining.get(keyword.len()..)?;
        let Some(next) = after_keyword.chars().next() else {
            continue;
        };
        if next.is_whitespace() {
            return Some((keyword, keyword.len()));
        }
    }
    None
}

fn split_keyword(bytes: &[u8]) -> Option<(&'static str, usize)> {
    const UPDATE_SECTION_KEYWORDS: [(&[u8], &str); 4] = [
        (b" SET ", "SET "),
        (b" REMOVE ", "REMOVE "),
        (b" ADD ", "ADD "),
        (b" DELETE ", "DELETE "),
    ];

    for (pattern, keyword) in UPDATE_SECTION_KEYWORDS {
        if bytes.starts_with(pattern) {
            return Some((keyword, pattern.len()));
        }
    }
    None
}

fn sorted_attribute_names(
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> Vec<(String, String)> {
    let mut names = expression_attribute_names
        .into_iter()
        .flat_map(HashMap::iter)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    names.sort();
    names
}

/// Resolve attribute name from expression attribute names
fn resolve_attribute_name(
    name: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<String> {
    resolve_document_path(name, expression_attribute_names)
}

/// Represents a parsed function call in SET expressions
#[derive(Debug, Clone, PartialEq)]
pub enum SetFunction {
    IfNotExists {
        path: String,
        operand: AttributeValue,
    },
    ListAppend {
        operand1: AttributeValue,
        operand2: AttributeValue,
    },
}

/// Resolve attribute value from expression attribute values, handling function
/// calls
pub fn resolve_attribute_value(
    value_expr: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<Result<AttributeValue, SetFunction>> {
    // First, check if it's a reference to ExpressionAttributeValues
    if value_expr.starts_with(':') {
        if let Some(values) = expression_attribute_values {
            if let Some(value) = values.get(value_expr) {
                Ok(Ok(value.clone()))
            } else {
                Err(StorageError::validation(format!(
                    "Attribute value {value_expr} not found in ExpressionAttributeValues"
                )))
            }
        } else {
            Err(StorageError::validation(format!(
                "Attribute value {value_expr} requires ExpressionAttributeValues"
            )))
        }
    } else if let Some(inner) = value_expr
        .strip_prefix("if_not_exists(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (path_part, operand_part) = inner
            .split_once(',')
            .map(|(a, b)| (a.trim(), b.trim()))
            .ok_or_else(|| {
                StorageError::validation(format!(
                    "if_not_exists requires exactly 2 arguments: {value_expr}"
                ))
            })?;

        let path = resolve_attribute_name(path_part, expression_attribute_names)?;
        let Ok(operand) = resolve_attribute_value(
            operand_part,
            expression_attribute_names,
            expression_attribute_values,
        )?
        else {
            return Err(StorageError::validation(format!(
                "if_not_exists operand cannot be a function call: {value_expr}"
            )));
        };

        Ok(Err(SetFunction::IfNotExists { path, operand }))
    } else if let Some(inner) = value_expr
        .strip_prefix("list_append(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (op1_part, op2_part) = inner
            .split_once(',')
            .map(|(a, b)| (a.trim(), b.trim()))
            .ok_or_else(|| {
                StorageError::validation(format!(
                    "list_append requires exactly 2 arguments: {value_expr}"
                ))
            })?;

        let Ok(operand1) = resolve_attribute_value(
            op1_part,
            expression_attribute_names,
            expression_attribute_values,
        )?
        else {
            return Err(StorageError::validation(format!(
                "list_append operand1 cannot be a function call: {value_expr}"
            )));
        };

        let Ok(operand2) = resolve_attribute_value(
            op2_part,
            expression_attribute_names,
            expression_attribute_values,
        )?
        else {
            return Err(StorageError::validation(format!(
                "list_append operand2 cannot be a function call: {value_expr}"
            )));
        };

        Ok(Err(SetFunction::ListAppend { operand1, operand2 }))
    } else {
        // Try to parse as JSON literal value
        match serde_json::from_str::<serde_json::Value>(value_expr) {
            Ok(json_value) => {
                // Convert JSON value to AttributeValue
                match json_value {
                    serde_json::Value::String(s) => Ok(Ok(AttributeValue::S(s))),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Ok(AttributeValue::N(i.to_string())))
                        } else if let Some(f) = n.as_f64() {
                            Ok(Ok(AttributeValue::N(f.to_string())))
                        } else {
                            Err(StorageError::validation(format!("Invalid number: {n}")))
                        }
                    }
                    serde_json::Value::Bool(b) => Ok(Ok(AttributeValue::BOOL(b))),
                    _ => Err(StorageError::validation(format!(
                        "Unsupported literal value: {value_expr}"
                    ))),
                }
            }
            Err(_) => {
                // If JSON parsing fails, check if we have ExpressionAttributeValues
                if let Some(values) = expression_attribute_values {
                    if let Some(value) = values.get(value_expr) {
                        Ok(Ok(value.clone()))
                    } else {
                        Err(StorageError::validation(format!(
                            "Invalid value expression: {value_expr}"
                        )))
                    }
                } else {
                    Err(StorageError::validation(format!(
                        "Invalid value expression: {value_expr}"
                    )))
                }
            }
        }
    }
}

/// Apply update operations to an item
pub fn apply_update_operations(
    mut item: HashMap<String, AttributeValue>,
    operations: &[UpdateOperation],
) -> StorageResult<HashMap<String, AttributeValue>> {
    let timestamp_attr = existing_updated_at_attr(&item);
    let touches_updated_at = if timestamp_attr.is_some() {
        operations.iter().any(update_operation_touches_updated_at)
    } else {
        false
    };

    for operation in operations {
        match operation {
            UpdateOperation::Set { field, value } => {
                set_attribute_value(&mut item, field, value.clone())?;
            }
            UpdateOperation::SetExpression { field, value } => {
                let value = evaluate_update_operand(&item, value)?;
                set_attribute_value(&mut item, field, value)?;
            }
            UpdateOperation::SetIfNotExists {
                field,
                path,
                operand,
            } => {
                if let Some(existing_value) = get_attribute_value(&item, path).cloned() {
                    set_attribute_value(&mut item, field, existing_value)?;
                } else {
                    set_attribute_value(&mut item, field, operand.clone())?;
                }
            }
            UpdateOperation::SetListAppend {
                field,
                operand1,
                operand2,
            } => {
                let result = list_append_values(operand1, operand2)?;
                set_attribute_value(&mut item, field, result)?;
            }
            _ => {}
        }
    }

    for operation in operations {
        if let UpdateOperation::Remove { field } = operation {
            remove_attribute_value(&mut item, field)?;
        }
    }

    for operation in operations {
        if let UpdateOperation::Add { field, value } = operation {
            apply_add_operation(&mut item, field, value)?;
        }
    }

    for operation in operations {
        if let UpdateOperation::Delete { field, value } = operation {
            let Some(existing_value) = item.get_mut(field.as_ref()) else {
                continue;
            };
            let remove_attribute = apply_delete_set_operation(existing_value, value, field)?;
            if remove_attribute {
                item.remove(field.as_ref());
            }
        }
    }

    if let Some(timestamp_attr) = timestamp_attr
        && !touches_updated_at
    {
        item.remove(storage_types::single_table_entity::UPDATED_AT_ATTR);
        item.remove(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR);
        item.insert(
            timestamp_attr.to_string(),
            AttributeValue::N(TimestampMillis::now().timestamp_millis().to_string()),
        );
    }

    Ok(item)
}

/// Apply bound update operations that may borrow request attribute values.
pub fn apply_bound_update_operations(
    mut item: HashMap<String, AttributeValue>,
    operations: &[BoundUpdateOperation<'_>],
) -> StorageResult<HashMap<String, AttributeValue>> {
    let timestamp_attr = existing_updated_at_attr(&item);
    let touches_updated_at = if timestamp_attr.is_some() {
        {
            operations
                .iter()
                .any(bound_update_operation_touches_updated_at)
        }
    } else {
        false
    };

    for operation in operations {
        match operation {
            BoundUpdateOperation::Set { field, value } => {
                set_attribute_value(&mut item, field, value.clone().into_owned())?;
            }
            BoundUpdateOperation::SetExpression { field, value } => {
                let value = evaluate_bound_update_operand(&item, value)?;
                set_attribute_value(&mut item, field, value)?;
            }
            BoundUpdateOperation::SetArithmetic {
                field,
                lhs,
                rhs,
                operator,
            } => {
                let value = evaluate_bound_set_arithmetic(&item, lhs, *operator, rhs)?;
                set_attribute_value(&mut item, field, value)?;
            }
            BoundUpdateOperation::SetIfNotExists {
                field,
                path,
                operand,
            } => {
                if let Some(existing_value) = get_attribute_value(&item, path).cloned() {
                    set_attribute_value(&mut item, field, existing_value)?;
                } else {
                    set_attribute_value(&mut item, field, operand.clone().into_owned())?;
                }
            }
            BoundUpdateOperation::SetListAppend {
                field,
                operand1,
                operand2,
            } => {
                let result = list_append_values(operand1.as_ref(), operand2.as_ref())?;
                set_attribute_value(&mut item, field, result)?;
            }
            _ => {}
        }
    }

    for operation in operations {
        if let BoundUpdateOperation::Remove { field } = operation {
            remove_attribute_value(&mut item, field)?;
        }
    }

    for operation in operations {
        if let BoundUpdateOperation::Add { field, value } = operation {
            apply_add_operation(&mut item, field, value.as_ref())?;
        }
    }

    for operation in operations {
        if let BoundUpdateOperation::Delete { field, value } = operation {
            let Some(existing_value) = item.get_mut(field.as_ref()) else {
                continue;
            };
            let remove_attribute =
                apply_delete_set_operation(existing_value, value.as_ref(), field)?;
            if remove_attribute {
                item.remove(field.as_ref());
            }
        }
    }

    if let Some(timestamp_attr) = timestamp_attr
        && !touches_updated_at
    {
        item.remove(storage_types::single_table_entity::UPDATED_AT_ATTR);
        item.remove(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR);
        item.insert(
            timestamp_attr.to_string(),
            AttributeValue::N(TimestampMillis::now().timestamp_millis().to_string()),
        );
    }

    Ok(item)
}

fn existing_updated_at_attr(item: &HashMap<String, AttributeValue>) -> Option<&'static str> {
    if item.contains_key(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR) {
        return Some(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR);
    }
    item.contains_key(storage_types::single_table_entity::UPDATED_AT_ATTR)
        .then_some(storage_types::single_table_entity::UPDATED_AT_ATTR)
}

fn update_operation_touches_updated_at(operation: &UpdateOperation) -> bool {
    match operation {
        UpdateOperation::Set { field, .. }
        | UpdateOperation::SetExpression { field, .. }
        | UpdateOperation::SetIfNotExists { field, .. }
        | UpdateOperation::SetListAppend { field, .. }
        | UpdateOperation::Add { field, .. }
        | UpdateOperation::Remove { field }
        | UpdateOperation::Delete { field, .. } => field_touches_updated_at(field),
    }
}

fn bound_update_operation_touches_updated_at(operation: &BoundUpdateOperation<'_>) -> bool {
    field_touches_updated_at(operation.field_name())
}

fn field_touches_updated_at(field: &str) -> bool {
    let root = field
        .split_once(['.', '['])
        .map_or(field, |(root, _)| root)
        .trim();
    root == storage_types::single_table_entity::UPDATED_AT_ATTR
        || root == storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR
}

fn apply_add_operation(
    item: &mut HashMap<String, AttributeValue>,
    field: &str,
    value: &AttributeValue,
) -> StorageResult<()> {
    validate_add_operand(field, value)?;

    let Some(existing_value) = get_attribute_value(item, field) else {
        if update_field_is_top_level(field) {
            item.insert(field.to_string(), value.clone());
            return Ok(());
        }
        return set_attribute_value(item, field, value.clone());
    };
    let mut updated_value = existing_value.clone();

    match (&mut updated_value, value) {
        (AttributeValue::N(existing_num), AttributeValue::N(add_num)) => {
            *existing_num = add_add_expression_numbers(field, existing_num, add_num)?;
        }
        (AttributeValue::SS(existing_set), AttributeValue::SS(add_set)) => {
            append_unique_values(existing_set, add_set);
        }
        (AttributeValue::NS(existing_set), AttributeValue::NS(add_set)) => {
            append_unique_values(existing_set, add_set);
        }
        (AttributeValue::BS(existing_set), AttributeValue::BS(add_set)) => {
            append_unique_values(existing_set, add_set);
        }
        _ => {
            return Err(update_operand_type_error());
        }
    }

    set_attribute_value(item, field, updated_value)?;
    Ok(())
}

fn update_field_is_top_level(field: &str) -> bool {
    !field
        .as_bytes()
        .iter()
        .any(|byte| matches!(byte, b'.' | b'['))
}

fn validate_add_operand(field: &str, value: &AttributeValue) -> StorageResult<()> {
    match value {
        AttributeValue::N(num) => {
            validate_add_number(field, num)?;
            Ok(())
        }
        AttributeValue::SS(values) | AttributeValue::NS(values) | AttributeValue::BS(values) => {
            if values.is_empty() {
                return Err(StorageError::validation(format!(
                    "ADD operation requires a non-empty set operand for field {field}"
                )));
            }
            Ok(())
        }
        _ => Err(StorageError::validation(format!(
            "ADD operation requires a number or set operand for field {field}, got {}",
            attribute_value_type_name(value),
        ))),
    }
}

fn append_unique_values(existing: &mut Vec<String>, values_to_add: &[String]) {
    for value in values_to_add {
        if !existing.contains(value) {
            existing.push(value.clone());
        }
    }
}

fn apply_delete_set_operation(
    existing_value: &mut AttributeValue,
    delete_value: &AttributeValue,
    _field: &str,
) -> StorageResult<bool> {
    match (existing_value, delete_value) {
        (AttributeValue::SS(existing_set), AttributeValue::SS(delete_set)) => {
            remove_set_members(existing_set, delete_set);
            Ok(existing_set.is_empty())
        }
        (AttributeValue::NS(existing_set), AttributeValue::NS(delete_set)) => {
            remove_set_members(existing_set, delete_set);
            Ok(existing_set.is_empty())
        }
        (AttributeValue::BS(existing_set), AttributeValue::BS(delete_set)) => {
            remove_set_members(existing_set, delete_set);
            Ok(existing_set.is_empty())
        }
        _ => Err(update_operand_type_error()),
    }
}

fn update_operand_type_error() -> StorageError {
    StorageError::validation("An operand in the update expression has an incorrect data type")
}

fn remove_set_members(existing_set: &mut Vec<String>, delete_set: &[String]) {
    for item_to_delete in delete_set {
        existing_set.retain(|item| item != item_to_delete);
    }
}

fn attribute_value_type_name(value: &AttributeValue) -> &'static str {
    match value {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
        AttributeValue::BOOL(_) => "BOOL",
        AttributeValue::NULL(_) => "NULL",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
    }
}

/// Execute an update item operation with condition checking and retry logic
pub fn before_update_item<'a>(
    update_expression: &str,
    condition_expression: Option<&str>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&'a HashMap<String, AttributeValue>>,
) -> StorageResult<(Vec<BoundUpdateOperation<'a>>, Option<Condition>)> {
    before_update_item_optional(
        Some(update_expression),
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
    )
}

pub fn before_update_item_optional<'a>(
    update_expression: Option<&str>,
    condition_expression: Option<&str>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&'a HashMap<String, AttributeValue>>,
) -> StorageResult<(Vec<BoundUpdateOperation<'a>>, Option<Condition>)> {
    let operations = if let Some(update_expression) = update_expression {
        let plan = cached_update_expression_plan(update_expression, expression_attribute_names)?;
        bind_borrowed_update_expression_plan(&plan, expression_attribute_values)?
    } else {
        Vec::new()
    };

    let condition = if let Some(expr) = condition_expression {
        Some(
            cached_condition_expression(
                expr,
                expression_attribute_names,
                expression_attribute_values,
            )
            .map_err(StorageError::validation)?,
        )
    } else {
        None
    };

    Ok((operations, condition))
}

#[derive(Debug, Clone)]
struct CachedConditionExpression {
    attribute_names: Vec<(String, String)>,
    attribute_values: Vec<(String, AttributeValue)>,
    condition: Arc<Condition>,
}

fn cached_condition_expression(
    condition_expression: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> Result<Condition, String> {
    static CONDITION_EXPRESSION_CACHE: OnceLock<
        RwLock<HashMap<String, Vec<CachedConditionExpression>>>,
    > = OnceLock::new();

    let cache = CONDITION_EXPRESSION_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
    if let Ok(guard) = cache.read()
        && let Some(conditions) = guard.get(condition_expression)
        && let Some(cached) = conditions.iter().find(|cached| {
            attribute_names_match(expression_attribute_names, &cached.attribute_names)
                && condition_attribute_values_match(
                    expression_attribute_values,
                    &cached.attribute_values,
                )
        })
    {
        return Ok((*cached.condition).clone());
    }

    let condition = Arc::new(parse_condition_expression(
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
    )?);
    if let Ok(mut guard) = cache.write() {
        let conditions = guard.entry(condition_expression.to_string()).or_default();
        if let Some(cached) = conditions.iter().find(|cached| {
            attribute_names_match(expression_attribute_names, &cached.attribute_names)
                && condition_attribute_values_match(
                    expression_attribute_values,
                    &cached.attribute_values,
                )
        }) {
            return Ok((*cached.condition).clone());
        }
        conditions.push(CachedConditionExpression {
            attribute_names: sorted_attribute_names(expression_attribute_names),
            attribute_values: sorted_condition_attribute_values(
                condition_expression,
                expression_attribute_values,
            ),
            condition: Arc::clone(&condition),
        });
    }
    Ok((*condition).clone())
}

fn sorted_condition_attribute_values(
    condition_expression: &str,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> Vec<(String, AttributeValue)> {
    let values = subset_expression_attribute_values_for_expression(
        condition_expression,
        expression_attribute_values,
    );
    let mut values = values
        .as_ref()
        .into_iter()
        .flat_map(|values| values.iter())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.0.cmp(&right.0));
    values
}

fn condition_attribute_values_match(
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
    cached_values: &[(String, AttributeValue)],
) -> bool {
    cached_values.iter().all(|(key, value)| {
        expression_attribute_values.and_then(|values| values.get(key)) == Some(value)
    })
}
