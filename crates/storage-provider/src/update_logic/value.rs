use std::{borrow::Cow, collections::HashMap, sync::Arc};

use storage_types::{AttributeValue, StorageError, StorageResult};

use crate::update_logic::path::{get_attribute_value, resolve_document_path};

#[derive(Debug, Clone)]
pub(super) enum UpdateValuePlan {
    Placeholder(Arc<str>),
    Literal(AttributeValue),
    Path(Arc<str>),
    IfNotExists {
        path: Arc<str>,
        operand: Box<UpdateValuePlan>,
    },
    ListAppend {
        operand1: Box<UpdateValuePlan>,
        operand2: Box<UpdateValuePlan>,
    },
    Arithmetic {
        lhs: Box<UpdateValuePlan>,
        operator: ArithmeticOperator,
        rhs: Box<UpdateValuePlan>,
    },
}

#[derive(Debug, Clone)]
pub(super) enum SetFunctionPlan {
    IfNotExists {
        path: Arc<str>,
        operand: Box<UpdateValuePlan>,
    },
    ListAppend {
        operand1: Box<UpdateValuePlan>,
        operand2: Box<UpdateValuePlan>,
    },
}

#[derive(Debug, Clone)]
pub enum UpdateOperand {
    Value(AttributeValue),
    Path(Arc<str>),
    IfNotExists {
        path: Arc<str>,
        operand: Box<UpdateOperand>,
    },
    ListAppend {
        operand1: Box<UpdateOperand>,
        operand2: Box<UpdateOperand>,
    },
    Arithmetic {
        lhs: Box<UpdateOperand>,
        operator: ArithmeticOperator,
        rhs: Box<UpdateOperand>,
    },
}

#[derive(Debug, Clone)]
pub enum BoundUpdateOperand<'a> {
    Value(Cow<'a, AttributeValue>),
    Path(Arc<str>),
    IfNotExists {
        path: Arc<str>,
        operand: Box<BoundUpdateOperand<'a>>,
    },
    ListAppend {
        operand1: Box<BoundUpdateOperand<'a>>,
        operand2: Box<BoundUpdateOperand<'a>>,
    },
    Arithmetic {
        lhs: Box<BoundUpdateOperand<'a>>,
        operator: ArithmeticOperator,
        rhs: Box<BoundUpdateOperand<'a>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
}

pub(super) fn resolve_attribute_value_plan(
    value_expr: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<Result<UpdateValuePlan, SetFunctionPlan>> {
    if let Some((lhs, operator, rhs)) = split_top_level_arithmetic(value_expr) {
        let lhs = resolve_attribute_value_plan(lhs, expression_attribute_names)?.into_ok_plan();
        let rhs = resolve_attribute_value_plan(rhs, expression_attribute_names)?.into_ok_plan();
        return Ok(Ok(UpdateValuePlan::Arithmetic {
            lhs: Box::new(lhs?),
            operator,
            rhs: Box::new(rhs?),
        }));
    }

    if value_expr.starts_with(':') {
        return Ok(Ok(UpdateValuePlan::Placeholder(Arc::from(value_expr))));
    }

    if let Some(inner) = value_expr
        .strip_prefix("if_not_exists(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (path_part, operand_part) = split_top_level_comma_pair(inner).ok_or_else(|| {
            StorageError::validation(format!(
                "if_not_exists requires exactly 2 arguments: {value_expr}"
            ))
        })?;

        let path = Arc::from(resolve_document_path(
            path_part.trim(),
            expression_attribute_names,
        )?);
        let operand =
            resolve_attribute_value_plan(operand_part.trim(), expression_attribute_names)?
                .into_ok_plan()?;

        return Ok(Err(SetFunctionPlan::IfNotExists {
            path,
            operand: Box::new(operand),
        }));
    }

    if let Some(inner) = value_expr
        .strip_prefix("list_append(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (op1_part, op2_part) = split_top_level_comma_pair(inner).ok_or_else(|| {
            StorageError::validation(format!(
                "list_append requires exactly 2 arguments: {value_expr}"
            ))
        })?;

        let operand1 = resolve_attribute_value_plan(op1_part.trim(), expression_attribute_names)?
            .into_ok_plan()?;
        let operand2 = resolve_attribute_value_plan(op2_part.trim(), expression_attribute_names)?
            .into_ok_plan()?;

        return Ok(Err(SetFunctionPlan::ListAppend {
            operand1: Box::new(operand1),
            operand2: Box::new(operand2),
        }));
    }

    if is_document_path_expression(value_expr) {
        let path = Arc::from(resolve_document_path(
            value_expr.trim(),
            expression_attribute_names,
        )?);
        return Ok(Ok(UpdateValuePlan::Path(path)));
    }

    match serde_json::from_str::<serde_json::Value>(value_expr) {
        Ok(json_value) => {
            let value = match json_value {
                serde_json::Value::String(s) => AttributeValue::S(s),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        AttributeValue::N(i.to_string())
                    } else if let Some(f) = n.as_f64() {
                        AttributeValue::N(f.to_string())
                    } else {
                        return Err(StorageError::validation(format!("Invalid number: {n}")));
                    }
                }
                serde_json::Value::Bool(b) => AttributeValue::BOOL(b),
                _ => {
                    return Err(StorageError::validation(format!(
                        "Unsupported literal value: {value_expr}"
                    )));
                }
            };
            Ok(Ok(UpdateValuePlan::Literal(value)))
        }
        Err(_) => {
            if value_expr.starts_with(['{', '[', '"']) {
                return Err(StorageError::validation(format!(
                    "Invalid value expression: {value_expr}"
                )));
            }
            let path = Arc::from(resolve_document_path(
                value_expr.trim(),
                expression_attribute_names,
            )?);
            Ok(Ok(UpdateValuePlan::Path(path)))
        }
    }
}

fn is_document_path_expression(value_expr: &str) -> bool {
    let value_expr = value_expr.trim();
    if value_expr.starts_with('#') || value_expr.contains(['.', '[']) {
        return true;
    }
    let Some(first) = value_expr.as_bytes().first().copied() else {
        return false;
    };
    first.is_ascii_alphabetic()
        && !matches!(value_expr, "true" | "false" | "null")
        && !value_expr.starts_with("if_not_exists(")
        && !value_expr.starts_with("list_append(")
}

fn split_top_level_arithmetic(value_expr: &str) -> Option<(&str, ArithmeticOperator, &str)> {
    let mut paren_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (idx, ch) in value_expr.char_indices() {
        if in_string {
            escaped = ch == '\\' && !escaped;
            if ch == '"' && !escaped {
                in_string = false;
            }
            if ch != '\\' {
                escaped = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '+' | '-' if paren_depth == 0 && idx > 0 => {
                let lhs = value_expr.get(..idx)?.trim();
                let rhs = value_expr.get(idx + ch.len_utf8()..)?.trim();
                if lhs.is_empty() || rhs.is_empty() {
                    return None;
                }
                let operator = if ch == '+' {
                    ArithmeticOperator::Add
                } else {
                    ArithmeticOperator::Subtract
                };
                return Some((lhs, operator, rhs));
            }
            _ => {}
        }
    }
    None
}

fn split_top_level_comma_pair(value: &str) -> Option<(&str, &str)> {
    let mut paren_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut comma = None;
    for (idx, ch) in value.char_indices() {
        if in_string {
            escaped = ch == '\\' && !escaped;
            if ch == '"' && !escaped {
                in_string = false;
            }
            if ch != '\\' {
                escaped = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            ',' if paren_depth == 0 => {
                if comma.is_some() {
                    return None;
                }
                comma = Some(idx);
            }
            _ => {}
        }
    }
    let idx = comma?;
    Some((value.get(..idx)?, value.get(idx + 1..)?))
}

trait IntoUpdateValuePlan {
    fn into_ok_plan(self) -> StorageResult<UpdateValuePlan>;
}

impl IntoUpdateValuePlan for Result<UpdateValuePlan, SetFunctionPlan> {
    fn into_ok_plan(self) -> StorageResult<UpdateValuePlan> {
        match self {
            Ok(value) => Ok(value),
            Err(SetFunctionPlan::IfNotExists { path, operand }) => {
                Ok(UpdateValuePlan::IfNotExists { path, operand })
            }
            Err(SetFunctionPlan::ListAppend { operand1, operand2 }) => {
                Ok(UpdateValuePlan::ListAppend { operand1, operand2 })
            }
        }
    }
}

pub(super) fn bind_update_value_plan(
    plan: &UpdateValuePlan,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<UpdateOperand> {
    match plan {
        UpdateValuePlan::Placeholder(name) => Ok(UpdateOperand::Value(
            resolve_planned_attribute_value(name, expression_attribute_values)?,
        )),
        UpdateValuePlan::Literal(value) => Ok(UpdateOperand::Value(value.clone())),
        UpdateValuePlan::Path(path) => Ok(UpdateOperand::Path(Arc::clone(path))),
        UpdateValuePlan::IfNotExists { path, operand } => {
            let operand = bind_update_value_plan(operand, expression_attribute_values)?;
            Ok(UpdateOperand::IfNotExists {
                path: Arc::clone(path),
                operand: Box::new(operand),
            })
        }
        UpdateValuePlan::ListAppend { operand1, operand2 } => {
            let operand1 = bind_update_value_plan(operand1, expression_attribute_values)?;
            let operand2 = bind_update_value_plan(operand2, expression_attribute_values)?;
            Ok(UpdateOperand::ListAppend {
                operand1: Box::new(operand1),
                operand2: Box::new(operand2),
            })
        }
        UpdateValuePlan::Arithmetic { lhs, operator, rhs } => {
            let lhs = bind_update_value_plan(lhs, expression_attribute_values)?;
            let rhs = bind_update_value_plan(rhs, expression_attribute_values)?;
            Ok(UpdateOperand::Arithmetic {
                lhs: Box::new(lhs),
                operator: *operator,
                rhs: Box::new(rhs),
            })
        }
    }
}

pub(super) fn bind_borrowed_update_value_plan<'a>(
    plan: &UpdateValuePlan,
    expression_attribute_values: Option<&'a HashMap<String, AttributeValue>>,
) -> StorageResult<BoundUpdateOperand<'a>> {
    match plan {
        UpdateValuePlan::Placeholder(name) => Ok(BoundUpdateOperand::Value(Cow::Borrowed(
            resolve_planned_attribute_value_ref(name, expression_attribute_values)?,
        ))),
        UpdateValuePlan::Literal(value) => Ok(BoundUpdateOperand::Value(Cow::Owned(value.clone()))),
        UpdateValuePlan::Path(path) => Ok(BoundUpdateOperand::Path(Arc::clone(path))),
        UpdateValuePlan::IfNotExists { path, operand } => {
            let operand = bind_borrowed_update_value_plan(operand, expression_attribute_values)?;
            Ok(BoundUpdateOperand::IfNotExists {
                path: Arc::clone(path),
                operand: Box::new(operand),
            })
        }
        UpdateValuePlan::ListAppend { operand1, operand2 } => {
            let operand1 = bind_borrowed_update_value_plan(operand1, expression_attribute_values)?;
            let operand2 = bind_borrowed_update_value_plan(operand2, expression_attribute_values)?;
            Ok(BoundUpdateOperand::ListAppend {
                operand1: Box::new(operand1),
                operand2: Box::new(operand2),
            })
        }
        UpdateValuePlan::Arithmetic { lhs, operator, rhs } => {
            let lhs = bind_borrowed_update_value_plan(lhs, expression_attribute_values)?;
            let rhs = bind_borrowed_update_value_plan(rhs, expression_attribute_values)?;
            Ok(BoundUpdateOperand::Arithmetic {
                lhs: Box::new(lhs),
                operator: *operator,
                rhs: Box::new(rhs),
            })
        }
    }
}

pub(super) fn bind_scalar_update_value_plan(
    plan: &UpdateValuePlan,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<AttributeValue> {
    match bind_update_value_plan(plan, expression_attribute_values)? {
        UpdateOperand::Value(value) => Ok(value),
        _ => Err(StorageError::validation(
            "Update operation does not support document paths or function calls",
        )),
    }
}

pub(super) fn bind_borrowed_scalar_update_value_plan<'a>(
    plan: &UpdateValuePlan,
    expression_attribute_values: Option<&'a HashMap<String, AttributeValue>>,
) -> StorageResult<Cow<'a, AttributeValue>> {
    match bind_borrowed_update_value_plan(plan, expression_attribute_values)? {
        BoundUpdateOperand::Value(value) => Ok(value),
        _ => Err(StorageError::validation(
            "Update operation does not support document paths or function calls",
        )),
    }
}

fn resolve_planned_attribute_value(
    name: &str,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<AttributeValue> {
    let Some(values) = expression_attribute_values else {
        return Err(StorageError::validation(format!(
            "Attribute value {name} requires ExpressionAttributeValues"
        )));
    };
    values.get(name).cloned().ok_or_else(|| {
        StorageError::validation(format!(
            "Attribute value {name} not found in ExpressionAttributeValues"
        ))
    })
}

fn resolve_planned_attribute_value_ref<'a>(
    name: &str,
    expression_attribute_values: Option<&'a HashMap<String, AttributeValue>>,
) -> StorageResult<&'a AttributeValue> {
    let Some(values) = expression_attribute_values else {
        return Err(StorageError::validation(format!(
            "Attribute value {name} requires ExpressionAttributeValues"
        )));
    };
    values.get(name).ok_or_else(|| {
        StorageError::validation(format!(
            "Attribute value {name} not found in ExpressionAttributeValues"
        ))
    })
}

pub(super) fn evaluate_update_operand(
    item: &HashMap<String, AttributeValue>,
    operand: &UpdateOperand,
) -> StorageResult<AttributeValue> {
    match operand {
        UpdateOperand::Value(value) => Ok(value.clone()),
        UpdateOperand::Path(path) => get_attribute_value(item, path).cloned().ok_or_else(|| {
            StorageError::validation(
                "The provided expression refers to an attribute that does not exist in the item",
            )
        }),
        UpdateOperand::IfNotExists { path, operand } => {
            if let Some(value) = get_attribute_value(item, path) {
                Ok(value.clone())
            } else {
                evaluate_update_operand(item, operand)
            }
        }
        UpdateOperand::ListAppend { operand1, operand2 } => {
            let operand1 = evaluate_update_operand(item, operand1)?;
            let operand2 = evaluate_update_operand(item, operand2)?;
            list_append_values(&operand1, &operand2)
        }
        UpdateOperand::Arithmetic { lhs, operator, rhs } => {
            let lhs = evaluate_update_operand(item, lhs)?;
            let rhs = evaluate_update_operand(item, rhs)?;
            apply_set_arithmetic(&lhs, *operator, &rhs)
        }
    }
}

pub(super) fn evaluate_bound_update_operand(
    item: &HashMap<String, AttributeValue>,
    operand: &BoundUpdateOperand<'_>,
) -> StorageResult<AttributeValue> {
    match operand {
        BoundUpdateOperand::Value(value) => Ok(value.clone().into_owned()),
        BoundUpdateOperand::Path(path) => {
            get_attribute_value(item, path).cloned().ok_or_else(|| {
                StorageError::validation(
                    "The provided expression refers to an attribute that does not exist in the \
                     item",
                )
            })
        }
        BoundUpdateOperand::IfNotExists { path, operand } => {
            if let Some(value) = get_attribute_value(item, path) {
                Ok(value.clone())
            } else {
                evaluate_bound_update_operand(item, operand)
            }
        }
        BoundUpdateOperand::ListAppend { operand1, operand2 } => {
            let operand1 = evaluate_bound_update_operand(item, operand1)?;
            let operand2 = evaluate_bound_update_operand(item, operand2)?;
            list_append_values(&operand1, &operand2)
        }
        BoundUpdateOperand::Arithmetic { lhs, operator, rhs } => {
            let lhs = evaluate_bound_update_operand(item, lhs)?;
            let rhs = evaluate_bound_update_operand(item, rhs)?;
            apply_set_arithmetic(&lhs, *operator, &rhs)
        }
    }
}

pub(super) fn evaluate_bound_set_arithmetic(
    item: &HashMap<String, AttributeValue>,
    lhs: &BoundUpdateOperand<'_>,
    operator: ArithmeticOperator,
    rhs: &BoundUpdateOperand<'_>,
) -> StorageResult<AttributeValue> {
    let lhs = evaluate_bound_update_operand(item, lhs)?;
    let rhs = evaluate_bound_update_operand(item, rhs)?;
    apply_set_arithmetic(&lhs, operator, &rhs)
}

pub(super) fn list_append_values(
    operand1: &AttributeValue,
    operand2: &AttributeValue,
) -> StorageResult<AttributeValue> {
    let (AttributeValue::L(list1), AttributeValue::L(list2)) = (operand1, operand2) else {
        return Err(StorageError::validation(
            "An operand in the update expression has an incorrect data type",
        ));
    };
    let mut result = Vec::with_capacity(list1.len() + list2.len());
    result.extend(list1.iter().cloned());
    result.extend(list2.iter().cloned());
    Ok(AttributeValue::L(result))
}

fn apply_set_arithmetic(
    lhs: &AttributeValue,
    operator: ArithmeticOperator,
    rhs: &AttributeValue,
) -> StorageResult<AttributeValue> {
    let (AttributeValue::N(lhs), AttributeValue::N(rhs)) = (lhs, rhs) else {
        return Err(StorageError::validation(
            "An operand in the update expression has an incorrect data type",
        ));
    };
    let value = match operator {
        ArithmeticOperator::Add => add_arithmetic_numbers(lhs, rhs)?,
        ArithmeticOperator::Subtract => subtract_arithmetic_numbers(lhs, rhs)?,
    };
    Ok(AttributeValue::N(value))
}

pub(super) fn add_arithmetic_numbers(lhs: &str, rhs: &str) -> StorageResult<String> {
    let lhs = DynamoDecimal::parse(lhs)?;
    let rhs = DynamoDecimal::parse(rhs)?;
    lhs.add(&rhs)?.format()
}

pub(super) fn subtract_arithmetic_numbers(lhs: &str, rhs: &str) -> StorageResult<String> {
    let lhs = DynamoDecimal::parse(lhs)?;
    let rhs = DynamoDecimal::parse(rhs)?;
    lhs.subtract(&rhs)?.format()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DynamoDecimal {
    negative: bool,
    digits: Vec<u8>,
    scale: i32,
}

impl DynamoDecimal {
    fn parse(value: &str) -> StorageResult<Self> {
        let (mantissa, exponent) = if let Some((mantissa, exponent)) = value.split_once(['e', 'E'])
        {
            let exponent = exponent.parse::<i32>().map_err(|_| {
                StorageError::validation(
                    "An operand in the update expression has an incorrect data type",
                )
            })?;
            (mantissa, exponent)
        } else {
            (value, 0)
        };

        let mut chars = mantissa.chars();
        let negative = matches!(chars.clone().next(), Some('-'));
        if matches!(chars.clone().next(), Some('-' | '+')) {
            chars.next();
        }

        let mut digits = Vec::new();
        let mut fractional_digits = 0i32;
        let mut after_decimal = false;
        let mut saw_digit = false;
        for ch in chars {
            match ch {
                '0'..='9' => {
                    saw_digit = true;
                    digits.push(ch as u8 - b'0');
                    if after_decimal {
                        fractional_digits += 1;
                    }
                }
                '.' if !after_decimal => after_decimal = true,
                _ => {
                    return Err(StorageError::validation(
                        "An operand in the update expression has an incorrect data type",
                    ));
                }
            }
        }

        if !saw_digit {
            return Err(StorageError::validation(
                "An operand in the update expression has an incorrect data type",
            ));
        }

        let scale = fractional_digits - exponent;
        let value = Self {
            negative,
            digits,
            scale,
        }
        .normalized();
        value.validate_range()?;
        Ok(value)
    }

    fn add(&self, rhs: &Self) -> StorageResult<Self> {
        let value = if self.negative == rhs.negative {
            Self {
                negative: self.negative,
                digits: add_abs_digits(&self.aligned_digits(rhs), &rhs.aligned_digits(self)),
                scale: self.scale.max(rhs.scale),
            }
        } else {
            self.subtract_abs_with_sign(rhs)
        }
        .normalized();
        value.validate_range()?;
        Ok(value)
    }

    fn subtract(&self, rhs: &Self) -> StorageResult<Self> {
        let value = if self.negative != rhs.negative {
            Self {
                negative: self.negative,
                digits: add_abs_digits(&self.aligned_digits(rhs), &rhs.aligned_digits(self)),
                scale: self.scale.max(rhs.scale),
            }
        } else {
            self.subtract_abs_with_sign(rhs)
        }
        .normalized();
        value.validate_range()?;
        Ok(value)
    }

    fn subtract_abs_with_sign(&self, rhs: &Self) -> Self {
        let scale = self.scale.max(rhs.scale);
        let lhs_digits = self.aligned_digits(rhs);
        let rhs_digits = rhs.aligned_digits(self);
        match compare_abs_digits(&lhs_digits, &rhs_digits) {
            std::cmp::Ordering::Greater => Self {
                negative: self.negative,
                digits: subtract_abs_digits(&lhs_digits, &rhs_digits),
                scale,
            },
            std::cmp::Ordering::Less => Self {
                negative: !self.negative,
                digits: subtract_abs_digits(&rhs_digits, &lhs_digits),
                scale,
            },
            std::cmp::Ordering::Equal => Self::zero(),
        }
    }

    fn aligned_digits(&self, other: &Self) -> Vec<u8> {
        let target_scale = self.scale.max(other.scale);
        let mut digits = self.digits.clone();
        digits.extend(std::iter::repeat_n(0, (target_scale - self.scale) as usize));
        digits
    }

    fn normalized(mut self) -> Self {
        let first_non_zero = self.digits.iter().position(|digit| *digit != 0);
        let Some(first_non_zero) = first_non_zero else {
            return Self::zero();
        };
        if first_non_zero > 0 {
            self.digits.drain(..first_non_zero);
        }
        while self.digits.len() > 1 && self.digits.last() == Some(&0) {
            self.digits.pop();
            self.scale -= 1;
        }
        self
    }

    fn validate_range(&self) -> StorageResult<()> {
        if self.is_zero() {
            return Ok(());
        }
        let adjusted_exponent = self.digits.len() as i32 - self.scale - 1;
        if adjusted_exponent > 125 {
            return Err(StorageError::validation(
                "Number overflow. Attempting to store a number with magnitude larger than \
                 supported range",
            ));
        }
        if adjusted_exponent < -130 {
            return Err(StorageError::validation(
                "Number underflow. Attempting to store a number with magnitude smaller than \
                 supported range",
            ));
        }
        Ok(())
    }

    fn format(&self) -> StorageResult<String> {
        self.validate_range()?;
        if self.is_zero() {
            return Ok("0".to_string());
        }

        let mut value = String::new();
        if self.negative {
            value.push('-');
        }
        if self.scale <= 0 {
            value.extend(self.digits.iter().map(|digit| char::from(b'0' + *digit)));
            value.extend(std::iter::repeat_n('0', (-self.scale) as usize));
            return Ok(value);
        }

        let scale = self.scale as usize;
        if self.digits.len() > scale {
            let split = self.digits.len() - scale;
            value.extend(
                self.digits[..split]
                    .iter()
                    .map(|digit| char::from(b'0' + *digit)),
            );
            value.push('.');
            value.extend(
                self.digits[split..]
                    .iter()
                    .map(|digit| char::from(b'0' + *digit)),
            );
        } else {
            value.push_str("0.");
            value.extend(std::iter::repeat_n('0', scale - self.digits.len()));
            value.extend(self.digits.iter().map(|digit| char::from(b'0' + *digit)));
        }
        Ok(value)
    }

    fn is_zero(&self) -> bool {
        self.digits == [0]
    }

    fn zero() -> Self {
        Self {
            negative: false,
            digits: vec![0],
            scale: 0,
        }
    }
}

fn add_abs_digits(lhs: &[u8], rhs: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(lhs.len().max(rhs.len()) + 1);
    let mut carry = 0u8;
    let mut lhs_index = lhs.len();
    let mut rhs_index = rhs.len();
    while lhs_index > 0 || rhs_index > 0 || carry > 0 {
        let lhs_digit = if lhs_index > 0 {
            lhs_index -= 1;
            lhs[lhs_index]
        } else {
            0
        };
        let rhs_digit = if rhs_index > 0 {
            rhs_index -= 1;
            rhs[rhs_index]
        } else {
            0
        };
        let sum = lhs_digit + rhs_digit + carry;
        result.push(sum % 10);
        carry = sum / 10;
    }
    result.reverse();
    result
}

fn subtract_abs_digits(lhs: &[u8], rhs: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(lhs.len());
    let mut borrow = 0i8;
    let mut lhs_index = lhs.len();
    let mut rhs_index = rhs.len();
    while lhs_index > 0 {
        lhs_index -= 1;
        let rhs_digit = if rhs_index > 0 {
            rhs_index -= 1;
            rhs[rhs_index] as i8
        } else {
            0
        };
        let mut digit = lhs[lhs_index] as i8 - borrow - rhs_digit;
        if digit < 0 {
            digit += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        result.push(digit as u8);
    }
    result.reverse();
    result
}

fn compare_abs_digits(lhs: &[u8], rhs: &[u8]) -> std::cmp::Ordering {
    let lhs_start = lhs
        .iter()
        .position(|digit| *digit != 0)
        .unwrap_or(lhs.len().saturating_sub(1));
    let rhs_start = rhs
        .iter()
        .position(|digit| *digit != 0)
        .unwrap_or(rhs.len().saturating_sub(1));
    lhs[lhs_start..]
        .len()
        .cmp(&rhs[rhs_start..].len())
        .then_with(|| lhs[lhs_start..].cmp(&rhs[rhs_start..]))
}

fn parse_add_number(field: &str, value: &str) -> StorageResult<DynamoDecimal> {
    DynamoDecimal::parse(value).map_err(|err| {
        if err.to_string().starts_with("Number ") {
            err
        } else {
            StorageError::validation(format!(
                "ADD operation requires a valid number for field {field}"
            ))
        }
    })
}

pub(super) fn add_add_expression_numbers(
    field: &str,
    lhs: &str,
    rhs: &str,
) -> StorageResult<String> {
    let lhs = parse_add_number(field, lhs)?;
    let rhs = parse_add_number(field, rhs)?;
    lhs.add(&rhs)?.format()
}

pub(super) fn validate_add_number(field: &str, value: &str) -> StorageResult<()> {
    parse_add_number(field, value).map(|_| ())
}
