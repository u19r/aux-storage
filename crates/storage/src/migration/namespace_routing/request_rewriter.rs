use std::collections::HashMap;

use storage_types::{
    AttributeValue, EncodeWriteRequest, KeyAttributes, QueryTableRequest, ScanTableRequest,
    StorageError, StorageResult, TableNamespace, TransactEncodeItem, TransactWriteItem,
    UpdateItemRequest, WireItem, WriteRequest,
};

const PARTITION_KEY_NAMES: [&str; 6] = ["pk", "gsi1pk", "gsi2pk", "gsi3pk", "gsi4pk", "gsi5pk"];

pub struct NamespaceRequestRewriter;

struct SharedTableRewriter {
    namespace_prefix: String,
}

impl SharedTableRewriter {
    fn new(namespace: &TableNamespace) -> Self {
        Self {
            namespace_prefix: namespace_partition_prefix(namespace),
        }
    }

    fn rewrite_item(&self, item: &mut HashMap<String, AttributeValue>) -> StorageResult<()> {
        let mut rewrote_pk = false;
        for key_name in PARTITION_KEY_NAMES {
            if let Some(value) = item.get_mut(key_name) {
                rewrite_partition_value(&self.namespace_prefix, value, key_name)?;
                if key_name == "pk" {
                    rewrote_pk = true;
                }
            }
        }
        if !rewrote_pk {
            return Err(StorageError::validation(
                "shared table routing failed closed: item missing rewritable partition key 'pk'",
            ));
        }
        Ok(())
    }

    fn normalize_item(&self, item: &mut HashMap<String, AttributeValue>) -> StorageResult<()> {
        for key_name in PARTITION_KEY_NAMES {
            if let Some(value) = item.get_mut(key_name) {
                normalize_partition_value(&self.namespace_prefix, value, key_name)?;
            }
        }
        Ok(())
    }

    fn rewrite_key(&self, key: &mut KeyAttributes) -> StorageResult<()> {
        let Some(pk) = key.get_mut("pk") else {
            return Err(StorageError::validation(
                "shared table routing failed closed: key missing rewritable partition key 'pk'",
            ));
        };
        rewrite_partition_value(&self.namespace_prefix, pk, "pk")
    }

    fn normalize_key(&self, key: &mut KeyAttributes) -> StorageResult<()> {
        let Some(pk) = key.get_mut("pk") else {
            return Err(StorageError::validation(
                "shared table routing failed closed: key missing rewritable partition key 'pk'",
            ));
        };
        normalize_partition_value(&self.namespace_prefix, pk, "pk")
    }

    fn rewrite_query(&self, request: &mut QueryTableRequest) -> StorageResult<()> {
        let (partition_name, placeholder) = parse_partition_key_condition(
            &request.key_condition_expression,
            request.expression_attribute_names.as_ref(),
        )?;

        let expected_partition = match request.index_name.as_ref() {
            Some(index_name) => format!("{index_name}pk"),
            None => "pk".to_string(),
        };
        if partition_name != expected_partition {
            return Err(StorageError::validation(format!(
                "shared table routing failed closed: expected partition key \
                 '{expected_partition}' in key condition, got '{partition_name}'"
            )));
        }

        let expression_values = request
            .expression_attribute_values
            .as_mut()
            .ok_or_else(|| {
                StorageError::validation(
                    "shared table routing failed closed: missing expression values for key \
                     condition",
                )
            })?;
        let value = expression_values.get_mut(&placeholder).ok_or_else(|| {
            StorageError::validation(format!(
                "shared table routing failed closed: missing key condition value '{placeholder}'"
            ))
        })?;
        rewrite_partition_value(&self.namespace_prefix, value, &partition_name)
    }

    fn rewrite_update(&self, request: &mut UpdateItemRequest) -> StorageResult<()> {
        self.rewrite_key(&mut request.key)?;
        if let Some(update_expression) = request.update_expression.as_ref() {
            rewrite_update_partition_assignments(
                &self.namespace_prefix,
                update_expression,
                request.expression_attribute_names.as_ref(),
                request.expression_attribute_values.as_mut(),
            )?;
        }
        rewrite_condition_partition_placeholders(
            &self.namespace_prefix,
            request.condition_expression.as_deref(),
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_mut(),
        )
    }
}

impl Default for NamespaceRequestRewriter {
    fn default() -> Self {
        Self::new()
    }
}

impl NamespaceRequestRewriter {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn rewrite_item_for_shared_table(
        &self,
        namespace: &TableNamespace,
        item: &mut HashMap<String, AttributeValue>,
    ) -> StorageResult<()> {
        SharedTableRewriter::new(namespace).rewrite_item(item)
    }

    pub fn rewrite_wire_item_for_shared_table(
        &self,
        namespace: &TableNamespace,
        item: &mut WireItem,
    ) -> StorageResult<()> {
        let mut map = item.clone().into_attribute_map()?;
        self.rewrite_item_for_shared_table(namespace, &mut map)?;
        *item = WireItem::from_attribute_map(&map)?;
        Ok(())
    }

    pub fn normalize_item_from_shared_table(
        &self,
        namespace: &TableNamespace,
        item: &mut HashMap<String, AttributeValue>,
    ) -> StorageResult<()> {
        SharedTableRewriter::new(namespace).normalize_item(item)
    }

    pub fn normalize_wire_item_from_shared_table(
        &self,
        namespace: &TableNamespace,
        item: &mut WireItem,
    ) -> StorageResult<()> {
        let mut map = item.clone().into_attribute_map()?;
        self.normalize_item_from_shared_table(namespace, &mut map)?;
        *item = WireItem::from_attribute_map(&map)?;
        Ok(())
    }

    pub fn rewrite_key_for_shared_table(
        &self,
        namespace: &TableNamespace,
        key: &mut KeyAttributes,
    ) -> StorageResult<()> {
        SharedTableRewriter::new(namespace).rewrite_key(key)
    }

    pub fn normalize_key_from_shared_table(
        &self,
        namespace: &TableNamespace,
        key: &mut KeyAttributes,
    ) -> StorageResult<()> {
        SharedTableRewriter::new(namespace).normalize_key(key)
    }

    pub fn rewrite_query_for_shared_table(
        &self,
        namespace: &TableNamespace,
        request: &mut QueryTableRequest,
    ) -> StorageResult<()> {
        SharedTableRewriter::new(namespace).rewrite_query(request)
    }

    pub fn rewrite_scan_for_shared_table(
        &self,
        _namespace: &TableNamespace,
        request: &mut ScanTableRequest,
    ) -> StorageResult<()> {
        if request.index_name.is_some() {
            return Err(StorageError::validation(
                "shared table routing failed closed: index scans are not allowed",
            ));
        }
        Err(StorageError::validation(
            "shared table routing failed closed: base table scans are not supported on routed \
             namespace tables",
        ))
    }

    pub fn rewrite_update_for_shared_table(
        &self,
        namespace: &TableNamespace,
        request: &mut UpdateItemRequest,
    ) -> StorageResult<()> {
        SharedTableRewriter::new(namespace).rewrite_update(request)
    }

    pub fn rewrite_condition_for_shared_table(
        &self,
        namespace: &TableNamespace,
        condition_expression: Option<&str>,
        expression_attribute_names: Option<&HashMap<String, String>>,
        expression_attribute_values: Option<&mut HashMap<String, AttributeValue>>,
    ) -> StorageResult<()> {
        rewrite_condition_partition_placeholders(
            &namespace_partition_prefix(namespace),
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        )
    }

    pub fn rewrite_write_request_for_shared_table(
        &self,
        namespace: &TableNamespace,
        request: &mut WriteRequest,
    ) -> StorageResult<()> {
        if let Some(put) = request.put_request.as_mut() {
            self.rewrite_item_for_shared_table(namespace, &mut put.item)?;
        }
        if let Some(delete) = request.delete_request.as_mut() {
            self.rewrite_key_for_shared_table(namespace, &mut delete.key)?;
        }
        Ok(())
    }

    pub fn rewrite_encode_write_request_for_shared_table(
        &self,
        namespace: &TableNamespace,
        request: &mut EncodeWriteRequest,
    ) -> StorageResult<()> {
        if let Some(put) = request.put_request.as_mut() {
            self.rewrite_wire_item_for_shared_table(namespace, &mut put.item)?;
        }
        if let Some(delete) = request.delete_request.as_mut() {
            self.rewrite_key_for_shared_table(namespace, &mut delete.key)?;
        }
        Ok(())
    }

    pub fn rewrite_transact_item_for_shared_table(
        &self,
        namespace: &TableNamespace,
        item: &mut TransactWriteItem,
    ) -> StorageResult<()> {
        if let Some(put) = item.put.as_mut() {
            self.rewrite_item_for_shared_table(namespace, &mut put.item)?;
        }
        if let Some(update) = item.update.as_mut() {
            self.rewrite_key_for_shared_table(namespace, &mut update.key)?;
            rewrite_update_partition_assignments(
                &namespace_partition_prefix(namespace),
                &update.update_expression,
                update.expression_attribute_names.as_ref(),
                update.expression_attribute_values.as_mut(),
            )?;
            rewrite_condition_partition_placeholders(
                &namespace_partition_prefix(namespace),
                update.condition_expression.as_deref(),
                update.expression_attribute_names.as_ref(),
                update.expression_attribute_values.as_mut(),
            )?;
        }
        if let Some(delete) = item.delete.as_mut() {
            self.rewrite_key_for_shared_table(namespace, &mut delete.key)?;
        }
        if let Some(check) = item.condition_check.as_mut() {
            self.rewrite_key_for_shared_table(namespace, &mut check.key)?;
            rewrite_condition_partition_placeholders(
                &namespace_partition_prefix(namespace),
                Some(check.condition_expression.as_str()),
                check.expression_attribute_names.as_ref(),
                check.expression_attribute_values.as_mut(),
            )?;
        }
        Ok(())
    }

    pub fn rewrite_transact_encode_item_for_shared_table(
        &self,
        namespace: &TableNamespace,
        item: &mut TransactEncodeItem,
    ) -> StorageResult<()> {
        if let Some(put) = item.put.as_mut() {
            self.rewrite_wire_item_for_shared_table(namespace, &mut put.item)?;
        }
        if let Some(update) = item.update.as_mut() {
            self.rewrite_key_for_shared_table(namespace, &mut update.key)?;
            rewrite_update_partition_assignments(
                &namespace_partition_prefix(namespace),
                &update.update_expression,
                update.expression_attribute_names.as_ref(),
                update.expression_attribute_values.as_mut(),
            )?;
            rewrite_condition_partition_placeholders(
                &namespace_partition_prefix(namespace),
                update.condition_expression.as_deref(),
                update.expression_attribute_names.as_ref(),
                update.expression_attribute_values.as_mut(),
            )?;
        }
        if let Some(delete) = item.delete.as_mut() {
            self.rewrite_key_for_shared_table(namespace, &mut delete.key)?;
        }
        if let Some(check) = item.condition_check.as_mut() {
            self.rewrite_key_for_shared_table(namespace, &mut check.key)?;
            rewrite_condition_partition_placeholders(
                &namespace_partition_prefix(namespace),
                Some(check.condition_expression.as_str()),
                check.expression_attribute_names.as_ref(),
                check.expression_attribute_values.as_mut(),
            )?;
        }
        Ok(())
    }
}

fn namespace_partition_prefix(namespace: &TableNamespace) -> String {
    format!("{}#", namespace.as_str())
}

fn rewrite_partition_value(
    prefix: &str,
    value: &mut AttributeValue,
    attribute_name: &str,
) -> StorageResult<()> {
    match value {
        AttributeValue::S(raw) => {
            if !raw.starts_with(prefix) {
                *raw = format!("{prefix}{raw}");
            }
            Ok(())
        }
        _ => Err(StorageError::validation(format!(
            "shared table routing failed closed: partition key '{attribute_name}' must be a \
             string value"
        ))),
    }
}

fn normalize_partition_value(
    prefix: &str,
    value: &mut AttributeValue,
    attribute_name: &str,
) -> StorageResult<()> {
    match value {
        AttributeValue::S(raw) => {
            let Some(stripped) = raw.strip_prefix(prefix) else {
                return Err(StorageError::validation(format!(
                    "shared table routing failed closed: partition key '{attribute_name}' must \
                     start with namespace prefix '{prefix}'"
                )));
            };
            *raw = stripped.to_string();
            Ok(())
        }
        _ => Err(StorageError::validation(format!(
            "shared table routing failed closed: partition key '{attribute_name}' must be a \
             string value"
        ))),
    }
}

fn parse_partition_key_condition(
    expression: &str,
    names: Option<&HashMap<String, String>>,
) -> StorageResult<(String, String)> {
    let key_clause = first_key_clause(expression).ok_or_else(|| {
        StorageError::validation(
            "shared table routing failed closed: unsupported key condition expression",
        )
    })?;
    let (lhs, rhs) = key_clause.split_once('=').ok_or_else(|| {
        StorageError::validation(
            "shared table routing failed closed: key condition must contain partition equality",
        )
    })?;
    let lhs = lhs.trim().trim_matches('(').trim_matches(')');
    let rhs = rhs.trim().trim_matches('(').trim_matches(')');
    if !rhs.starts_with(':') {
        return Err(StorageError::validation(
            "shared table routing failed closed: partition equality must bind to a placeholder",
        ));
    }
    let partition_name = resolve_attribute_name(lhs, names)?;
    if !PARTITION_KEY_NAMES.contains(&partition_name.as_str()) {
        return Err(StorageError::validation(format!(
            "shared table routing failed closed: '{partition_name}' is not a supported partition \
             key"
        )));
    }
    Ok((partition_name, rhs.to_string()))
}

fn first_key_clause(expression: &str) -> Option<&str> {
    let upper = expression.to_ascii_uppercase();
    if let Some(and_pos) = upper.find(" AND ") {
        return expression.get(..and_pos).map(str::trim);
    }
    Some(expression.trim())
}

fn resolve_attribute_name(
    token: &str,
    names: Option<&HashMap<String, String>>,
) -> StorageResult<String> {
    let token = token.trim();
    if token.starts_with('#') {
        let names = names.ok_or_else(|| {
            StorageError::validation(format!(
                "shared table routing failed closed: missing expression names for '{token}'"
            ))
        })?;
        return names.get(token).cloned().ok_or_else(|| {
            StorageError::validation(format!(
                "shared table routing failed closed: expression name '{token}' not found"
            ))
        });
    }
    Ok(token.to_string())
}

fn rewrite_update_partition_assignments(
    namespace_prefix: &str,
    update_expression: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&mut HashMap<String, AttributeValue>>,
) -> StorageResult<()> {
    let set_section = extract_set_section(update_expression).ok_or_else(|| {
        StorageError::validation(
            "shared table routing failed closed: update expression must contain a SET section",
        )
    })?;

    let Some(values) = expression_attribute_values else {
        return Err(StorageError::validation(
            "shared table routing failed closed: missing expression values in update request",
        ));
    };

    for assignment in split_top_level_assignments(set_section) {
        let Some((lhs, rhs)) = assignment.split_once('=') else {
            continue;
        };
        let lhs = lhs.trim();
        if lhs.contains(['.', '[']) {
            continue;
        }
        let attr_name = resolve_attribute_name(lhs, expression_attribute_names)?;
        if !PARTITION_KEY_NAMES.contains(&attr_name.as_str()) {
            continue;
        }

        let placeholder =
            extract_partition_assignment_placeholder(rhs.trim()).ok_or_else(|| {
                StorageError::validation(format!(
                    "shared table routing failed closed: partition key assignment for \
                     '{attr_name}' must use a placeholder value"
                ))
            })?;
        if !placeholder.starts_with(':') {
            return Err(StorageError::validation(format!(
                "shared table routing failed closed: partition key assignment for '{attr_name}' \
                 must use a placeholder value"
            )));
        }
        let value = values.get_mut(placeholder).ok_or_else(|| {
            StorageError::validation(format!(
                "shared table routing failed closed: missing update placeholder '{placeholder}'"
            ))
        })?;
        rewrite_partition_value(namespace_prefix, value, &attr_name)?;
    }

    Ok(())
}

fn rewrite_condition_partition_placeholders(
    namespace_prefix: &str,
    condition_expression: Option<&str>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&mut HashMap<String, AttributeValue>>,
) -> StorageResult<()> {
    let Some(condition_expression) = condition_expression else {
        return Ok(());
    };

    let rewrites =
        partition_condition_placeholders(condition_expression, expression_attribute_names)?;
    if rewrites.is_empty() {
        return Ok(());
    }

    let Some(values) = expression_attribute_values else {
        return Err(StorageError::validation(
            "shared table routing failed closed: missing expression values in condition expression",
        ));
    };

    for (attribute_name, placeholder) in rewrites {
        let value = values.get_mut(placeholder.as_str()).ok_or_else(|| {
            StorageError::validation(format!(
                "shared table routing failed closed: missing condition placeholder '{placeholder}'"
            ))
        })?;
        rewrite_partition_value(namespace_prefix, value, attribute_name.as_str())?;
    }

    Ok(())
}

fn partition_condition_placeholders(
    condition_expression: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<Vec<(String, String)>> {
    let mut placeholders = Vec::new();

    for clause in split_top_level_condition_clauses(condition_expression) {
        let Some((lhs, rhs)) = clause.split_once('=') else {
            continue;
        };

        let lhs = lhs.trim().trim_matches('(').trim_matches(')');
        let rhs = rhs.trim().trim_matches('(').trim_matches(')');
        let lhs_partition = resolve_partition_condition_operand(lhs, expression_attribute_names)?;
        let rhs_partition = resolve_partition_condition_operand(rhs, expression_attribute_names)?;

        match (lhs_partition, rhs_partition) {
            (Some(attribute_name), None) if rhs.starts_with(':') => {
                placeholders.push((attribute_name, rhs.to_string()));
            }
            (None, Some(attribute_name)) if lhs.starts_with(':') => {
                placeholders.push((attribute_name, lhs.to_string()));
            }
            (Some(attribute_name), None) | (None, Some(attribute_name)) => {
                return Err(StorageError::validation(format!(
                    "shared table routing failed closed: partition key comparison for \
                     '{attribute_name}' must bind to a placeholder value"
                )));
            }
            _ => {}
        }
    }

    Ok(placeholders)
}

fn resolve_partition_condition_operand(
    operand: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<Option<String>> {
    let operand = operand.trim();
    let is_attribute_token = operand.starts_with('#')
        || operand
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_');
    if !is_attribute_token {
        return Ok(None);
    }

    let resolved = resolve_attribute_name(operand, expression_attribute_names)?;
    if PARTITION_KEY_NAMES.contains(&resolved.as_str()) {
        return Ok(Some(resolved));
    }

    Ok(None)
}

fn extract_partition_assignment_placeholder(rhs: &str) -> Option<&str> {
    let rhs = rhs.trim();
    if rhs.starts_with(':') {
        return Some(rhs);
    }

    let inner = rhs
        .strip_prefix("if_not_exists(")?
        .strip_suffix(')')?
        .trim();
    let (_lhs, fallback) = inner.rsplit_once(',')?;
    Some(fallback.trim())
}

fn split_top_level_assignments(set_section: &str) -> Vec<&str> {
    let mut assignments = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;

    for (idx, ch) in set_section.char_indices() {
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                if let Some(segment) = set_section.get(start..idx) {
                    let trimmed = segment.trim();
                    if !trimmed.is_empty() {
                        assignments.push(trimmed);
                    }
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    if let Some(segment) = set_section.get(start..) {
        let trimmed = segment.trim();
        if !trimmed.is_empty() {
            assignments.push(trimmed);
        }
    }

    assignments
}

fn split_top_level_condition_clauses(condition_expression: &str) -> Vec<&str> {
    let upper = condition_expression.to_ascii_uppercase();
    let mut clauses = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut idx = 0usize;

    while idx < condition_expression.len() {
        let Some(ch) = condition_expression
            .get(idx..)
            .and_then(|s| s.chars().next())
        else {
            break;
        };
        match ch {
            '(' => {
                depth = depth.saturating_add(1);
                idx += ch.len_utf8();
            }
            ')' => {
                depth = depth.saturating_sub(1);
                idx += ch.len_utf8();
            }
            _ if depth == 0 && upper.get(idx..).is_some_and(|s| s.starts_with(" AND ")) => {
                if let Some(segment) = condition_expression.get(start..idx) {
                    let trimmed = segment.trim();
                    if !trimmed.is_empty() {
                        clauses.push(trimmed);
                    }
                }
                idx += " AND ".len();
                start = idx;
            }
            _ if depth == 0 && upper.get(idx..).is_some_and(|s| s.starts_with(" OR ")) => {
                if let Some(segment) = condition_expression.get(start..idx) {
                    let trimmed = segment.trim();
                    if !trimmed.is_empty() {
                        clauses.push(trimmed);
                    }
                }
                idx += " OR ".len();
                start = idx;
            }
            _ => {
                idx += ch.len_utf8();
            }
        }
    }

    if let Some(segment) = condition_expression.get(start..) {
        let trimmed = segment.trim();
        if !trimmed.is_empty() {
            clauses.push(trimmed);
        }
    }

    clauses
}

fn extract_set_section(update_expression: &str) -> Option<&str> {
    let upper = update_expression.to_ascii_uppercase();
    let set_pos = upper.find("SET ")?;
    let section_start = set_pos + 4;
    let mut section_end = update_expression.len();
    let upper_tail = upper.get(section_start..)?;
    for marker in [" REMOVE ", " ADD ", " DELETE "] {
        if let Some(marker_pos) = upper_tail.find(marker) {
            section_end = section_end.min(section_start + marker_pos);
        }
    }
    update_expression
        .get(section_start..section_end)
        .map(str::trim)
}
