use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    sync::LazyLock,
};

use smallvec::SmallVec;

use crate::collect_expression_attribute_placeholder_refs;

const MAX_EXPRESSION_SUBSTITUTION_BYTES: usize = 2 * 1024 * 1024;

const DYNAMODB_RESERVED_WORDS: &str =
    "\
ABORT ABSOLUTE ACTION ADD AFTER AGENT AGGREGATE ALL ALLOCATE ALTER ANALYZE AND ANY ARCHIVE ARE \
     ARRAY AS ASC ASCII ASENSITIVE ASSERTION ASYMMETRIC AT ATOMIC ATTACH ATTRIBUTE AUTH \
     AUTHORIZATION AUTHORIZE AUTO AVG BACK BACKUP BASE BATCH BEFORE BEGIN BETWEEN BIGINT BINARY \
     BIT BLOB BLOCK BOOLEAN BOTH BREADTH BUCKET BULK BY BYTE CALL CALLED CALLING CAPACITY CASCADE \
     CASCADED CASE CAST CATALOG CHAR CHARACTER CHECK CLASS CLOB CLOSE CLUSTER CLUSTERED \
     CLUSTERING CLUSTERS COALESCE COLLATE COLLATION COLLECTION COLUMN COLUMNS COMBINE COMMENT \
     COMMIT COMPACT COMPILE COMPRESS CONDITION CONFLICT CONNECT CONNECTION CONSISTENCY CONSISTENT \
     CONSTRAINT CONSTRAINTS CONSTRUCTOR CONSUMED CONTINUE CONVERT COPY CORRESPONDING COUNT \
     COUNTER CREATE CROSS CUBE CURRENT CURSOR CYCLE DATA DATABASE DATE DATETIME DAY DEALLOCATE \
     DEC DECIMAL DECLARE DEFAULT DEFERRABLE DEFERRED DEFINE DEFINED DEFINITION DELETE DELIMITED \
     DEPTH DEREF DESC DESCRIBE DESCRIPTOR DETACH DETERMINISTIC DIAGNOSTICS DIRECTORIES DISABLE \
     DISCONNECT DISTINCT DISTRIBUTE DO DOMAIN DOUBLE DROP DUMP DURATION DYNAMIC EACH ELEMENT ELSE \
     ELSEIF EMPTY ENABLE END EQUAL EQUALS ERROR ESCAPE ESCAPED EVAL EVALUATE EXCEEDED EXCEPT \
     EXCEPTION EXCEPTIONS EXCLUSIVE EXEC EXECUTE EXISTS EXIT EXPLAIN EXPLODE EXPORT EXPRESSION \
     EXTENDED EXTERNAL EXTRACT FAIL FALSE FAMILY FETCH FIELDS FILE FILTER FILTERING FINAL FINISH \
     FIRST FIXED FLATTERN FLOAT FOR FORCE FOREIGN FORMAT FORWARD FOUND FREE FROM FULL FUNCTION \
     FUNCTIONS GENERAL GENERATE GET GLOB GLOBAL GO GOTO GRANT GREATER GROUP GROUPING HANDLER HASH \
     HAVE HAVING HEAP HIDDEN HOLD HOUR IDENTIFIED IDENTITY IF IGNORE IMMEDIATE IMPORT IN \
     INCLUDING INCLUSIVE INCREMENT INCREMENTAL INDEX INDEXED INDEXES INDICATOR INFINITE INITIALLY \
     INLINE INNER INNTER INOUT INPUT INSENSITIVE INSERT INSTEAD INT INTEGER INTERSECT INTERVAL \
     INTO INVALIDATE IS ISOLATION ITEM ITEMS ITERATE JOIN KEY KEYS LAG LANGUAGE LARGE LAST \
     LATERAL LEAD LEADING LEAVE LEFT LENGTH LESS LEVEL LIKE LIMIT LIMITED LINES LIST LOAD LOCAL \
     LOCALTIME LOCALTIMESTAMP LOCATION LOCATOR LOCK LOCKS LOG LOGED LONG LOOP LOWER MAP MATCH \
     MATERIALIZED MAX MAXLEN MEMBER MERGE METHOD METRICS MIN MINUS MINUTE MISSING MOD MODE \
     MODIFIES MODIFY MODULE MONTH MULTI MULTISET NAME NAMES NATIONAL NATURAL NCHAR NCLOB NEW NEXT \
     NO NONE NOT NULL NULLIF NUMBER NUMERIC OBJECT OF OFFLINE OFFSET OLD ON ONLINE ONLY OPAQUE \
     OPEN OPERATOR OPTION OR ORDER ORDINALITY OTHER OTHERS OUT OUTER OUTPUT OVER OVERLAPS \
     OVERRIDE OWNER PAD PARALLEL PARAMETER PARAMETERS PARTIAL PARTITION PARTITIONED PARTITIONS \
     PATH PERCENT PERCENTILE PERMISSION PERMISSIONS PIPE PIPELINED PLAN POOL POSITION PRECISION \
     PREPARE PRESERVE PRIMARY PRIOR PRIVATE PRIVILEGES PROCEDURE PROCESSED PROJECT PROJECTION \
     PROPERTY PROVISIONING PUBLIC PUT QUERY QUIT QUORUM RAISE RANDOM RANGE RANK RAW READ READS \
     REAL REBUILD RECORD RECURSIVE REDUCE REF REFERENCE REFERENCES REFERENCING REGEXP REGION \
     REINDEX RELATIVE RELEASE REMAINDER RENAME REPEAT REPLACE REQUEST RESET RESIGNAL RESOURCE \
     RESPONSE RESTORE RESTRICT RESULT RETURN RETURNING RETURNS REVERSE REVOKE RIGHT ROLE ROLES \
     ROLLBACK ROLLUP ROUTINE ROW ROWS RULE RULES SAMPLE SATISFIES SAVE SAVEPOINT SCAN SCHEMA \
     SCOPE SCROLL SEARCH SECOND SECTION SEGMENT SEGMENTS SELECT SELF SEMI SENSITIVE SEPARATE \
     SEQUENCE SERIALIZABLE SESSION SET SETS SHARD SHARE SHARED SHORT SHOW SIGNAL SIMILAR SIZE \
     SKEWED SMALLINT SNAPSHOT SOME SOURCE SPACE SPACES SPARSE SPECIFIC SPECIFICTYPE SPLIT SQL \
     SQLCODE SQLERROR SQLEXCEPTION SQLSTATE SQLWARNING START STATE STATIC STATUS STORAGE STORE \
     STORED STREAM STRING STRUCT STYLE SUB SUBMULTISET SUBPARTITION SUBSTRING SUBTYPE SUM SUPER \
     SYMMETRIC SYNONYM SYSTEM TABLE TABLESAMPLE TEMP TEMPORARY TERMINATED TEXT THAN THEN \
     THROUGHPUT TIME TIMESTAMP TIMEZONE TINYINT TO TOKEN TOTAL TOUCH TRAILING TRANSACTION \
     TRANSFORM TRANSLATE TRANSLATION TREAT TRIGGER TRIM TRUE TRUNCATE TTL TUPLE TYPE UNDER UNDO \
     UNION UNIQUE UNIT UNKNOWN UNLOGGED UNNEST UNPROCESSED UNSIGNED UNTIL UPDATE UPPER URL USAGE \
     USE USER USERS USING UUID VACUUM VALUE VALUED VALUES VARCHAR VARIABLE VARIANCE VARINT \
     VARYING VIEW VIEWS VIRTUAL VOID WAIT WHEN WHENEVER WHERE WHILE WINDOW WITH WITHIN WITHOUT \
     WORK WRAPPED WRITE YEAR ZONE ";

#[derive(Clone, Copy, Eq)]
struct AsciiCaseInsensitive<'a>(&'a str);

impl PartialEq for AsciiCaseInsensitive<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(other.0)
    }
}

impl Hash for AsciiCaseInsensitive<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_uppercase());
        }
    }
}

static DYNAMODB_RESERVED_WORD_SET: LazyLock<HashSet<AsciiCaseInsensitive<'static>>> =
    LazyLock::new(|| {
        DYNAMODB_RESERVED_WORDS
            .split_ascii_whitespace()
            .map(AsciiCaseInsensitive)
            .collect()
    });

pub(crate) fn validate_expression_attribute_value_keys(
    values: Option<&std::collections::HashMap<String, crate::AttributeValue>>,
    prefixed: bool,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    if values.is_empty() {
        return Err(validation_message(
            "ExpressionAttributeValues must not be empty".to_string(),
            prefixed,
        ));
    }
    let value_size = values.iter().try_fold(0usize, |size, (key, value)| {
        serde_json::to_vec(value)
            .map(|encoded| size + key.len() + encoded.len())
            .map_err(|err| format!("Invalid ExpressionAttributeValues: {err}"))
    })?;
    if value_size > MAX_EXPRESSION_SUBSTITUTION_BYTES {
        return Err(validation_message(
            "ExpressionAttributeValues exceeds max size".to_string(),
            prefixed,
        ));
    }
    for key in values.keys() {
        if key.len() > 255 {
            let size_suffix = if prefixed {
                format!(" size of key: {}", key.len())
            } else {
                String::new()
            };
            return Err(validation_message(
                format!(
                    "ExpressionAttributeValues contains invalid key: The expression attribute map \
                     contains a key that is too long;{size_suffix}"
                ),
                prefixed,
            ));
        }
        let mut chars = key.chars();
        if chars.next() != Some(':')
            || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            || key.len() == 1
        {
            return Err(validation_message(
                format!(
                    "ExpressionAttributeValues contains invalid key: Syntax error; key: \"{key}\""
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_expression_attribute_name_keys(
    names: Option<&std::collections::HashMap<String, String>>,
    prefixed: bool,
) -> Result<(), String> {
    let Some(names) = names else {
        return Ok(());
    };
    if names.is_empty() {
        return Err(validation_message(
            "ExpressionAttributeNames must not be empty".to_string(),
            prefixed,
        ));
    }
    let name_size = names
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum::<usize>();
    if name_size > MAX_EXPRESSION_SUBSTITUTION_BYTES {
        return Err(validation_message(
            "ExpressionAttributeNames exceeds max size".to_string(),
            prefixed,
        ));
    }
    for key in names.keys() {
        if key.len() > 255 {
            return Err(validation_message(
                format!(
                    "ExpressionAttributeNames contains invalid key: The expression attribute map \
                     contains a key that is too long; size of key: {}",
                    key.len()
                ),
                prefixed,
            ));
        }
        let mut chars = key.chars();
        if chars.next() != Some('#')
            || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            || key.len() == 1
        {
            return Err(validation_message(
                format!(
                    "ExpressionAttributeNames contains invalid key: Syntax error; key: \"{key}\""
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_expression_set<'a, I>(
    expressions: I,
    names: Option<&std::collections::HashMap<String, String>>,
    values: Option<&std::collections::HashMap<String, crate::AttributeValue>>,
    prefixed: bool,
) -> Result<(), String>
where
    I: IntoIterator<Item = (Option<&'a str>, &'static str)>,
{
    let expressions = expressions
        .into_iter()
        .filter_map(|(expression, label)| expression.map(|expression| (expression, label)))
        .map(ExpressionValidationContext::new)
        .collect::<SmallVec<[_; 2]>>();

    for context in &expressions {
        validate_expression_shape(context, names, prefixed)?;
        validate_expression_names(context, names, prefixed)?;
        validate_projection_expression_paths(context.expression, context.label, names, prefixed)?;
        validate_update_expression_paths(context, names, prefixed)?;
        validate_expression_values(context, values, prefixed)?;
        validate_update_add_delete_value_types(context, values, prefixed)?;
    }

    validate_unused_expression_names(names, &expressions, prefixed)?;
    validate_unused_expression_values(values, &expressions, prefixed)
}

struct ExpressionValidationContext<'a> {
    expression: &'a str,
    label: &'static str,
    used_names: SmallVec<[&'a str; 8]>,
    used_values: SmallVec<[&'a str; 8]>,
    update_sections: Option<SmallVec<[UpdateExpressionSection<'a>; 4]>>,
}

impl<'a> ExpressionValidationContext<'a> {
    fn new((expression, label): (&'a str, &'static str)) -> Self {
        let mut used_names = SmallVec::new();
        let mut used_values = SmallVec::new();
        collect_expression_attribute_placeholder_refs(
            expression,
            &mut used_names,
            &mut used_values,
        );
        let update_sections =
            (label == "UpdateExpression").then(|| update_expression_sections(expression));
        Self {
            expression,
            label,
            used_names,
            used_values,
            update_sections,
        }
    }
}

fn validate_expression_names(
    context: &ExpressionValidationContext<'_>,
    names: Option<&std::collections::HashMap<String, String>>,
    prefixed: bool,
) -> Result<(), String> {
    for &name in &context.used_names {
        if names.is_none_or(|names| !names.contains_key(name)) {
            return Err(validation_message(
                format!(
                    "Invalid {}: An expression attribute name used in the document path is not \
                     defined; attribute name: {name}",
                    context.label
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

fn validate_update_add_delete_value_types(
    context: &ExpressionValidationContext<'_>,
    values: Option<&std::collections::HashMap<String, crate::AttributeValue>>,
    prefixed: bool,
) -> Result<(), String> {
    if context.label != "UpdateExpression" {
        return Ok(());
    }
    let Some(values) = values else {
        return Ok(());
    };

    for section in context
        .update_sections
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|section| matches!(section.keyword, "ADD" | "DELETE"))
    {
        for action in split_top_level_args(section.body) {
            let mut parts = action.split_whitespace();
            let _path = parts.next();
            let Some(value_ref) = parts.next() else {
                continue;
            };
            let Some(value) = values.get(value_ref) else {
                continue;
            };
            if !update_action_value_type_allowed(section.keyword, value) {
                return Err(validation_message(
                    format!(
                        "Invalid UpdateExpression: Incorrect operand type for operator or \
                         function; operator: {}, operand type: {}, typeSet: \
                         ALLOWED_FOR_ADD_OPERAND",
                        section.keyword,
                        update_action_operand_type_name(value)
                    ),
                    prefixed,
                ));
            }
            crate::validate_attribute_value_for_write(value, "ExpressionAttributeValues")
                .map_err(|message| validation_message(message, prefixed))?;
        }
    }
    Ok(())
}

fn update_action_value_type_allowed(keyword: &str, value: &crate::AttributeValue) -> bool {
    match keyword {
        "ADD" => matches!(
            value,
            crate::AttributeValue::N(_)
                | crate::AttributeValue::SS(_)
                | crate::AttributeValue::NS(_)
                | crate::AttributeValue::BS(_)
        ),
        "DELETE" => matches!(
            value,
            crate::AttributeValue::SS(_)
                | crate::AttributeValue::NS(_)
                | crate::AttributeValue::BS(_)
        ),
        _ => true,
    }
}

fn update_action_operand_type_name(value: &crate::AttributeValue) -> &'static str {
    match value {
        crate::AttributeValue::S(_) => "STRING",
        crate::AttributeValue::N(_) => "NUMBER",
        crate::AttributeValue::B(_) => "BINARY",
        crate::AttributeValue::SS(_) => "STRING_SET",
        crate::AttributeValue::NS(_) => "NUMBER_SET",
        crate::AttributeValue::BS(_) => "BINARY_SET",
        crate::AttributeValue::BOOL(_) => "BOOL",
        crate::AttributeValue::NULL(_) => "NULL",
        crate::AttributeValue::L(_) => "LIST",
        crate::AttributeValue::M(_) => "MAP",
    }
}

fn validate_expression_values(
    context: &ExpressionValidationContext<'_>,
    values: Option<&std::collections::HashMap<String, crate::AttributeValue>>,
    prefixed: bool,
) -> Result<(), String> {
    for &value in &context.used_values {
        if values.is_none_or(|values| !values.contains_key(value)) {
            return Err(validation_message(
                format!(
                    "Invalid {}: An expression attribute value used in expression is not defined; \
                     attribute value: {value}",
                    context.label
                ),
                prefixed,
            ));
        }
    }
    validate_expression_attribute_value_payloads(context.label, values, prefixed)?;
    validate_begins_with_value_operands(context.expression, context.label, values, prefixed)?;
    validate_attribute_type_value_operands(context.expression, context.label, values, prefixed)?;
    validate_between_bounds(context.expression, context.label, values, prefixed)?;
    Ok(())
}

fn validate_expression_attribute_value_payloads(
    label: &str,
    values: Option<&std::collections::HashMap<String, crate::AttributeValue>>,
    prefixed: bool,
) -> Result<(), String> {
    if label != "UpdateExpression" {
        return Ok(());
    }
    let Some(values) = values else {
        return Ok(());
    };
    for value in values.values() {
        crate::validate_attribute_value_for_write(value, "ExpressionAttributeValues")
            .map_err(|message| validation_message(message, prefixed))?;
    }
    Ok(())
}

fn validate_unused_expression_names(
    names: Option<&std::collections::HashMap<String, String>>,
    expressions: &[ExpressionValidationContext<'_>],
    prefixed: bool,
) -> Result<(), String> {
    let Some(names) = names else {
        return Ok(());
    };
    let mut unused_names = names
        .keys()
        .filter(|key| {
            !expressions
                .iter()
                .any(|context| context.used_names.contains(&key.as_str()))
        })
        .cloned()
        .collect::<Vec<_>>();
    unused_names.sort();
    if unused_names.is_empty() {
        return Ok(());
    }
    Err(validation_message(
        format!(
            "Value provided in ExpressionAttributeNames unused in expressions: keys: {{{}}}",
            unused_names.join(", ")
        ),
        prefixed,
    ))
}

fn validate_unused_expression_values(
    values: Option<&std::collections::HashMap<String, crate::AttributeValue>>,
    expressions: &[ExpressionValidationContext<'_>],
    prefixed: bool,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    let mut unused_values = values
        .keys()
        .filter(|key| {
            !expressions
                .iter()
                .any(|context| context.used_values.contains(&key.as_str()))
        })
        .cloned()
        .collect::<Vec<_>>();
    unused_values.sort();
    if unused_values.is_empty() {
        return Ok(());
    }
    Err(validation_message(
        format!(
            "Value provided in ExpressionAttributeValues unused in expressions: keys: {{{}}}",
            unused_values.join(", ")
        ),
        prefixed,
    ))
}

fn validate_expression_shape(
    context: &ExpressionValidationContext<'_>,
    names: Option<&std::collections::HashMap<String, String>>,
    prefixed: bool,
) -> Result<(), String> {
    let expression = context.expression;
    let label = context.label;
    if let Some(error) = invalid_function_name_error(expression, label) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = document_path_index_syntax_error(expression, label) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = function_context_error(expression, label) {
        return Err(validation_message(error, prefixed));
    }
    if label == "UpdateExpression"
        && let Some(error) = update_expression_grammar_error_for_sections(
            context.update_sections.as_deref().unwrap_or_default(),
        )
    {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = function_syntax_or_arity_error(expression, label) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = contains_same_operand_error(expression, label, names) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = attribute_type_literal_operand_error(expression, label) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = in_operand_count_error(expression, label) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(reserved_word) = reserved_word_in_expression(expression, label) {
        return Err(validation_message(
            format!(
                "Invalid {label}: Attribute name is a reserved keyword; reserved keyword: \
                 {reserved_word}"
            ),
            prefixed,
        ));
    }
    if label == "ConditionExpression" && has_redundant_parentheses(expression) {
        return Err(validation_message(
            "Invalid ConditionExpression: The expression has redundant parentheses;".to_string(),
            prefixed,
        ));
    }
    if expression.trim_end().ends_with('=') {
        return Err(validation_message(
            format!("Invalid {label}: Syntax error; token: \"<EOF>\", near: \"=\""),
            prefixed,
        ));
    }
    if expression.trim_end().ends_with('(') {
        return Err(validation_message(
            format!("Invalid {label}: Syntax error; token: \"<EOF>\", near: \"(\""),
            prefixed,
        ));
    }
    Ok(())
}

fn invalid_function_name_error(expression: &str, label: &str) -> Option<String> {
    let allowed = match label {
        "ConditionExpression" | "FilterExpression" | "KeyConditionExpression" => &[
            "attribute_exists",
            "attribute_not_exists",
            "attribute_type",
            "begins_with",
            "contains",
            "size",
        ][..],
        "UpdateExpression" => &["if_not_exists", "list_append"][..],
        _ => return None,
    };

    for call in expression_function_names(expression) {
        if allowed
            .iter()
            .any(|function_name| call.eq_ignore_ascii_case(function_name) && call != *function_name)
        {
            return Some(format!(
                "Invalid {label}: Invalid function name; function: {call}"
            ));
        }
    }
    None
}

fn expression_function_names(expression: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut index = 0usize;
    while index < expression.len() {
        let Some((relative_start, _)) = expression.get(index..).and_then(|tail| {
            tail.char_indices()
                .find(|(_, ch)| ch.is_ascii_alphabetic() || *ch == '_')
        }) else {
            break;
        };
        let start = index + relative_start;
        let mut end = start;
        for (relative_index, ch) in expression.get(start..).unwrap_or_default().char_indices() {
            if relative_index == 0 {
                end = start + ch.len_utf8();
                continue;
            }
            if ch.is_ascii_alphanumeric() || ch == '_' {
                end = start + relative_index + ch.len_utf8();
            } else {
                break;
            }
        }
        let tail = expression.get(end..).unwrap_or_default().trim_start();
        if tail.starts_with('(')
            && let Some(name) = expression.get(start..end)
        {
            names.push(name);
        }
        index = end;
    }
    names
}

fn in_operand_count_error(expression: &str, label: &str) -> Option<String> {
    if !matches!(
        label,
        "ConditionExpression" | "FilterExpression" | "KeyConditionExpression"
    ) {
        return None;
    }
    for operands in in_operand_lists(expression) {
        let count = split_nonempty_top_level_args(operands).len();
        if count > 100 {
            return Some(format!(
                "Invalid {label}: The IN operator is provided with too many operands; number of \
                 operands: {count}"
            ));
        }
    }
    None
}

fn in_operand_lists(expression: &str) -> Vec<&str> {
    let mut lists = Vec::new();
    let mut offset = 0usize;
    let upper = expression.to_ascii_uppercase();
    while let Some(relative_index) = upper.get(offset..).and_then(|tail| tail.find(" IN ")) {
        let after_in = offset + relative_index + 4;
        let Some(open_relative) = expression.get(after_in..).and_then(|tail| tail.find('(')) else {
            break;
        };
        let open = after_in + open_relative;
        let mut depth = 1usize;
        let args_start = open + 1;
        let Some(tail) = expression.get(args_start..) else {
            break;
        };
        let mut found_close = false;
        for (relative_arg_index, ch) in tail.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let close = args_start + relative_arg_index;
                        if let Some(args) = expression.get(args_start..close) {
                            lists.push(args);
                        }
                        offset = close + 1;
                        found_close = true;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !found_close {
            break;
        }
    }
    lists
}

fn validate_between_bounds(
    expression: &str,
    label: &str,
    values: Option<&std::collections::HashMap<String, crate::AttributeValue>>,
    prefixed: bool,
) -> Result<(), String> {
    if label != "ConditionExpression" {
        return Ok(());
    }
    let Some(values) = values else {
        return Ok(());
    };

    for (lower_key, upper_key) in between_bound_value_keys(expression) {
        let (Some(lower), Some(upper)) = (values.get(lower_key), values.get(upper_key)) else {
            continue;
        };
        if between_upper_is_less_than_lower(lower, upper) {
            return Err(validation_message(
                format!(
                    "Invalid ConditionExpression: The BETWEEN operator requires upper bound to be \
                     greater than or equal to lower bound; lower bound operand: {}, upper bound \
                     operand: {}",
                    dynamodb_attribute_value_display(lower),
                    dynamodb_attribute_value_display(upper)
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

fn between_bound_value_keys(expression: &str) -> Vec<(&str, &str)> {
    let mut bounds = Vec::new();
    let mut index = 0;
    while let Some((between_index, between_len)) = next_word_matching(expression, index, "BETWEEN")
    {
        let Some((lower_key, after_lower)) =
            read_expression_value_key(expression, between_index + between_len)
        else {
            index = between_index + between_len;
            continue;
        };
        let Some((and_index, and_len)) = next_word_matching(expression, after_lower, "AND") else {
            index = after_lower;
            continue;
        };
        let Some((upper_key, after_upper)) =
            read_expression_value_key(expression, and_index + and_len)
        else {
            index = and_index + and_len;
            continue;
        };
        bounds.push((lower_key, upper_key));
        index = after_upper;
    }
    bounds
}

fn next_word_matching(expression: &str, start: usize, expected: &str) -> Option<(usize, usize)> {
    let mut search_start = start;
    while let Some(relative_index) = expression.get(search_start..)?.find(expected) {
        let index = search_start + relative_index;
        let end = index + expected.len();
        if expression_word_boundary(expression, index, end) {
            return Some((index, expected.len()));
        }
        search_start = end;
    }
    None
}

fn expression_word_boundary(expression: &str, start: usize, end: usize) -> bool {
    let before = start
        .checked_sub(1)
        .and_then(|index| expression.as_bytes().get(index));
    let after = expression.as_bytes().get(end);
    !matches!(before, Some(b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9'))
        && !matches!(after, Some(b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9'))
}

fn read_expression_value_key(expression: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = expression.as_bytes();
    let mut index = start;
    while matches!(bytes.get(index), Some(b' ' | b'\t' | b'\n' | b'\r')) {
        index += 1;
    }
    if bytes.get(index) != Some(&b':') {
        return None;
    }
    let key_start = index;
    index += 1;
    while matches!(
        bytes.get(index),
        Some(b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9')
    ) {
        index += 1;
    }
    if index <= key_start + 1 {
        return None;
    }
    Some((expression.get(key_start..index)?, index))
}

fn between_upper_is_less_than_lower(
    lower: &crate::AttributeValue,
    upper: &crate::AttributeValue,
) -> bool {
    match (lower, upper) {
        (crate::AttributeValue::N(lower), crate::AttributeValue::N(upper)) => {
            compare_dynamodb_numbers(upper, lower).is_some_and(std::cmp::Ordering::is_lt)
        }
        (crate::AttributeValue::S(lower), crate::AttributeValue::S(upper)) => upper < lower,
        (crate::AttributeValue::B(lower), crate::AttributeValue::B(upper)) => {
            match (
                crate::dynamodb_binary::decode_base64_string(lower),
                crate::dynamodb_binary::decode_base64_string(upper),
            ) {
                (Ok(lower), Ok(upper)) => upper < lower,
                _ => upper < lower,
            }
        }
        _ => false,
    }
}

fn compare_dynamodb_numbers(left: &str, right: &str) -> Option<std::cmp::Ordering> {
    Some(
        left.parse::<f64>()
            .ok()?
            .total_cmp(&right.parse::<f64>().ok()?),
    )
}

fn dynamodb_attribute_value_display(value: &crate::AttributeValue) -> String {
    match value {
        crate::AttributeValue::S(value) => format!("AttributeValue: {{S:{value}}}"),
        crate::AttributeValue::N(value) => format!("AttributeValue: {{N:{value}}}"),
        crate::AttributeValue::B(value) => format!("AttributeValue: {{B:{value}}}"),
        crate::AttributeValue::BOOL(value) => format!("AttributeValue: {{BOOL:{value}}}"),
        crate::AttributeValue::NULL(value) => format!("AttributeValue: {{NULL:{value}}}"),
        crate::AttributeValue::SS(values) => format!("AttributeValue: {{SS:{values:?}}}"),
        crate::AttributeValue::NS(values) => format!("AttributeValue: {{NS:{values:?}}}"),
        crate::AttributeValue::BS(values) => format!("AttributeValue: {{BS:{values:?}}}"),
        crate::AttributeValue::L(values) => format!("AttributeValue: {{L:{values:?}}}"),
        crate::AttributeValue::M(values) => format!("AttributeValue: {{M:{values:?}}}"),
    }
}

fn update_expression_grammar_error_for_sections(
    sections: &[UpdateExpressionSection<'_>],
) -> Option<String> {
    for keyword in ["SET", "REMOVE", "ADD", "DELETE"] {
        if sections
            .iter()
            .filter(|section| section.keyword == keyword)
            .count()
            > 1
        {
            return Some(format!(
                "Invalid UpdateExpression: The \"{keyword}\" section can only be used once in an \
                 update expression;"
            ));
        }
    }

    set_action_syntax_error(sections).or_else(|| add_delete_action_syntax_error(sections))
}

fn function_context_error(expression: &str, label: &str) -> Option<String> {
    match label {
        "ConditionExpression" => disallowed_function_error(
            expression,
            label,
            &["if_not_exists", "list_append"],
            "a condition",
        ),
        "UpdateExpression" => disallowed_function_error(
            expression,
            label,
            &["contains", "begins_with", "attribute_type", "size"],
            "an update",
        )
        .or_else(|| nested_update_function_error(expression, label)),
        _ => None,
    }
}

#[derive(Clone, Copy)]
struct UpdateExpressionSection<'a> {
    keyword: &'a str,
    body: &'a str,
}

fn update_expression_sections(expression: &str) -> SmallVec<[UpdateExpressionSection<'_>; 4]> {
    let mut sections = SmallVec::new();
    let mut current_keyword: Option<(&'static str, usize)> = None;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;

    for (index, ch) in expression.char_indices() {
        match ch {
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            _ => {}
        }
        if paren_depth != 0 || bracket_depth != 0 || !is_identifier_start(ch) {
            continue;
        }

        let Some(token) = identifier_at(expression, index) else {
            continue;
        };
        let Some(keyword) = update_section_keyword(token) else {
            continue;
        };
        if !is_top_level_update_keyword(expression, index, token.len()) {
            continue;
        }

        if let Some((keyword, body_start)) = current_keyword.take()
            && let Some(body) = expression.get(body_start..index)
        {
            sections.push(UpdateExpressionSection {
                keyword,
                body: body.trim(),
            });
        }
        current_keyword = Some((keyword, index + token.len()));
    }

    if let Some((keyword, body_start)) = current_keyword
        && let Some(body) = expression.get(body_start..)
    {
        sections.push(UpdateExpressionSection {
            keyword,
            body: body.trim(),
        });
    }

    sections
}

fn update_section_keyword(token: &str) -> Option<&'static str> {
    if token.eq_ignore_ascii_case("SET") {
        Some("SET")
    } else if token.eq_ignore_ascii_case("REMOVE") {
        Some("REMOVE")
    } else if token.eq_ignore_ascii_case("ADD") {
        Some("ADD")
    } else if token.eq_ignore_ascii_case("DELETE") {
        Some("DELETE")
    } else {
        None
    }
}

fn is_top_level_update_keyword(expression: &str, start: usize, len: usize) -> bool {
    let bytes = expression.as_bytes();
    let previous = start.checked_sub(1).and_then(|index| bytes.get(index));
    if matches!(
        previous,
        Some(b'#' | b':' | b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9')
    ) {
        return false;
    }
    let next = bytes.get(start + len);
    !matches!(next, Some(b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9'))
}

fn identifier_at(expression: &str, start: usize) -> Option<&str> {
    let mut end = start;
    for (relative_index, ch) in expression.get(start..)?.char_indices() {
        if relative_index == 0 {
            if !is_identifier_start(ch) {
                return None;
            }
            end = start + ch.len_utf8();
            continue;
        }
        if !is_identifier_continue(ch) {
            break;
        }
        end = start + relative_index + ch.len_utf8();
    }
    expression.get(start..end)
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_identifier_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn add_delete_action_syntax_error(sections: &[UpdateExpressionSection<'_>]) -> Option<String> {
    for section in sections
        .iter()
        .filter(|section| matches!(section.keyword, "ADD" | "DELETE"))
    {
        for action in split_top_level_args(section.body) {
            let action = action.trim();
            if action.is_empty() {
                continue;
            }
            let mut parts = action.split_whitespace();
            let path = parts.next().unwrap_or_default();
            let Some(value) = parts.next() else {
                return Some(format!(
                    "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"{path}\""
                ));
            };
            if !value.starts_with(':') {
                return Some(format!(
                    "Invalid UpdateExpression: Syntax error; token: \"{value}\", near: \"{path} \
                     {value}\""
                ));
            }
        }
    }
    None
}

fn set_action_syntax_error(sections: &[UpdateExpressionSection<'_>]) -> Option<String> {
    for section in sections.iter().filter(|section| section.keyword == "SET") {
        for action in split_top_level_args(section.body) {
            let action = action.trim();
            if action.is_empty() {
                continue;
            }
            let Some((_, value)) = top_level_split_once(action, '=') else {
                return Some(format!(
                    "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"{action}\""
                ));
            };
            let value = value.trim_end();
            if let Some(operator) = trailing_arithmetic_operator(value) {
                return Some(format!(
                    "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \
                     \"{operator}\""
                ));
            }
            if let Some((operator, near)) = unsupported_arithmetic_operator(value) {
                return Some(format!(
                    "Invalid UpdateExpression: Syntax error; token: \"{operator}\", near: \
                     \"{near}\""
                ));
            }
        }
    }
    None
}

fn unsupported_arithmetic_operator(value: &str) -> Option<(char, &str)> {
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for ch in value.chars() {
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
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '*' | '/' | '^' if paren_depth == 0 && bracket_depth == 0 => {
                return Some((ch, value.trim()));
            }
            _ => {}
        }
    }
    None
}

fn trailing_arithmetic_operator(value: &str) -> Option<char> {
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    for ch in value.chars().rev() {
        match ch {
            ')' => paren_depth += 1,
            '(' => paren_depth = paren_depth.saturating_sub(1),
            ']' => bracket_depth += 1,
            '[' => bracket_depth = bracket_depth.saturating_sub(1),
            '+' | '-' if paren_depth == 0 && bracket_depth == 0 => return Some(ch),
            ch if ch.is_whitespace() => continue,
            _ => return None,
        }
    }
    None
}

fn validate_update_expression_paths(
    context: &ExpressionValidationContext<'_>,
    names: Option<&std::collections::HashMap<String, String>>,
    prefixed: bool,
) -> Result<(), String> {
    if context.label != "UpdateExpression" {
        return Ok(());
    }

    let paths = update_expression_paths(
        context.update_sections.as_deref().unwrap_or_default(),
        names,
    );
    for (left_index, left) in paths.iter().enumerate() {
        for right in paths.iter().skip(left_index + 1) {
            if document_paths_overlap(&left.parts, &right.parts) {
                return Err(validation_message(
                    format!(
                        "Invalid UpdateExpression: Two document paths overlap with each other; \
                         must remove or rewrite one of these paths; path one: [{}], path two: [{}]",
                        left.parts.join(", "),
                        right.parts.join(", ")
                    ),
                    prefixed,
                ));
            }
        }
    }
    Ok(())
}

struct UpdatePath {
    parts: Vec<String>,
}

fn validate_projection_expression_paths(
    expression: &str,
    label: &str,
    names: Option<&std::collections::HashMap<String, String>>,
    prefixed: bool,
) -> Result<(), String> {
    if label != "ProjectionExpression" {
        return Ok(());
    }

    let paths = split_top_level_args(expression)
        .into_iter()
        .filter_map(|path| document_path_parts(path.trim(), names))
        .collect::<Vec<_>>();
    for (left_index, left) in paths.iter().enumerate() {
        for right in paths.iter().skip(left_index + 1) {
            if document_paths_overlap(left, right) {
                return Err(validation_message(
                    format!(
                        "Invalid ProjectionExpression: Two document paths overlap with each \
                         other; must remove or rewrite one of these paths; path one: [{}], path \
                         two: [{}]",
                        left.join(", "),
                        right.join(", ")
                    ),
                    prefixed,
                ));
            }
        }
    }
    Ok(())
}

fn update_expression_paths(
    sections: &[UpdateExpressionSection<'_>],
    names: Option<&std::collections::HashMap<String, String>>,
) -> Vec<UpdatePath> {
    let mut paths = Vec::new();
    for section in sections {
        for action in split_top_level_args(section.body) {
            let action = action.trim();
            if action.is_empty() {
                continue;
            }
            let path = match section.keyword {
                "SET" => top_level_split_once(action, '=').map(|(path, _)| path.trim()),
                "REMOVE" => Some(action),
                "ADD" | "DELETE" => action.split_whitespace().next(),
                _ => None,
            };
            if let Some(path) = path
                && let Some(parts) = document_path_parts(path, names)
            {
                paths.push(UpdatePath { parts });
            }
        }
    }
    paths
}

fn top_level_split_once(input: &str, separator: char) -> Option<(&str, &str)> {
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    for (index, ch) in input.char_indices() {
        match ch {
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            _ => {}
        }
        if ch == separator && paren_depth == 0 && bracket_depth == 0 {
            return Some((input.get(..index)?, input.get(index + ch.len_utf8()..)?));
        }
    }
    None
}

fn document_path_parts(
    path: &str,
    names: Option<&std::collections::HashMap<String, String>>,
) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    for part in path.split('.') {
        let part = part.trim();
        if part.is_empty() {
            return None;
        }
        let mut name_end = part.len();
        if let Some(index_start) = part.find('[') {
            name_end = index_start;
        }
        if name_end > 0 {
            let raw_name = part.get(..name_end)?;
            parts.push(resolve_expression_path_name(raw_name, names));
        }
        let mut rest = part.get(name_end..)?;
        while let Some(index_start) = rest.strip_prefix('[') {
            let end = index_start.find(']')?;
            parts.push(format!("[{}]", index_start.get(..end)?));
            rest = index_start.get(end + 1..)?;
        }
        if !rest.is_empty() {
            return None;
        }
    }
    Some(parts)
}

fn resolve_expression_path_name(
    name: &str,
    names: Option<&std::collections::HashMap<String, String>>,
) -> String {
    if name.starts_with('#')
        && let Some(resolved) = names.and_then(|names| names.get(name))
    {
        return resolved.clone();
    }
    name.to_string()
}

fn document_paths_overlap(left: &[String], right: &[String]) -> bool {
    let min_len = left.len().min(right.len());
    min_len > 0 && left[..min_len] == right[..min_len]
}

fn disallowed_function_error(
    expression: &str,
    label: &str,
    function_names: &[&str],
    expression_context: &str,
) -> Option<String> {
    function_names
        .iter()
        .find(|function_name| !function_calls(expression, function_name).is_empty())
        .map(|function_name| {
            format!(
                "Invalid {label}: The function is not allowed in {expression_context} expression; \
                 function: {function_name}"
            )
        })
}

fn nested_update_function_error(expression: &str, label: &str) -> Option<String> {
    for call in function_calls(expression, "if_not_exists") {
        let args = split_nonempty_top_level_args(call.args);
        if let Some(first_arg) = args.first()
            && nested_update_function_name(first_arg).is_some()
        {
            return Some(format!(
                "Invalid {label}: Operator or function requires a document path; operator or \
                 function: if_not_exists"
            ));
        }
    }

    for call in function_calls(expression, "list_append") {
        let args = split_nonempty_top_level_args(call.args);
        if args
            .iter()
            .any(|arg| nested_update_function_name(arg) == Some("list_append"))
        {
            return Some(format!(
                "Invalid {label}: The function is not allowed to be used this way in an \
                 expression; function: list_append"
            ));
        }
    }

    None
}

fn nested_update_function_name(argument: &str) -> Option<&str> {
    let argument = argument.trim_start();
    ["if_not_exists", "list_append"]
        .into_iter()
        .find(|function_name| {
            argument
                .get(function_name.len()..)
                .is_some_and(|tail| argument.starts_with(function_name) && tail.starts_with('('))
        })
}

fn function_syntax_or_arity_error(expression: &str, label: &str) -> Option<String> {
    for function_name in ["contains", "begins_with", "attribute_type", "size"] {
        for call in function_calls(expression, function_name) {
            let args = split_nonempty_top_level_args(call.args);
            if args.is_empty() {
                return Some(format!(
                    "Invalid {label}: Syntax error; token: \")\", near: \"{}\"",
                    empty_function_near(expression, call.open_paren, call.close_paren)
                ));
            }
            let expected = if function_name == "size" { 1 } else { 2 };
            if args.len() != expected {
                return Some(format!(
                    "Invalid {label}: Incorrect number of operands for operator or function; \
                     operator or function: {function_name}, number of operands: {}",
                    args.len()
                ));
            }
            if function_name == "size" {
                if let Some(nested_function) = nested_function_name(args[0]) {
                    return Some(format!(
                        "Invalid {label}: The function is not allowed to be used this way in an \
                         expression; function: {nested_function}"
                    ));
                }
                if !size_function_has_comparison(expression, call.close_paren) {
                    return Some(format!(
                        "Invalid {label}: The function is not allowed to be used this way in an \
                         expression; function: size"
                    ));
                }
            }
        }
    }
    None
}

#[derive(Clone, Copy)]
struct FunctionCall<'a> {
    args: &'a str,
    open_paren: usize,
    close_paren: usize,
}

fn function_calls<'a>(expression: &'a str, function_name: &str) -> Vec<FunctionCall<'a>> {
    let mut calls = Vec::new();
    let mut offset = 0usize;
    let needle = format!("{function_name}(");
    let lower_expression = expression.to_ascii_lowercase();
    while let Some(relative_start) = lower_expression
        .get(offset..)
        .and_then(|tail| tail.find(&needle))
    {
        let open_paren = offset + relative_start + function_name.len();
        let args_start = open_paren + 1;
        let mut depth = 1usize;
        let mut found_close = false;
        let Some(args_tail) = expression.get(args_start..) else {
            break;
        };
        for (relative_index, ch) in args_tail.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let close_paren = args_start + relative_index;
                        if let Some(args) = expression.get(args_start..close_paren) {
                            calls.push(FunctionCall {
                                args,
                                open_paren,
                                close_paren,
                            });
                        }
                        offset = close_paren + 1;
                        found_close = true;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !found_close {
            break;
        }
    }
    calls
}

fn empty_function_near(expression: &str, open_paren: usize, close_paren: usize) -> String {
    let mut near = expression
        .get(open_paren..=close_paren)
        .unwrap_or("()")
        .to_string();
    let tail = expression.get(close_paren + 1..).unwrap_or_default();
    let tail = tail.trim_start();
    if let Some(next) = tail.chars().next()
        && matches!(next, '=' | '<' | '>')
    {
        near.push(' ');
        near.push(next);
    }
    near
}

fn split_nonempty_top_level_args(args: &str) -> Vec<&str> {
    split_top_level_args(args)
        .into_iter()
        .map(str::trim)
        .filter(|arg| !arg.is_empty())
        .collect()
}

fn nested_function_name(argument: &str) -> Option<&str> {
    let argument = argument.trim_start();
    ["contains", "begins_with", "attribute_type", "size"]
        .into_iter()
        .find(|function_name| {
            argument
                .get(function_name.len()..)
                .is_some_and(|tail| argument.starts_with(function_name) && tail.starts_with('('))
        })
}

fn size_function_has_comparison(expression: &str, close_paren: usize) -> bool {
    let tail = expression
        .get(close_paren + 1..)
        .unwrap_or_default()
        .trim_start();
    tail.starts_with('=')
        || tail.starts_with("<>")
        || tail.starts_with("<=")
        || tail.starts_with(">=")
        || tail.starts_with('<')
        || tail.starts_with('>')
        || tail
            .get(..7)
            .is_some_and(|token| token.eq_ignore_ascii_case("BETWEEN"))
}

fn contains_same_operand_error(
    expression: &str,
    label: &str,
    names: Option<&std::collections::HashMap<String, String>>,
) -> Option<String> {
    for args in function_argument_lists(expression, "contains") {
        let parts = split_top_level_args(args);
        if parts.len() == 2 && parts[0].trim() == parts[1].trim() {
            let operand = resolve_expression_path_for_message(parts[0].trim(), names);
            return Some(format!(
                "Invalid {label}: The first operand must be distinct from the remaining operands \
                 for this operator or function; operator: contains, first operand: [{operand}]"
            ));
        }
    }
    None
}

fn resolve_expression_path_for_message(
    path: &str,
    names: Option<&std::collections::HashMap<String, String>>,
) -> String {
    let Some(names) = names else {
        return path.to_string();
    };
    names.get(path).cloned().unwrap_or_else(|| path.to_string())
}

fn attribute_type_literal_operand_error(expression: &str, label: &str) -> Option<String> {
    for args in function_argument_lists(expression, "attribute_type") {
        let parts = split_top_level_args(args);
        let Some(attribute_type_operand) = parts.get(1).map(|part| part.trim()) else {
            continue;
        };
        if !attribute_type_operand.starts_with(':') {
            return Some(format!(
                "Invalid {label}: Incorrect operand type for operator or function; operator or \
                 function: attribute_type, operand type: \
                 {{S,SS,N,NS,B,BS,BOOL,NULL,L,M,HD,DOUBLE,FLOAT,HDS,FS,DOUBLESET,DICT,DECIMAL,INT,\
                 DECIMALSET,INTSET}}"
            ));
        }
    }
    None
}

fn validate_attribute_type_value_operands(
    expression: &str,
    label: &str,
    values: Option<&std::collections::HashMap<String, crate::AttributeValue>>,
    prefixed: bool,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    for args in function_argument_lists(expression, "attribute_type") {
        let parts = split_top_level_args(args);
        let Some(attribute_type_operand) = parts.get(1).map(|part| part.trim()) else {
            continue;
        };
        if !attribute_type_operand.starts_with(':') {
            continue;
        }
        let Some(value) = values.get(attribute_type_operand) else {
            continue;
        };
        let crate::AttributeValue::S(attribute_type) = value else {
            return Err(validation_message(
                format!(
                    "Invalid {label}: Incorrect operand type for operator or function; operator \
                     or function: attribute_type, operand type: \
                     {{S,SS,N,NS,B,BS,BOOL,NULL,L,M,HD,DOUBLE,FLOAT,HDS,FS,DOUBLESET,DICT,DECIMAL,\
                     INT,DECIMALSET,INTSET}}"
                ),
                prefixed,
            ));
        };
        if !matches!(
            attribute_type.as_str(),
            "S" | "SS" | "N" | "NS" | "B" | "BS" | "BOOL" | "NULL" | "L" | "M"
        ) {
            let valid_types = if prefixed {
                "{S,SS,N,NS,B,BS,BOOL,NULL,L,M}"
            } else {
                "{N,BS,L,B,NULL,M,S,SS,NS,BOOL}"
            };
            return Err(validation_message(
                format!(
                    "Invalid {label}: Invalid attribute type name found; type: {attribute_type}, \
                     valid types: {valid_types}"
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

fn validate_begins_with_value_operands(
    expression: &str,
    label: &str,
    values: Option<&std::collections::HashMap<String, crate::AttributeValue>>,
    prefixed: bool,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    for args in function_argument_lists(expression, "begins_with") {
        let parts = split_top_level_args(args);
        let Some(prefix_operand) = parts.get(1).map(|part| part.trim()) else {
            continue;
        };
        if !prefix_operand.starts_with(':') {
            continue;
        }
        let Some(value) = values.get(prefix_operand) else {
            continue;
        };
        if !matches!(
            value,
            crate::AttributeValue::S(_) | crate::AttributeValue::B(_)
        ) {
            return Err(validation_message(
                format!(
                    "Invalid {label}: Incorrect operand type for operator or function; operator \
                     or function: begins_with, operand type: {}",
                    attribute_value_type_name(value)
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

fn attribute_value_type_name(value: &crate::AttributeValue) -> &'static str {
    match value {
        crate::AttributeValue::S(_) => "S",
        crate::AttributeValue::N(_) => "N",
        crate::AttributeValue::B(_) => "B",
        crate::AttributeValue::SS(_) => "SS",
        crate::AttributeValue::NS(_) => "NS",
        crate::AttributeValue::BS(_) => "BS",
        crate::AttributeValue::BOOL(_) => "BOOL",
        crate::AttributeValue::NULL(_) => "NULL",
        crate::AttributeValue::L(_) => "L",
        crate::AttributeValue::M(_) => "M",
    }
}

fn function_argument_lists<'a>(expression: &'a str, function_name: &str) -> Vec<&'a str> {
    let mut args = Vec::new();
    let mut offset = 0usize;
    let needle = format!("{function_name}(");
    let lower_expression = expression.to_ascii_lowercase();
    while let Some(relative_start) = lower_expression
        .get(offset..)
        .and_then(|tail| tail.find(&needle))
    {
        let args_start = offset + relative_start + needle.len();
        let mut depth = 1usize;
        let Some(args_tail) = expression.get(args_start..) else {
            break;
        };
        for (relative_index, ch) in args_tail.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let args_end = args_start + relative_index;
                        if let Some(argument_list) = expression.get(args_start..args_end) {
                            args.push(argument_list);
                        }
                        offset = args_end + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        if offset < args_start {
            break;
        }
    }
    args
}

fn split_top_level_args(args: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    for (index, ch) in args.char_indices() {
        match ch {
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            ',' if paren_depth == 0 && bracket_depth == 0 => {
                if let Some(part) = args.get(start..index) {
                    parts.push(part);
                }
                start = index + 1;
            }
            _ => {}
        }
    }
    if let Some(part) = args.get(start..) {
        parts.push(part);
    }
    parts
}

fn document_path_index_syntax_error(expression: &str, label: &str) -> Option<String> {
    let bytes = expression.as_bytes();
    let mut offset = 0usize;
    while let Some(relative_start) = expression.get(offset..)?.find('[') {
        let start = offset + relative_start;
        let after_start = start + 1;
        let first = bytes.get(after_start).copied();
        match first {
            Some(b'0'..=b'9') => {
                let mut end = after_start + 1;
                while matches!(bytes.get(end), Some(b'0'..=b'9')) {
                    end += 1;
                }
                match bytes.get(end).copied() {
                    Some(b']') => {
                        offset = end + 1;
                    }
                    Some(byte) => {
                        let token = char::from(byte).to_string();
                        return Some(document_path_syntax_message(
                            label,
                            &token,
                            near_until_whitespace(expression, after_start),
                        ));
                    }
                    None => {
                        return Some(document_path_syntax_message(
                            label,
                            "<EOF>",
                            expression.get(start..).unwrap_or_default(),
                        ));
                    }
                }
            }
            Some(b']') => {
                return Some(document_path_syntax_message(
                    label,
                    "]",
                    empty_list_index_near(expression, start),
                ));
            }
            Some(b'-') => {
                return Some(document_path_syntax_message(
                    label,
                    "-",
                    signed_list_index_near(expression, start),
                ));
            }
            Some(byte) if byte.is_ascii_alphabetic() || byte == b'_' => {
                let mut end = after_start + 1;
                while matches!(bytes.get(end), Some(byte) if byte.is_ascii_alphanumeric() || *byte == b'_')
                {
                    end += 1;
                }
                return Some(document_path_syntax_message(
                    label,
                    expression.get(after_start..end).unwrap_or_default(),
                    expression
                        .get(start..end + usize::from(bytes.get(end) == Some(&b']')))
                        .unwrap_or_default(),
                ));
            }
            Some(byte) => {
                let token = char::from(byte).to_string();
                return Some(document_path_syntax_message(
                    label,
                    &token,
                    expression.get(start..).unwrap_or_default(),
                ));
            }
            None => {
                return Some(document_path_syntax_message(label, "<EOF>", "["));
            }
        }
    }
    None
}

fn empty_list_index_near(expression: &str, start: usize) -> &str {
    expression
        .get(start..)
        .and_then(|near| near.starts_with("[].").then_some("[]."))
        .or_else(|| expression.get(start..start.saturating_add(2)))
        .unwrap_or_default()
}

fn signed_list_index_near(expression: &str, start: usize) -> &str {
    let bytes = expression.as_bytes();
    let mut end = start + 2;
    while matches!(bytes.get(end), Some(b'0'..=b'9')) {
        end += 1;
    }
    expression.get(start..end).unwrap_or_default()
}

fn near_until_whitespace(expression: &str, start: usize) -> &str {
    let end = expression
        .get(start..)
        .and_then(|tail| {
            tail.char_indices()
                .find(|(_, ch)| ch.is_ascii_whitespace())
                .map(|(index, _)| start + index)
        })
        .unwrap_or(expression.len());
    expression.get(start..end).unwrap_or_default()
}

fn document_path_syntax_message(label: &str, token: &str, near: &str) -> String {
    format!("Invalid {label}: Syntax error; token: \"{token}\", near: \"{near}\"")
}

fn reserved_word_in_expression<'a>(expression: &'a str, label: &str) -> Option<&'a str> {
    let bytes = expression.as_bytes();
    let mut start = None;

    for (index, ch) in expression.char_indices() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            if start.is_none() {
                start = Some(index);
            }
            continue;
        }

        if let Some(token_start) = start.take()
            && let Some(token) =
                reserved_expression_token(expression, bytes, token_start, index, label)
        {
            return Some(token);
        }
    }

    start.and_then(|token_start| {
        reserved_expression_token(expression, bytes, token_start, expression.len(), label)
    })
}

fn reserved_expression_token<'a>(
    expression: &'a str,
    bytes: &[u8],
    start: usize,
    end: usize,
    label: &str,
) -> Option<&'a str> {
    let previous = start
        .checked_sub(1)
        .and_then(|index| bytes.get(index).copied());
    if matches!(previous, Some(b'#' | b':')) {
        return None;
    }

    let token = expression.get(start..end)?;
    let next = expression
        .get(end..)?
        .chars()
        .find(|ch| !ch.is_ascii_whitespace());
    if matches!(next, Some('(')) || is_expression_grammar_keyword(token, label) {
        return None;
    }

    is_dynamodb_reserved_word(token).then_some(token)
}

pub(crate) fn is_dynamodb_reserved_word(token: &str) -> bool {
    DYNAMODB_RESERVED_WORD_SET.contains(&AsciiCaseInsensitive(token))
}

#[cfg(test)]
pub(crate) fn is_dynamodb_reserved_word_linear(token: &str) -> bool {
    DYNAMODB_RESERVED_WORDS
        .split_ascii_whitespace()
        .any(|word| word.eq_ignore_ascii_case(token))
}

fn is_expression_grammar_keyword(token: &str, label: &str) -> bool {
    let is_one_of = |words: &[&str]| words.iter().any(|word| word.eq_ignore_ascii_case(token));
    match label {
        "ProjectionExpression" => false,
        "UpdateExpression" => is_one_of(&[
            "AND", "OR", "NOT", "BETWEEN", "IN", "SET", "ADD", "REMOVE", "DELETE",
        ]),
        _ => is_one_of(&["AND", "OR", "NOT", "BETWEEN", "IN"]),
    }
}

fn has_redundant_parentheses(expression: &str) -> bool {
    let Some((start, end)) = trimmed_bounds(expression) else {
        return false;
    };
    let Some(expression) = expression.get(start..end) else {
        return false;
    };
    if fully_parenthesized(expression)
        && let Some(inner) = inner_parenthesized_expression(expression)
        && fully_parenthesized(inner.trim())
    {
        return true;
    }
    contains_double_parenthesized_segment(expression)
}

fn contains_double_parenthesized_segment(expression: &str) -> bool {
    let mut stack = Vec::new();
    for (index, ch) in expression.char_indices() {
        match ch {
            '(' => stack.push(index),
            ')' => {
                let Some(start) = stack.pop() else {
                    continue;
                };
                let Some(inner) = expression.get(start + 1..index) else {
                    continue;
                };
                if fully_parenthesized(inner.trim()) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

fn fully_parenthesized(expression: &str) -> bool {
    let Some(inner) = inner_parenthesized_expression(expression) else {
        return false;
    };
    inner.len() + 2 == expression.len()
}

fn inner_parenthesized_expression(expression: &str) -> Option<&str> {
    if !expression.starts_with('(') || !expression.ends_with(')') {
        return None;
    }
    let mut depth = 0usize;
    for (index, ch) in expression.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return (index == expression.len() - 1)
                        .then(|| expression.get(1..index))
                        .flatten();
                }
            }
            _ => {}
        }
    }
    None
}

fn trimmed_bounds(value: &str) -> Option<(usize, usize)> {
    let start = value
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_whitespace()).then_some(index))?;
    let end = value
        .char_indices()
        .rev()
        .find_map(|(index, ch)| (!ch.is_whitespace()).then_some(index + ch.len_utf8()))?;
    Some((start, end))
}

fn validation_message(message: String, prefixed: bool) -> String {
    if prefixed {
        format!("1 validation error detected: {message}")
    } else {
        message
    }
}
