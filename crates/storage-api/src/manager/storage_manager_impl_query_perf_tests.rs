use std::{
    collections::HashMap,
    process::Command,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateGlobalSecondaryIndex, GlobalSecondaryIndex,
    IndexName, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, Projection,
    ProjectionType, QueryRequest, StoredTableInfo, TableName, TableStatus, TimestampMillis,
    subset_expression_attribute_names_for_expression,
    subset_expression_attribute_values_for_expression, validate_key_attributes_for_schema,
};

use crate::manager::storage_manager_impl_query::{
    PreparedQuery, tokenize_key_condition_expression, validate_query_key_condition_values,
};

const ITERATIONS: usize = 25_000;

fn realistic_table_info() -> StoredTableInfo {
    StoredTableInfo {
        max_indexers: storage_types::MaxIndexers::ZERO,
        table_name: TableName::new("perf_query_table"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi1pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi1sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi2pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi2sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        global_secondary_indexes: Some(vec![
            gsi("gsi1", "gsi1pk", "gsi1sk"),
            gsi("gsi2", "gsi2pk", "gsi2sk"),
        ]),
        table_size_bytes: 0,
        item_count: 10_000,
        stream_specification: None,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

fn gsi(name: &str, pk: &str, sk: &str) -> GlobalSecondaryIndex {
    CreateGlobalSecondaryIndex {
        index_name: IndexName::new(name),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: pk.to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: sk.to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }
    .into()
}

fn realistic_query_request() -> QueryRequest {
    let key = "x".repeat(100);
    let mut request = QueryRequest::new(
        TableName::new("perf_query_table"),
        "#pk = :pk AND begins_with(#sk, :sk_prefix)".to_string(),
    );
    request.expression_attribute_names = Some(HashMap::from([
        ("#pk".to_string(), "pk".to_string()),
        ("#sk".to_string(), "sk".to_string()),
    ]));
    request.expression_attribute_values = Some(HashMap::from([
        (
            ":pk".to_string(),
            AttributeValue::S(format!("tenant#{key}")),
        ),
        (
            ":sk_prefix".to_string(),
            AttributeValue::S(format!("item#{key}")),
        ),
    ]));
    request
}

fn measure_query_key_validation_allocations() -> alloc_counter::AllocationReport<'static> {
    let table_info = realistic_table_info();
    let request = realistic_query_request();
    let guard = AllocationGuard::start(
        module_path!(),
        "query_key_condition_validation_allocation_profile_tests",
        file!(),
        line!(),
        Some("optimized"),
    );

    for _ in 0..ITERATIONS {
        validate_query_key_condition_values(&request, &table_info).expect("valid query keys");
    }

    guard.finish()
}

fn measure_legacy_query_key_validation_allocations() -> alloc_counter::AllocationReport<'static> {
    let table_info = realistic_table_info();
    let request = realistic_query_request();
    let guard = AllocationGuard::start(
        module_path!(),
        "legacy_query_key_condition_validation_allocation_profile_tests",
        file!(),
        line!(),
        Some("legacy"),
    );

    for _ in 0..ITERATIONS {
        legacy_validate_query_key_condition_values(&request, &table_info)
            .expect("valid query keys");
    }

    guard.finish()
}

fn measure_query_key_validation_runtime(validate: fn(&QueryRequest, &StoredTableInfo)) -> Duration {
    let table_info = realistic_table_info();
    let request = realistic_query_request();
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        validate(&request, &table_info);
    }
    started.elapsed()
}

fn measure_prepared_query_allocations(
    build: fn(&QueryRequest) -> storage::QueryTableInput,
    label: &'static str,
) -> alloc_counter::AllocationReport<'static> {
    let request = realistic_query_request();
    let guard = AllocationGuard::start(
        module_path!(),
        "prepared_query_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );

    for _ in 0..ITERATIONS {
        let input = build(&request);
        assert_eq!(input.table_name.as_ref(), "perf_query_table");
    }

    guard.finish()
}

fn measure_prepared_query_runtime(
    build: fn(&QueryRequest) -> storage::QueryTableInput,
) -> Duration {
    let request = realistic_query_request();
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        let input = build(&request);
        assert_eq!(input.table_name.as_ref(), "perf_query_table");
    }
    started.elapsed()
}

#[test]
fn query_key_condition_validation_allocation_profile_tests() {
    let legacy = measure_legacy_query_key_validation_allocations();
    let report = measure_query_key_validation_allocations();
    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
    assert!(report.allocation_count < legacy.allocation_count);
    assert!(report.allocated_bytes < legacy.allocated_bytes);
}

#[test]
fn prepared_query_allocation_profile_tests() {
    const ISOLATED_ENV: &str = "AUX_STORAGE_API_PREPARED_QUERY_ALLOCATION_ISOLATED";
    if std::env::var_os(ISOLATED_ENV).is_none() {
        let status = Command::new(
            std::env::current_exe()
                .expect("prepared query allocation test executable should be available"),
        )
        .arg("--exact")
        .arg(
            "manager::storage_manager_impl_query_perf_tests::prepared_query_allocation_profile_tests",
        )
        .arg("--nocapture")
        .env(ISOLATED_ENV, "1")
        .status()
        .expect("isolated prepared query allocation test child should start");
        assert!(
            status.success(),
            "isolated prepared query allocation test failed"
        );
        return;
    }

    let legacy = measure_prepared_query_allocations(legacy_prepared_query_table_input, "legacy");
    let borrowed =
        measure_prepared_query_allocations(borrowed_prepared_query_table_input, "borrowed");
    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&borrowed);
    assert!(borrowed.allocation_count < legacy.allocation_count);
    assert!(borrowed.allocated_bytes < legacy.allocated_bytes);
}

#[test]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture --test-threads=1"]
fn query_key_condition_validation_runtime_perf_probe() {
    let tokens = tokenize_key_condition_expression("#pk = :pk AND begins_with(#sk, :sk_prefix)");
    assert_eq!(tokens.len(), 10);
    let legacy = measure_query_key_validation_runtime(|request, table_info| {
        legacy_validate_query_key_condition_values(request, table_info).expect("valid query keys");
    });
    let elapsed = measure_query_key_validation_runtime(|request, table_info| {
        validate_query_key_condition_values(request, table_info).expect("valid query keys");
    });
    println!(
        "legacy_query_key_condition_validation iterations={ITERATIONS} elapsed_ms={:.3} \
         ns_per_iter={:.2}",
        legacy.as_secs_f64() * 1_000.0,
        legacy.as_nanos() as f64 / ITERATIONS as f64
    );
    println!(
        "optimized_query_key_condition_validation iterations={ITERATIONS} elapsed_ms={:.3} \
         ns_per_iter={:.2}",
        elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_nanos() as f64 / ITERATIONS as f64
    );
    assert!(elapsed.as_nanos() > 0);
}

#[test]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture --test-threads=1"]
fn prepared_query_runtime_perf_probe() {
    let legacy = measure_prepared_query_runtime(legacy_prepared_query_table_input);
    let borrowed = measure_prepared_query_runtime(borrowed_prepared_query_table_input);
    println!(
        "legacy_prepared_query iterations={ITERATIONS} elapsed_ms={:.3} ns_per_iter={:.2}",
        legacy.as_secs_f64() * 1_000.0,
        legacy.as_nanos() as f64 / ITERATIONS as f64
    );
    println!(
        "borrowed_prepared_query iterations={ITERATIONS} elapsed_ms={:.3} ns_per_iter={:.2}",
        borrowed.as_secs_f64() * 1_000.0,
        borrowed.as_nanos() as f64 / ITERATIONS as f64
    );
    assert!(borrowed.as_nanos() > 0);
}

fn borrowed_prepared_query_table_input(request: &QueryRequest) -> storage::QueryTableInput {
    let names = subset_expression_attribute_names_for_expression(
        &request.key_condition_expression,
        request.expression_attribute_names.as_ref(),
    );
    let values = subset_expression_attribute_values_for_expression(
        &request.key_condition_expression,
        request.expression_attribute_values.as_ref(),
    );
    PreparedQuery::from_request(request, None, names.as_ref(), values.as_ref()).table_input()
}

fn legacy_prepared_query_table_input(request: &QueryRequest) -> storage::QueryTableInput {
    let names = subset_expression_attribute_names_for_expression(
        &request.key_condition_expression,
        request.expression_attribute_names.as_ref(),
    );
    let values = subset_expression_attribute_values_for_expression(
        &request.key_condition_expression,
        request.expression_attribute_values.as_ref(),
    );
    let query = LegacyPreparedQuery {
        table_name: request.table_name.clone(),
        key_condition_expression: request.key_condition_expression.clone(),
        expression_attribute_names: names,
        expression_attribute_values: values,
        limit: request.limit,
        exclusive_start_key: None,
        scan_index_forward: request.scan_index_forward,
        consistent_read: request.consistent_read.unwrap_or(false),
    };
    query.table_input()
}

struct LegacyPreparedQuery {
    table_name: TableName,
    key_condition_expression: String,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    limit: Option<u32>,
    exclusive_start_key: Option<String>,
    scan_index_forward: Option<bool>,
    consistent_read: bool,
}

impl LegacyPreparedQuery {
    fn table_input(&self) -> storage::QueryTableInput {
        storage::QueryTableInput {
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

fn legacy_validate_query_key_condition_values(
    request: &QueryRequest,
    table_info: &StoredTableInfo,
) -> Result<(), storage_types::StorageError> {
    let key_schema = &table_info.key_schema;
    let Some(values) = request.expression_attribute_values.as_ref() else {
        return Ok(());
    };
    let tokens = legacy_tokenize_key_condition_expression(&request.key_condition_expression);

    for (attribute_name, value_token) in legacy_key_condition_value_tokens(
        &tokens,
        request.expression_attribute_names.as_ref(),
        key_schema,
    ) {
        let Some(value) = values.get(&value_token) else {
            continue;
        };
        let Some(schema) = key_schema
            .iter()
            .find(|element| element.attribute_name == attribute_name)
        else {
            continue;
        };
        let mut key_attributes = KeyAttributes::with_capacity(1);
        key_attributes.insert(attribute_name, value.clone());
        validate_key_attributes_for_schema(std::slice::from_ref(schema), &key_attributes)?;
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LegacyKeyConditionToken {
    Identifier(String),
    Value(String),
    Function(String),
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

fn legacy_tokenize_key_condition_expression(expression: &str) -> Vec<LegacyKeyConditionToken> {
    let mut tokens = Vec::new();
    let mut chars = expression.char_indices().peekable();

    while let Some((start, ch)) = chars.next() {
        match ch {
            c if c.is_whitespace() => {}
            '(' => tokens.push(LegacyKeyConditionToken::LeftParen),
            ')' => tokens.push(LegacyKeyConditionToken::RightParen),
            ',' => tokens.push(LegacyKeyConditionToken::Comma),
            '=' => tokens.push(LegacyKeyConditionToken::Eq),
            '<' => {
                if matches!(chars.peek(), Some((_, '='))) {
                    chars.next();
                    tokens.push(LegacyKeyConditionToken::Le);
                } else {
                    tokens.push(LegacyKeyConditionToken::Lt);
                }
            }
            '>' => {
                if matches!(chars.peek(), Some((_, '='))) {
                    chars.next();
                    tokens.push(LegacyKeyConditionToken::Ge);
                } else {
                    tokens.push(LegacyKeyConditionToken::Gt);
                }
            }
            ':' => {
                let end = legacy_read_expression_token_end(expression, &mut chars);
                if let Some(token) = expression.get(start..end) {
                    tokens.push(LegacyKeyConditionToken::Value(token.to_string()));
                }
            }
            '#' => {
                let end = legacy_read_expression_token_end(expression, &mut chars);
                if let Some(token) = expression.get(start..end) {
                    tokens.push(LegacyKeyConditionToken::Identifier(token.to_string()));
                }
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let end = legacy_read_expression_token_end(expression, &mut chars);
                if let Some(word) = expression.get(start..end) {
                    match word {
                        "AND" | "and" => tokens.push(LegacyKeyConditionToken::And),
                        "BETWEEN" | "between" => tokens.push(LegacyKeyConditionToken::Between),
                        "begins_with" => {
                            tokens.push(LegacyKeyConditionToken::Function(word.to_string()));
                        }
                        _ => tokens.push(LegacyKeyConditionToken::Identifier(word.to_string())),
                    }
                }
            }
            _ => {}
        }
    }

    tokens
}

fn legacy_read_expression_token_end(
    expression: &str,
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
) -> usize {
    while let Some((index, ch)) = chars.peek() {
        if !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '#')) {
            return *index;
        }
        chars.next();
    }
    expression.len()
}

fn legacy_key_condition_value_tokens(
    tokens: &[LegacyKeyConditionToken],
    names: Option<&HashMap<String, String>>,
    key_schema: &[KeySchemaElement],
) -> Vec<(String, String)> {
    let mut value_tokens = Vec::new();

    for index in 0..tokens.len() {
        legacy_collect_comparison_key_value(tokens, index, names, key_schema, &mut value_tokens);
        legacy_collect_begins_with_key_value(tokens, index, names, key_schema, &mut value_tokens);
    }

    value_tokens
}

fn legacy_collect_comparison_key_value(
    tokens: &[LegacyKeyConditionToken],
    index: usize,
    names: Option<&HashMap<String, String>>,
    key_schema: &[KeySchemaElement],
    value_tokens: &mut Vec<(String, String)>,
) {
    let Some(operator) = tokens.get(index + 1) else {
        return;
    };
    if !matches!(
        operator,
        LegacyKeyConditionToken::Eq
            | LegacyKeyConditionToken::Lt
            | LegacyKeyConditionToken::Le
            | LegacyKeyConditionToken::Gt
            | LegacyKeyConditionToken::Ge
    ) {
        return;
    }

    match (tokens.get(index), tokens.get(index + 2)) {
        (
            Some(LegacyKeyConditionToken::Identifier(identifier)),
            Some(LegacyKeyConditionToken::Value(value_token)),
        ) => legacy_push_key_value_token(identifier, value_token, names, key_schema, value_tokens),
        (
            Some(LegacyKeyConditionToken::Value(value_token)),
            Some(LegacyKeyConditionToken::Identifier(identifier)),
        ) => legacy_push_key_value_token(identifier, value_token, names, key_schema, value_tokens),
        _ => {}
    }
}

fn legacy_collect_begins_with_key_value(
    tokens: &[LegacyKeyConditionToken],
    index: usize,
    names: Option<&HashMap<String, String>>,
    key_schema: &[KeySchemaElement],
    value_tokens: &mut Vec<(String, String)>,
) {
    let (
        Some(LegacyKeyConditionToken::Function(function)),
        Some(LegacyKeyConditionToken::LeftParen),
        Some(LegacyKeyConditionToken::Identifier(identifier)),
        Some(LegacyKeyConditionToken::Comma),
        Some(LegacyKeyConditionToken::Value(value_token)),
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
    if function != "begins_with" {
        return;
    }

    legacy_push_key_value_token(identifier, value_token, names, key_schema, value_tokens);
}

fn legacy_push_key_value_token(
    identifier: &str,
    value_token: &str,
    names: Option<&HashMap<String, String>>,
    key_schema: &[KeySchemaElement],
    value_tokens: &mut Vec<(String, String)>,
) {
    let attribute_name = if identifier.starts_with('#') {
        names
            .and_then(|names| names.get(identifier))
            .cloned()
            .unwrap_or_else(|| identifier.to_string())
    } else {
        identifier.to_string()
    };
    if key_schema
        .iter()
        .any(|element| element.attribute_name == attribute_name)
    {
        value_tokens.push((attribute_name, value_token.to_string()));
    }
}
