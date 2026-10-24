use std::hint::black_box;

use alloc_counter::AllocationGuard;

use crate::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeyAttributes, KeySchemaElement,
    KeyType, StorageEnum, StoredTableInfo, TableName, TableStatus, TimestampMillis,
    TransactGetItem, TransactGetRequest, context::WrappedError as _,
    preflight_transact_get_item_key_with_table_info, preflight_transact_write_key_with_table_info,
    validate_key_attributes_for_schema,
};

fn key_cases() -> Vec<(&'static str, StoredTableInfo, KeyAttributes)> {
    let mut cases = Vec::new();
    for (label, value) in [
        ("valid", "123.45"),
        ("invalid", "x"),
        ("precision", "123456789012345678901234567890123456789"),
        ("underflow", "1e-131"),
        ("overflow", "1e126"),
    ] {
        cases.push((
            label,
            number_table_info(),
            [
                ("pk".to_string(), AttributeValue::N(value.to_string())),
                ("sk".to_string(), AttributeValue::N("1".to_string())),
            ]
            .into_iter()
            .collect(),
        ));
    }
    for (label, attribute_type, value) in [
        (
            "empty_string",
            KeyAttributeType::S,
            AttributeValue::S(String::new()),
        ),
        (
            "empty_binary",
            KeyAttributeType::B,
            AttributeValue::B(String::new()),
        ),
        (
            "oversize",
            KeyAttributeType::S,
            AttributeValue::S("x".repeat(2049)),
        ),
        (
            "wrong_type",
            KeyAttributeType::N,
            AttributeValue::S("x".to_string()),
        ),
    ] {
        let mut table = number_table_info();
        table.attribute_definitions[0].attribute_type = attribute_type;
        cases.push((
            label,
            table,
            [
                ("pk".to_string(), value),
                ("sk".to_string(), AttributeValue::N("1".to_string())),
            ]
            .into_iter()
            .collect(),
        ));
    }
    cases
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_key_validation_failures_when_classified_then_report_cpu_and_allocations() {
    for (label, table, key) in key_cases() {
        if let Err(error) = validate_key_attributes_for_schema(&table.key_schema, &key) {
            report(&format!("classify_write_{label}"), || {
                black_box(&error).is_transact_write_key_error_top_level()
            });
            report(&format!("classify_get_{label}"), || {
                black_box(&error).is_transact_get_key_error_top_level()
            });
        }
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_transaction_key_shapes_when_preflighted_then_report_cpu_and_allocations() {
    for (label, table, key) in key_cases() {
        let get = TransactGetItem {
            get: TransactGetRequest {
                table_name: table.table_name.clone(),
                key: key.clone(),
                projection_expression: None,
                expression_attribute_names: None,
            },
        };
        report(&format!("preflight_write_{label}"), || {
            preflight_transact_write_key_with_table_info(black_box(&table), black_box(&key))
        });
        report(&format!("preflight_get_{label}"), || {
            preflight_transact_get_item_key_with_table_info(black_box(&get), black_box(&table))
        });
    }
}

#[test]
fn given_transaction_key_failures_when_preflighted_then_preserve_top_level_and_member_errors() {
    for (label, table, key) in key_cases() {
        let get = TransactGetItem {
            get: TransactGetRequest {
                table_name: table.table_name.clone(),
                key: key.clone(),
                projection_expression: None,
                expression_attribute_names: None,
            },
        };
        let write = preflight_transact_write_key_with_table_info(&table, &key);
        let read = preflight_transact_get_item_key_with_table_info(&get, &table);
        if matches!(
            label,
            "invalid" | "precision" | "underflow" | "empty_string" | "empty_binary"
        ) {
            assert!(
                matches!(
                    write.unwrap_err().to_enum(),
                    StorageEnum::RawValidation { .. }
                ),
                "{label}"
            );
        } else {
            assert!(
                write.unwrap().validation_reason.is_some() == (label != "valid"),
                "{label}"
            );
        }
        if matches!(label, "empty_string" | "empty_binary") {
            assert!(
                matches!(read.unwrap_err().to_enum(), StorageEnum::Validation { .. }),
                "{label}"
            );
        } else {
            assert!(
                read.unwrap().validation_reason.is_some() == (label != "valid"),
                "{label}"
            );
        }
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_numeric_validation_shapes_when_validated_then_report_cpu_and_allocations() {
    for (label, value) in [
        ("valid", "123.45"),
        ("empty", ""),
        ("invalid", "x"),
        ("precision", "123456789012345678901234567890123456789"),
        ("underflow", "1e-131"),
        ("overflow", "1e126"),
        ("bad_exponent", "1e"),
        ("exponent_range", "1e999999999999999999999"),
    ] {
        report(&format!("number_{label}"), || {
            crate::dynamodb_limits::validate_number(black_box(value))
        });
    }
}

fn report<T>(label: &str, operation: impl Fn() -> T) {
    for _ in 0..100 {
        drop(black_box(operation()));
    }
    let guard = AllocationGuard::start(
        module_path!(),
        "transaction_errors",
        file!(),
        line!(),
        Some(label),
    );
    let started = cpu_time::ProcessTime::now();
    for _ in 0..100_000 {
        drop(black_box(operation()));
    }
    let cpu = started.elapsed();
    alloc_counter::emit_report(&guard.finish()).unwrap();
    eprintln!(
        "transaction_errors label={label} iterations=100000 cpu_ns={}",
        cpu.as_nanos()
    );
}

fn number_table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("transaction_runtime_number_keys"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        max_indexers: crate::MaxIndexers::ZERO,
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::N,
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
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: crate::StreamRetentionDuration::default(),
        default_item_stream_duration: crate::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

#[test]
fn given_typed_and_untyped_failures_when_wrapped_and_bound_then_classification_uses_kind() {
    use crate::{
        StorageError, StorageValidationKind, context::ErrorContext as _,
        transaction_write_binding_error_with_len,
    };
    for kind in [
        StorageValidationKind::EmptyStringKey,
        StorageValidationKind::EmptyBinaryKey,
        StorageValidationKind::EmptyIndexer,
        StorageValidationKind::InvalidIndexerType,
        StorageValidationKind::InvalidNumber,
        StorageValidationKind::NumberPrecision,
        StorageValidationKind::NumberUnderflow,
    ] {
        let error = StorageError::validation_detail(kind, "detail without classification keywords");
        let wrapped = Result::<(), StorageError>::Err(error)
            .context("outer context")
            .unwrap_err();
        assert_eq!(wrapped.validation_kind(), kind);
        let (root, contexts) = wrapped.recursive_context(Vec::new());
        assert!(
            matches!(root, StorageEnum::Validation { message, .. } if message == "detail without classification keywords")
        );
        assert_eq!(contexts, ["outer context"]);
        let mapped = transaction_write_binding_error_with_len(1, 3, wrapped);
        assert!(
            matches!(mapped.to_enum(), StorageEnum::RawValidation { message } if message == "detail without classification keywords")
        );
    }
    for message in [
        "Indexers:empty_string",
        "unrelated detail: The parameter cannot be converted to a numeric value",
        "key attribute cannot contain an empty string value",
    ] {
        let mapped =
            transaction_write_binding_error_with_len(1, 3, StorageError::validation(message));
        assert!(
            matches!(mapped.to_enum(), StorageEnum::TransactionCanceled { reasons } if reasons == &["None", &format!("ValidationError\t{message}"), "None"])
        );
    }
    let overflow = transaction_write_binding_error_with_len(
        1,
        3,
        StorageError::validation(StorageValidationKind::NumberOverflow),
    );
    assert!(
        matches!(overflow.to_enum(), StorageEnum::TransactionCanceled { reasons } if reasons[1].starts_with("ValidationError\tNumber overflow."))
    );
    let non_validation = Result::<(), StorageError>::Err(StorageError::internal("failure"))
        .context("preserved")
        .unwrap_err()
        .into_raw_validation();
    assert_eq!(
        non_validation.recursive_context(Vec::new()).1,
        ["preserved"]
    );
    assert!(
        matches!(non_validation.to_enum(), StorageEnum::InternalServerError { message } if message == "failure")
    );
}

#[test]
fn given_indexer_failures_when_bound_then_preserve_top_level_message() {
    use crate::{IndexedWireItem, StorageValidationKind, transaction_write_binding_error_with_len};
    for (value, kind) in [
        (
            AttributeValue::S(String::new()),
            StorageValidationKind::EmptyIndexer,
        ),
        (
            AttributeValue::N("1".to_string()),
            StorageValidationKind::InvalidIndexerType,
        ),
    ] {
        let item: crate::AttributeMap = [("indexed".to_string(), value)].into_iter().collect();
        let error = IndexedWireItem::validate_logical_item_names(&item, &["indexed".to_string()])
            .unwrap_err();
        assert_eq!(error.validation_kind(), kind);
        let error = transaction_write_binding_error_with_len(0, 1, error);
        assert!(
            matches!(error.to_enum(), StorageEnum::RawValidation { message } if message == kind.message())
        );
    }
}

#[test]
fn given_number_failures_when_parsed_then_preserve_codec_details_and_protocol_messages() {
    use crate::{DynamodbNumberError, StorageValidationKind, canonical_dynamodb_number};
    for (raw, error, kind) in [
        (
            "",
            DynamodbNumberError::Empty,
            StorageValidationKind::InvalidNumber,
        ),
        (
            "x",
            DynamodbNumberError::MissingDigit,
            StorageValidationKind::InvalidNumber,
        ),
        (
            "1x",
            DynamodbNumberError::Invalid,
            StorageValidationKind::InvalidNumber,
        ),
        (
            "1e",
            DynamodbNumberError::InvalidExponent,
            StorageValidationKind::InvalidNumber,
        ),
        (
            "1e999999999999999999999",
            DynamodbNumberError::ExponentOutOfRange,
            StorageValidationKind::InvalidNumber,
        ),
        (
            "1e9223372036854775807",
            DynamodbNumberError::AdjustedExponentOutOfRange,
            StorageValidationKind::InvalidNumber,
        ),
        (
            "123456789012345678901234567890123456789",
            DynamodbNumberError::Precision,
            StorageValidationKind::NumberPrecision,
        ),
        (
            "1e-131",
            DynamodbNumberError::Underflow,
            StorageValidationKind::NumberUnderflow,
        ),
        (
            "1e126",
            DynamodbNumberError::Overflow,
            StorageValidationKind::NumberOverflow,
        ),
    ] {
        assert_eq!(canonical_dynamodb_number(raw).unwrap_err(), error);
        assert_eq!(
            crate::dynamodb_limits::validate_number(raw).unwrap_err(),
            kind.message()
        );
        assert_eq!(
            crate::encode_dynamodb_number_key(raw).unwrap_err(),
            error.to_string()
        );
    }
}

crate::err_context!(UntypedStorageError, StorageEnum);

fn original_context() -> UntypedStorageError {
    UntypedStorageError::from(StorageEnum::Validation {
        kind: crate::StorageValidationKind::Message,
        message: "The parameter cannot be converted to a numeric value".to_string(),
    })
    .with_context("transaction key")
}

fn typed_context() -> crate::StorageError {
    crate::StorageError::validation(crate::StorageValidationKind::InvalidNumber)
        .with_context("transaction key")
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_contextual_validation_when_constructed_or_converted_then_report_cpu_and_allocations() {
    eprintln!(
        "error_layout original={} optimized={}",
        std::mem::size_of::<UntypedStorageError>(),
        std::mem::size_of::<crate::StorageError>()
    );
    let original = original_context();
    let typed = typed_context();
    assert_eq!(original.to_string(), typed.to_string());
    assert_eq!(
        original.recursive_context(Vec::new()).1,
        typed.recursive_context(Vec::new()).1
    );
    assert_eq!(
        original.into_enum().to_string(),
        typed.into_enum().to_string()
    );
    report("context_original_make", original_context);
    report("context_optimized_make", typed_context);
    report("context_original_raw", || {
        let error = original_context();
        let StorageEnum::Validation { message, .. } = error.to_enum() else {
            unreachable!()
        };
        crate::StorageError::raw_validation(message.clone())
    });
    report("context_optimized_raw", || {
        typed_context().into_raw_validation()
    });
}
