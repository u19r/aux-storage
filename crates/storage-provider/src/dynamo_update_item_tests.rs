use std::sync::Arc;

use alloc_counter::AllocationGuard;
use storage_backfill::GsiProjectionLifecycle;
use storage_types::{
    AttributeMap, BoundTableInfo, StorageEnum, StoredWireItem, UpdateItemRequest, WireItem,
    context::WrappedError as _,
};

use crate::{
    AcceptedUpsert, ItemWriteObservation, SemanticDecision, UpdateItemShape,
    dynamo_update_item::UpdateReturnShape,
};

fn table() -> Arc<BoundTableInfo> {
    let info = serde_json::from_value(serde_json::json!({
        "table_name": "key-update",
        "table_status": "ACTIVE",
        "created_at": 0,
        "attribute_definitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "key_schema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "max_indexers": 0,
        "global_secondary_indexes": null,
        "table_size_bytes": 0,
        "item_count": 0,
        "stream_specification": null
    }))
    .expect("table info");
    Arc::new(BoundTableInfo {
        binding: Default::default(),
        info: Arc::new(info),
        history_id: 1_u32.into(),
    })
}

#[test]
fn given_key_attribute_set_when_bound_then_validation_rejects_it() {
    let request: UpdateItemRequest = serde_json::from_value(serde_json::json!({
        "TableName": "key-update",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET pk = :new",
        "ExpressionAttributeValues": {":new": {"S": "b"}}
    }))
    .expect("request");

    let error = Arc::new(UpdateItemShape::try_from_request(request).expect("shape"))
        .bind_table(table())
        .err()
        .expect("key update must be rejected");

    assert!(
        matches!(error.to_enum(), StorageEnum::Validation { .. }),
        "expected validation, got {error:?}"
    );
}

#[test]
fn given_literal_legacy_updates_when_parsed_then_unsupported_operation_fails_explicitly() {
    let request: UpdateItemRequest = serde_json::from_value(serde_json::json!({
        "TableName": "key-update", "Key": {"pk": {"S": "a"}},
        "AttributeUpdates": {"a.b": {"Action": "PUT", "Value": {"S": "value"}}}
    }))
    .unwrap();
    assert!(UpdateItemShape::try_from_request(request).is_err());
}

#[test]
fn given_literal_path_alias_when_parsed_then_it_addresses_one_top_level_attribute() {
    let request: UpdateItemRequest = serde_json::from_value(serde_json::json!({
        "TableName": "key-update", "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "SET #literal = :value",
        "ExpressionAttributeNames": {"#literal": "a.b"},
        "ExpressionAttributeValues": {":value": {"S": "value"}}
    }))
    .unwrap();
    let shape = UpdateItemShape::try_from_request(request).unwrap();
    assert_eq!(shape.operations.len(), 1);
    assert_eq!(shape.operations[0].field_path().top_level(), Some("a.b"));
}

const OLD_ITEM: &str = r#"{"pk":{"S":"a"},"x":{"N":"1"},"y":{"M":{"z":{"N":"1"}}}}"#;
const ALLOC_ITERATIONS: usize = 64;

fn live(json: &str) -> ItemWriteObservation {
    ItemWriteObservation::Live {
        stored: Box::new(StoredWireItem {
            item: WireItem::dynamo_json(json.as_bytes().to_vec()),
            indexers: Default::default(),
        }),
        provenance: Default::default(),
        ttl_epoch: None,
    }
}

fn expected(json: &str) -> AttributeMap {
    AttributeMap::from_dynamo_json(json.as_bytes()).expect("expected map")
}

fn request(extra: serde_json::Value) -> UpdateItemRequest {
    let mut value = serde_json::json!({
        "TableName": "key-update",
        "Key": {"pk": {"S": "a"}},
    });
    value
        .as_object_mut()
        .expect("object")
        .extend(extra.as_object().expect("object").clone());
    serde_json::from_value(value).expect("request")
}

fn set_request(return_values: &str) -> UpdateItemRequest {
    request(serde_json::json!({
        "UpdateExpression": "SET x = :v, y.z = :w",
        "ExpressionAttributeValues": {":v": {"N": "2"}, ":w": {"N": "3"}},
        "ReturnValues": return_values,
    }))
}

fn accept(
    request: UpdateItemRequest,
    observation: ItemWriteObservation,
) -> SemanticDecision<(AcceptedUpsert, UpdateReturnShape)> {
    Arc::new(UpdateItemShape::try_from_request(request).expect("shape"))
        .bind_table(table())
        .expect("bind")
        .accept_old(observation, GsiProjectionLifecycle::default())
        .expect("accept_old")
}

fn response_attributes(
    request: UpdateItemRequest,
    observation: ItemWriteObservation,
) -> Option<AttributeMap> {
    let (_, shape) = accept(request, observation)
        .into_result()
        .expect("accepted");
    shape.into_response().attributes
}

#[test]
fn update_response_matrix_old_present() {
    let cases = [
        ("NONE", None),
        ("ALL_OLD", Some(OLD_ITEM)),
        (
            "UPDATED_OLD",
            Some(r#"{"x":{"N":"1"},"y":{"M":{"z":{"N":"1"}}}}"#),
        ),
        (
            "ALL_NEW",
            Some(r#"{"pk":{"S":"a"},"x":{"N":"2"},"y":{"M":{"z":{"N":"3"}}}}"#),
        ),
        (
            "UPDATED_NEW",
            Some(r#"{"x":{"N":"2"},"y":{"M":{"z":{"N":"3"}}}}"#),
        ),
    ];
    for (return_values, want) in cases {
        let got = response_attributes(set_request(return_values), live(OLD_ITEM));
        assert_eq!(got, want.map(expected), "ReturnValues {return_values}");
    }
    for (return_values, want) in [
        ("UPDATED_OLD", r#"{"x":{"N":"1"}}"#),
        ("UPDATED_NEW", r#"{"x":{"N":"2"}}"#),
    ] {
        let request = request(serde_json::json!({
            "UpdateExpression": "SET x = :v",
            "ExpressionAttributeValues": {":v": {"N": "2"}},
            "ReturnValues": return_values,
        }));
        let got = response_attributes(request, live(OLD_ITEM));
        assert_eq!(
            got,
            Some(expected(want)),
            "flat ReturnValues {return_values}"
        );
    }
}

#[test]
fn update_response_matrix_old_absent() {
    let cases = [
        ("ALL_OLD", None),
        ("UPDATED_OLD", None),
        ("ALL_NEW", Some(r#"{"pk":{"S":"a"},"x":{"N":"2"}}"#)),
        ("UPDATED_NEW", Some(r#"{"x":{"N":"2"}}"#)),
    ];
    for (return_values, want) in cases {
        let request = request(serde_json::json!({
            "UpdateExpression": "SET x = :v",
            "ExpressionAttributeValues": {":v": {"N": "2"}},
            "ReturnValues": return_values,
        }));
        let got = response_attributes(request, ItemWriteObservation::Absent);
        assert_eq!(got, want.map(expected), "ReturnValues {return_values}");
    }
}

#[test]
fn update_new_image_bytes_equal_response() {
    let (accepted, shape) = accept(set_request("ALL_NEW"), live(OLD_ITEM))
        .into_result()
        .expect("accepted");
    let response = shape
        .into_response()
        .attributes
        .expect("ALL_NEW attributes");
    assert_eq!(
        AttributeMap::try_from(&*accepted.new_item).expect("decode new image"),
        response
    );
    assert!(
        accepted
            .new_item
            .has_verified_keys(&accepted.key)
            .expect("key check")
    );
}

#[test]
fn update_condition_false_with_all_old_returns_item() {
    let conditional = |flag: bool| {
        let mut extra = serde_json::json!({
            "UpdateExpression": "SET x = :v",
            "ConditionExpression": "x = :other",
            "ExpressionAttributeValues": {":v": {"N": "2"}, ":other": {"N": "9"}},
        });
        if flag {
            extra["ReturnValuesOnConditionCheckFailure"] = "ALL_OLD".into();
        }
        request(extra)
    };
    let SemanticDecision::Rejected(error) = accept(conditional(true), live(OLD_ITEM)) else {
        panic!("condition must fail");
    };
    match error.to_enum() {
        StorageEnum::ConditionalCheckFailedWithItem { item } => {
            assert_eq!(*item, expected(OLD_ITEM));
        }
        other => panic!("expected failure with item, got {other:?}"),
    }
    let SemanticDecision::Rejected(error) = accept(conditional(false), live(OLD_ITEM)) else {
        panic!("condition must fail");
    };
    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
}

#[test]
fn update_condition_on_absent_item() {
    let conditional = |condition: &str| {
        request(serde_json::json!({
            "UpdateExpression": "SET x = :v",
            "ConditionExpression": condition,
            "ExpressionAttributeValues": {":v": {"N": "2"}},
        }))
    };
    assert!(matches!(
        accept(
            conditional("attribute_not_exists(pk)"),
            ItemWriteObservation::Absent
        ),
        SemanticDecision::Accepted(_)
    ));
    let SemanticDecision::Rejected(error) = accept(
        conditional("attribute_exists(pk)"),
        ItemWriteObservation::Absent,
    ) else {
        panic!("condition must fail");
    };
    assert!(matches!(
        error.to_enum(),
        StorageEnum::ConditionalCheckFailed
    ));
}

fn measure_accept(
    name: &'static str,
    return_values: &'static str,
) -> alloc_counter::AllocationReport<'static> {
    let mut bound = (0..ALLOC_ITERATIONS)
        .map(|_| {
            Arc::new(UpdateItemShape::try_from_request(set_request(return_values)).expect("shape"))
                .bind_table(table())
                .expect("bind")
        })
        .collect::<Vec<_>>();
    let mut observations = (0..ALLOC_ITERATIONS)
        .map(|_| live(OLD_ITEM))
        .collect::<Vec<_>>();
    let lifecycles = vec![GsiProjectionLifecycle::default(); ALLOC_ITERATIONS];
    let mut lifecycles = lifecycles;
    let guard = AllocationGuard::start(module_path!(), name, file!(), line!(), Some(return_values));
    while let (Some(update), Some(observation), Some(lifecycle)) =
        (bound.pop(), observations.pop(), lifecycles.pop())
    {
        let (accepted, shape) = update
            .accept_old(observation, lifecycle)
            .expect("accept_old")
            .into_result()
            .expect("accepted");
        std::hint::black_box((accepted, shape.into_response()));
    }
    guard.finish()
}

#[test]
fn update_all_new_allocates_as_none() {
    let none = measure_accept("update_return_none", "NONE");
    let all_new = measure_accept("update_return_all_new", "ALL_NEW");
    let _ = alloc_counter::emit_report(&none);
    let _ = alloc_counter::emit_report(&all_new);
    assert_eq!(
        all_new.allocation_count, none.allocation_count,
        "NONE={} ALL_NEW={}",
        none.allocation_count, all_new.allocation_count
    );
}

#[test]
fn update_all_old_allocates_one_clone() {
    let none = measure_accept("update_return_none_for_old", "NONE");
    let all_old = measure_accept("update_return_all_old", "ALL_OLD");
    let old_map = AttributeMap::from_dynamo_json(OLD_ITEM.as_bytes()).expect("old map");
    let guard = AllocationGuard::start(
        module_path!(),
        "old_map_clone",
        file!(),
        line!(),
        Some("clone"),
    );
    for _ in 0..ALLOC_ITERATIONS {
        std::hint::black_box(old_map.clone());
    }
    let clones = guard.finish();
    let _ = alloc_counter::emit_report(&all_old);
    assert!(
        all_old.allocation_count <= none.allocation_count + clones.allocation_count,
        "NONE={} ALL_OLD={} clones={}",
        none.allocation_count,
        all_old.allocation_count,
        clones.allocation_count
    );
}
