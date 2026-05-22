use alloc_counter::AllocationGuard;
use storage_types::WireItem;

use crate::{
    query_wire_response::QueryWireResponse,
    raw_dynamodb_response::{write_field_name, write_wire_item_array},
};

const ITEM_COUNT: usize = 64;
const ITERATIONS: usize = 128;

#[test]
fn query_wire_response_serialization_allocation_baseline_tests() {
    let response = query_wire_response();
    let legacy =
        measure_query_wire_response_serialization("legacy_vec_new", &response, |response| {
            legacy_into_json_bytes(response.clone())
        });
    alloc_counter::emit_report(&legacy);
    let optimized =
        measure_query_wire_response_serialization("capacity_hint", &response, |response| {
            response.clone().into_json_bytes()
        });
    alloc_counter::emit_report(&optimized);

    assert!(
        optimized.allocation_count < legacy.allocation_count,
        "capacity hint should reduce allocation count: optimized={} legacy={}",
        optimized.allocation_count,
        legacy.allocation_count
    );
    assert!(
        optimized.allocated_bytes < legacy.allocated_bytes,
        "capacity hint should reduce allocated bytes: optimized={} legacy={}",
        optimized.allocated_bytes,
        legacy.allocated_bytes
    );
}

fn measure_query_wire_response_serialization(
    label: &'static str,
    response: &QueryWireResponse,
    serialize: impl Fn(&QueryWireResponse) -> serde_json::Result<Vec<u8>>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "query_wire_response_serialization_allocation_baseline_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..ITERATIONS {
        let bytes = serialize(response).expect("serialize query wire response");
        assert!(bytes.len() > ITEM_COUNT * 512);
    }
    guard.finish()
}

fn legacy_into_json_bytes(response: QueryWireResponse) -> serde_json::Result<Vec<u8>> {
    let mut out = Vec::new();
    out.push(b'{');
    let mut first = true;

    if let Some(items) = response.items {
        write_field_name(&mut out, &mut first, "Items")?;
        write_wire_item_array(&mut out, items)?;
    }

    write_field_name(&mut out, &mut first, "Count")?;
    serde_json::to_writer(&mut out, &response.count)?;

    write_field_name(&mut out, &mut first, "ScannedCount")?;
    serde_json::to_writer(&mut out, &response.scanned_count)?;

    if let Some(last_evaluated_key) = response.last_evaluated_key {
        write_field_name(&mut out, &mut first, "LastEvaluatedKey")?;
        serde_json::to_writer(&mut out, &last_evaluated_key)?;
    }

    if let Some(consumed_capacity) = response.consumed_capacity {
        write_field_name(&mut out, &mut first, "ConsumedCapacity")?;
        serde_json::to_writer(&mut out, &consumed_capacity)?;
    }

    out.push(b'}');
    Ok(out)
}

fn query_wire_response() -> QueryWireResponse {
    QueryWireResponse {
        items: Some(
            (0..ITEM_COUNT)
                .map(|index| WireItem::dynamo_json(item_json(index).into_bytes()))
                .collect(),
        ),
        count: ITEM_COUNT as u32,
        scanned_count: ITEM_COUNT as u32,
        last_evaluated_key: None,
        consumed_capacity: None,
    }
}

fn item_json(index: usize) -> String {
    format!(
        r#"{{"pk":{{"S":"tenant#{index:04}"}},"sk":{{"S":"item#{index:04}"}},"payload":{{"S":"{}"}},"status":{{"S":"active"}},"counter":{{"N":"{}"}}}}"#,
        "x".repeat(768),
        index
    )
}
