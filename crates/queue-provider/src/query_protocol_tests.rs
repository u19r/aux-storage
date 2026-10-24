use std::hint::black_box;

use alloc_counter::AllocationGuard;

use super::query_protocol::query_fields_to_json;

fn query_fixture() -> Vec<(String, String)> {
    let mut fields = vec![
        ("Action".to_string(), "SendMessageBatch".to_string()),
        ("Version".to_string(), "2012-11-05".to_string()),
        (
            "QueueUrl".to_string(),
            "https://queue.example.test/jobs".to_string(),
        ),
    ];
    for index in 1..=10 {
        fields.push((
            format!("SendMessageBatchRequestEntry.{index}.Id"),
            format!("entry-{index}"),
        ));
        fields.push((
            format!("SendMessageBatchRequestEntry.{index}.MessageBody"),
            "message body with a representative payload".to_string(),
        ));
        fields.push((
            format!("MessageAttribute.{index}.Name"),
            format!("attribute-{index}"),
        ));
        fields.push((
            format!("MessageAttribute.{index}.Value.DataType"),
            "String".to_string(),
        ));
        fields.push((
            format!("MessageAttribute.{index}.Value.StringValue"),
            "attribute value".to_string(),
        ));
    }
    fields
}

#[test]
fn query_batch_projection_allocation_profile() {
    let guard = AllocationGuard::start(
        module_path!(),
        "queue_query_projection_allocation_profile_tests",
        file!(),
        line!(),
        Some("owned_query_fields"),
    );
    for _ in 0..100 {
        black_box(query_fields_to_json(query_fixture()));
    }
    let report = guard.finish();
    let _ = alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
}
