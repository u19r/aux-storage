use std::hint::black_box;

use alloc_counter::AllocationGuard;

use crate::decode_query_request;

const ITERATIONS: usize = 64;

fn publish_body(message_len: usize, attributes: usize) -> Vec<u8> {
    let mut form = url::form_urlencoded::Serializer::new(String::new());
    form.append_pair("Action", "Publish")
        .append_pair("TopicArn", "arn:aws:sns:us-east-1:000000000000:orders")
        .append_pair("Message", &"x".repeat(message_len));
    for index in 0..attributes {
        form.append_pair(
            &format!("MessageAttributes.entry.{index}.Name"),
            &format!("attribute{index}"),
        )
        .append_pair(
            &format!("MessageAttributes.entry.{index}.Value.StringValue"),
            "value雪",
        );
    }
    form.finish().into_bytes()
}

#[test]
fn given_query_workloads_when_decoded_then_allocations_are_reported() {
    for (label, message_len, attributes) in [
        ("publish_1k", 1024, 0),
        ("publish_256k", 262_144, 0),
        ("publish_1k_attrs", 1024, 10),
        ("publish_256k_attrs", 262_144, 10),
    ] {
        let body = publish_body(message_len, attributes);
        let guard = AllocationGuard::start(
            module_path!(),
            "protocol_decode",
            file!(),
            line!(),
            Some(label),
        );
        for _ in 0..ITERATIONS {
            black_box(decode_query_request(&body).unwrap());
        }
        let _ = alloc_counter::emit_report(&guard.finish());
    }
}
