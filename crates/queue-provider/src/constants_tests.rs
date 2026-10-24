pub(crate) const WIRE_REQUEST_ITERATIONS: usize = 1_000;

pub(crate) const WIRE_REQUEST_SEND_JSON: &[u8] =
    br#"{"QueueUrl":"https://queue.example/jobs","MessageBody":"hello","DelaySeconds":1}"#;

pub(crate) const NEWTYPES_PERF_ITERATIONS: usize = 200_000;

pub(crate) const NEWTYPES_PERF_MESSAGE_ID_HEX: &str = "018f1f612a6f7ac3b9b67f65";
