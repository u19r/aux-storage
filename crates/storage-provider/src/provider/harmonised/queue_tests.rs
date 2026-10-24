use queue_provider::SendMessageRequest;

use super::models::{QueueSend, QueueWriteInput};

fn send(ordinal: usize) -> QueueSend {
    QueueSend {
        ordinal,
        request: SendMessageRequest {
            queue_url: "http://127.0.0.1:3000/queue/000000000000/orders".to_owned(),
            message_body: "hello".to_owned(),
            delay_seconds: None,
            message_attributes: None,
        },
    }
}

#[test]
fn given_empty_sends_when_construct_then_error() {
    QueueWriteInput::try_new(Vec::new()).expect_err("empty sends reject");
}

#[test]
fn given_over_max_sends_when_construct_then_error() {
    let sends = (0..11).map(send).collect();
    QueueWriteInput::try_new(sends).expect_err("over-max sends reject");
}

#[test]
fn given_bounded_sends_when_construct_then_proven() {
    let input = QueueWriteInput::try_new(vec![send(4)]).expect("bounded sends construct");
    assert_eq!(input.sends.len(), 1);
    assert_eq!(input.sends.as_slice()[0].ordinal, 4);
}
