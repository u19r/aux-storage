use pubsub_provider::SubscriptionArn;
use storage_types::TimestampMillis;

use super::pubsub_subscription::*;

#[test]
fn given_created_at_and_arn_when_token_value_round_trips_then_both_return() {
    let arn = SubscriptionArn::new(
        "arn:aws:sns:us-east-1:000000000000:t:018f1f61-2a6f-7ac3-b9b6-7f65bb2d91fd",
    )
    .unwrap();
    let created = TimestampMillis::from_timestamp(1_700_000_000_123);

    let value = token_value(created, &arn);

    assert_eq!(value.len(), 8 + arn.as_str().len());
    let (decoded_created, decoded_arn) = decode_token_value(&value).unwrap();
    assert_eq!(
        decoded_created.timestamp_millis(),
        created.timestamp_millis()
    );
    assert_eq!(decoded_arn, arn);

    let error = decode_token_value(&[1, 2, 3]).unwrap_err();
    assert!(
        error.to_string().starts_with("internal error"),
        "a short value is an internal error"
    );
}
