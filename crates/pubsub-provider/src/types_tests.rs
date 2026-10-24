use std::str::FromStr as _;

use super::{DeliveryRecordKind, DeliveryStatus, DeliveryTarget};

#[test]
fn delivery_wire_values_round_trip_through_common_traits() {
    for (status, wire) in [
        (DeliveryStatus::Pending, "pending"),
        (DeliveryStatus::Delivered, "delivered"),
        (
            DeliveryStatus::AcceptedByCustomSender,
            "accepted_by_custom_sender",
        ),
        (DeliveryStatus::RetryScheduled, "retry_scheduled"),
        (DeliveryStatus::Failed, "failed"),
    ] {
        assert_eq!(status.to_string(), wire);
        assert_eq!(DeliveryStatus::from_str(wire).unwrap(), status);
    }
    for (target, wire) in [
        (DeliveryTarget::BuiltIn, "built_in"),
        (DeliveryTarget::CustomSender, "custom_sender"),
    ] {
        assert_eq!(target.to_string(), wire);
        assert_eq!(DeliveryTarget::from_str(wire).unwrap(), target);
    }
}

#[test]
fn delivery_kind_uses_one_wire_conversion() {
    let confirmation = DeliveryRecordKind::SubscriptionConfirmation {
        token: "token".to_string(),
    };
    let (kind, token): (&str, Option<&str>) = (&confirmation).into();
    assert_eq!(kind, "subscription_confirmation");
    assert_eq!(token, Some("token"));
    assert_eq!(
        DeliveryRecordKind::try_from((kind, token.map(str::to_string))).unwrap(),
        confirmation
    );
}
