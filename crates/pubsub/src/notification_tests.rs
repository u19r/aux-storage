use std::{collections::HashMap, hint::black_box, sync::Mutex};

use alloc_counter::AllocationGuard;
use pubsub_provider::{
    PublishRequest, PubsubError, PubsubMessageId, PubsubResult, Subscription, SubscriptionArn,
    SubscriptionConfirmation, SubscriptionProtocol, TopicArn,
};
use serde_json::Value;
use storage_types::TimestampMillis;

use crate::{
    manager::PubsubDeliveryConfig,
    notification::{
        ConfirmationRenderContext, NotificationRenderContext, PubsubNotificationSignRequest,
        PubsubNotificationSigner, confirmation_body, confirmation_headers,
        confirmation_string_to_sign, notification_body, notification_headers,
        notification_string_to_sign,
    },
};

const ITERATIONS: usize = 64;

#[derive(Default)]
struct RecordingSigner {
    inputs: Mutex<Vec<String>>,
    fail: bool,
}

impl PubsubNotificationSigner for RecordingSigner {
    fn sign(&self, request: PubsubNotificationSignRequest<'_>) -> PubsubResult<String> {
        assert_eq!(request.signature_version, "1");
        self.inputs
            .lock()
            .unwrap()
            .push(request.string_to_sign.to_string());
        if self.fail {
            Err(PubsubError::storage("signing unavailable"))
        } else {
            Ok("signature".to_string())
        }
    }
}

struct FixedSigner;

impl PubsubNotificationSigner for FixedSigner {
    fn sign(&self, request: PubsubNotificationSignRequest<'_>) -> PubsubResult<String> {
        black_box(request.string_to_sign);
        Ok("signature".to_string())
    }
}

fn create_subscription() -> Subscription {
    Subscription {
        subscription_arn: SubscriptionArn::new(
            "arn:aws:sns:us-east-1:000000000000:orders:subscription",
        )
        .unwrap(),
        topic_arn: TopicArn::new("arn:aws:sns:us-east-1:000000000000:orders").unwrap(),
        protocol: SubscriptionProtocol::Https,
        endpoint: "https://example.com/hook".to_string(),
        raw_message_delivery: false,
        confirmation: SubscriptionConfirmation::Confirmed,
        extra_json: Value::Null,
        created_at: TimestampMillis::from_timestamp(0),
    }
}

#[test]
fn given_signed_and_unsigned_notifications_when_rendered_then_signature_and_fields_match() {
    let subscription = create_subscription();
    let message_id = PubsubMessageId::new_from_string("message-id").unwrap();
    let config = PubsubDeliveryConfig::default();
    for subject in [None, Some("subject\n\"雪".to_string())] {
        let request = PublishRequest {
            topic_arn: subscription.topic_arn.clone(),
            message: "body\n\"雪".to_string(),
            subject,
            message_attributes: HashMap::from([
                ("z".to_string(), "雪".to_string()),
                ("a".to_string(), "\"".to_string()),
            ]),
        };
        let signer = RecordingSigner::default();
        for signed in [false, true] {
            let body = notification_body(NotificationRenderContext {
                request: &request,
                message_id: &message_id,
                subscription: &subscription,
                delivery_config: &config,
                signer: signed.then_some(&signer),
            })
            .unwrap();
            let value: Value = serde_json::from_str(&body).unwrap();
            eprintln!(
                "notification_wire signed={signed} body={}",
                serde_json::to_string(
                    &body.replace(value["Timestamp"].as_str().unwrap(), "<TIMESTAMP>")
                )
                .unwrap()
            );
            assert_eq!(value["Message"], request.message);
            assert_eq!(
                value["Subject"],
                serde_json::to_value(&request.subject).unwrap()
            );
            assert_eq!(value["SignatureVersion"], "1");
            assert_eq!(value["Signature"], if signed { "signature" } else { "" });
            assert!(body.find("\"a\" :").unwrap() < body.find("\"z\" :").unwrap());
            if signed {
                assert_eq!(
                    signer.inputs.lock().unwrap().as_slice(),
                    &[notification_string_to_sign(
                        &request,
                        &message_id,
                        value["Timestamp"].as_str().unwrap()
                    )]
                );
            } else {
                assert!(signer.inputs.lock().unwrap().is_empty());
            }
        }
        let failed = RecordingSigner {
            fail: true,
            ..RecordingSigner::default()
        };
        assert!(
            notification_body(NotificationRenderContext {
                request: &request,
                message_id: &message_id,
                subscription: &subscription,
                delivery_config: &config,
                signer: Some(&failed),
            })
            .is_err()
        );
    }
}

#[test]
fn given_signed_and_unsigned_confirmations_when_rendered_then_canonical_input_and_errors_match() {
    let subscription = create_subscription();
    let message_id = PubsubMessageId::new_from_string("message-id").unwrap();
    let config = PubsubDeliveryConfig::default();
    for signed in [false, true] {
        let signer = RecordingSigner::default();
        let body = confirmation_body(ConfirmationRenderContext {
            topic_arn: &subscription.topic_arn,
            message_id: &message_id,
            token: "token雪",
            delivery_config: &config,
            signer: signed.then_some(&signer),
        })
        .unwrap();
        let value: Value = serde_json::from_str(&body).unwrap();
        eprintln!(
            "notification_wire signed={signed} body={}",
            serde_json::to_string(
                &body.replace(value["Timestamp"].as_str().unwrap(), "<TIMESTAMP>")
            )
            .unwrap()
        );
        assert_eq!(value["SignatureVersion"], "1");
        assert_eq!(value["Signature"], if signed { "signature" } else { "" });
        assert_eq!(value["Token"], "token雪");
        if signed {
            let expected = confirmation_string_to_sign(
                value["Message"].as_str().unwrap(),
                &message_id,
                value["SubscribeURL"].as_str().unwrap(),
                value["Timestamp"].as_str().unwrap(),
                "token雪",
                &subscription.topic_arn,
            );
            assert_eq!(signer.inputs.lock().unwrap().as_slice(), &[expected]);
        } else {
            assert!(signer.inputs.lock().unwrap().is_empty());
        }
    }
    let failed = RecordingSigner {
        fail: true,
        ..RecordingSigner::default()
    };
    assert!(
        confirmation_body(ConfirmationRenderContext {
            topic_arn: &subscription.topic_arn,
            message_id: &message_id,
            token: "token",
            delivery_config: &config,
            signer: Some(&failed),
        })
        .is_err()
    );
}

#[test]
fn given_notification_workloads_when_rendered_then_allocations_are_reported() {
    let subscription = create_subscription();
    let message_id = PubsubMessageId::new_from_string("message-id").unwrap();
    let config = PubsubDeliveryConfig::default();
    for (label, size, signed, attributes) in [
        ("unsigned_1k", 1024, false, 0),
        ("signed_1k", 1024, true, 0),
        ("unsigned_256k", 262144, false, 0),
        ("signed_256k", 262144, true, 0),
        ("unsigned_1k_attrs", 1024, false, 10),
        ("signed_1k_attrs", 1024, true, 10),
        ("unsigned_256k_attrs", 262144, false, 10),
        ("signed_256k_attrs", 262144, true, 10),
    ] {
        let request = PublishRequest {
            topic_arn: subscription.topic_arn.clone(),
            message: "x".repeat(size),
            subject: (attributes > 0).then(|| "subject雪".to_string()),
            message_attributes: (0..attributes)
                .map(|n| (format!("attribute{n}"), "value雪".repeat(8)))
                .collect(),
        };
        let guard = AllocationGuard::start(
            module_path!(),
            "notification_render",
            file!(),
            line!(),
            Some(label),
        );
        for _ in 0..ITERATIONS {
            black_box(
                notification_body(NotificationRenderContext {
                    request: &request,
                    message_id: &message_id,
                    subscription: &subscription,
                    delivery_config: &config,
                    signer: signed.then_some(&FixedSigner),
                })
                .unwrap(),
            );
            black_box(notification_headers(&message_id, &subscription, &request).unwrap());
        }
        let _ = alloc_counter::emit_report(&guard.finish());
    }
    for (label, signed) in [
        ("confirmation_unsigned", false),
        ("confirmation_signed", true),
    ] {
        let guard = AllocationGuard::start(
            module_path!(),
            "notification_render",
            file!(),
            line!(),
            Some(label),
        );
        for _ in 0..ITERATIONS {
            black_box(
                confirmation_body(ConfirmationRenderContext {
                    topic_arn: &subscription.topic_arn,
                    message_id: &message_id,
                    token: "token",
                    delivery_config: &config,
                    signer: signed.then_some(&FixedSigner),
                })
                .unwrap(),
            );
            black_box(confirmation_headers(&message_id, &subscription.topic_arn).unwrap());
        }
        let _ = alloc_counter::emit_report(&guard.finish());
    }
}

#[test]
fn given_invalid_header_characters_when_headers_rendered_then_errors_are_preserved() {
    let subscription = create_subscription();
    let invalid = PubsubMessageId::new_from_string("invalid\nheader").unwrap();
    let request = PublishRequest {
        topic_arn: subscription.topic_arn.clone(),
        message: "message".to_string(),
        subject: None,
        message_attributes: HashMap::new(),
    };
    assert!(notification_headers(&invalid, &subscription, &request).is_err());
    assert!(confirmation_headers(&invalid, &subscription.topic_arn).is_err());
}
