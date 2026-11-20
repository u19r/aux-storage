use storage_types::{
    AttemptDurability::{Durable, Relaxed},
    AttemptMode::{ReadOnly, ReadWrite, ReadWriteItems},
    AttemptPlan, DeadlineClass, NativeWork,
};

use crate::constants::*;

#[test]
fn given_pubsub_request_constants_when_read_then_each_has_its_mode_class_durability_retry_and_work()
{
    let table = [
        (
            "CATALOG_WRITE",
            CATALOG_WRITE,
            ReadWrite,
            DeadlineClass::SchemaChange,
            Durable,
            NativeWork::Write,
        ),
        (
            "CATALOG_POINT_READ",
            CATALOG_POINT_READ,
            ReadOnly,
            DeadlineClass::PointRead,
            Durable,
            NativeWork::PointRead,
        ),
        (
            "TOPIC_ATTRIBUTES_READ",
            TOPIC_ATTRIBUTES_READ,
            ReadOnly,
            DeadlineClass::PointRead,
            Durable,
            NativeWork::RangeRead,
        ),
        (
            "CATALOG_RANGE_READ",
            CATALOG_RANGE_READ,
            ReadOnly,
            DeadlineClass::RangeRead,
            Durable,
            NativeWork::RangeRead,
        ),
        (
            "PUBLISH_WRITE",
            PUBLISH_WRITE,
            ReadWrite,
            DeadlineClass::Write,
            Durable,
            NativeWork::Write,
        ),
        (
            "CHUNK_ITEMS",
            CHUNK_ITEMS,
            ReadWriteItems,
            DeadlineClass::BackgroundPage,
            Relaxed,
            NativeWork::Write,
        ),
        (
            "PUBLISH_DUE_PROBE",
            PUBLISH_DUE_PROBE,
            ReadOnly,
            DeadlineClass::LongPollProbe,
            Relaxed,
            NativeWork::PointRead,
        ),
        (
            "DELIVERY_CLAIM",
            DELIVERY_CLAIM,
            ReadWriteItems,
            DeadlineClass::BackgroundPage,
            Relaxed,
            NativeWork::Write,
        ),
        (
            "DESTINATION_HEADS_READ",
            DESTINATION_HEADS_READ,
            ReadOnly,
            DeadlineClass::BackgroundPage,
            Relaxed,
            NativeWork::RangeRead,
        ),
        (
            "DELIVERY_READ",
            DELIVERY_READ,
            ReadOnly,
            DeadlineClass::BackgroundPage,
            Relaxed,
            NativeWork::PointRead,
        ),
        (
            "DELIVERY_SETTLE",
            DELIVERY_SETTLE,
            ReadWriteItems,
            DeadlineClass::Write,
            Relaxed,
            NativeWork::Write,
        ),
    ];
    for (name, request, mode, class, durability, work) in table {
        let request: AttemptPlan = request;
        assert_eq!(request.mode, mode, "{name} request.mode");
        assert_eq!(request.class, class, "{name} request.class");
        assert_eq!(request.durability, durability, "{name} request.durability");
        assert_eq!(
            request.retry.max_attempts(),
            5,
            "{name} request.retry.max_attempts()"
        );
        assert_eq!(request.work, work, "{name} request.work");
    }
}
