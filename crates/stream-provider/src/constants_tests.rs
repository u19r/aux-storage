use storage_types::{
    AttemptDurability::Durable,
    AttemptMode::{ReadOnly, ReadWrite},
    AttemptPlan, DeadlineClass, NativeWork,
};

use crate::constants::*;

#[test]
fn given_stream_request_constants_when_read_then_each_has_its_mode_class_durability_retry_and_work()
{
    let table = [
        (
            "STREAM_CATALOG_WRITE",
            STREAM_CATALOG_WRITE,
            ReadWrite,
            DeadlineClass::SchemaChange,
            Durable,
            5,
            NativeWork::Write,
        ),
        (
            "STREAM_WRITE",
            STREAM_WRITE,
            ReadWrite,
            DeadlineClass::Write,
            Durable,
            5,
            NativeWork::Write,
        ),
        (
            "CURSOR_PAGE_READ",
            CURSOR_PAGE_READ,
            ReadWrite,
            DeadlineClass::RangeRead,
            Durable,
            5,
            NativeWork::Write,
        ),
        (
            "SYSTEM_PAGE_READ",
            SYSTEM_PAGE_READ,
            ReadOnly,
            DeadlineClass::RangeRead,
            Durable,
            5,
            NativeWork::RangeRead,
        ),
        (
            "STREAM_POINT_READ",
            STREAM_POINT_READ,
            ReadOnly,
            DeadlineClass::PointRead,
            Durable,
            1,
            NativeWork::PointRead,
        ),
        (
            "STREAM_RANGE_READ",
            STREAM_RANGE_READ,
            ReadOnly,
            DeadlineClass::RangeRead,
            Durable,
            1,
            NativeWork::RangeRead,
        ),
    ];
    for (name, request, mode, class, durability, max_attempts, work) in table {
        let request: AttemptPlan = request;
        assert_eq!(request.mode, mode, "{name} request.mode");
        assert_eq!(request.class, class, "{name} request.class");
        assert_eq!(request.durability, durability, "{name} request.durability");
        assert_eq!(
            request.retry.max_attempts(),
            max_attempts,
            "{name} request.retry.max_attempts()"
        );
        assert_eq!(request.work, work, "{name} request.work");
    }
}
