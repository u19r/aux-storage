pub(crate) const UPDATE_CLONE_AUDIT_ITERATIONS: usize = 512;

pub(crate) const UPDATE_EXPRESSION_AUDIT_ITERATIONS: usize = 1024;

#[test]
fn given_dynamo_request_constants_when_read_then_each_has_its_mode_class_durability_retry_and_work()
{
    use storage_types::{
        AttemptDurability::Durable,
        AttemptMode::{ReadOnly, ReadWrite, ReadWriteItems},
        AttemptPlan, DeadlineClass, NativeWork,
    };

    use crate::constants::*;

    let table = [
        (
            "SCHEMA_CHANGE",
            SCHEMA_CHANGE,
            ReadWrite,
            DeadlineClass::SchemaChange,
            Durable,
            12,
            NativeWork::Write,
        ),
        (
            "POINT_READ",
            POINT_READ,
            ReadOnly,
            DeadlineClass::PointRead,
            Durable,
            12,
            NativeWork::PointRead,
        ),
        (
            "RANGE_READ",
            RANGE_READ,
            ReadOnly,
            DeadlineClass::RangeRead,
            Durable,
            12,
            NativeWork::RangeRead,
        ),
        (
            "BACKGROUND_ITEM_PAGE",
            BACKGROUND_ITEM_PAGE,
            ReadWriteItems,
            DeadlineClass::BackgroundPage,
            Durable,
            12,
            NativeWork::Write,
        ),
        (
            "BACKGROUND_PAGE",
            BACKGROUND_PAGE,
            ReadWrite,
            DeadlineClass::BackgroundPage,
            Durable,
            12,
            NativeWork::Write,
        ),
        (
            "BACKGROUND_READ_PAGE",
            BACKGROUND_READ_PAGE,
            ReadOnly,
            DeadlineClass::BackgroundPage,
            Durable,
            12,
            NativeWork::RangeRead,
        ),
        (
            "BACKGROUND_SNAPSHOT_READ",
            BACKGROUND_SNAPSHOT_READ,
            ReadOnly,
            DeadlineClass::BackgroundPage,
            Durable,
            1,
            NativeWork::PointRead,
        ),
        (
            "TABLE_APPEND",
            TABLE_APPEND,
            ReadWrite,
            DeadlineClass::Write,
            Durable,
            12,
            NativeWork::Write,
        ),
        (
            "TABLE_APPEND_RECONCILE",
            TABLE_APPEND_RECONCILE,
            ReadOnly,
            DeadlineClass::PointRead,
            Durable,
            1,
            NativeWork::PointRead,
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
