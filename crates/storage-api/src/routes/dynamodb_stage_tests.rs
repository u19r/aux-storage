use std::time::Instant;

use crate::{
    constants::{
        DYNAMODB_BODY_READ_STAGE as BODY_READ_STAGE, DYNAMODB_ERROR_STATUS as ERROR_STATUS,
        DYNAMODB_JSON_DECODE_STAGE as JSON_DECODE_STAGE, DYNAMODB_MANAGER_STAGE as MANAGER_STAGE,
        DYNAMODB_REQUEST_CONVERT_STAGE as REQUEST_CONVERT_STAGE,
        DYNAMODB_RESPONSE_ENCODE_STAGE as RESPONSE_ENCODE_STAGE,
        DYNAMODB_SUCCESS_STATUS as SUCCESS_STATUS,
    },
    routes::dynamodb_metrics::DynamoRouteTimer,
};

#[test]
fn write_route_stage_metrics_accept_known_write_stages() {
    for operation in [
        "put_item",
        "update_item",
        "delete_item",
        "batch_write_item",
        "transact_write_items",
    ] {
        for stage in [
            BODY_READ_STAGE,
            JSON_DECODE_STAGE,
            REQUEST_CONVERT_STAGE,
            MANAGER_STAGE,
            RESPONSE_ENCODE_STAGE,
        ] {
            let timer = DynamoRouteTimer::new(operation);
            timer.record_stage(stage, SUCCESS_STATUS, Instant::now());
        }

        let timer = DynamoRouteTimer::new(operation);
        timer.record_stage(MANAGER_STAGE, ERROR_STATUS, Instant::now());
    }
}
