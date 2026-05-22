use std::time::Instant;

use crate::routes::{
    dynamodb::{
        BODY_READ_STAGE, ERROR_STATUS, JSON_DECODE_STAGE, MANAGER_STAGE, REQUEST_CONVERT_STAGE,
        RESPONSE_ENCODE_STAGE, SUCCESS_STATUS,
    },
    dynamodb_metrics::DynamoRouteTimer,
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
            let timer = DynamoRouteTimer::new(operation.to_string());
            timer.record_stage(stage, SUCCESS_STATUS, Instant::now());
        }

        let timer = DynamoRouteTimer::new(operation.to_string());
        timer.record_stage(MANAGER_STAGE, ERROR_STATUS, Instant::now());
    }
}
