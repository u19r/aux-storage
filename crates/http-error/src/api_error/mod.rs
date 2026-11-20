mod aws_query;
mod model;

pub use aws_query::{
    AWS_QUERY_SERVICE_UNAVAILABLE_ERROR_TYPE, AWS_QUERY_SERVICE_UNAVAILABLE_MESSAGE,
    AwsQueryAnswer, AwsQueryInternal,
};
pub use model::{ErrorResponse, HttpApiError};

#[cfg(test)]
mod api_error_tests;

#[cfg(test)]
mod aws_query_tests;

#[cfg(test)]
mod storage_error_message_tests;
