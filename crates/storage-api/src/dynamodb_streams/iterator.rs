use http_error::HttpApiError;
use storage_types::{
    DYNAMODB_STREAM_RECORDS_LIMIT_MAX, DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE,
    DYNAMODB_STREAM_RECORDS_LIMIT_MIN, StreamItemId,
};

use super::read::stream_not_found;
use crate::constants::STREAM_ITERATOR_VERSION;

pub(super) struct ParsedStreamArn<'arn> {
    pub table_name: &'arn str,
    pub stream_label: &'arn str,
}

pub(super) struct StreamIterator {
    pub stream_arn: String,
    pub shard_id: String,
    page_token: Option<String>,
    sequence_number: Option<String>,
    after_sequence_number: bool,
}

impl StreamIterator {
    pub(super) fn new(stream_arn: String, shard_id: String) -> Self {
        Self {
            stream_arn,
            shard_id,
            page_token: None,
            sequence_number: None,
            after_sequence_number: false,
        }
    }

    pub(super) fn with_page_token(mut self, page_token: Option<String>) -> Self {
        self.page_token = page_token;
        self
    }

    pub(super) fn with_sequence_number(
        mut self,
        sequence_number: Option<String>,
        after_sequence_number: bool,
    ) -> Self {
        self.sequence_number = sequence_number;
        self.after_sequence_number = after_sequence_number;
        self
    }

    pub(super) fn encode(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}",
            STREAM_ITERATOR_VERSION,
            self.stream_arn,
            self.shard_id,
            self.page_token.as_deref().unwrap_or(""),
            self.sequence_number.as_deref().unwrap_or(""),
            u8::from(self.after_sequence_number)
        )
    }

    pub(super) fn decode(value: &str) -> Result<Self, HttpApiError> {
        let parts = value.split('|').collect::<Vec<_>>();
        if parts.len() != 6 || parts[0] != STREAM_ITERATOR_VERSION {
            return Err(HttpApiError::dynamodb_protocol_error(
                "TrimmedDataAccessException",
                "The data you are trying to access has been trimmed.",
                400,
            ));
        }
        Ok(Self {
            stream_arn: parts[1].to_string(),
            shard_id: parts[2].to_string(),
            page_token: (!parts[3].is_empty()).then(|| parts[3].to_string()),
            sequence_number: (!parts[4].is_empty()).then(|| parts[4].to_string()),
            after_sequence_number: parts[5] == "1",
        })
    }
}

pub(super) fn parse_stream_arn(stream_arn: &str) -> Result<ParsedStreamArn<'_>, HttpApiError> {
    let Some((_, table_and_stream)) = stream_arn.split_once(":table/") else {
        return Err(stream_not_found());
    };
    let Some((table_name, stream_label)) = table_and_stream.split_once("/stream/") else {
        return Err(stream_not_found());
    };
    Ok(ParsedStreamArn {
        table_name,
        stream_label,
    })
}

pub(super) fn validate_streams_page_limit(
    limit: Option<u32>,
    max: u32,
) -> Result<(), HttpApiError> {
    if let Some(limit) = limit
        && !(DYNAMODB_STREAM_RECORDS_LIMIT_MIN..=max).contains(&limit)
    {
        let message = if max == DYNAMODB_STREAM_RECORDS_LIMIT_MAX {
            DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE.to_string()
        } else {
            format!("Limit must be between 1 and {max}")
        };
        return Err(HttpApiError::validation_error(message));
    }
    Ok(())
}

impl StreamIterator {
    /// Resolve the exclusive table-stream start once, before opening a read.
    pub(super) fn page_start(&self) -> Result<Option<String>, HttpApiError> {
        let Some(sequence) = self.sequence_number.as_deref() else {
            return Ok(self.page_token.clone());
        };
        let sequence = sequence
            .parse::<StreamItemId>()
            .map_err(|_| HttpApiError::validation_error("Invalid sequence number"))?;
        let start = if self.after_sequence_number {
            Some(sequence)
        } else {
            sequence.predecessor()
        };
        Ok(start.map(|id| id.to_string()))
    }
}
