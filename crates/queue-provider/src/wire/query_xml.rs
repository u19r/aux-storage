//! The SQS Query protocol output: XML results and the XML error shape.

use std::collections::HashMap;

use http_error::HttpApiError;

use super::{QueueResponse, WireResponse};
use crate::{
    BatchResultErrorEntry, ChangeMessageVisibilityBatchResponse, CreateQueueResponse,
    DeleteMessageBatchResponse, GetQueueAttributesResponse, GetQueueUrlResponse,
    ListDeadLetterSourceQueuesResponse, ListQueueTagsResponse, ListQueuesResponse,
    MessageAttributeValue, ReceiveMessageResponse, SQS_NON_EXISTENT_QUEUE_ERROR_TYPE,
    SendMessageBatchResponse, SendMessageResponse,
};

const XML_CONTENT_TYPE: &str = "text/xml; charset=utf-8";
const QUEUE_NAMESPACE: &str = "http://queue.amazonaws.com/doc/2012-11-05/";
/// Probe 6: the Query text of a missing queue differs from the JSON text.
const QUERY_NON_EXISTENT_QUEUE_MESSAGE: &str =
    "The specified queue does not exist for this wsdl version.";

/// `<{Action}Response>` with `<{Action}Result>` only for an action that has
/// a result. Batch, receive, and tag shapes are documentation-derived, not
/// SDK-verified.
pub(super) fn encode_success(request_id: &str, response: &QueueResponse) -> WireResponse {
    let action = response.action().name();
    let mut xml = XmlWriter::new();
    xml.open_with_namespace(&format!("{action}Response"));
    if let Some(result) = query_result(response) {
        xml.open(&format!("{action}Result"));
        result.write_result(&mut xml);
        xml.close(&format!("{action}Result"));
    }
    xml.open("ResponseMetadata");
    xml.element("RequestId", request_id);
    xml.close("ResponseMetadata");
    xml.close(&format!("{action}Response"));
    xml.into_response(200)
}

/// The Query result elements of one response type.
pub(super) trait QueryResult {
    fn write_result(&self, xml: &mut XmlWriter);
}

fn query_result(response: &QueueResponse) -> Option<&dyn QueryResult> {
    match response {
        QueueResponse::CreateQueue(result) => Some(result),
        QueueResponse::ListQueues(result) => Some(result),
        QueueResponse::GetQueueUrl(result) => Some(result),
        QueueResponse::GetQueueAttributes(result) => Some(result),
        QueueResponse::SendMessage(result) | QueueResponse::PublishMessage(result) => Some(result),
        QueueResponse::SendMessageBatch(result) => Some(result),
        QueueResponse::ReceiveMessage(result) => Some(result),
        QueueResponse::DeleteMessageBatch(result) => Some(result),
        QueueResponse::ChangeMessageVisibilityBatch(result) => Some(result),
        QueueResponse::ListDeadLetterSourceQueues(result) => Some(result),
        QueueResponse::ListQueueTags(result) => Some(result),
        QueueResponse::DeleteQueue
        | QueueResponse::SetQueueAttributes
        | QueueResponse::PurgeQueue
        | QueueResponse::DeleteMessage
        | QueueResponse::ChangeMessageVisibility
        | QueueResponse::TagQueue
        | QueueResponse::UntagQueue => None,
    }
}

impl QueryResult for CreateQueueResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        xml.element("QueueUrl", &self.queue_url);
    }
}

impl QueryResult for GetQueueUrlResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        xml.element("QueueUrl", &self.queue_url);
    }
}

impl QueryResult for ListQueuesResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        for queue_url in &self.queue_urls {
            xml.element("QueueUrl", queue_url);
        }
        xml.optional("NextToken", self.next_token.as_deref());
    }
}

impl QueryResult for ListDeadLetterSourceQueuesResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        for queue_url in &self.queue_urls {
            xml.element("QueueUrl", queue_url);
        }
        xml.optional("NextToken", self.next_token.as_deref());
    }
}

impl QueryResult for ListQueueTagsResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        for (key, value) in &self.tags {
            xml.open("Tag");
            xml.element("Key", key);
            xml.element("Value", value);
            xml.close("Tag");
        }
    }
}

impl QueryResult for GetQueueAttributesResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        xml.name_value_pairs("Attribute", "Name", sorted(&self.attributes));
    }
}

impl QueryResult for SendMessageResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        xml.element("MD5OfMessageBody", &self.md5_of_body);
        xml.optional(
            "MD5OfMessageAttributes",
            self.md5_of_message_attributes.as_deref(),
        );
        xml.optional(
            "MD5OfMessageSystemAttributes",
            self.md5_of_message_system_attributes.as_deref(),
        );
        xml.element("MessageId", &self.message_id.to_string());
    }
}

impl QueryResult for SendMessageBatchResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        for entry in &self.successful {
            xml.open("SendMessageBatchResultEntry");
            xml.element("Id", &entry.id);
            xml.element("MessageId", &entry.message_id.to_string());
            xml.element("MD5OfMessageBody", &entry.md5_of_message_body);
            xml.optional(
                "MD5OfMessageAttributes",
                entry.md5_of_message_attributes.as_deref(),
            );
            xml.optional(
                "MD5OfMessageSystemAttributes",
                entry.md5_of_message_system_attributes.as_deref(),
            );
            xml.close("SendMessageBatchResultEntry");
        }
        write_failed(xml, &self.failed);
    }
}

impl QueryResult for DeleteMessageBatchResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        for entry in &self.successful {
            xml.open("DeleteMessageBatchResultEntry");
            xml.element("Id", &entry.id);
            xml.close("DeleteMessageBatchResultEntry");
        }
        write_failed(xml, &self.failed);
    }
}

impl QueryResult for ChangeMessageVisibilityBatchResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        for entry in &self.successful {
            xml.open("ChangeMessageVisibilityBatchResultEntry");
            xml.element("Id", &entry.id);
            xml.close("ChangeMessageVisibilityBatchResultEntry");
        }
        write_failed(xml, &self.failed);
    }
}

impl QueryResult for ReceiveMessageResponse {
    fn write_result(&self, xml: &mut XmlWriter) {
        for message in &self.messages {
            xml.open("Message");
            xml.element("MessageId", &message.message_id);
            xml.element("ReceiptHandle", &message.receipt_handle);
            xml.element("MD5OfBody", &message.md5_of_body);
            xml.element("Body", &message.body);
            if let Some(attributes) = &message.attributes {
                xml.name_value_pairs("Attribute", "Name", sorted(attributes));
            }
            xml.optional(
                "MD5OfMessageAttributes",
                message.md5_of_message_attributes.as_deref(),
            );
            if let Some(attributes) = &message.message_attributes {
                write_message_attributes(xml, attributes);
            }
            xml.close("Message");
        }
    }
}

fn write_failed(xml: &mut XmlWriter, failed: &[BatchResultErrorEntry]) {
    for entry in failed {
        xml.open("BatchResultErrorEntry");
        xml.element("Id", &entry.id);
        xml.element(
            "SenderFault",
            if entry.sender_fault { "true" } else { "false" },
        );
        xml.element("Code", &entry.code);
        xml.element("Message", &entry.message);
        xml.close("BatchResultErrorEntry");
    }
}

fn write_message_attributes(
    xml: &mut XmlWriter,
    attributes: &HashMap<String, MessageAttributeValue>,
) {
    for (name, value) in sorted(attributes) {
        xml.open("MessageAttribute");
        xml.element("Name", name);
        xml.open("Value");
        xml.optional("StringValue", value.string_value.as_deref());
        xml.optional("BinaryValue", value.binary_value.as_deref());
        xml.element("DataType", &value.data_type);
        xml.close("Value");
        xml.close("MessageAttribute");
    }
}

/// Map entries in name order, so that one response always has the same bytes.
fn sorted<V>(map: &HashMap<String, V>) -> Vec<(&str, &V)> {
    let mut entries = map
        .iter()
        .map(|(name, value)| (name.as_str(), value))
        .collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(name, _)| *name);
    entries
}

pub(super) fn encode_error(request_id: &str, error: &HttpApiError, fault: &str) -> WireResponse {
    let message = if error.error_type == SQS_NON_EXISTENT_QUEUE_ERROR_TYPE {
        QUERY_NON_EXISTENT_QUEUE_MESSAGE
    } else {
        error.message.as_str()
    };
    let mut xml = XmlWriter::new();
    xml.open_with_namespace("ErrorResponse");
    xml.open("Error");
    xml.element("Type", fault);
    xml.element("Code", &error.error_type);
    xml.element("Message", message);
    xml.raw("<Detail/>");
    xml.close("Error");
    xml.element("RequestId", request_id);
    xml.close("ErrorResponse");
    xml.into_response(error.status_code)
}

/// Appends escaped XML to one buffer.
pub(super) struct XmlWriter {
    buffer: String,
}

impl XmlWriter {
    fn new() -> Self {
        Self {
            buffer: String::from("<?xml version=\"1.0\"?>"),
        }
    }

    fn open_with_namespace(&mut self, name: &str) {
        self.buffer
            .push_str(&format!("<{name} xmlns=\"{QUEUE_NAMESPACE}\">"));
    }

    fn open(&mut self, name: &str) {
        self.buffer.push_str(&format!("<{name}>"));
    }

    fn close(&mut self, name: &str) {
        self.buffer.push_str(&format!("</{name}>"));
    }

    fn element(&mut self, name: &str, value: &str) {
        self.open(name);
        self.text(value);
        self.close(name);
    }

    fn raw(&mut self, value: &str) {
        self.buffer.push_str(value);
    }

    fn optional(&mut self, name: &str, value: Option<&str>) {
        if let Some(value) = value {
            self.element(name, value);
        }
    }

    /// `<{element}><{key}>k</{key}><Value>v</Value></{element}>` per entry.
    fn name_value_pairs(&mut self, element: &str, key: &str, entries: Vec<(&str, &String)>) {
        for (name, value) in entries {
            self.open(element);
            self.element(key, name);
            self.element("Value", value);
            self.close(element);
        }
    }

    fn text(&mut self, value: &str) {
        for character in value.chars() {
            match character {
                '&' => self.buffer.push_str("&amp;"),
                '<' => self.buffer.push_str("&lt;"),
                '>' => self.buffer.push_str("&gt;"),
                '"' => self.buffer.push_str("&quot;"),
                '\'' => self.buffer.push_str("&apos;"),
                // A raw CR becomes LF in an XML parser and breaks the MD5.
                '\r' => self.buffer.push_str("&#xD;"),
                other => self.buffer.push(other),
            }
        }
    }

    fn into_response(self, status: u16) -> WireResponse {
        WireResponse {
            status,
            content_type: XML_CONTENT_TYPE,
            headers: Vec::new(),
            body: self.buffer.into_bytes(),
        }
    }
}
