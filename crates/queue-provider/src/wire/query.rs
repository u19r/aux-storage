//! The SQS Query protocol input: a form body with an `Action` field. The
//! flattened Query names become the JSON members of the same request, so
//! both protocols decode through one typed request and one validation.

use std::collections::BTreeMap;

use http_error::HttpApiError;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use super::{QueueAction, QueueRequest, RequestMembers, invalid_action};
use crate::{SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE, SQS_MISSING_PARAMETER_ERROR_TYPE};

/// How one flattened Query name maps to a JSON member.
#[derive(Clone, Copy)]
enum Shape {
    /// `Name.N` to a list member, ordered by `N`.
    List(&'static str),
    /// `Name.N.{key}` and `Name.N.{value}` to a map member. A structured
    /// value has its own members, such as `Value.StringValue`.
    Map {
        member: &'static str,
        key: &'static str,
        is_structured: bool,
    },
    /// `Name.N.*` to the `Entries` list; each entry has the shapes of a
    /// request.
    Entries,
}

/// The Query names that are not plain members (section 6.5 of the W14 plan).
const SHAPES: &[(&str, Shape)] = &[
    ("AttributeName", Shape::List("AttributeNames")),
    ("MessageAttributeName", Shape::List("MessageAttributeNames")),
    (
        "MessageSystemAttributeName",
        Shape::List("MessageSystemAttributeNames"),
    ),
    ("TagKey", Shape::List("TagKeys")),
    (
        "Attribute",
        Shape::Map {
            member: "Attributes",
            key: "Name",
            is_structured: false,
        },
    ),
    (
        "Tag",
        Shape::Map {
            member: "Tags",
            key: "Key",
            is_structured: false,
        },
    ),
    (
        "MessageAttribute",
        Shape::Map {
            member: "MessageAttributes",
            key: "Name",
            is_structured: true,
        },
    ),
    (
        "MessageSystemAttribute",
        Shape::Map {
            member: "MessageSystemAttributes",
            key: "Name",
            is_structured: true,
        },
    ),
    ("SendMessageBatchRequestEntry", Shape::Entries),
    ("DeleteMessageBatchRequestEntry", Shape::Entries),
    ("ChangeMessageVisibilityBatchRequestEntry", Shape::Entries),
];

/// Members that the JSON shapes declare as integers.
const INTEGER_MEMBERS: &[&str] = &[
    "DelaySeconds",
    "MaxNumberOfMessages",
    "VisibilityTimeout",
    "WaitTimeSeconds",
    "MaxResults",
];

/// Dot-separated segments of the longest accepted Query parameter name.
const MAX_PARAMETER_SEGMENTS: usize = 8;

/// The Query parameters that select the operation, not a member.
const PROTOCOL_PARAMETERS: &[&str] = &["Action", "Version"];

pub(super) fn decode(body: &[u8], queue_url: Option<&str>) -> Result<QueueRequest, HttpApiError> {
    let mut fields = url::form_urlencoded::parse(body)
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    if let Some(queue_url) = queue_url
        && !fields.iter().any(|(key, _)| key == "QueueUrl")
    {
        fields.push(("QueueUrl".to_owned(), queue_url.to_owned()));
    }
    let name = fields
        .iter()
        .rev()
        .find_map(|(key, value)| (key == "Action").then_some(value.as_str()));
    let action = name
        .and_then(QueueAction::from_name)
        .filter(|action| *action != QueueAction::PublishMessage)
        .ok_or_else(|| invalid_action(name))?;
    QueueRequest::decode(action, query_to_value(action, fields)?)
}

impl RequestMembers for Value {
    /// A member sent as `null` is an absent member, so a required one is
    /// `MissingParameter` (review L1).
    fn into_request<T: DeserializeOwned>(mut self) -> Result<T, super::MembersError> {
        remove_nulls(&mut self);
        serde_path_to_error::deserialize(self)
    }
}

fn remove_nulls(value: &mut Value) {
    match value {
        Value::Object(members) => {
            members.retain(|_, member| !member.is_null());
            members.values_mut().for_each(remove_nulls);
        }
        Value::Array(items) => items.iter_mut().for_each(remove_nulls),
        _ => {}
    }
}

/// The JSON members of one Query request. Unknown parameters are kept as
/// strings, which the typed request ignores.
pub(super) fn query_to_value(
    action: QueueAction,
    fields: Vec<(String, String)>,
) -> Result<Value, HttpApiError> {
    // `object` recurses once per segment: a client-chosen depth must not
    // reach it. The deepest shape has 6 segments
    // (`SendMessageBatchRequestEntry.N.MessageAttribute.M.Value.StringValue`).
    if fields
        .iter()
        .any(|(key, _)| key.split('.').nth(MAX_PARAMETER_SEGMENTS).is_some())
    {
        return Err(HttpApiError::aws_query_error(
            SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
            "A parameter name has too many parts.",
            400,
        ));
    }
    let members = fields
        .into_iter()
        .filter(|(key, _)| !PROTOCOL_PARAMETERS.contains(&key.as_str()))
        .map(|(key, value)| {
            let path = key.split('.').map(str::to_owned).collect::<Vec<_>>();
            (path, value)
        })
        .collect();
    object(action, members, "").map(Value::Object)
}

type Members = Vec<(Vec<String>, String)>;

/// Build one object from parameter paths relative to `prefix`.
fn object(
    action: QueueAction,
    members: Members,
    prefix: &str,
) -> Result<Map<String, Value>, HttpApiError> {
    let mut groups = BTreeMap::<String, Members>::new();
    let mut output = Map::new();
    for (mut path, value) in members {
        let name = path.remove(0);
        if path.is_empty() {
            let member = scalar(&format!("{prefix}{name}"), &name, value)?;
            output.insert(name, member);
        } else {
            groups.entry(name).or_default().push((path, value));
        }
    }
    for (name, members) in groups {
        let parameter = format!("{prefix}{name}");
        let Some(shape) = shape_of(action, &name) else {
            // A structure without a shape, such as `Value.StringValue`.
            let nested = object(action, members, &format!("{parameter}."))?;
            output.insert(name, Value::Object(nested));
            continue;
        };
        let (member, value) = shaped(action, shape, &parameter, members)?;
        output.insert(member, value);
    }
    Ok(output)
}

fn shaped(
    action: QueueAction,
    shape: Shape,
    parameter: &str,
    members: Members,
) -> Result<(String, Value), HttpApiError> {
    let indexed = by_index(parameter, members)?;
    match shape {
        Shape::List(member) => {
            let values = indexed
                .into_values()
                .filter_map(|mut members| members.pop())
                .filter(|(path, _)| path.is_empty())
                .map(|(_, value)| Value::String(value))
                .collect();
            Ok((member.to_owned(), Value::Array(values)))
        }
        Shape::Map {
            member,
            key,
            is_structured,
        } => {
            let member = if parameter == "Tag" {
                tag_member(action)
            } else {
                member
            };
            let mut map = Map::new();
            for (index, members) in indexed {
                let entry = object(action, members, &format!("{parameter}.{index}."))?;
                let (name, value) = map_entry(parameter, index, key, entry, is_structured)?;
                map.insert(name, value);
            }
            Ok((member.to_owned(), Value::Object(map)))
        }
        Shape::Entries => {
            let mut entries = Vec::new();
            for (index, members) in indexed {
                let entry = object(action, members, &format!("{parameter}.{index}."))?;
                entries.push(Value::Object(entry));
            }
            Ok(("Entries".to_owned(), Value::Array(entries)))
        }
    }
}

/// `CreateQueue` names its tags `tags`; `TagQueue` names them `Tags`.
fn tag_member(action: QueueAction) -> &'static str {
    if action == QueueAction::CreateQueue {
        "tags"
    } else {
        "Tags"
    }
}

/// One map entry from its `{key}` and `Value` members. A missing value is
/// 400 `MissingParameter`; an empty value stays `""`.
fn map_entry(
    parameter: &str,
    index: u32,
    key: &str,
    mut entry: Map<String, Value>,
    is_structured: bool,
) -> Result<(String, Value), HttpApiError> {
    let missing = |member: &str| {
        HttpApiError::aws_query_error(
            SQS_MISSING_PARAMETER_ERROR_TYPE,
            format!("The request must contain the parameter {parameter}.{index}.{member}."),
            400,
        )
    };
    let Some(Value::String(name)) = entry.remove(key) else {
        return Err(missing(key));
    };
    let value = entry.remove("Value").ok_or_else(|| missing("Value"))?;
    if is_structured != value.is_object() {
        return Err(missing("Value"));
    }
    Ok((name, value))
}

/// The shape of a parameter prefix. A batch entry prefix has its shape only
/// for its own action (review L4); another action ignores it as unknown.
fn shape_of(action: QueueAction, name: &str) -> Option<Shape> {
    SHAPES
        .iter()
        .find_map(|(shape_name, shape)| (*shape_name == name).then_some(*shape))
        .filter(|shape| {
            !matches!(shape, Shape::Entries)
                || name
                    .strip_suffix("RequestEntry")
                    .is_some_and(|entry| entry == action.name())
        })
}

/// Group `N.rest` paths by `N`. `N` is a positive decimal index without a
/// leading zero; another index is 400 `InvalidParameterValue`, so no member
/// is dropped or merged with another.
fn by_index(parameter: &str, members: Members) -> Result<BTreeMap<u32, Members>, HttpApiError> {
    let mut indexed = BTreeMap::<u32, Members>::new();
    for (mut path, value) in members {
        let segment = path.first().map(String::as_str).unwrap_or_default();
        let index = segment
            .parse::<u32>()
            .ok()
            .filter(|index| *index > 0 && !segment.starts_with('0'))
            .ok_or_else(|| {
                HttpApiError::aws_query_error(
                    SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
                    format!("Index {segment} of parameter {parameter} is invalid."),
                    400,
                )
            })?;
        path.remove(0);
        indexed.entry(index).or_default().push((path, value));
    }
    Ok(indexed)
}

/// A plain member. An integer member with text that is not an integer is
/// 400 `InvalidParameterValue`.
fn scalar(parameter: &str, name: &str, value: String) -> Result<Value, HttpApiError> {
    if !INTEGER_MEMBERS.contains(&name) {
        return Ok(Value::String(value));
    }
    value.trim().parse::<i64>().map(Value::from).map_err(|_| {
        HttpApiError::aws_query_error(
            SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE,
            format!("Value {value} for parameter {parameter} is invalid."),
            400,
        )
    })
}
