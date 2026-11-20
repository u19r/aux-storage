//! One attribute rule for `CreateTopic` and `SetTopicAttributes`, and topic
//! tags (W3 design, section 8.3, with orchestrator decision O8: tags are
//! accepted and stored).

use std::collections::{BTreeMap, HashMap};

use queue_provider::{TagRuleViolation, apply_tag_change};
use storage_types::{MessagingIdentity, TimestampMillis};

use crate::{
    CreateTopicRequest, PubsubError, PubsubResult, PubsubValidationKind, Topic, TopicArn,
    constants::DISPLAY_NAME_MAX_CHARS,
};

impl Topic {
    /// Applied: `DisplayName`, and `FifoTopic=false` (no effect). Any other
    /// name, or `FifoTopic=true`, is rejected (AWS: "Unknown attribute").
    pub fn try_new(
        request: CreateTopicRequest,
        identity: &MessagingIdentity,
    ) -> PubsubResult<Self> {
        let mut display_name = None;
        for (name, value) in &request.attributes {
            match name.as_str() {
                "DisplayName" => {
                    check_display_name(value)?;
                    display_name = Some(value.clone());
                }
                "FifoTopic" if value.eq_ignore_ascii_case("false") => {}
                _ => return Err(unsupported(name)),
            }
        }
        let mut tags = BTreeMap::new();
        apply_tag_change(&mut tags, request.tags, &[]).map_err(tag_error)?;
        Ok(Self {
            topic_arn: TopicArn::compose(
                &identity.partition,
                &identity.region,
                &identity.account_id,
                &request.name,
            ),
            name: request.name,
            display_name,
            tags,
            created_at: TimestampMillis::now(),
        })
    }

    /// `SetTopicAttributes` supports only `DisplayName`.
    pub fn validate_attribute_update(attributes: &HashMap<String, String>) -> PubsubResult<()> {
        if let Some(name) = attributes.keys().find(|name| *name != "DisplayName") {
            return Err(unsupported(name));
        }
        attributes
            .get("DisplayName")
            .map_or(Ok(()), |value| check_display_name(value))
    }

    /// The account of the topic ARN; it is the `Owner` attribute.
    #[must_use]
    pub fn owner(&self) -> &str {
        account_of(self.topic_arn.as_str())
    }

    /// `TagResource`: add or replace tags; at most 50 in total.
    pub fn tag(&mut self, tags: BTreeMap<String, String>) -> PubsubResult<()> {
        apply_tag_change(&mut self.tags, tags, &[]).map_err(tag_error)
    }

    /// `UntagResource`: an unknown key is not an error.
    pub fn untag(&mut self, keys: &[String]) {
        // Removing keys cannot break the rule.
        let _ = apply_tag_change(&mut self.tags, BTreeMap::new(), keys);
    }
}

/// The account segment of an SNS ARN
/// (`arn:{partition}:sns:{region}:{account}:...`).
pub(crate) fn account_of(arn: &str) -> &str {
    arn.split(':').nth(4).unwrap_or_default()
}

/// A `DisplayName` holds at most 100 characters (suite script
/// `display_name_over_100_characters_rejected`).
fn check_display_name(value: &str) -> PubsubResult<()> {
    if value.chars().count() > DISPLAY_NAME_MAX_CHARS {
        return Err(PubsubError::validation_with_detail(
            PubsubValidationKind::InvalidAttributeValue,
            "DisplayName",
        ));
    }
    Ok(())
}

fn unsupported(name: &str) -> PubsubError {
    PubsubError::validation_with_detail(PubsubValidationKind::UnsupportedAttribute, name)
}

/// The SNS text of each tag rule violation.
fn tag_error(violation: TagRuleViolation) -> PubsubError {
    let detail = match violation {
        TagRuleViolation::TooMany => {
            "Could not complete request: tag quota of per resource exceeded"
        }
        TagRuleViolation::InvalidKey | TagRuleViolation::ReservedPrefix => "Tag key is invalid",
        TagRuleViolation::InvalidValue => "Tag value is invalid",
    };
    PubsubError::validation_with_detail(
        PubsubValidationKind::InvalidAttributeValue,
        format!("Tags Reason: {detail}"),
    )
}
