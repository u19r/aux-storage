//! The one owner of SQS queue ARNs, for SQS (`QueueArn` attribute, redrive
//! policies, `DeadLetterQueueSourceArn`) and for SNS (the `sqs` endpoint and
//! the subscription dead-letter queue).

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use storage_types::MessagingIdentity;

use crate::{Queue, request_validation::validate_queue_name};

/// `arn:{partition}:sqs:{region}:{account}:{name}`. A queue's own ARN is
/// computed from the deployment's `MessagingIdentity` and the queue name;
/// it is not stored (D6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueArn {
    partition: String,
    region: String,
    account: String,
    name: String,
}

impl QueueArn {
    #[must_use]
    pub fn of(identity: &MessagingIdentity, queue_name: &str) -> Self {
        Self {
            partition: identity.partition.clone(),
            region: identity.region.clone(),
            account: identity.account_id.clone(),
            name: queue_name.to_owned(),
        }
    }

    /// A queue ARN of this deployment: its partition and account. The region
    /// is not checked here: SNS accepts a queue ARN in another region (AWS
    /// probe); SQS checks it with `is_in_region`.
    pub fn parse(value: &str, identity: &MessagingIdentity) -> Result<Self, String> {
        let arn = value.parse::<Self>()?;
        if !arn.is_owned_by(identity) {
            return Err("Invalid value for deadLetterTargetArn.".to_owned());
        }
        Ok(arn)
    }

    #[must_use]
    pub fn is_in_region(&self, identity: &MessagingIdentity) -> bool {
        self.region == identity.region
    }

    /// Whether this ARN is in the deployment's partition and account.
    #[must_use]
    pub fn is_owned_by(&self, identity: &MessagingIdentity) -> bool {
        self.partition == identity.partition && self.account == identity.account_id
    }

    #[must_use]
    pub fn queue_name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn account(&self) -> &str {
        &self.account
    }

    #[must_use]
    pub fn is_fifo(&self) -> bool {
        self.name.ends_with(".fifo")
    }

    /// Whether this ARN names `queue`. A stored redrive target passed
    /// `check_redrive_identity`, so its partition, region and account are the
    /// deployment's and only the name can differ.
    #[must_use]
    pub fn names(&self, queue: &Queue) -> bool {
        self.name == queue.queue_name
    }

    /// The ARN of `queue` in this ARN's partition, region and account: the
    /// source of a dead-letter move, whose policy names this target.
    #[must_use]
    pub fn sibling(&self, queue: &Queue) -> Self {
        Self {
            name: queue.queue_name.clone(),
            ..self.clone()
        }
    }
}

impl fmt::Display for QueueArn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "arn:{}:sqs:{}:{}:{}",
            self.partition, self.region, self.account, self.name
        )
    }
}

/// The shape of any queue ARN; `parse` adds the deployment checks.
impl FromStr for QueueArn {
    /// The SQS reason text.
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || "Invalid value for deadLetterTargetArn.".to_owned();
        let parts = value.split(':').collect::<Vec<_>>();
        let ["arn", partition, "sqs", region, account, name] = parts.as_slice() else {
            return Err(invalid());
        };
        let base = name.strip_suffix(".fifo").unwrap_or(name);
        if partition.is_empty()
            || region.is_empty()
            || !is_account_id(account)
            || validate_queue_name(base).is_err()
        {
            return Err(invalid());
        }
        Ok(Self {
            partition: (*partition).to_owned(),
            region: (*region).to_owned(),
            account: (*account).to_owned(),
            name: (*name).to_owned(),
        })
    }
}

fn is_account_id(value: &str) -> bool {
    value.len() == 12 && value.bytes().all(|byte| byte.is_ascii_digit())
}
