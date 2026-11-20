//! The partition, region, and account that SNS topic ARNs and SQS queue
//! endpoints of one deployment use (W3 design, section 8.2).

use serde::{Deserialize, Serialize};

/// One value for the whole deployment, built once from configuration. The
/// defaults equal the constants that were used before the setting existed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessagingIdentity {
    pub partition: String,
    pub region: String,
    pub account_id: String,
}

impl Default for MessagingIdentity {
    fn default() -> Self {
        Self {
            partition: "aws".to_owned(),
            region: "us-east-1".to_owned(),
            account_id: "000000000000".to_owned(),
        }
    }
}
