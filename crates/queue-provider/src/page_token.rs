//! `NextToken` of the SQS and SNS listings: the URL-safe base64 form of the
//! last name or ARN of the page, without padding. AWS tokens are opaque; each
//! caller checks the decoded value with its own rule and error.

use base64::Engine as _;

const ENGINE: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;

#[must_use]
pub fn encode_page_token(last: &str) -> String {
    ENGINE.encode(last.as_bytes())
}

/// `None` when the token is not base64 or not UTF-8.
#[must_use]
pub fn decode_page_token(token: &str) -> Option<String> {
    let bytes = ENGINE.decode(token.as_bytes()).ok()?;
    String::from_utf8(bytes).ok()
}
