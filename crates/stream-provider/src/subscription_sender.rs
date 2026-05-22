use std::{future::Future, pin::Pin};

use crate::{
    errors::StreamResult,
    types::{SubscriptionMessage, SubscriptionSendOutcome},
};

pub type SubscriptionSendFuture<'a> =
    Pin<Box<dyn Future<Output = StreamResult<SubscriptionSendOutcome>> + Send + 'a>>;

pub trait SubscriptionMessageSender: Send + Sync {
    fn send_subscription_message<'a>(
        &'a self,
        message: SubscriptionMessage,
    ) -> SubscriptionSendFuture<'a>;
}
