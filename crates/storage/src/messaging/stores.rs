//! Queue and pubsub store values of one native connection, one per
//! admission lane. A customer value takes one admission permit per native
//! attempt, so a waiting receive holds no capacity between attempts; the
//! delivery value uses only free foreground slots and never queues ahead of
//! customers.
use std::sync::Arc;

/// The admission a messaging store value applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessagingLane {
    /// One foreground permit per native attempt (SQS and SNS customer
    /// requests).
    Customer,
    /// No permit in the store: the caller admits the whole operation, or
    /// nothing does (tests and tools).
    Caller,
}

/// The queue and pubsub views of one store value.
#[derive(Clone)]
pub struct MessagingStore {
    pub queue: Arc<dyn queue_provider::QueueOperations>,
    pub pubsub: Arc<dyn pubsub_provider::PubsubProvider>,
}

pub(crate) struct MessagingStores {
    pub(crate) caller: MessagingStore,
    pub(crate) customer: MessagingStore,
    /// Scheduled SNS delivery: background lane on the connection's
    /// controller.
    pub(crate) delivery: Arc<dyn pubsub_provider::PubsubProvider>,
}

impl MessagingStores {
    pub(crate) fn lane(&self, lane: MessagingLane) -> MessagingStore {
        match lane {
            MessagingLane::Customer => self.customer.clone(),
            MessagingLane::Caller => self.caller.clone(),
        }
    }
}
