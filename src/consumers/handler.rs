//! The `Handler` trait is heavily inspired by `tide`'s approach to endpoint handlers.
use crate::consumers::{error::HandlerError, Incoming};

use super::BrokerAction;

/// Implementers of the `Handler` trait can be used in our RabbitMq [`MessageHandler`]s
/// to process messages retrieved from a queue.
///
/// # Scope
///
/// `handle` does not get access to the underlying RabbitMq channel.
/// The framework takes care of acking/nacking the message with the broker according to the outcome
/// of processing (check out [`HandlerError`] for more details).
/// This decouples the low-level interactions with the message broker and the retry logic from
/// the actual business logic associated with the processing of a message.
///
/// # Implementers
///
/// While you can implement `Handler` for a struct or enum, 99% of the time you will be relying
/// on our implementation of `Handle` for async functions that have a matching signature -
/// `Fn(Delivery, Arc<Context>) -> Fut`. See below for more details.
///
/// [`MessageHandler`]: crate::consumers::MessageHandler
#[async_trait::async_trait]
pub trait Handler: Send + Sync + 'static {
    type Context;
    type Error;

    async fn handle(
        &self,
        incoming: &Incoming<Self::Context>,
    ) -> Result<BrokerAction, HandlerError<Self::Error>>;
}
