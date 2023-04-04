use crate::{
    pool::ChannelPool,
    publishers::{MessageEnvelope, PublisherError},
};
use anyhow::Context;
use lapin::{
    options::BasicPublishOptions, publisher_confirm::Confirmation, BasicProperties, Channel,
};
use std::sync::Arc;
use task_local_extensions::Extensions;

#[async_trait::async_trait]
/// Middlewares to execute logic before and after a message is published by [`Publisher`].
///
/// # Use case
///
/// The main purpose of middlewares is to extract and centralise common non-business logic that would
/// otherwise have to be copy-pasted for all publishers.  
/// With middlewares we can build re-usable components to perform common tasks across multiple
/// message consumers applications - e.g. logging or signing of messages.
///
/// # Extensions
///
/// `extensions` can be used to store values that are needed in this middleware on the way
/// back, after having called [`Next::run`], or should be shared with other middlewares downstream.
///
/// [`Publisher`]: super::Publisher
pub trait PublisherMiddleware: Send + Sync {
    async fn handle(
        &self,
        envelope: MessageEnvelope,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> Result<(), PublisherError>;
}

/// The remainder of the publishing middleware chain, including the final publishing action.
#[allow(missing_debug_implementations)]
pub struct Next<'a> {
    /// Channel pool to get a channel to actually execute the publishing action.
    pub(super) channel_pool: ChannelPool,
    /// Timeout on publishing.
    pub(super) timeout: std::time::Duration,
    /// The remainder of the processing middleware chain.
    pub(super) next_middleware: &'a [Arc<dyn PublisherMiddleware>],
    pub(super) options: BasicPublishOptions,
}

impl<'a> Next<'a> {
    /// Asynchronously execute the remaining processing middleware chain.
    pub async fn run(
        mut self,
        envelope: MessageEnvelope,
        extensions: &mut Extensions,
    ) -> Result<(), PublisherError> {
        // If there is at least one middleware in the chain, get a reference to it and store
        // the remaining ones in `next_middleware`.
        // Then call the middleware passing `self` in the handler, recursively.
        if let Some((current, next)) = self.next_middleware.split_first() {
            self.next_middleware = next;
            current.handle(envelope, extensions, self).await
        } else {
            // We have executed all middlewares (or simply there were none) and it's now
            // the turn of the publishing itself.

            // We combine the connection/channel pool operation with the publishing
            // and put the both under the same timeout.
            // Instead of having the complexity to handle timeouts per operation.

            let MessageEnvelope {
                payload,
                properties,
                exchange_name,
                routing_key,
            } = &envelope;

            let publish_future = async move {
                let channel = self.channel_pool
                    .get()
                    .await
                    .map_err(anyhow::Error::msg)
                    .context("Failed to acquire a healthy channel from the pool when trying to publish to RabbitMq")
                    .map_err(PublisherError::GenericError)?;

                publish(
                    &channel,
                    payload,
                    self.options,
                    exchange_name,
                    routing_key,
                    properties.clone(),
                )
                .await
                .map_err(PublisherError::from)
            };

            match tokio::time::timeout(self.timeout, publish_future).await {
                Ok(result) => result,
                Err(_) => Err(PublisherError::TimeoutError),
            }
        }
    }
}

/// Publish a payload on a RabbitMq exchange, waiting for publisher confirmation from the
/// RabbitMq broker.
///
/// The mandatory flag tells the broker how to react if the message cannot be routed to a queue.
/// If this flag is `true`, the broker will return an unroutable message with a Return method.
/// If this flag is `false`, the broker silently drops the message.
#[tracing::instrument(level = "debug", skip(channel, payload))]
async fn publish(
    channel: &Channel,
    payload: &[u8],
    options: BasicPublishOptions,
    exchange: &str,
    routing_key: &str,
    properties: BasicProperties,
) -> Result<(), PublisherError> {
    // Delivery mode: Non-persistent (1) or persistent (2).
    let properties = properties.with_delivery_mode(2);

    let confirm = channel
        .basic_publish(exchange, routing_key, options, payload, properties)
        .await
        .map_err(|e| PublisherError::GenericError(e.into()))?
        .await
        .map_err(|e| PublisherError::GenericError(e.into()))?;

    match confirm {
        Confirmation::Ack(ack) => {
            if let Some(return_message) = ack {
                // Reply Code 312 - NO_ROUTE
                // See https://www.rabbitmq.com/amqp-0-9-1-reference.html
                if return_message.reply_code == 312 {
                    return Err(PublisherError::UnroutableMessage(return_message));
                }
            }
            Ok(())
        }
        Confirmation::Nack(nack) => Err(PublisherError::NegativeAck(nack)),
        Confirmation::NotRequested => Ok(()),
    }
}
