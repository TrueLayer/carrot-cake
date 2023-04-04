use crate::publishers::{MessageEnvelope, PublisherError};
use carrot_cake_amqp_pool::ChannelPool;
use futures_util::TryFutureExt;
use std::sync::Arc;
use task_local_extensions::Extensions;

#[async_trait::async_trait]
/// Middlewares to execute logic before and after a message is published by [`Publisher`].
///
/// # Usecase
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
/// # Plug-ang-play middlewares
///
/// There is a rich ecosystem of ready-to-go middlewares for message publishers:
///
/// - [`truelayer-pubsub-observability`](https://github.com/TrueLayer/rusty-bunny/tree/main/src/pubsub-observability/)
///   provides middlewares to propagate OpenTelemetry headers;
/// - [`amqp-auth`](https://github.com/TrueLayer/rusty-bunny/tree/main/src/amqp-auth) provides a middleware to sign a
///   message before publishing it.
///
/// [`Publisher`]: super::Publisher
pub trait PublisherMiddleware<const PUBLISHER_CONFIRMATION: bool>: Send + Sync {
    async fn handle(
        &self,
        envelope: MessageEnvelope,
        extensions: &mut Extensions,
        next: Next<'_, { PUBLISHER_CONFIRMATION }>,
    ) -> Result<(), PublisherError>;
}

/// The remainder of the publishing middleware chain, including the final publishing action.
#[allow(missing_debug_implementations)]
pub struct Next<'a, const PUBLISHER_CONFIRMATION: bool> {
    /// Channel pool to get a channel to actually execute the publishing action.
    pub(super) channel_pool: ChannelPool<{ PUBLISHER_CONFIRMATION }>,
    /// Timeout on publishing.
    pub(super) timeout: std::time::Duration,
    /// The remainder of the processing middleware chain.
    pub(super) next_middleware: &'a [Arc<dyn PublisherMiddleware<{ PUBLISHER_CONFIRMATION }>>],
}

impl<'a, const PUBLISHER_CONFIRMATION: bool> Next<'a, { PUBLISHER_CONFIRMATION }> {
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
            let publish_future = self
                .channel_pool
                .get()
                .map_err(|err| anyhow::Error::msg(err).context("Failed to acquire a healthy channel from the pool when trying to publish to RabbitMq"))
                .map_err(PublisherError::GenericError)
                .and_then(|channel| {
                    let MessageEnvelope {
                        payload,
                        exchange_name,
                        routing_key,
                        properties,
                    } = envelope;

                    // Using `move` to pass ownership of `routing_key` and `exchange` to the future
                    // so that we do not get lifetimes issues when using them as references across an `.await` point
                    async move {
                        channel
                            .publish(payload, &exchange_name, &routing_key, properties)
                            .await
                            .map_err(Into::into)
                    }
                });

            match tokio::time::timeout(self.timeout, publish_future).await {
                Ok(result) => result,
                Err(_) => Err(PublisherError::TimeoutError),
            }
        }
    }
}
