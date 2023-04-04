use futures_util::{future::try_join_all, stream::FuturesUnordered, StreamExt};
use shutdown_handler::ShutdownHandler;

use crate::{
    amqp::ConnectionFactory,
    consumers::{
        builders::{
            consumer_group_configuration::ConsumerGroupConfiguration,
            message_handler::MessageHandler,
        },
        consumer::Consumer,
        ConsumerGroupConfigurationBuilder,
    },
};
use std::sync::Arc;

/// A collection of RabbitMq consumers sharing the same context and connection settings.
///
/// `ConsumerGroup` is the main entrypoint if you want to pull and process messages from queues.
/// Each `ConsumerGroup` connects to a single host, but each consumer uses a unique connection
/// to that host.
///
/// # Learn by doing
///
/// Check out the [`consumer` example on GitHub](https://github.com/TrueLayer/carrot-cake/tree/main/carrot-cake/examples)
/// to see `ConsumerGroup` in action.
///
/// The example showcases most of the available knobs and what they are used for.
///
/// # How do I build a `ConsumerGroup`?
///
/// `ConsumerGroup` provides a fluent API to add configuration step-by-step, known as
/// "builder pattern" in Rust.
/// The starting point is [`ConsumerGroup::builder`].
///
/// Once you are done with group-level configuration, you can start adding message handlers using
/// [`ConsumerGroupBuilder::message_handler`].
///
/// # Layered configuration
///
/// `ConsumerGroup` supports a layered approach to configuring message handlers.
///
/// Certain types of configuration values can only be added at the group level (e.g. connection
/// factory, context, queue name prefix) while others can be set both at the group and message
/// handler level (e.g. lifecycle hooks).
///
/// Check out the builder methods for an in-depth explanation for each configuration option.
pub struct ConsumerGroup<Context, Error>
where
    Context: Send + Sync + 'static,
    Error: Send + Sync + 'static,
{
    consumers: Vec<Consumer<Context, Error>>,
}

impl<Context, Error> ConsumerGroup<Context, Error>
where
    Context: Send + Sync + 'static,
    Error: Send + Sync + 'static,
{
    /// Start building a [`ConsumerGroup`].
    ///
    /// You will need a connection factory and a context.
    ///
    /// # Context
    ///
    /// In message handlers you will often need to use resources with a significant initialisation
    /// cost - e.g. a HTTP client, a database connection, etc.
    /// Instead of creating a new instance of these expensive resources every single time you handle
    /// a message, you can put those resources in the _context_.
    ///
    /// The context is created once, before the consumer group is built, and each message handler
    /// gets a shared reference (&) to the context together with the incoming message.
    /// You can therefore retrieve the HTTP client or the database connection pool from the
    /// context without having to initialise them from scratch.
    ///
    /// ## Implementation Notes
    ///
    /// The context is wrapped in an `Arc` by `ConsumerGroup` - if your context is already behind
    /// an `Arc` pointer, it won't be "double-wrapped".
    pub fn builder(
        transport_factory: ConnectionFactory,
        context: impl Into<Arc<Context>>,
    ) -> ConsumerGroupConfigurationBuilder<Context, Error> {
        ConsumerGroupConfigurationBuilder::new(transport_factory, context.into())
    }

    /// You can call `run_until_sigterm` to start consuming messages from the queues you bound.
    /// As the name implies, `run_until_sigterm` returns control to the caller only if:
    /// - one the message handlers crashes (e.g. disconnection);
    /// - the application is stopped via SIGTERM.
    pub async fn run_until_sigterm(self) -> Result<(), anyhow::Error> {
        self.run_until_shutdown(ShutdownHandler::sigterm()?).await
    }

    /// You can call `run_until_shutdown` to start consuming messages from the queues you bound.
    /// As the name implies, `run_until_shutdown` returns control to the caller only if:
    /// - one the message handlers crashes (e.g. disconnection);
    /// - the application is stopped via the shutdown handler.
    #[tracing::instrument(skip_all, name = "consumer_group_run")]
    pub async fn run_until_shutdown(
        self,
        shutdown: Arc<ShutdownHandler>,
    ) -> Result<(), anyhow::Error> {
        // the channel only needs to support 1 event (the shutdown event)
        let mut consumers = FuturesUnordered::from_iter(
            self.consumers
                .into_iter()
                .map(|c| c.run_until_shutdown(shutdown.clone()))
                .map(tokio::spawn),
        );

        // wait for all consumers to shutdown
        while let Some(res) = consumers.next().await {
            if let Err(e) = res {
                tracing::error!("Consumer failed: {}", e);
            }
            shutdown.shutdown();
        }
        Ok(())
    }
}

/// A builder to register message handlers once the group-level configuration of a [`ConsumerGroup`](super::ConsumerGroup)
/// has been finalised.
///
/// Use [`ConsumerGroup::builder`](super::ConsumerGroup::builder) as entrypoint.
pub struct ConsumerGroupBuilder<Context, Error>
where
    Context: Send + Sync + 'static,
    Error: Send + Sync + 'static,
{
    pub(super) group_configuration: ConsumerGroupConfiguration<Context, Error>,
    pub(super) message_handlers: Vec<MessageHandler<Context, Error>>,
}

impl<Context, Error> ConsumerGroupBuilder<Context, Error>
where
    Context: Send + Sync + 'static,
    Error: Send + Sync + 'static,
{
    /// Add another [`MessageHandler`] to the [`ConsumerGroup`].
    ///
    /// Check out [`MessageHandler::builder`] to build out a handler.
    #[must_use]
    pub fn message_handler(mut self, message_handler: MessageHandler<Context, Error>) -> Self {
        self.message_handlers.push(message_handler);
        self
    }

    /// Merge the message handler-level and the group-level configuration to build the underlying
    /// [`Consumer`](super::super::consumer::Consumer) instance.
    async fn build_consumer(
        group_configuration: &ConsumerGroupConfiguration<Context, Error>,
        message_handler: MessageHandler<Context, Error>,
    ) -> Result<Consumer<Context, Error>, anyhow::Error> {
        // Add a prefix to the queue name, if specified.
        let queue_name = if let Some(prefix) = group_configuration.queue_name_prefix.as_ref() {
            format!("{0}_{1}", prefix, message_handler.queue_name)
        } else {
            message_handler.queue_name.clone()
        };

        // Use the message handler consume options, if provided.
        // Rely on the group-level one otherwise.
        let consume_options = message_handler
            .consume_options
            .unwrap_or(group_configuration.consume_options);

        // Use the message handler pre-start hook, if provided.
        // Rely on the group-level one otherwise.
        let pre_start_hooks = if !message_handler.pre_start_hooks.is_empty() {
            message_handler.pre_start_hooks
        } else {
            group_configuration.pre_start_hooks.clone()
        };

        // Use the message handler transient error hook, if provided.
        // Rely on the group-level one otherwise.
        let transient_error_hook = if let Some(custom_hook) = message_handler.transient_error_hook {
            custom_hook
        } else {
            group_configuration.transient_error_hook.clone()
        };

        let prefetch_count = message_handler
            .prefetch_count_override
            .unwrap_or(group_configuration.prefetch_count);

        // Concatenate the group-level processing middlewares with the message handler middlewares.
        // Group-level processing middlewares are executed first.
        let processing_middleware_chain = group_configuration
            .processing_middleware_chain
            .clone()
            .into_iter()
            .chain(message_handler.processing_middleware_chain.into_iter())
            .collect();

        // Concatenate the group-level telemetry middlewares with the message handler middlewares.
        // Group-level telemetry middlewares are executed first.
        let telemetry_middleware_chain = group_configuration
            .telemetry_middleware_chain
            .clone()
            .into_iter()
            .chain(message_handler.telemetry_middleware_chain.into_iter())
            .collect();

        Consumer::new(
            &group_configuration.transport_factory,
            group_configuration.exit_after,
            &queue_name,
            prefetch_count,
            group_configuration.context.clone(),
            message_handler.handler,
            pre_start_hooks,
            processing_middleware_chain,
            telemetry_middleware_chain,
            transient_error_hook,
            message_handler.priority,
            consume_options,
        )
        .await
    }

    /// Once you have added all your [`MessageHandler`]s to the [`ConsumerGroup`], you can
    /// finalise the group by calling `build`.
    ///
    /// When you `.await` `build`, a connection is established with the message broker and all
    /// pre-start hooks are executed.
    ///
    /// `build` does NOT trigger consumptions of messages!
    /// Check out [`ConsumerGroup::run_until_sigterm`].
    pub async fn build(self) -> Result<ConsumerGroup<Context, Error>, anyhow::Error> {
        let Self {
            group_configuration,
            message_handlers,
        } = self;

        let consumers = message_handlers
            .into_iter()
            .map(|m| Self::build_consumer(&group_configuration, m));

        Ok(ConsumerGroup {
            consumers: try_join_all(consumers).await?,
        })
    }
}
