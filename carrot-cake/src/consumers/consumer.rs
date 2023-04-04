#![allow(clippy::too_many_arguments)]
use crate::{
    consumers::{
        extensions::with_extensions,
        handler::Handler,
        pre_start_hook::ConsumerPreStartHook,
        processing_middleware::{Next, ProcessingMiddleware},
        telemetry_middleware::MessageProcessing,
        transient_error_hook::ConsumerTransientErrorHook,
        Incoming, TelemetryMiddleware,
    },
    rabbit_mq::{Channel, WITHOUT_PUBLISHER_CONFIRMATION},
};
use amq_protocol_types::{AMQPValue, FieldTable};
use carrot_cake_amqp::transport::TransportFactory;
use futures_util::{stream::FuturesUnordered, Future, FutureExt, StreamExt};
use lapin::{
    message::Delivery,
    options::{BasicConsumeOptions, BasicQosOptions},
};
use std::sync::Arc;
use task_local_extensions::Extensions;
use uuid::Uuid;

/// The actual implementation of a RabbitMq consumer.
///
/// [`ConsumerGroup`] instantiates a `Consumer` for each [`MessageHandler`].
///
/// Users of the crate are never exposed to `Consumer` directly - it's an implementation detail
/// whose interface we are free to evolve over time (as long as it does not require changes to
/// the builder interface for [`ConsumerGroup`] and [`MessageHandler`]).
///
/// [`ConsumerGroup`]: super::ConsumerGroup
/// [`MessageHandler`]: super::MessageHandler
pub(super) struct Consumer<C: Send + Sync + 'static, E: Send + Sync + 'static> {
    /// An open channel to communicate with RabbitMq.
    /// Used to create a queue consumer/fetch single messages, according to the caller usage patterns.
    channel: Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
    /// The maximum number of messages `Consumer` is going to process before exiting the processing
    /// loop.
    /// The consumer will process messages indefinitely if set to `None`.
    exit_after: Option<usize>,
    /// The name of the queue we will be consuming messages from.
    queue_name: String,
    /// `context` is a set of resources that are required to process the message and outlive
    /// the lifecycle of the handler itself - e.g. an HTTP client for a third-party API, a db connection
    /// pool, etc.
    context: Arc<C>,
    /// `handler` determines what processing will be run on each incoming message.
    /// See `Handler`'s documentation for more information.
    handler: Arc<dyn Handler<C, E>>,
    /// `processing_middleware_chain` is an ordered collection of [`ProcessingMiddleware`]s
    /// that will be executed (in order!) before/after the message is processed by the `handler`.
    /// See [`ProcessingMiddleware`]'s documentation for more information.
    processing_middleware_chain: Vec<Arc<dyn ProcessingMiddleware<C, E>>>,
    /// `processing_middleware_chain` is an ordered collection of [`TelemetryMiddleware`]s
    /// that will be executed (in order!) before/after the message is processed by `handler` and
    /// [`ProcessingMiddleware`]s.
    /// See [`TelemetryMiddleware`]'s documentation for more information.
    telemetry_middleware_chain: Vec<Arc<dyn TelemetryMiddleware<C, E>>>,
    /// `transient_error_hook` specifies behaviour to be executed when message processing fails due to
    /// an error labelled as `transient`.
    transient_error_hook: Arc<dyn ConsumerTransientErrorHook>,
    /// Consumer priority. See https://www.rabbitmq.com/consumer-priority.html.
    priority: Option<i32>,
}

impl<C: Send + Sync + 'static, E: Send + Sync + 'static> Consumer<C, E> {
    /// Configure a new RabbitMq consumer.
    ///
    /// `Consumer::new` connects to RabbitMq, sets up the specified queue topology and returns a ready-to-run
    /// consumer.
    /// `Consumer::new` executes `pre_start_hooks`, when provided.
    ///
    /// Use [`Consumer::run`] to start consuming messages.
    #[tracing::instrument(
        skip(
            context,
            handler,
            pre_start_hooks,
            transport_factory,
            processing_middleware_chain,
            telemetry_middleware_chain,
            transient_error_hook
        ),
        name = "consumer_new"
    )]
    pub(super) async fn new(
        transport_factory: &impl TransportFactory,
        exit_after: Option<usize>,
        queue_name: &str,
        prefetch_count: u16,
        // If the context is already behind an Arc pointer, we won't double-wrap it.
        // Useful to share the same context across multiple consumers.
        context: impl Into<Arc<C>>,
        handler: Arc<dyn Handler<C, E>>,
        pre_start_hooks: Vec<Arc<dyn ConsumerPreStartHook>>,
        processing_middleware_chain: Vec<Arc<dyn ProcessingMiddleware<C, E>>>,
        telemetry_middleware_chain: Vec<Arc<dyn TelemetryMiddleware<C, E>>>,
        transient_error_hook: Arc<dyn ConsumerTransientErrorHook>,
        priority: Option<i32>,
    ) -> Result<Consumer<C, E>, anyhow::Error> {
        let channel = transport_factory
            .get_channel_without_confirmation()
            .await?
            .clone();

        Self::new_with_channel(
            channel,
            exit_after,
            queue_name,
            prefetch_count,
            context,
            handler,
            pre_start_hooks,
            processing_middleware_chain,
            telemetry_middleware_chain,
            transient_error_hook,
            priority,
        )
        .await
    }

    /// Configure a new RabbitMq consumer using an existing channel.
    ///
    /// `new_with_channel` sets up the specified queue topology and returns a ready-to-run
    /// consumer.
    /// `Consumer::new_with_channel` executes `pre_start_hooks`, when provided.
    ///
    /// Use [`Consumer::run`] to start consuming messages.
    #[tracing::instrument(
        skip(
            context,
            handler,
            pre_start_hooks,
            channel,
            processing_middleware_chain,
            telemetry_middleware_chain,
            transient_error_hook
        ),
        name = "consumer_new_with_channel"
    )]
    pub(super) async fn new_with_channel(
        channel: Channel<WITHOUT_PUBLISHER_CONFIRMATION>,
        exit_after: Option<usize>,
        queue_name: &str,
        prefetch_count: u16,
        // If the context is already behind an Arc pointer, we won't double-wrap it.
        // Useful to share the same context across multiple consumers.
        context: impl Into<Arc<C>>,
        handler: Arc<dyn Handler<C, E>>,
        pre_start_hooks: Vec<Arc<dyn ConsumerPreStartHook>>,
        processing_middleware_chain: Vec<Arc<dyn ProcessingMiddleware<C, E>>>,
        telemetry_middleware_chain: Vec<Arc<dyn TelemetryMiddleware<C, E>>>,
        transient_error_hook: Arc<dyn ConsumerTransientErrorHook>,
        priority: Option<i32>,
    ) -> Result<Consumer<C, E>, anyhow::Error> {
        channel
            .raw()
            .basic_qos(prefetch_count, BasicQosOptions { global: false })
            .await?;

        for hook in pre_start_hooks {
            hook.run(&channel, queue_name, FieldTable::default())
                .await?;
        }

        let context = context.into();
        Ok(Consumer {
            channel,
            exit_after,
            queue_name: queue_name.into(),
            context,
            handler,
            processing_middleware_chain,
            telemetry_middleware_chain,
            transient_error_hook,
            priority,
        })
    }

    /// Run the consumer, which will notify RabbitMq to start pushing messages on the specified
    /// queue.
    ///
    /// `run_until_shutdown` exits if the consumer fails with an error (e.g. the channel is closed
    /// or the connection with RabbitMq is lost) or if a shutdown signal is recieved.
    #[tracing::instrument(skip_all, name = "consumer_run", fields(queue_name = %self.queue_name))]
    pub async fn run_until_shutdown(
        self,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), anyhow::Error> {
        let mut consumer = self
            .channel
            .raw()
            .basic_consume(
                &self.queue_name,
                &Uuid::new_v4().to_string(),
                BasicConsumeOptions::default(),
                {
                    let mut args = FieldTable::default();
                    if let Some(priority) = self.priority {
                        args.insert("x-priority".into(), AMQPValue::LongInt(priority));
                    }
                    args
                },
            )
            .await?;

        let mut task_handles = FuturesUnordered::new();
        let mut counter = 0;

        let shutdown = shutdown.fuse();
        tokio::pin!(shutdown);

        let result = 'event_loop: loop {
            // have we consumed all the events we want?
            if self.exit_after == Some(counter) {
                break 'event_loop Ok(());
            }

            // select! takes the first future to return ready.
            // select! has [issues] but it's the easiest way to represent
            // this concept.
            //
            // To be explicit, we're trying to either run some code if we have a shutdown or
            // a new event - without blocking either.
            //
            // The current futures are race-safe so it doesn't
            // matter if they drop early, since they're all mut-references to
            // a value that won't actually be dropped.
            //
            // [issues]: https://blog.yoshuawuyts.com/futures-concurrency-3/#issues-with-futures-select
            tokio::select! {
                // we want to poll in the specified order - preferring the handling of shutdowns before
                // going on with processing more events
                biased;

                // check for a shutdown signal
                _ = &mut shutdown => {
                    tracing::info!("consumer group received shutdown event");
                    let _ = self.channel.raw().basic_cancel(consumer.tag().as_str(), <_>::default()).await;
                }

                // clear out some of our task handles
                _ = task_handles.next(), if !task_handles.is_empty() => {}

                // try get the next consumer event
                event = consumer.next() => {
                    match event {
                        // consumer has shutdown
                        None => { break 'event_loop Ok(()) }
                        Some(Err(e)) => {
                            tracing::error!("Consumer error: {}", e);
                            break 'event_loop Err(e.into())
                        }
                        Some(Ok(delivery)) => {
                            // Spawn up the message handler as its own task in order to process multiple messages
                            // concurrently, up to the limit established by our QoS settings.
                            //
                            // This also isolates failures: failing to process one message (even with a panic!)
                            // does not tear the whole consumer down.
                            let future = Self::process(
                                delivery,
                                self.context.clone(),
                                self.handler.clone(),
                                self.processing_middleware_chain.clone(),
                                self.telemetry_middleware_chain.clone(),
                                self.queue_name.clone(),
                                self.transient_error_hook.clone(),
                            );
                            let handle = tokio::spawn(future);
                            if self.exit_after.is_some() {
                                task_handles.push(handle);
                            }
                            counter += 1;
                        }
                    }
                }
            }
        };

        // Make sure all tasks in flight complete before returning.
        // If the set is empty, this returns immediately.
        while task_handles.next().await.is_some() {}

        result
    }

    /// Process an incoming message - middlewares, handlers, ack/nack against the AMQP broker.
    #[tracing::instrument(
        name = "process_message",
        skip(
            delivery,
            context,
            handler,
            processing_middleware_chain,
            telemetry_middleware_chain,
            transient_error_hook
        ),
        level = tracing::Level::DEBUG
    )]
    async fn process(
        delivery: Delivery,
        context: Arc<C>,
        handler: Arc<dyn Handler<C, E>>,
        processing_middleware_chain: Vec<Arc<dyn ProcessingMiddleware<C, E>>>,
        telemetry_middleware_chain: Vec<Arc<dyn TelemetryMiddleware<C, E>>>,
        queue_name: String,
        transient_error_hook: Arc<dyn ConsumerTransientErrorHook>,
    ) {
        let next = Next {
            handler: handler.as_ref(),
            next_middleware: &processing_middleware_chain,
        };
        let message_processing_pipeline = MessageProcessing {
            transient_error_hook,
            processing_chain: next,
            next_telemetry_middleware: &telemetry_middleware_chain,
        };
        let incoming = Incoming {
            context,
            message: &delivery,
            queue_name,
        };
        let task_future = message_processing_pipeline.run(incoming);
        let (_, _) = with_extensions(Extensions::default(), task_future).await;
    }
}
