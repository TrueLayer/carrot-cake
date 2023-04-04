use crate::consumers::{
    handler::{AsyncClosure, ClosureHandler},
    ConsumerPreStartHook, ConsumerTransientErrorHook, Handler, ProcessingMiddleware,
    TelemetryMiddleware,
};
use std::sync::Arc;

/// A handler processing messages from a RabbitMq queue.
///
/// Use [`MessageHandler::builder`] to start composing a `MessageHandler` using a fluent builder
/// API.
///
/// # Learn by doing
///
/// Check out the [`consumer` example on GitHub](https://github.com/TrueLayer/rusty-bunny/tree/main/src/pubsub/examples)
/// to see `MessageHandler` in action.
///
/// The example showcases most of the available knobs and what they are used for.
///
/// # `MessageHandler` vs `ConsumerGroup`
///
/// A `MessageHandler` is always part of a [`ConsumerGroup`] - it relies on its context and
/// inherits its group-level configuration.
///
/// It is possible to:
///
/// - override some group-level configuration for a specific
/// message handler (see [`MessageHandlerBuilder::pre_start_hook`]
/// and [`MessageHandlerBuilder::transient_error_hook`])
/// - add on top of what the group-level configuration provides
/// (e.g. [`MessageHandlerBuilder::with_processing_middleware`] or [`MessageHandlerBuilder::with_telemetry_middleware`]).
///
/// [`ConsumerGroup`]: super::ConsumerGroup
pub struct MessageHandler<Context>
where
    Context: Send + Sync + 'static,
{
    pub(super) queue_name: String,
    pub(super) prefetch_count_override: Option<u16>,
    pub(super) processing_middleware_chain: Vec<Arc<dyn ProcessingMiddleware<Context>>>,
    pub(super) telemetry_middleware_chain: Vec<Arc<dyn TelemetryMiddleware<Context>>>,
    pub(super) pre_start_hooks: Vec<Arc<dyn ConsumerPreStartHook>>,
    pub(super) priority: Option<i32>,
    pub(super) transient_error_hook: Option<Arc<dyn ConsumerTransientErrorHook>>,
    pub(super) handler: Arc<dyn Handler<Context>>,
}

impl<Context> MessageHandler<Context>
where
    Context: Send + Sync + 'static,
{
    /// Start building a [`MessageHandler`].
    ///
    /// You need to provide the name of the queue you want to consume messages from.
    pub fn builder<T: Into<String>>(queue_name: T) -> MessageHandlerBuilder<Context> {
        MessageHandlerBuilder::new(queue_name.into())
    }
}

/// A builder to compose a [`MessageHandler`] with a fluent API.
///
/// Use [`MessageHandler::builder`] as entrypoint.
pub struct MessageHandlerBuilder<Context>
where
    Context: Send + Sync + 'static,
{
    queue_name: String,
    prefetch_count_override: Option<u16>,
    processing_middleware_chain: Vec<Arc<dyn ProcessingMiddleware<Context>>>,
    telemetry_middleware_chain: Vec<Arc<dyn TelemetryMiddleware<Context>>>,
    pre_start_hooks: Vec<Arc<dyn ConsumerPreStartHook>>,
    transient_error_hook: Option<Arc<dyn ConsumerTransientErrorHook>>,
    priority: Option<i32>,
}

impl<Context> MessageHandlerBuilder<Context>
where
    Context: Send + Sync + 'static,
{
    pub(super) fn new<T: Into<String>>(queue_name: T) -> Self {
        Self {
            queue_name: queue_name.into(),
            prefetch_count_override: None,
            processing_middleware_chain: vec![],
            telemetry_middleware_chain: vec![],
            pre_start_hooks: vec![],
            priority: None,
            transient_error_hook: None,
        }
    }

    /// Configure the prefetch count of the handler.
    /// If not configured, the handler inherits the prefetch count
    /// configured at the consumer group level.
    pub fn with_prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.prefetch_count_override = Some(prefetch_count);
        self
    }

    /// You can add processing middleware to inject logic before and after the handler logic.
    ///
    /// Middlewares are executed in the order they are registered: the first registered
    /// middleware executes first on the way in and last on the way out.
    ///
    /// Processing middlewares registered at the [`MessageHandler`] level are executed AFTER
    /// processing middlewares registered at the [`ConsumerGroup`] level.
    ///
    /// Check out [`ProcessingMiddleware`](crate::consumers::ProcessingMiddleware)'s documentation for more details.
    ///
    /// [`ConsumerGroup`]: super::ConsumerGroup
    #[must_use]
    pub fn with_processing_middleware<M: ProcessingMiddleware<Context>>(
        self,
        middleware: M,
    ) -> Self {
        self.with_dyn_processing_middleware(Arc::new(middleware))
    }

    /// Append dynamic processing middleware logic, see [`MessageHandlerBuilder::with_processing_middleware`].
    #[must_use]
    pub fn with_dyn_processing_middleware(
        mut self,
        middleware: Arc<dyn ProcessingMiddleware<Context>>,
    ) -> Self {
        self.processing_middleware_chain.push(middleware);
        self
    }

    /// Append multiple dynamic processing middlewares, see [`MessageHandlerBuilder::with_processing_middleware`].
    #[must_use]
    pub fn with_processing_middlewares<I>(mut self, middlewares: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn ProcessingMiddleware<Context>>>,
    {
        self.processing_middleware_chain.extend(middlewares);
        self
    }

    /// You can add telemetry middleware to inject logic before and after the message processing
    /// has taken place.
    ///
    /// Telemetry middlewares are executed before all processing middlewares and the handler.
    /// Telemetry middlewares are executed in the order they are registered: the first registered
    /// telemetry middleware executes first on the way in and last on the way out.
    ///
    /// Telemetry middlewares registered at the [`MessageHandler`] level are executed AFTER
    /// telemetry middlewares registered at the [`ConsumerGroup`] level.
    /// Telemetry middlewares registered at the [`MessageHandler`] level are executed BEFORE
    /// processing middlewares registered at the [`ConsumerGroup`] level and [`MessageHandler`] level.
    ///
    /// Check out [`TelemetryMiddleware`]'s documentation for more details.
    ///
    /// [`ConsumerGroup`]: super::ConsumerGroup
    #[must_use]
    pub fn with_telemetry_middleware<M: TelemetryMiddleware<Context>>(self, middleware: M) -> Self {
        self.with_dyn_telemetry_middleware(Arc::new(middleware))
    }

    /// Append dynamic telemetry middleware logic, see [`MessageHandlerBuilder::with_telemetry_middleware`].
    #[must_use]
    pub fn with_dyn_telemetry_middleware(
        mut self,
        middleware: Arc<dyn TelemetryMiddleware<Context>>,
    ) -> Self {
        self.telemetry_middleware_chain.push(middleware);
        self
    }

    /// Append multiple dynamic telemetry middlewares, see [`MessageHandlerBuilder::with_telemetry_middleware`].
    #[must_use]
    pub fn with_telemetry_middlewares<I>(mut self, middlewares: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn TelemetryMiddleware<Context>>>,
    {
        self.telemetry_middleware_chain.extend(middlewares);
        self
    }

    /// Pre-start hooks are executed _before_ consumers start pulling messages from queues.
    /// Pre-start hooks are used to execute setup logic for resources against the message broker -
    /// e.g. create exchanges, bind queues, etc.
    ///
    /// # Note
    ///   This has been deprecated in favor of [`MessageHandlerBuilder::with_pre_start_hook`] and
    ///   [`MessageHandlerBuilder::with_pre_start_hooks`] to support adding multiple hooks.
    ///
    /// Check out [`ConsumerPreStartHook`](crate::consumers::ConsumerPreStartHook)'s documentation
    /// for more details.
    ///
    /// If no pre-start hook is specified at the [`MessageHandler`] level, the hook
    /// specified at the [`ConsumerGroup`] level is executed.
    ///
    /// [`ConsumerGroup`]: super::ConsumerGroup
    #[deprecated(note = "Use 'with_pre_start_hook' instead")]
    #[must_use]
    pub fn pre_start_hook<H: ConsumerPreStartHook>(mut self, hook: H) -> Self {
        self.pre_start_hooks.push(Arc::new(hook));
        self
    }

    /// Pre-start hooks are executed _before_ consumers start pulling messages from queues.
    /// Pre-start hooks are used to execute setup logic for resources against the message broker -
    /// e.g. create exchanges, bind queues, etc.
    ///
    /// Check out [`ConsumerPreStartHook`](crate::consumers::ConsumerPreStartHook)'s documentation
    /// for more details.
    ///
    /// If no pre-start hook is specified at the [`MessageHandler`] level, the hook
    /// specified at the [`ConsumerGroup`] level is executed.
    ///
    /// [`ConsumerGroup`]: super::ConsumerGroup
    #[must_use]
    pub fn with_pre_start_hook<H: ConsumerPreStartHook>(mut self, hook: H) -> Self {
        self.pre_start_hooks.push(Arc::new(hook));
        self
    }

    /// Append multiple pre-start hooks, see [`MessageHandlerBuilder::with_pre_start_hook`].
    #[must_use]
    pub fn with_pre_start_hooks<I>(mut self, hooks: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn ConsumerPreStartHook>>,
    {
        self.pre_start_hooks.extend(hooks);
        self
    }

    /// Consumer priorities allow you to ensure that high priority consumers receive messages
    /// while they are active, with messages only going to lower priority consumers when the
    /// high priority consumers block.
    ///
    /// Sets the `x-priority` consume argument.
    /// See <https://www.rabbitmq.com/consumer-priority.html#how-to-use>.
    ///
    /// Default not specified, equivalent to `0`.
    #[must_use]
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// A transient error hook allows you to customise the behaviour of the message handlers
    /// when the processing of an incoming message fails with an error classified as transient -
    /// e.g. retryable.
    ///
    /// Check out [`ConsumerTransientErrorHook`](crate::consumers::ConsumerTransientErrorHook)'s
    /// documentation for more details.
    ///
    /// If no transient error hook is specified at the [`MessageHandler`] level, the hook
    /// specified at the [`ConsumerGroup`] level is executed.
    ///
    /// [`ConsumerGroup`]: super::ConsumerGroup
    #[must_use]
    pub fn transient_error_hook<H: ConsumerTransientErrorHook>(self, hook: H) -> Self {
        self.dyn_transient_error_hook(Arc::new(hook))
    }

    /// A version of [`MessageHandlerBuilder::transient_error_hook`] for already Arc-ed hooks.
    ///
    /// Useful for sharing `!Clone` hooks.
    ///
    /// # Example
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use carrot_cake::consumers::{MessageHandlerBuilder, ConsumerTransientErrorHook};
    /// # let message_handler_builder: MessageHandlerBuilder<()> = unimplemented!();
    /// # let other_message_handler_builder: MessageHandlerBuilder<()> = unimplemented!();
    /// let hook: Arc<dyn ConsumerTransientErrorHook>; // some dynamic shared error hook
    /// # hook = unimplemented!();
    ///
    /// message_handler_builder.dyn_transient_error_hook(Arc::clone(&hook));
    /// other_message_handler_builder.dyn_transient_error_hook(hook);
    /// ```
    #[must_use]
    pub fn dyn_transient_error_hook(mut self, hook: Arc<dyn ConsumerTransientErrorHook>) -> Self {
        self.transient_error_hook = Some(hook);
        self
    }

    /// The handler function  used to process incoming messages.
    ///
    /// Passing in the handler function finalises the `MessageHandler` construction - you will
    /// not be able to register additional middlewares or hooks after having specified the handler.
    ///
    /// # Scope
    ///
    /// The function does not get access to the underlying RabbitMq channel.
    /// The framework takes care of acking/nacking the message with the broker according to the outcome
    /// of processing (check out [`HandlerError`] for more details).
    /// This decouples the low-level interactions with the message broker and the retry logic from
    /// the actual business logic associated with the processing of a message.
    ///
    /// [`HandlerError`]: crate::consumers::HandlerError
    pub fn handler<H>(self, handler: H) -> MessageHandler<Context>
    where
        H: for<'a> AsyncClosure<'a, Context>,
    {
        self.raw_handler(ClosureHandler(handler))
    }

    /// The raw handler used to process incoming messages. Prefer `handler` if you
    /// only want to register a function handler. This method is provided for more
    /// complex implementation requirements
    ///
    /// Check out [`Handler`]'s documentation for more details.
    ///
    /// Passing in the handler finalises the `MessageHandler` construction - you will
    /// not be able to register additional middlewares or hooks after having specified the handler.
    pub fn raw_handler<H: Handler<Context>>(self, handler: H) -> MessageHandler<Context> {
        self.raw_arc_handler(Arc::new(handler))
    }

    /// The raw Arc<handler> used to process incoming messages. Prefer `handler` if you
    /// only want to register a function handler. This method is provided for more
    /// complex implementation requirements
    ///
    /// Check out [`Handler`]'s documentation for more details.
    ///
    /// Passing in the handler finalises the `MessageHandler` construction - you will
    /// not be able to register additional middlewares or hooks after having specified the handler.
    pub fn raw_arc_handler(self, handler: Arc<dyn Handler<Context>>) -> MessageHandler<Context> {
        let Self {
            queue_name,
            prefetch_count_override,
            processing_middleware_chain,
            telemetry_middleware_chain,
            pre_start_hooks,
            priority,
            transient_error_hook,
        } = self;
        MessageHandler {
            queue_name,
            prefetch_count_override,
            processing_middleware_chain,
            telemetry_middleware_chain,
            pre_start_hooks,
            priority,
            transient_error_hook,
            handler,
        }
    }
}
