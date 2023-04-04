use lapin::options::BasicConsumeOptions;

use crate::amqp::ConnectionFactory;
use crate::consumers::builders::consumer_group::ConsumerGroupBuilder;
use crate::consumers::builders::message_handler::MessageHandler;
use crate::consumers::hooks::transient_error::NeverRequeue;
use crate::consumers::{
    ConsumerPreStartHook, ConsumerTransientErrorHook, ProcessingMiddleware, TelemetryMiddleware,
};
use std::sync::Arc;

const DEFAULT_PREFETCH_COUNT: u16 = 50;

/// Group-level configuration values for a [`ConsumerGroup`](super::ConsumerGroup).
///
/// Use [`ConsumerGroupConfigurationBuilder`] to build an instance of `ConsumerGroupConfiguration`.
pub(super) struct ConsumerGroupConfiguration<Context, Error>
where
    Context: Send + Sync + 'static,
    Error: Send + Sync + 'static,
{
    pub(super) transport_factory: ConnectionFactory,
    pub(super) queue_name_prefix: Option<String>,
    pub(super) prefetch_count: u16,
    pub(super) context: Arc<Context>,
    pub(super) processing_middleware_chain: Vec<Arc<dyn ProcessingMiddleware<Context, Error>>>,
    pub(super) telemetry_middleware_chain: Vec<Arc<dyn TelemetryMiddleware<Context, Error>>>,
    pub(super) pre_start_hooks: Vec<Arc<dyn ConsumerPreStartHook>>,
    pub(super) exit_after: Option<usize>,
    pub(super) transient_error_hook: Arc<dyn ConsumerTransientErrorHook>,
    pub(super) consume_options: BasicConsumeOptions,
}

/// A builder for group-level configuration of a [`ConsumerGroup`](super::ConsumerGroup).
///
/// Use [`ConsumerGroup::builder`](super::ConsumerGroup::builder) as entrypoint.
pub struct ConsumerGroupConfigurationBuilder<Context, Error>(
    ConsumerGroupConfiguration<Context, Error>,
)
where
    Context: Send + Sync + 'static,
    Error: Send + Sync + 'static;

impl<Context, Error> ConsumerGroupConfigurationBuilder<Context, Error>
where
    Context: Send + Sync + 'static,
    Error: Send + Sync + 'static,
{
    pub(super) fn new(transport_factory: ConnectionFactory, context: Arc<Context>) -> Self {
        Self(ConsumerGroupConfiguration {
            transport_factory,
            queue_name_prefix: None,
            prefetch_count: DEFAULT_PREFETCH_COUNT,
            context,
            processing_middleware_chain: vec![],
            telemetry_middleware_chain: vec![],
            pre_start_hooks: Vec::new(),
            // We default to never requeueing a transiently failed message.
            // It minimises surprising behaviour/phenomena in production.
            transient_error_hook: Arc::new(NeverRequeue),
            // By default, the consumer group will continue to consume message indefinitely.
            exit_after: None,
            consume_options: BasicConsumeOptions::default(),
        })
    }

    /// Add a prefix to the name of queues used by message handlers in the group.
    ///
    /// E.g. `test` as prefix will give you `test_X` queue names.
    #[must_use]
    pub fn queue_name_prefix<T: Into<String>>(mut self, prefix: T) -> Self {
        self.0.queue_name_prefix = Some(prefix.into());
        self
    }

    /// Configure the prefetch count of consumers in the group.
    /// If not configured, the prefetch count is set to a default value of 50.
    #[must_use]
    pub fn with_prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.0.prefetch_count = prefetch_count;
        self
    }

    /// To configure the consume options for the consumer group
    #[must_use]
    pub fn with_consume_options(mut self, consume_options: BasicConsumeOptions) -> Self {
        self.0.consume_options = consume_options;
        self
    }

    /// You can add a processing middleware to inject logic before and after the handler logic.
    ///
    /// Processing middlewares execute after all [`TelemetryMiddleware`]s have run.
    /// Processing middlewares are executed in the order they are registered: the first registered
    /// middleware executes first on the way in and last on the way out.
    ///
    /// Check out [`ProcessingMiddleware`](crate::consumers::ProcessingMiddleware)'s documentation for more details.
    #[must_use]
    pub fn with_processing_middleware<M: ProcessingMiddleware<Context, Error>>(
        self,
        middleware: M,
    ) -> Self {
        self.with_dyn_processing_middleware(Arc::new(middleware))
    }

    /// Append dynamic processing middleware logic, see [`ConsumerGroupConfigurationBuilder::with_processing_middleware`].
    ///
    /// # Example
    /// ```no_run
    /// use carrot_cake::consumers::{ConsumerGroupConfigurationBuilder, ProcessingMiddleware};
    ///
    /// # type Ctx = ();
    /// # type Error = ();
    /// # let consumer_group_builder: ConsumerGroupConfigurationBuilder<Ctx,Error> = unimplemented!();
    /// let middleware: std::sync::Arc<dyn ProcessingMiddleware<Ctx, Error>>; // some dynamic pointer to middleware
    /// # middleware = unimplemented!();
    ///
    /// consumer_group_builder.with_dyn_processing_middleware(middleware);
    /// ```
    #[must_use]
    pub fn with_dyn_processing_middleware(
        mut self,
        middleware: Arc<dyn ProcessingMiddleware<Context, Error>>,
    ) -> Self {
        self.0.processing_middleware_chain.push(middleware);
        self
    }

    /// Append multiple dynamic processing middlewares, see [`ConsumerGroupConfigurationBuilder::with_processing_middleware`].
    #[must_use]
    pub fn with_processing_middlewares<I>(mut self, middlewares: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn ProcessingMiddleware<Context, Error>>>,
    {
        self.0.processing_middleware_chain.extend(middlewares);
        self
    }

    /// You can add a telemetry middleware to collect data on the outcome and performance of
    /// message processing.
    ///
    /// [`TelemetryMiddleware`]s execute before and after all processing has taken place (including
    /// acking/nacking the message with the AMQP broker).
    /// Telemetry middlewares are executed in the order they are registered: the first registered
    /// middleware executes first on the way in and last on the way out.
    ///
    /// Check out [`TelemetryMiddleware`]'s documentation for more details.
    #[must_use]
    pub fn with_telemetry_middleware<M: TelemetryMiddleware<Context, Error>>(
        self,
        middleware: M,
    ) -> Self {
        self.with_dyn_telemetry_middleware(Arc::new(middleware))
    }

    /// Append dynamic processing middleware logic, see [`ConsumerGroupConfigurationBuilder::with_processing_middleware`].
    ///
    /// # Example
    /// ```no_run
    /// use carrot_cake::consumers::{ConsumerGroupConfigurationBuilder, TelemetryMiddleware};
    ///
    /// # type Ctx = ();
    /// # type Error = ();
    /// # let consumer_group_builder: ConsumerGroupConfigurationBuilder<Ctx, Error> = unimplemented!();
    /// let middleware: std::sync::Arc<dyn TelemetryMiddleware<Ctx, Error>>; // some dynamic pointer to middleware
    /// # middleware = unimplemented!();
    ///
    /// consumer_group_builder.with_dyn_telemetry_middleware(middleware);
    /// ```
    #[must_use]
    pub fn with_dyn_telemetry_middleware(
        mut self,
        middleware: Arc<dyn TelemetryMiddleware<Context, Error>>,
    ) -> Self {
        self.0.telemetry_middleware_chain.push(middleware);
        self
    }

    /// Append multiple dynamic telemetry middlewares, see [`ConsumerGroupConfigurationBuilder::with_telemetry_middleware`].
    #[must_use]
    pub fn with_telemetry_middlewares<I>(mut self, middlewares: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn TelemetryMiddleware<Context, Error>>>,
    {
        self.0.telemetry_middleware_chain.extend(middlewares);
        self
    }

    /// By default, a [`ConsumerGroup`] keeps running indefinitely, consuming messages as soon as
    /// they are available in the queues bound by its [`MessageHandler`]s.
    ///
    /// With `exit_after` you can configure the [`MessageHandler`]s in a [`ConsumerGroup`] to
    /// stop consuming messages as soon as they have processed `max_n_messages`.
    ///
    /// This is mostly useful for testing purposes: it allows you to know, when the group has
    /// exited, that a certain number of messages have been processed and you can start performing
    /// your assertions around the side-effects produced by said processing.
    ///
    /// See [the integration test example](https://github.com/TrueLayer/carrot-cake/tree/main/carrot-cake/examples)
    /// as a reference implementation.
    ///
    /// [`ConsumerGroup`]: super::ConsumerGroup
    #[must_use]
    pub fn exit_after(mut self, max_n_messages: usize) -> Self {
        self.0.exit_after = Some(max_n_messages);
        self
    }

    /// Pre-start hooks are executed _before_ consumers start pulling messages from queues.
    /// Pre-start hooks are used to execute setup logic for resources against the message broker -
    /// e.g. create exchanges, bind queues, etc.
    ///
    /// Check out [`ConsumerPreStartHook`](crate::consumers::ConsumerPreStartHook)'s documentation
    /// for more details.
    ///
    /// By default, no pre-start logic is executed unless one or more pre-start hooks are explicitly specified.
    #[must_use]
    pub fn with_pre_start_hook<H: ConsumerPreStartHook>(mut self, hook: H) -> Self {
        self.0.pre_start_hooks.push(Arc::new(hook));
        self
    }

    /// Append multiple pre-start hooks, see
    /// [`ConsumerGroupConfigurationBuilder::with_pre_start_hook`].
    #[must_use]
    pub fn with_pre_start_hooks<I>(mut self, hooks: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn ConsumerPreStartHook>>,
    {
        self.0.pre_start_hooks.extend(hooks);
        self
    }

    /// A transient error hook allows you to customise the behaviour of the message handlers
    /// when the processing of an incoming message fails with an error classified as transient -
    /// e.g. retryable.
    ///
    /// Check out [`ConsumerTransientErrorHook`](crate::consumers::ConsumerTransientErrorHook)'s
    /// documentation for more details.
    ///
    /// By default, nothing is requeued unless a transient error hook is explicitly specified.
    #[must_use]
    pub fn transient_error_hook<H: ConsumerTransientErrorHook>(self, hook: H) -> Self {
        self.dyn_transient_error_hook(Arc::new(hook))
    }

    /// A version of [`ConsumerGroupConfigurationBuilder::transient_error_hook`] for already Arc-ed hooks.
    ///
    /// Useful for sharing `!Clone` hooks.
    ///
    /// # Example
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use carrot_cake::consumers::{ConsumerGroupConfigurationBuilder, ConsumerTransientErrorHook};
    /// # let consumer_group_builder: ConsumerGroupConfigurationBuilder<(), ()> = unimplemented!();
    /// # let other_consumer_group_builder: ConsumerGroupConfigurationBuilder<(), ()> = unimplemented!();
    /// let hook: Arc<dyn ConsumerTransientErrorHook>; // some dynamic shared error hook
    /// # hook = unimplemented!();
    ///
    /// consumer_group_builder.dyn_transient_error_hook(Arc::clone(&hook));
    /// other_consumer_group_builder.dyn_transient_error_hook(hook);
    /// ```
    #[must_use]
    pub fn dyn_transient_error_hook(mut self, hook: Arc<dyn ConsumerTransientErrorHook>) -> Self {
        self.0.transient_error_hook = hook;
        self
    }

    /// Once you have specified all the group-level configuration you need,
    /// you can start adding [`MessageHandler`]s!
    ///
    /// Check out [`MessageHandler::builder`] to build out a handler.
    ///
    /// # Implementation Notes
    ///
    /// After you start adding message handlers you are prevented from introducing new group-level
    /// configuration.
    /// This is enforced by returning a different builder type, [`ConsumerGroupBuilder`], which
    /// only exposes methods to add other handlers and build the whole group.
    ///
    /// This limitation is in place to avoid confusing corner cases: what happens if you add
    /// a group-level middleware AFTER you have registered a message handler?
    /// Does that middleware get executed for the handler you registered? Does it not?
    ///
    /// A phased builder prevents these oddities entirely.
    pub fn message_handler(
        self,
        message_handler: MessageHandler<Context, Error>,
    ) -> ConsumerGroupBuilder<Context, Error> {
        let group_configuration = self.0;
        ConsumerGroupBuilder {
            group_configuration,
            message_handlers: vec![message_handler],
        }
    }
}
