use carrot_cake_amqp_pool::ChannelPool;
use lapin::message::BasicReturnMessage;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use task_local_extensions::Extensions;
use tracing::warn;
use uuid::Uuid;

use crate::publishers::publisher_middleware::Next;
use crate::publishers::{MessageEnvelope, PublisherMiddleware};
use crate::rabbit_mq::RabbitMqPublishError;

/// A high-level interface to publish messages.
///
/// It supports middlewares which can be used for a variety of purposes (e.g. injecting headers,
/// registering metrics, etc.).
///
/// # Fault tolerance
///
/// Internally, the publishers will auto-reconnect to RabbitMq if the connection is broken.
///
/// # How do I build a `Publisher`?
///
/// `Publisher` provides a fluent API to add configuration step-by-step, known as
/// "builder pattern" in Rust.  
/// The starting point is [`Publisher::builder`].
///
/// # Examples
///
/// Check the [`publishers` example on GitHub](https://github.com/TrueLayer/carrot-cake/tree/main/carrot-cake/examples)
/// to see `Publisher` in action.
pub struct Publisher<const PUBLISHER_CONFIRMATION: bool> {
    /// Channel pool for the underlying AMQP channel.
    /// It determines if `Publisher` waits for AMQP publishers confirms or not.
    channel_pool: ChannelPool<{ PUBLISHER_CONFIRMATION }>,
    /// Timeout on publishing.
    timeout: std::time::Duration,
    /// The middleware chain that will be executed before publishing the message.
    middleware_chain: Vec<Arc<dyn PublisherMiddleware<{ PUBLISHER_CONFIRMATION }> + 'static>>,
}

impl<const PUBLISHER_CONFIRMATION: bool> Publisher<{ PUBLISHER_CONFIRMATION }> {
    /// Start building a [`Publisher`].
    ///
    /// You will need a channel pool.
    ///
    /// # Example
    ///
    /// ```rust
    /// use carrot_cake_amqp_pool::{ChannelManager, ChannelPool, ConnectionPool, ConnectionManager};
    /// use carrot_cake::configuration::RabbitMqSettings;
    /// use carrot_cake::publishers::Publisher;
    /// use carrot_cake::rabbit_mq::{ConnectionFactory, WITH_PUBLISHER_CONFIRMATION};
    ///
    /// pub async fn get_publisher() -> Publisher<WITH_PUBLISHER_CONFIRMATION> {
    ///     let settings = RabbitMqSettings::default();
    ///     let connection_factory = ConnectionFactory::new_from_config(&settings).unwrap();
    ///     let connection_pool = ConnectionPool::builder(ConnectionManager::new(connection_factory))
    ///         .max_size(2)
    ///         .build()
    ///         .unwrap();
    ///     let channel_pool = ChannelPool::builder(ChannelManager::new(connection_pool))
    ///         .max_size(10)
    ///         .build()
    ///         .unwrap();
    ///
    ///     Publisher::builder(channel_pool)
    ///         .publish_timeout(std::time::Duration::from_secs(3))
    ///         .build()
    /// }
    /// ```
    pub fn builder(
        channel_pool: ChannelPool<{ PUBLISHER_CONFIRMATION }>,
    ) -> PublisherBuilder<{ PUBLISHER_CONFIRMATION }> {
        PublisherBuilder::new(channel_pool)
    }

    /// Publish a message to RabbitMq.
    pub async fn publish(&self, envelope: MessageEnvelope) -> Result<(), PublisherError> {
        // There must be a better way than duplicating almost everything here.
        let mut extensions = Extensions::new();

        // Inject the current timestamp and a message_id to the message envelope
        let envelope = inject_amqp_properties(envelope);

        let next = Next {
            channel_pool: self.channel_pool.clone(),
            timeout: self.timeout,
            next_middleware: self.middleware_chain.as_slice(),
        };

        next.run(envelope, &mut extensions).await
    }
}

/// Error returned when trying to publish a message using `Publisher`.
#[derive(thiserror::Error, Debug)]
pub enum PublisherError {
    #[error("Generic error encountered when interacting with the RabbitMq broker")]
    GenericError(#[source] anyhow::Error),
    #[error("A middleware failed to process the message before publishing")]
    MiddlewareError(#[source] anyhow::Error),
    #[error("The timeout threshold was reached while trying to publish the message")]
    TimeoutError,
    #[error("The message could not be routed: {0:?}")]
    UnroutableMessage(Box<BasicReturnMessage>),
    #[error("The RabbitMq broker nacked the publishing of the message: {0:?}")]
    NegativeAck(Option<Box<BasicReturnMessage>>),
}

impl From<RabbitMqPublishError> for PublisherError {
    fn from(e: RabbitMqPublishError) -> Self {
        match e {
            RabbitMqPublishError::GenericError(e, _) => Self::GenericError(e.into()),
            RabbitMqPublishError::UnroutableMessage(e, _) => Self::UnroutableMessage(e),
            RabbitMqPublishError::NegativeAck(e, _) => Self::NegativeAck(e),
        }
    }
}

/// A builder for [`Publisher`].
///
/// Use [`Publisher::builder`] as entrypoint.
pub struct PublisherBuilder<const PUBLISHER_CONFIRMATION: bool> {
    channel_pool: ChannelPool<{ PUBLISHER_CONFIRMATION }>,
    timeout: std::time::Duration,
    middleware_chain: Vec<Arc<dyn PublisherMiddleware<{ PUBLISHER_CONFIRMATION }>>>,
}

impl<const PUBLISHER_CONFIRMATION: bool> PublisherBuilder<{ PUBLISHER_CONFIRMATION }> {
    fn new(channel_pool: ChannelPool<{ PUBLISHER_CONFIRMATION }>) -> Self {
        Self {
            channel_pool,
            timeout: std::time::Duration::from_secs(3),
            middleware_chain: vec![],
        }
    }

    /// Timeout applied when attempting to publish a message.
    /// Defaults to 3 seconds if left unspecified.
    #[must_use]
    pub fn publish_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// You can add middleware to inject logic before and after publishing a message.
    /// Middlewares are executed in the order they are registered: the first registered
    /// middleware executes first on the way in and last on the way out.
    ///
    /// Check out [`PublisherMiddleware`]'s documentation for more details.
    #[must_use]
    pub fn with_middleware<M: PublisherMiddleware<{ PUBLISHER_CONFIRMATION }> + 'static>(
        self,
        middleware: M,
    ) -> Self {
        self.with_dyn_middleware(Arc::new(middleware))
    }

    /// Append dynamic middleware logic, see [`PublisherBuilder::with_middleware`].
    #[must_use]
    pub fn with_dyn_middleware(
        mut self,
        middleware: Arc<dyn PublisherMiddleware<{ PUBLISHER_CONFIRMATION }> + 'static>,
    ) -> Self {
        self.middleware_chain.push(middleware);
        self
    }

    /// Append multiple dynamic middlewares, see [`PublisherBuilder::with_middleware`].
    #[must_use]
    pub fn with_middlewares<I>(mut self, middlewares: I) -> Self
    where
        I: IntoIterator<Item = Arc<dyn PublisherMiddleware<{ PUBLISHER_CONFIRMATION }> + 'static>>,
    {
        self.middleware_chain.extend(middlewares);
        self
    }

    /// Finalise the builder and get an instance of [`Publisher`].
    pub fn build(self) -> Publisher<{ PUBLISHER_CONFIRMATION }> {
        Publisher {
            channel_pool: self.channel_pool,
            timeout: self.timeout,
            middleware_chain: self.middleware_chain,
        }
    }
}

fn inject_amqp_properties(mut envelope: MessageEnvelope) -> MessageEnvelope {
    let current_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|ct| ct.as_secs());

    let props = envelope.properties.unwrap_or_default();
    let props = if let Some(ct) = current_timestamp {
        let ts = *props.timestamp();
        props.with_timestamp(ts.unwrap_or(ct))
    } else {
        warn!("System time is before 1970");
        props
    };

    let message_id = props.message_id().clone();
    envelope.properties = Some(
        props.with_message_id(message_id.unwrap_or_else(|| Uuid::new_v4().to_string().into())),
    );

    envelope
}
