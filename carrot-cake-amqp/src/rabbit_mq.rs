//! This modules provides abstractions on top of [`lapin`]'s [`Channel`] and [`Connection`].
//!
//! [`lapin`]: https://docs.rs/crate/lapin
//! [`Channel`]: https://docs.rs/lapin/1.6.6/lapin/struct.Channel.html
//! [`Connection`]: https://docs.rs/lapin/1.6.6/lapin/struct.Connection.html

use crate::configuration::RabbitMqSettings;
use anyhow::Context;
use lapin::{
    message::BasicReturnMessage,
    options::{
        BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    publisher_confirm::Confirmation,
    tcp::{AMQPUriTcpExt, NativeTlsConnector},
    types::FieldTable,
    uri::{AMQPScheme, AMQPUri},
    BasicProperties, ConnectionProperties, ExchangeKind,
};
use std::sync::Arc;
use tracing::warn;
use tracing_error::SpanTrace;

pub const WITH_PUBLISHER_CONFIRMATION: bool = true;
pub const WITHOUT_PUBLISHER_CONFIRMATION: bool = false;

#[derive(Clone)]
/// All the information required to connect to a RabbitMq broker.
pub struct ConnectionFactory {
    uri: AMQPUri,
    /// The timeout observed when trying to connect to RabbitMq.
    connection_timeout: std::time::Duration,
    /// TLS configuration for the connection to RabbitMq.
    /// If `None`, the connection will not be encrypted.
    tls: Option<Arc<Tls>>,
}

#[derive(Clone)]
struct Tls {
    connector: NativeTlsConnector,
    domain_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Unhealthy,
}

/// A connection to a RabbitMq broker.
///
/// Connections should re-used across multiple actions given the initial setup cost.
pub struct Connection(lapin::Connection);

/// A RabbitMq channel, parametrised via a type to distinguish its confirmation settings.
pub struct Channel<const CONFIRMATION: bool>(lapin::Channel);

impl<const CONFIRMATION: bool> Clone for Channel<{ CONFIRMATION }> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl ConnectionFactory {
    /// Create a new connection factory from settings.
    ///
    /// It allows you to customize the TLS configuration.
    ///
    /// A connection timeout can be (optionally) specified in `settings`.
    /// If the connection timeout is left unspecified, it will be defaulted to 10 seconds.
    pub fn new_from_config(settings: &RabbitMqSettings) -> Result<Self, anyhow::Error> {
        let tls = settings
            .tls
            .as_ref()
            .map::<Result<Tls, anyhow::Error>, _>(|tls_settings| {
                let server_domain_name = tls_settings
                    .domain
                    .clone()
                    .unwrap_or_else(|| settings.amqp_uri().authority.host);
                let root_certificate = tls_settings
                    .ca_certificate_chain()
                    .with_context(|| "Failed to parse CA certificate for RabbitMq TLS.")?;
                let connector = NativeTlsConnector::builder()
                    .add_root_certificate(root_certificate)
                    .build()
                    .expect("TLS configuration failed");
                Ok(Tls {
                    domain_name: server_domain_name,
                    connector,
                })
            })
            .transpose()?;
        let connection_timeout = settings
            .connection_timeout()
            .unwrap_or_else(|| std::time::Duration::from_secs(10));
        Ok(Self {
            uri: settings.amqp_uri(),
            connection_timeout,
            tls: tls.map(Arc::new),
        })
    }

    /// Replaces the TLS Connector for the connection factory
    pub fn set_tls_connector(&mut self, connector: NativeTlsConnector) {
        self.set_tls_connector_with_domain(connector, self.uri.authority.host.clone());
    }

    /// Replaces the TLS Connector for the connection factory, along with the expected domain name for the certificate
    pub fn set_tls_connector_with_domain(
        &mut self,
        connector: NativeTlsConnector,
        domain_name: String,
    ) {
        self.tls = Some(Arc::new(Tls {
            connector,
            domain_name,
        }));
    }

    /// Create a new connection to a RabbitMq broker.
    ///
    /// It establishes an encrypted connection if `self.tls` is `Some`.
    /// It establishes an unencrypted connection if `self.tls` is `None`.
    #[tracing::instrument(name = "rabbitmq_connect", skip(self))]
    pub async fn new_connection(&self) -> Result<Connection, anyhow::Error> {
        let properties =
            ConnectionProperties::default().with_executor(tokio_executor_trait::Tokio::current());
        let connection = match &self.tls {
            None => self.connect_without_tls(properties).await,
            Some(tls) => self.connect_with_tls(properties, Arc::clone(tls)).await,
        }?;
        // Register a callback to log connection errors.
        connection.on_error(|e| {
            warn!("RabbitMQ broken connection: {:?}", e);
        });
        Ok(Connection(connection))
    }

    /// Establish a new unencrypted connection to a RabbitMq broker.
    async fn connect_without_tls(
        &self,
        properties: ConnectionProperties,
    ) -> Result<lapin::Connection, anyhow::Error> {
        match tokio::time::timeout(
            self.connection_timeout,
            lapin::Connection::connect_uri(self.uri.clone(), properties),
        )
        .await
        {
            Ok(result) => result.with_context(|| "Failed to connect to RabbitMQ."),
            Err(_) => Err(anyhow::anyhow!(
                "Timed out while trying to connect to RabbitMQ."
            )),
        }
    }

    /// Establish a new TLS connection to a RabbitMq broker.
    async fn connect_with_tls(
        &self,
        properties: ConnectionProperties,
        tls_configuration: Arc<Tls>,
    ) -> Result<lapin::Connection, anyhow::Error> {
        match tokio::time::timeout(
            self.connection_timeout,
            lapin::Connection::connector(
                self.uri.clone(),
                Box::new(move |uri| {
                    // First establish a plain TCP connection using the AMQP protocol
                    let mut amqp_uri = uri.clone();
                    amqp_uri.scheme = AMQPScheme::AMQP;
                    amqp_uri
                        .connect()
                        // Then perform a TLS handshake with custom settings
                        // including customisation of the expected domain for the server certificate
                        .and_then(|tcp| {
                            tcp.into_native_tls(
                                &tls_configuration.connector,
                                &tls_configuration.domain_name,
                            )
                        })
                }),
                properties,
            ),
        )
        .await
        {
            Ok(result) => {
                result.with_context(|| "Failed to establish a TLS connection to RabbitMQ.")
            }
            Err(_) => Err(anyhow::anyhow!(
                "Timed out while trying to establish a TLS connection to RabbitMQ."
            )),
        }
    }
}

impl Connection {
    #[tracing::instrument(name = "rabbitmq_create_channel", skip(self))]
    pub async fn create_channel<const CONFIRMATION: bool>(
        &self,
    ) -> Result<Channel<CONFIRMATION>, lapin::Error> {
        let channel = self.0.create_channel().await?;

        if CONFIRMATION {
            // Enable publish confirms on the channel
            // See https://www.rabbitmq.com/amqp-0-9-1-reference.html#confirm.select.nowait
            channel
                .confirm_select(ConfirmSelectOptions { nowait: false })
                .await?;
        }

        Ok(Channel(channel))
    }

    pub fn status(&self) -> HealthStatus {
        if self.0.status().connected() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy
        }
    }

    pub fn raw(self) -> lapin::Connection {
        self.0
    }
}

impl AsRef<lapin::Connection> for Connection {
    fn as_ref(&self) -> &lapin::Connection {
        &self.0
    }
}

/// Error returned when trying to publish a message via RabbitMq.
#[derive(thiserror::Error, Debug)]
pub enum RabbitMqPublishError {
    #[error("Generic error encountered when interacting with the RabbitMq broker.\n{1}")]
    GenericError(#[source] lapin::Error, SpanTrace),
    #[error("The message could not be routed: {0:?}\n{1}")]
    UnroutableMessage(Box<BasicReturnMessage>, SpanTrace),
    #[error("The RabbitMq broker nacked the publishing of the message: {0:?}\n{1}")]
    NegativeAck(Option<Box<BasicReturnMessage>>, SpanTrace),
}

/// Shared methods whose behaviour does not depend on having publisher confirms enabled or disabled.
impl<const CONFIRMATION: bool> Channel<{ CONFIRMATION }> {
    /// Get access to the underlying raw channel
    pub fn raw(&self) -> &lapin::Channel {
        &self.0
    }

    /// Create a RabbitMq exchange.
    #[tracing::instrument(name = "rabbit_create_exchange", skip(self))]
    pub async fn create_exchange(
        &self,
        exchange: &str,
        exchange_kind: ExchangeKind,
        options: ExchangeDeclareOptions,
    ) -> Result<(), lapin::Error> {
        self.0
            .exchange_declare(exchange, exchange_kind, options, FieldTable::default())
            .await
    }

    /// Create a durable RabbitMq direct exchange.
    #[tracing::instrument(name = "rabbitmq_create_durable_exchange", skip(self))]
    pub async fn create_durable_exchange(&self, exchange: &str) -> Result<(), lapin::Error> {
        let options = ExchangeDeclareOptions {
            passive: false,
            // The exchange will survive RabbitMq server restarts
            durable: true,
            auto_delete: false,
            internal: false,
            nowait: false,
        };
        self.0
            .exchange_declare(
                exchange,
                ExchangeKind::Direct,
                options,
                FieldTable::default(),
            )
            .await
    }

    /// Create a durable RabbitMq queue.
    pub async fn create_durable_queue(&self, queue: &str) -> Result<(), lapin::Error> {
        self.create_durable_queue_with_args(queue, FieldTable::default())
            .await?;

        Ok(())
    }

    /// Create a durable RabbitMq queue.
    #[tracing::instrument(name = "rabbitmq_create_durable_queue", skip(self))]
    pub async fn create_durable_queue_with_args(
        &self,
        queue: &str,
        arguments: FieldTable,
    ) -> Result<(), lapin::Error> {
        let options = QueueDeclareOptions {
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: false,
            nowait: false,
        };
        self.0.queue_declare(queue, options, arguments).await?;
        Ok(())
    }

    /// Declare a RabbitMq queue.
    #[tracing::instrument(name = "rabbitmq_declare_queue", skip(self))]
    pub async fn declare_queue(
        &self,
        queue: &str,
        options: &QueueOptions,
    ) -> Result<(), lapin::Error> {
        let options = QueueDeclareOptions {
            passive: false,
            durable: options.durability == Durability::Durable,
            exclusive: options.access == Access::Exclusive,
            auto_delete: false,
            nowait: false,
        };
        self.0
            .queue_declare(queue, options, FieldTable::default())
            .await?;
        Ok(())
    }

    /// Bind a queue to an exchange.
    #[tracing::instrument(name = "rabbitmq_bind_queue", skip(self))]
    pub async fn bind_queue(
        &self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
    ) -> Result<(), lapin::Error> {
        let options = QueueBindOptions { nowait: false };
        self.0
            .queue_bind(queue, exchange, routing_key, options, FieldTable::default())
            .await?;
        Ok(())
    }

    /// Unbind a queue from an exchange.
    #[tracing::instrument(name = "rabbitmq_unbind_queue", skip(self))]
    pub async fn unbind_queue(
        &self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
    ) -> Result<(), lapin::Error> {
        self.0
            .queue_unbind(queue, exchange, routing_key, FieldTable::default())
            .await?;
        Ok(())
    }

    pub fn status(&self) -> HealthStatus {
        if self.0.status().connected() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy
        }
    }
}

impl<const PUBLISHER_CONFIRMATION: bool> Channel<{ PUBLISHER_CONFIRMATION }> {
    /// Publish a payload on a RabbitMq exchange, waiting for publisher confirmation from the
    /// RabbitMq broker.
    #[tracing::instrument(level = "debug", skip(self, payload))]
    pub async fn publish(
        &self,
        payload: Vec<u8>,
        exchange: &str,
        routing_key: &str,
        properties: Option<BasicProperties>,
    ) -> Result<(), RabbitMqPublishError> {
        let options = BasicPublishOptions {
            // This flag tells the server how to react if the message cannot be routed to a queue.
            // If this flag is `true`, the server will return an unroutable message with a Return method.
            // If this flag is `false`, the server silently drops the message.
            mandatory: PUBLISHER_CONFIRMATION,
            // The immediate flag was dropped in RabbitMQ 3.0 - see https://www.rabbitmq.com/blog/2012/11/19/breaking-things-with-rabbitmq-3-0/
            // Setting `true` will cause a not-supported error
            immediate: false,
        };
        // Delivery mode: Non-persistent (1) or persistent (2).
        let properties = properties.unwrap_or_default().with_delivery_mode(2);
        if PUBLISHER_CONFIRMATION {
            let confirm = self
                .0
                .basic_publish(exchange, routing_key, options, &payload, properties)
                .await
                .map_err(|e| RabbitMqPublishError::GenericError(e, SpanTrace::capture()))?
                .await
                .map_err(|e| RabbitMqPublishError::GenericError(e, SpanTrace::capture()))?;

            match confirm {
                Confirmation::Ack(ack) => {
                    if let Some(return_message) = ack {
                        // Reply Code 312 - NO_ROUTE
                        // See https://www.rabbitmq.com/amqp-0-9-1-reference.html
                        if return_message.reply_code == 312 {
                            return Err(RabbitMqPublishError::UnroutableMessage(
                                return_message,
                                SpanTrace::capture(),
                            ));
                        }
                    }
                    Ok(())
                }
                Confirmation::Nack(nack) => Err(RabbitMqPublishError::NegativeAck(
                    nack,
                    SpanTrace::capture(),
                )),
                Confirmation::NotRequested => {
                    unreachable!("ChannelWithConfirmation requires ack/nack on publish.")
                }
            }
        } else {
            let _ = self
                .0
                .basic_publish(exchange, routing_key, options, &payload, properties)
                .await
                .map_err(|e| RabbitMqPublishError::GenericError(e, SpanTrace::capture()))?
                .await
                .map_err(|e| RabbitMqPublishError::GenericError(e, SpanTrace::capture()))?;
            Ok(())
        }
    }
}

impl Channel<WITHOUT_PUBLISHER_CONFIRMATION> {
    /// Enable publisher confirmation on the underlying AMQP channel.
    #[tracing::instrument(skip(self))]
    pub async fn enable_publisher_confirmation(
        self,
    ) -> Result<Channel<WITH_PUBLISHER_CONFIRMATION>, lapin::Error> {
        // Enable publish confirms on the channel
        // See https://www.rabbitmq.com/amqp-0-9-1-reference.html#confirm.select.nowait
        self.0
            .confirm_select(ConfirmSelectOptions { nowait: false })
            .await?;
        Ok(Channel(self.0))
    }
}

/// Retrieve the current length of a queue.
///
/// # Implementation details
///
/// The most convenient way to get the length of a queue in AMQP is... re-declaring it.
/// We use `passive=true` to avoid settings conflict.
#[tracing::instrument(name = "get_current_queue_length", skip(channel))]
pub async fn get_current_queue_length<const CONFIRMATION: bool>(
    channel: &Channel<{ CONFIRMATION }>,
    queue_name: &str,
) -> Result<u32, anyhow::Error> {
    let queue_declare_options = QueueDeclareOptions {
        passive: true,
        ..QueueDeclareOptions::default()
    };
    let queue = channel
        .0
        .queue_declare(queue_name, queue_declare_options, FieldTable::default())
        .await?;
    Ok(queue.message_count())
}

#[derive(Clone, PartialEq, Eq, Debug)]
/// Configuration options when declaring a new queue.
pub struct QueueOptions {
    /// Will the queue survive a broker restart?
    pub durability: Durability,
    pub access: Access,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            durability: Durability::Durable,
            access: Access::Shared,
        }
    }
}

/// Will the queue survive a broker restart?
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Durability {
    /// The queue survives a broker restart.
    ///
    /// Metadata of a durable queue is stored on disk.
    ///
    /// Durable queues will be recovered on node boot, including messages in them published as
    /// persistent. Messages published as transient will be discarded during recovery,
    /// even if they were stored in durable queues.
    Durable,
    /// Transient queues are deleted on node boot. They therefore will not survive a node
    /// restart, by design. Messages in transient queues will also be discarded.
    ///
    /// Metadata of a transient queue is stored in memory when possible.
    Transient,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Access {
    /// The queue is used by only one connection and it will be deleted when that connection closes.
    Exclusive,
    /// The queue can be used by multiple connections concurrently.
    Shared,
}

#[cfg(test)]
mod tests {
    use crate::{
        configuration::RabbitMqSettings,
        rabbit_mq::{
            get_current_queue_length, Connection, ConnectionFactory, RabbitMqPublishError,
            WITHOUT_PUBLISHER_CONFIRMATION, WITH_PUBLISHER_CONFIRMATION,
        },
    };
    use config::{Config, ConfigError, Environment, File};
    use lapin::{options::ExchangeDeclareOptions, ExchangeKind};
    use serde::Deserialize;
    use serde_json::json;
    use std::time::Duration;
    use std::{env, path::PathBuf};
    use uuid::Uuid;

    #[derive(Debug, Deserialize, Clone)]
    pub struct TestSettings {
        pub rabbit_mq: RabbitMqSettings,
    }

    impl TestSettings {
        pub fn new(base_path: PathBuf, environment: Option<String>) -> Result<Self, ConfigError> {
            let s = Config::builder();

            // Start off by merging in the "default" configuration file
            let path = base_path.join("configuration/base.yml");
            let s = s.add_source(File::from(path));

            // Detect the running environment.
            // Use the environment specified as parameter, if available.
            // Fallback to the APP_ENVIRONMENT env variable, if missing.
            // Use "test" as a default, if everything else fails.
            let environment = environment
                .unwrap_or_else(|| env::var("APP_ENVIRONMENT").unwrap_or_else(|_| "test".into()));

            // Add in environment-specific settings (optional)
            let path = base_path.join(format!("configuration/{}.yml", environment));
            let s = s.add_source(File::from(path).required(false));

            // Add in settings from environment variables (with a prefix of APP and '__' as separator)
            // Eg.. `APP_APPLICATION__PORT=5001 would set `Settings.application.port`
            let s = s.add_source(Environment::with_prefix("app").separator("__"));

            // Deserialize (and thus freeze) the entire configuration as
            s.build()?.try_deserialize()
        }
    }

    #[tokio::test]
    async fn connect() {
        // Arrange
        let connection_factory = get_rabbitmq_factory();

        // Act
        let connection = connection_factory.new_connection().await;

        // Assert
        assert!(connection.is_ok());
    }

    #[tokio::test]
    async fn create_durable_topic_exchange_should_succeed() {
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITHOUT_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let exchange_name = Uuid::new_v4().to_string();

        let result = channel
            .create_exchange(
                &exchange_name,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    passive: false,
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                },
            )
            .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn publishing_with_confirmation_to_a_non_existing_exchange_should_fail() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITH_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let non_existent_exchange_name = Uuid::new_v4().to_string();
        let routing_key = Uuid::new_v4().to_string();

        // Act
        let result = channel
            .publish(vec![], &non_existent_exchange_name, &routing_key, None)
            .await;

        // Assert
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn publishing_without_confirmation_to_a_non_existing_exchange_should_succeed() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITHOUT_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let non_existent_exchange_name = Uuid::new_v4().to_string();
        let routing_key = Uuid::new_v4().to_string();

        // Act
        let result = channel
            .publish(vec![], &non_existent_exchange_name, &routing_key, None)
            .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn publishing_an_unroutable_message_with_confirmation_fails() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITH_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let exchange_name = Uuid::new_v4().to_string();
        let routing_key = Uuid::new_v4().to_string();
        channel
            .create_durable_exchange(&exchange_name)
            .await
            .unwrap();
        let payload = serde_json::to_vec(&json!({})).unwrap();

        // Act
        let result = channel
            .publish(payload, &exchange_name, &routing_key, None)
            .await;

        // Assert
        assert!(result.is_err());
        matches!(
            result.unwrap_err(),
            RabbitMqPublishError::UnroutableMessage(_, _)
        );
    }

    #[tokio::test]
    async fn publishing_an_unroutable_message_without_confirmation_succeeds() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITHOUT_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let exchange_name = Uuid::new_v4().to_string();
        let routing_key = Uuid::new_v4().to_string();
        channel
            .create_durable_exchange(&exchange_name)
            .await
            .unwrap();
        let payload = serde_json::to_vec(&json!({})).unwrap();

        // Act
        let result = channel
            .publish(payload, &exchange_name, &routing_key, None)
            .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn publishing_a_routable_message_with_confirmation_directly_a_queue_succeeds() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITH_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let queue_name = Uuid::new_v4().to_string();
        channel.create_durable_queue(&queue_name).await.unwrap();
        let payload = serde_json::to_vec(&json!({})).unwrap();

        // Act
        let result = channel.publish(payload, "", &queue_name, None).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(
            get_current_queue_length(&channel, &queue_name)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn publishing_a_routable_message_without_confirmation_directly_a_queue_succeeds() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITHOUT_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let queue_name = Uuid::new_v4().to_string();
        channel.create_durable_queue(&queue_name).await.unwrap();
        let payload = serde_json::to_vec(&json!({})).unwrap();

        // Act
        let result = channel.publish(payload, "", &queue_name, None).await;

        // Assert
        assert!(result.is_ok());
        tokio::time::timeout(Duration::from_secs(5), async move {
            while get_current_queue_length(&channel, &queue_name)
                .await
                .unwrap()
                == 0
            {}
        })
        .await
        .expect("Message did not appear on the queue within the expected timeout");
    }

    #[tokio::test]
    async fn publishing_a_routable_message_with_confirmation_to_an_exchange_succeeds() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITH_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let queue_name = Uuid::new_v4().to_string();
        let exchange_name = Uuid::new_v4().to_string();
        let routing_key = Uuid::new_v4().to_string();

        channel
            .create_durable_exchange(&exchange_name)
            .await
            .unwrap();
        channel.create_durable_queue(&queue_name).await.unwrap();
        channel
            .bind_queue(&queue_name, &exchange_name, &routing_key)
            .await
            .unwrap();

        let payload = serde_json::to_vec(&json!({})).unwrap();

        // Act
        let result = channel
            .publish(payload, &exchange_name, &routing_key, None)
            .await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(
            get_current_queue_length(&channel, &queue_name)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn getting_the_queue_length_of_a_non_existing_queue_returns_an_error() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITH_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let queue_name = Uuid::new_v4().to_string();

        // Act
        let result = get_current_queue_length(&channel, &queue_name).await;

        // Assert
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn publishing_a_routable_message_without_confirmation_to_an_exchange_succeeds() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITHOUT_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let queue_name = Uuid::new_v4().to_string();
        let exchange_name = Uuid::new_v4().to_string();
        let routing_key = Uuid::new_v4().to_string();

        channel
            .create_durable_exchange(&exchange_name)
            .await
            .unwrap();
        channel.create_durable_queue(&queue_name).await.unwrap();
        channel
            .bind_queue(&queue_name, &exchange_name, &routing_key)
            .await
            .unwrap();

        let payload = serde_json::to_vec(&json!({})).unwrap();

        // Act
        let result = channel
            .publish(payload, &exchange_name, &routing_key, None)
            .await;

        // Assert
        assert!(result.is_ok());
        tokio::time::timeout(Duration::from_secs(5), async move {
            while get_current_queue_length(&channel, &queue_name)
                .await
                .unwrap()
                == 0
            {}
        })
        .await
        .expect("Message did not appear on the queue within the expected timeout");
    }

    #[tokio::test]
    async fn unbinding_a_queue_from_an_exchange_to_which_it_is_not_bound_succeeds() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITH_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let non_existent_exchange_name = Uuid::new_v4().to_string();
        let queue_name = Uuid::new_v4().to_string();
        let routing_key = Uuid::new_v4().to_string();
        channel
            .create_durable_queue(&queue_name)
            .await
            .expect("Could not create queue");

        // Act
        let result = channel
            .unbind_queue(&queue_name, &non_existent_exchange_name, &routing_key)
            .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn unbinding_a_queue_from_an_exchange_to_which_it_is_bound_succeeds() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITH_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let exchange_name = Uuid::new_v4().to_string();
        let queue_name = Uuid::new_v4().to_string();
        let routing_key = Uuid::new_v4().to_string();
        channel
            .create_durable_queue(&queue_name)
            .await
            .expect("Could not create queue");
        channel
            .create_durable_exchange(&exchange_name)
            .await
            .expect("Could not create exchange");
        channel
            .bind_queue(&queue_name, &exchange_name, &routing_key)
            .await
            .expect("Could not bind exchange to queue");

        // Act
        let result = channel
            .unbind_queue(&queue_name, &exchange_name, &routing_key)
            .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn message_published_to_exchange_is_not_received_on_queues_that_have_been_unbound() {
        // Arrange
        let connection = get_rabbitmq_connection().await;
        let channel = connection
            .create_channel::<WITHOUT_PUBLISHER_CONFIRMATION>()
            .await
            .expect("Failed to create channel");
        let exchange_name = Uuid::new_v4().to_string();
        let queue_name = Uuid::new_v4().to_string();
        let routing_key = Uuid::new_v4().to_string();
        channel
            .create_durable_queue(&queue_name)
            .await
            .expect("Could not create queue");
        channel
            .create_durable_exchange(&exchange_name)
            .await
            .expect("Could not create exchange");
        channel
            .bind_queue(&queue_name, &exchange_name, &routing_key)
            .await
            .expect("Could not bind exchange to queue");
        channel
            .unbind_queue(&queue_name, &exchange_name, &routing_key)
            .await
            .expect("Could not unbind exchange from queue");

        let payload = serde_json::to_vec(&json!({})).unwrap();

        // Act
        let result = channel
            .publish(payload, &exchange_name, &routing_key, None)
            .await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(
            get_current_queue_length(&channel, &queue_name)
                .await
                .unwrap(),
            0
        );
    }

    /// Retrieve settings for our rabbitmq server.
    fn get_rabbitmq_factory() -> ConnectionFactory {
        let base_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
        // Load test settings from configuration/env variables.
        let settings =
            TestSettings::new(base_dir, Some("test".into())).expect("Failed to load configuration");

        ConnectionFactory::new_from_config(&settings.rabbit_mq).unwrap()
    }

    /// Retrieve settings for our rabbitmq server.
    async fn get_rabbitmq_connection() -> Connection {
        get_rabbitmq_factory()
            .new_connection()
            .await
            .expect("Failed to connect")
    }
}
