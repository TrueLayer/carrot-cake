//! Implements [`TransportFactory`] that wraps [`ConnectionPool`] and [`ChannelPool`][crate::channel::ChannelPool]
//! that are used for talking to a RabbitMq broker.

use carrot_cake_amqp::{
    rabbit_mq::{
        Channel, ConnectionFactory, WITHOUT_PUBLISHER_CONFIRMATION, WITH_PUBLISHER_CONFIRMATION,
    },
    transport::TransportFactory,
};
use deadpool::managed::Object;

use crate::connection::{ConnectionManager, ConnectionPool, MAX_CONNECTION_POOL_SIZE};

/// Primitives for building a [`TransportPool`].
pub struct TransportPoolBuilder {
    max_n_connections: usize,
    connection_factory: ConnectionFactory,
}

impl TransportPoolBuilder {
    /// Create a new `PoolBuilder` using a `ConnectionFactory`.
    fn new(connection_factory: ConnectionFactory) -> Self {
        Self {
            connection_factory,
            max_n_connections: MAX_CONNECTION_POOL_SIZE,
        }
    }

    /// Change the roof for how many connections will be pooled internally.
    ///
    /// Default: 10.
    pub fn max_connections(self, max_n_connections: usize) -> Self {
        Self {
            max_n_connections,
            ..self
        }
    }

    /// Finalize the builder to create a [`TransportPool`].
    pub async fn build(self) -> Result<TransportPool, anyhow::Error> {
        let connection_pool = ConnectionManager::new(self.connection_factory)
            .max_connections(self.max_n_connections)
            .into_pool();

        Ok(TransportPool { connection_pool })
    }
}

/// [`TransportPool`] encapsulates all transport primitives to RabbitMq by implementing [`TransportFactory`].
///
///  The hierarchy is [`Connection`][carrot_cake_amqp::rabbit_mq::Connection] -> [`Channel`]: channels are multiplexed through connections.
///  Therefore internally getting a new [`Channel`] will always try to reuse an underlying
///  connection as much as possible.
#[derive(Clone)]
pub struct TransportPool {
    connection_pool: ConnectionPool,
}

impl TransportPool {
    /// Create a [`TransportPool`] using a [`ConnectionFactory`] by also providing the maximum number
    /// of channels and connections that can be pooled.
    ///
    /// # Note
    ///   [`TransportPool`] only keeps a connection pools internally and will always create
    ///   new channels but will reuse the same connection if feasible.
    ///
    pub fn builder(connection_factory: ConnectionFactory) -> TransportPoolBuilder {
        TransportPoolBuilder::new(connection_factory)
    }
}

impl AsRef<ConnectionPool> for TransportPool {
    fn as_ref(&self) -> &ConnectionPool {
        &self.connection_pool
    }
}

#[async_trait::async_trait]
impl TransportFactory for TransportPool {
    /// For applications that use multiple threads/processes for processing, it is
    /// very common to open a new channel per thread/process and not share
    /// channels between them.
    /// See [rabbitmq channel docs](https://www.rabbitmq.com/channels.html).
    type Connection = Object<ConnectionManager>;
    type ChannelWithConfirmation = Box<Channel<WITH_PUBLISHER_CONFIRMATION>>;
    type ChannelWithoutConfirmation = Box<Channel<WITHOUT_PUBLISHER_CONFIRMATION>>;

    async fn get_connection(&self) -> Result<Self::Connection, anyhow::Error> {
        Ok(self.connection_pool.get().await?)
    }

    async fn get_channel_with_confirmation(
        &self,
    ) -> Result<Self::ChannelWithConfirmation, anyhow::Error> {
        Ok(Box::new(
            self.connection_pool.get().await?.create_channel().await?,
        ))
    }

    async fn get_channel_without_confirmation(
        &self,
    ) -> Result<Self::ChannelWithoutConfirmation, anyhow::Error> {
        Ok(Box::new(
            self.connection_pool.get().await?.create_channel().await?,
        ))
    }
}
