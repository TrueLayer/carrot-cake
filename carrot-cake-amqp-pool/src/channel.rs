//! Implements [`deadpool::managed::Manager`] for
//! [Channel][carrot_cake_amqp::rabbit_mq::Channel].
use carrot_cake_amqp::rabbit_mq::{Channel, HealthStatus};
use deadpool::managed;

use crate::connection::ConnectionPool;

/// `ChannelPool` pools [`Channel`]s.
///
/// It maintains an internal pool of connections to the rabbitmq instance.
pub type ChannelPool<const PUBLISHER_CONFIRMATION: bool> =
    deadpool::managed::Pool<ChannelManager<{ PUBLISHER_CONFIRMATION }>>;

/// `ChannelManager` implements [Manager][deadpool::managed::Manager] to manage
/// a pool of [Channel][carrot_cake_amqp::rabbit_mq::Channel]s.
///
/// `ChannelManager` keeps an internal [Connection][carrot_cake_amqp::rabbit_mq::Connection] pool
///  in order to reuse connections across channels.
///
pub struct ChannelManager<const PUBLISHER_CONFIRMATION: bool> {
    connection_pool: ConnectionPool,
}

impl<const PUBLISHER_CONFIRMATION: bool> ChannelManager<{ PUBLISHER_CONFIRMATION }> {
    /// Construct `ChannelManager` for a [Channel][carrot_cake_amqp::rabbit_mq::Channel]
    ///
    /// Internally, all channels are multiplexed across `n` underlying connections.
    /// This number can be set by passing in _max_underlying_connections_,
    /// and if not set it will default to 5.
    pub fn new(connection_pool: ConnectionPool) -> Self {
        Self { connection_pool }
    }

    /// Both `WithConfirmation` and `WithoutConfirmation` are handled equally.
    fn recycle_common(
        &self,
        obj: &mut Channel<{ PUBLISHER_CONFIRMATION }>,
    ) -> managed::RecycleResult<crate::Error> {
        match obj.status() {
            HealthStatus::Healthy => Ok(()),
            HealthStatus::Unhealthy => Err(managed::RecycleError::Message(format!(
                "Channel is in an unhealthy state: {:?}",
                obj.raw().status().state(),
            ))),
        }
    }
}

/// Implements [Manager][deadpool::managed::Manager] for
/// [Channel][carrot_cake_amqp::rabbit_mq::Channel] with publisher confirms disabled.
#[async_trait::async_trait]
impl<const PUBLISHER_CONFIRMATION: bool> deadpool::managed::Manager
    for ChannelManager<PUBLISHER_CONFIRMATION>
{
    type Type = Channel<PUBLISHER_CONFIRMATION>;
    type Error = crate::Error;

    async fn create(&self) -> Result<Channel<PUBLISHER_CONFIRMATION>, crate::Error> {
        let connection = self.connection_pool.get().await?;
        Ok(connection.create_channel().await?)
    }

    async fn recycle(
        &self,
        obj: &mut Channel<PUBLISHER_CONFIRMATION>,
    ) -> managed::RecycleResult<crate::Error> {
        self.recycle_common(obj)
    }
}
