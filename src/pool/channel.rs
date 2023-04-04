//! Implements [`Manager`] for [`Channel`].
use deadpool::managed::{self, Manager};
use lapin::{options::ConfirmSelectOptions, Channel, ChannelState};

use super::connection::ConnectionPool;

/// `ChannelPool` pools [`Channel`]s.
///
/// It maintains an internal pool of connections to the rabbitmq instance.
pub type ChannelPool = deadpool::managed::Pool<ChannelManager>;

/// `ChannelManager` implements [Manager] to manage a pool of [`Channel`]s.
///
/// `ChannelManager` keeps an internal [`ConnectionPool`]
///  in order to reuse connections across channels.
pub struct ChannelManager {
    connection_pool: ConnectionPool,
    pub(crate) publisher_confirms: bool,
}

impl ChannelManager {
    /// Construct `ChannelManager` for a [`Channel`].
    ///
    /// By default, all channels will have publisher confirmations enabled,
    /// but you can opt out using [`ChannelManager::without_publisher_confirmations`]
    pub fn new(connection_pool: ConnectionPool) -> Self {
        Self {
            connection_pool,
            publisher_confirms: true,
        }
    }

    /// Disable publisher confirmations.
    ///
    /// By default, we enable publisher confirmations, but you can opt out using this flag
    pub fn without_publisher_confirmations(mut self) -> Self {
        self.publisher_confirms = false;
        self
    }
}

/// Implements [`Manager`] for [`Channel`] with publisher confirms enabled.
#[async_trait::async_trait]
impl Manager for ChannelManager {
    type Type = Channel;
    type Error = super::Error;

    async fn create(&self) -> Result<Channel, super::Error> {
        let connection = self.connection_pool.get().await?;
        let channel = connection.create_channel().await?;
        if self.publisher_confirms {
            channel
                .confirm_select(ConfirmSelectOptions { nowait: false })
                .await?;
        }
        Ok(channel)
    }

    async fn recycle(&self, obj: &mut Channel) -> managed::RecycleResult<super::Error> {
        match obj.status().state() {
            ChannelState::Connected => Ok(()),
            state => Err(managed::RecycleError::Message(format!(
                "Channel is not in an healthy state {state:?}",
            ))),
        }
    }
}
