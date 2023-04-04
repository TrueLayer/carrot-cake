use std::ops::Deref;

use crate::rabbit_mq::{
    Channel, Connection, ConnectionFactory, WITHOUT_PUBLISHER_CONFIRMATION,
    WITH_PUBLISHER_CONFIRMATION,
};

/// `TransportFactory` defines an interface for types that are capable of creating
/// all the transport primitives used to communicate with RabbitMQ - connections and channels.
///
/// The associate types returned by this trait are all required to implement [`Deref`] into
/// the given type.
/// This gives more flexibility and allows more types to implement this trait.
/// E.g, for connections pools that contain a reference to a [`Connection`] object,
/// using the trait will allow you to either borrow the [`Connection`] or clone it
/// to get ownership of it.
///
#[async_trait::async_trait]
pub trait TransportFactory {
    /// Type that derefs to a [`Connection`].
    type Connection: Deref<Target = Connection>;

    /// Type that derefs to a [`Channel`] where the publisher confirms.
    type ChannelWithConfirmation: Deref<Target = Channel<WITH_PUBLISHER_CONFIRMATION>>;

    /// Type that derefs to a [`Channel`] where the publisher does NOT confirm.
    type ChannelWithoutConfirmation: Deref<Target = Channel<WITHOUT_PUBLISHER_CONFIRMATION>>;

    /// Get a [`Self::Connection`].
    ///
    /// Fails if connections fails to establish.
    ///
    /// # Note
    ///
    /// Whether a new connection is established or just an existing
    /// one is returned will be up to the specific implementation.
    async fn get_connection(&self) -> Result<Self::Connection, anyhow::Error>;

    /// Get a [`Self::ChannelWithConfirmation`] with publisher confirms enabled.
    ///
    /// Fails if either the connections fails to establish
    /// or the channel.
    ///
    /// # Note
    ///
    /// Whether a new [`Self::ChannelWithConfirmation`] is created or just an existing
    /// one is returned (e.g. borrowed from a pool) will be up to the specific implementation.
    async fn get_channel_with_confirmation(
        &self,
    ) -> Result<Self::ChannelWithConfirmation, anyhow::Error>;

    /// Get a [`Self::ChannelWithoutConfirmation`] with publisher confirms DISABLED.
    ///
    /// Fails if either the connections fails to establish
    /// or the channel.
    ///
    /// # Note
    ///
    /// Whether a new [`Self::ChannelWithoutConfirmation`] is established or just an existing
    /// one is returned (e.g. borrowed from a pool) will be up to the specific implementation.
    async fn get_channel_without_confirmation(
        &self,
    ) -> Result<Self::ChannelWithoutConfirmation, anyhow::Error>;
}

#[repr(transparent)]
pub struct Owned<T>(T);

impl<T> Deref for Owned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

#[async_trait::async_trait]
impl TransportFactory for ConnectionFactory {
    type Connection = Owned<Connection>;
    type ChannelWithConfirmation = Owned<Channel<WITH_PUBLISHER_CONFIRMATION>>;
    type ChannelWithoutConfirmation = Owned<Channel<WITHOUT_PUBLISHER_CONFIRMATION>>;

    async fn get_connection(&self) -> Result<Self::Connection, anyhow::Error> {
        self.new_connection().await.map(Owned)
    }

    async fn get_channel_with_confirmation(
        &self,
    ) -> Result<Self::ChannelWithConfirmation, anyhow::Error> {
        self.new_connection()
            .await?
            .create_channel()
            .await
            .map(Owned)
            .map_err(anyhow::Error::from)
    }

    async fn get_channel_without_confirmation(
        &self,
    ) -> Result<Self::ChannelWithoutConfirmation, anyhow::Error> {
        self.new_connection()
            .await?
            .create_channel()
            .await
            .map(Owned)
            .map_err(anyhow::Error::from)
    }
}
