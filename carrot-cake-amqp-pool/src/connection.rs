//! Implements `deadpool::managed::Manager` for
//! [Connection][carrot_cake_amqp::rabbit_mq::Connection].

use carrot_cake_amqp::rabbit_mq::{Connection, ConnectionFactory, HealthStatus};
use deadpool::managed;

/// Standard connection pool size.
pub(crate) const MAX_CONNECTION_POOL_SIZE: usize = 10;

/// `ConnectionPool` is a connection pool for a `lapin::Channel`.
pub type ConnectionPool = deadpool::managed::Pool<ConnectionManager>;

/// `ConnectionManager` implements [`deadpool::managed::Manager`] which will manage
/// the connections for [`ConnectionPool`].
pub struct ConnectionManager {
    connection_factory: ConnectionFactory,
    max_n_connections: usize,
}

impl ConnectionManager {
    /// Create a new `ConnectionManager` using a [`ConnectionFactory`].
    ///
    /// A [`ConnectionManager`] is used for creating [`ConnectionPool`].
    pub fn new(connection_factory: ConnectionFactory) -> Self {
        Self {
            connection_factory,
            max_n_connections: MAX_CONNECTION_POOL_SIZE,
        }
    }

    /// Change the maximum number of connection that will be pooled internally.
    #[must_use]
    pub fn max_connections(self, max_n_connections: usize) -> Self {
        Self {
            max_n_connections,
            ..self
        }
    }

    /// Convert into a [`ConnectionPool`].
    pub fn into_pool(self) -> ConnectionPool {
        let max_n_connections = self.max_n_connections;
        ConnectionPool::builder(self)
            .max_size(max_n_connections)
            .build()
            .expect("could not build connection pool. This is a bug with carrot-cake-amqp-pool")
    }
}

#[async_trait::async_trait]
impl deadpool::managed::Manager for ConnectionManager {
    type Type = Connection;
    type Error = crate::Error;

    async fn create(&self) -> Result<Connection, crate::Error> {
        Ok(self.connection_factory.new_connection().await?)
    }

    async fn recycle(&self, obj: &mut Connection) -> managed::RecycleResult<crate::Error> {
        match obj.status() {
            HealthStatus::Healthy => Ok(()),
            HealthStatus::Unhealthy => Err(managed::RecycleError::Message(format!(
                "Connection is not in an healthy state {:?}",
                obj.as_ref().status().state(),
            ))),
        }
    }
}
