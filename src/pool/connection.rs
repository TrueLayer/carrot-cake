//! Implements [`Manager`] for [`Connection`].

use crate::amqp::ConnectionFactory;
use deadpool::managed::{self, Manager};
use lapin::{Connection, ConnectionState};

/// `ConnectionPool` is a connection pool for a `lapin::Channel`.
pub type ConnectionPool = deadpool::managed::Pool<ConnectionFactory>;

#[async_trait::async_trait]
impl Manager for ConnectionFactory {
    type Type = Connection;
    type Error = super::Error;

    async fn create(&self) -> Result<Connection, super::Error> {
        Ok(self.new_connection().await?)
    }

    async fn recycle(&self, obj: &mut Connection) -> managed::RecycleResult<super::Error> {
        match obj.status().state() {
            ConnectionState::Connected => Ok(()),
            state => Err(managed::RecycleError::Message(format!(
                "Connection is not in an healthy state {state:?}",
            ))),
        }
    }
}
