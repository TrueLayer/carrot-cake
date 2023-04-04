//! Provides pooling for a [`lapin::Channel`] and [`lapin::Connection`] using [`deadpool`].
//!
//! This library provides two key guarantees:
//! - Disposing of broken channels and recreating new ones on-demand.
//! - Reusing connection objects across channels to limit overhead.
//!
//! ```rust
//! use carrot_cake::pool::{ChannelManager, ChannelPool, ConnectionPool};
//! use carrot_cake::amqp::ConnectionFactory;
//! use carrot_cake::amqp::configuration::RabbitMqSettings;
//!
//! // Function for asyncness.
//! async fn example() -> anyhow::Result<()> {
//!     // initialize rabbitmq connection details and config.
//!     let settings = RabbitMqSettings::default();
//!
//!     // determine the maximum underlying connections.
//!     let max_connections = 16;
//!
//!     let connection_pool = ConnectionPool::builder(ConnectionFactory::new_from_config(&settings)?)
//!             .max_size(max_connections)
//!             .build()?;
//!
//!     // create a `PooledChannel` from the `ChannelManager`.
//!     let pool = ChannelPool::builder(ChannelManager::new(connection_pool))
//!         .max_size(16)
//!         .build()?;
//!
//!     // get a new Channel from the pool.
//!     let channel = pool.get().await?;
//!     Ok(())
//! }
//! ```

mod channel;
mod connection;
mod error;

pub use channel::{ChannelManager, ChannelPool};
pub use connection::ConnectionPool;
pub use error::Error;
