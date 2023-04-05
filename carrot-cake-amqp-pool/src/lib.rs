//! `carrot-cake-amqp-pool` provides channel pooling for a [Channel][carrot_cake_amqp::rabbit_mq::Channel].
//!
//! This library provides two key guarantees:
//! - Disposing of broken channels and recreating new ones on-demand.
//! - Reusing connection objects across channels to limit overhead.
//!
//! ```rust
//! use carrot_cake_amqp_pool::{ChannelManager, ChannelPool, ConnectionManager};
//! use carrot_cake_amqp::rabbit_mq::{ConnectionFactory, WITH_PUBLISHER_CONFIRMATION};
//! use carrot_cake_amqp::configuration::RabbitMqSettings;
//!
//! // Function for asyncness.
//! async fn example() -> anyhow::Result<()> {
//!     // initialize rabbitmq connection details and config.
//!     let settings = RabbitMqSettings::default();
//!
//!     // determine the maximum underlying connections.
//!     let max_connections = 16;
//!
//!     let connection_pool =
//!         ConnectionManager::new(ConnectionFactory::new_from_config(&settings)?)
//!             .max_connections(max_connections)
//!             .into_pool();
//!
//!     let cm = ChannelManager::new(connection_pool);
//!
//!     // determine the max poolsize.
//!     let pool_size = 16;
//!
//!     // create a `PooledChannel` from the `ChannelManager`.
//!     let pool = ChannelPool::<WITH_PUBLISHER_CONFIRMATION>::builder(cm)
//!         .max_size(pool_size)
//!         .build()?;
//!
//!     // get a new Channel from the pool.
//!     let channel = pool.get().await?;
//!     Ok(())
//! }
//!
//! ```

mod channel;
mod connection;
mod error;
mod pool;

pub use channel::{ChannelManager, ChannelPool};
pub use connection::{ConnectionManager, ConnectionPool};
pub use error::Error;
pub use pool::TransportPool;
