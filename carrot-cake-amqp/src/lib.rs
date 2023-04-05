//! `carrot-cake-amqp` is a wrapper on top of [`lapin`], our AMQP client.
//!
//! `carrot-cake-amqp` provides:
//! - (strongly-typed) abstractions to work with channels and connections (see the [`rabbit_mq`] module);
//! - convenient methods to manipulate and (de)serialize headers (see the [`amqp`] module);
//! - [`RabbitMqSettings`], to hold the required parameters to connect to a RabbitMq broker;
//! - re-exports of AMQP types from [`amq-protocol-types`] and [`lapin`] (see the [`amqp`] module).
//!
//! [`lapin`]: https://docs.rs/crate/lapin
//! [`RabbitMqSettings`]: configuration::RabbitMqSettings
//! [`amq-protocol-types`]: https://docs.rs/crate/amq-protocol-types

pub mod amqp;
pub mod configuration;
mod convenience;
pub mod rabbit_mq;
pub mod transport;

pub use convenience::*;
