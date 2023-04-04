//! Helpers for connecting to a rabbitmq broker

pub mod configuration;
pub mod convenience;
mod factory;
pub use factory::ConnectionFactory;
pub use lapin::{Channel, Connection};

pub use lapin::{options, types, BasicProperties};
