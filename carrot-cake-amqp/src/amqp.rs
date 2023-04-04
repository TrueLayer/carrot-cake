//! A module to re-export AMQP protocol types that are exposed by the public API of this crate.
//!
//! This ensures that users of this pubsub framework do not have to add `lapin` or lower-level
//! crates as direct dependencies, with all the associated waltzer of keeping dependencies in sync.
//! You should be able to bootstrap and write a consumer using only this framework as direct dependency.
use crate::amqp::protocol_types::AMQPValue;
use cookie_factory::gen_simple;
use lapin::protocol::basic::{gen_properties, parse_properties};

/// An AMQP message retrieved from a queue. Re-exported from `lapin`.
pub use lapin::message::Delivery;
/// The set of AMQP headers associated with a message. Re-exported from `lapin`.
pub use lapin::protocol::basic::AMQPProperties;
/// The error type returned when working with `lapin` types and functions.
pub use lapin::Error;

// Re-export protocol types from `amq-protocol-types`.
pub use lapin::types as protocol_types;

/// Set the value for a header inside a collection of AMQP properties.
///
/// # Implementation notes
///
/// The current version is wasteful - we are cloning all the headers, but `lapin` does not allow us
/// to do any better. We consume the `properties` input to make sure the caller does not re-use
/// it under the impression that it has been mutated to add the new header.
///
/// We should submit a PR upstream to get mutable access to headers from AMQPProperties.
///
/// # Example
///
/// ```rust
/// use carrot_cake_amqp::amqp::protocol_types::{AMQPValue, ShortString};
/// use carrot_cake_amqp::amqp::{AMQPProperties, set_header};
///
/// // Empty set of headers
/// let properties = AMQPProperties::default();
///
/// let header_name = "MyHeaderName";
/// // AMQP has various string types - see https://www.rabbitmq.com/amqp-0-9-1-reference.html#domains
/// let header_value = AMQPValue::LongString(header_name.into());
///
/// // `set_header` consumes `properties` and returns the updated collection
/// let properties = set_header(properties, header_name, header_value.clone());
///
/// // Assert
/// let headers = properties.headers().as_ref().unwrap().inner();
/// let header_name: ShortString = header_name.into();
/// assert_eq!(&header_value, headers.get(&header_name).unwrap());
/// ```
pub fn set_header(
    properties: AMQPProperties,
    header_name: &str,
    header_value: AMQPValue,
) -> AMQPProperties {
    let mut headers = properties
        .headers()
        .as_ref()
        .map(|h| h.to_owned())
        .unwrap_or_default();
    headers.insert(header_name.into(), header_value);
    properties.with_headers(headers)
}

// Lapin uses a weird serialisation mechanism based on a library called `cookie_factory`.
// We are exposing here a couple of handy functions to perform the tasks we need without having
// to spread this in multiple places across our project.

/// Serialize AMQP properties to a bytes buffer.
pub fn serialize_properties(properties: &AMQPProperties) -> Result<Vec<u8>, anyhow::Error> {
    gen_simple(gen_properties(properties), Vec::new()).map_err(Into::into)
}

/// Deserialize AMQP properties from a bytes buffer.
pub fn deserialize_properties(bytes: &[u8]) -> Result<AMQPProperties, anyhow::Error> {
    parse_properties(bytes).map_err(Into::into).map(|x| x.1)
}

#[cfg(test)]
mod tests {
    use crate::amqp::{
        deserialize_properties, protocol_types::ShortString, serialize_properties, set_header,
    };
    use fake::{Fake, Faker};
    use lapin::{protocol::basic::AMQPProperties, types::AMQPValue, BasicProperties};

    #[test]
    fn set_works() {
        // Arrange
        let header_name: String = Faker.fake();
        let header_value = AMQPValue::LongString(Faker.fake::<String>().into());
        let properties = AMQPProperties::default();

        // Act
        let properties = set_header(properties, &header_name, header_value.clone());

        // Assert
        let headers = properties.headers().as_ref().unwrap().inner();
        let header_name: ShortString = header_name.into();
        assert_eq!(&header_value, headers.get(&header_name).unwrap());
    }

    #[test]
    fn properties_serialisation_works() {
        // TODO: randomise the properties/fuzzying
        let properties = BasicProperties::default();
        let serialized = serialize_properties(&properties).expect("Failed to serialize");
        let deserialized = deserialize_properties(&serialized).expect("Failed to deserialize");
        assert_eq!(properties, deserialized);
    }
}
