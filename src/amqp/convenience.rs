use cookie_factory::gen_simple;
use lapin::{
    protocol::basic::{gen_properties, parse_properties},
    types::AMQPValue,
    BasicProperties,
};
use std::borrow::{Borrow, Cow};

/// Convenience methods for [`lapin::BasicProperties`].
pub trait BasicPropertiesExt {
    /// Lookup header by key.
    fn get_header<Q>(&self, key: &Q) -> Option<&AMQPValue>
    where
        lapin::types::ShortString: Borrow<Q> + Ord,
        Q: Ord + ?Sized;

    /// Lookup a header string value.
    ///
    /// Returns `None` if not a string.
    fn get_header_str<Q>(&self, key: &Q) -> Option<Cow<'_, str>>
    where
        lapin::types::ShortString: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        match self.get_header(key) {
            Some(AMQPValue::LongString(s)) => Some(String::from_utf8_lossy(s.as_bytes())),
            Some(AMQPValue::ShortString(s)) => Some(Cow::Borrowed(s.as_str())),
            _ => None,
        }
    }
}

impl BasicPropertiesExt for lapin::BasicProperties {
    fn get_header<Q>(&self, key: &Q) -> Option<&AMQPValue>
    where
        lapin::types::ShortString: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.headers().as_ref()?.inner().get(key)
    }
}

// Lapin uses a weird serialisation mechanism based on a library called `cookie_factory`.
// We are exposing here a couple of handy functions to perform the tasks we need without having
// to spread this in multiple places across our project.

/// Serialize AMQP properties to a bytes buffer.
pub fn serialize_properties(properties: &BasicProperties) -> Result<Vec<u8>, anyhow::Error> {
    gen_simple(gen_properties(properties), Vec::new()).map_err(Into::into)
}

pub fn dummy_test_to_trigger_release() {
    println!("does release please need an actual code change");
}

/// Deserialize AMQP properties from a bytes buffer.
pub fn deserialize_properties(bytes: &[u8]) -> Result<BasicProperties, anyhow::Error> {
    parse_properties(bytes).map_err(Into::into).map(|x| x.1)
}

/// Set the value for a header inside a collection of AMQP properties.
///
/// # Implementation notes
///
/// The current version is wasteful - we are cloning all the headers, but `lapin` does not allow us
/// to do any better. We consume the `properties` input to make sure the caller does not re-use
/// it under the impression that it has been mutated to add the new header.
///
/// We should submit a PR upstream to get mutable access to headers from BasicProperties.
///
/// # Example
///
/// ```rust
/// use carrot_cake::amqp::types::{AMQPValue, ShortString};
/// use carrot_cake::amqp::{BasicProperties, convenience::set_header};
///
/// // Empty set of headers
/// let properties = BasicProperties::default();
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
    properties: BasicProperties,
    header_name: &str,
    header_value: AMQPValue,
) -> BasicProperties {
    let mut headers = properties
        .headers()
        .as_ref()
        .map(|h| h.to_owned())
        .unwrap_or_default();
    headers.insert(header_name.into(), header_value);
    properties.with_headers(headers)
}

#[cfg(test)]
mod tests {
    use fake::{Fake, Faker};

    use crate::amqp::convenience::{deserialize_properties, serialize_properties, set_header};
    use crate::amqp::types::{AMQPValue, ShortString};
    use crate::amqp::BasicProperties;

    #[test]
    fn set_works() {
        // Arrange
        let header_name: String = Faker.fake();
        let header_value = AMQPValue::LongString(Faker.fake::<String>().into());
        let properties = BasicProperties::default();

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
