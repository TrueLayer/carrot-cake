use amq_protocol_types::{FieldTable, ShortShortUInt, ShortString, Timestamp};

use crate::amqp::AMQPProperties;

/// A message to be published via `Publisher`.
#[derive(Default)]
pub struct MessageEnvelope {
    // The body of the message - as a sequence of bytes.
    pub payload: Vec<u8>,
    // The name of the exchange we are publishing the message to.
    pub exchange_name: String,
    // The routing key used by exchange listeners to determine if they are interested or not
    // to the message.
    pub routing_key: String,
    // AMQP headers attached to the message.
    // It can be omitted by passing `None`.
    pub properties: Option<AMQPProperties>,
}

#[derive(Clone, Debug, PartialEq)]
/// `Message` represents the information about the message itself,
/// payload, content-type, correlation id, etc.
pub struct Message {
    pub payload: Vec<u8>,
    pub properties: AMQPProperties,
}

impl Message {
    /// Encode this message into [`MessageEnvelope`]
    pub fn to_envelope(self, routing: Routing) -> MessageEnvelope {
        MessageEnvelope {
            payload: self.payload,
            properties: Some(self.properties),
            exchange_name: routing.exchange_name,
            routing_key: routing.routing_key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// `Routing` represents how a [`Message`] is routed to it's destination
pub struct Routing {
    pub exchange_name: String,
    pub routing_key: String,
}

impl MessageEnvelope {
    pub fn with_payload(mut self, value: Vec<u8>) -> Self {
        self.payload = value;
        self
    }

    pub fn with_exchange_name(mut self, value: String) -> Self {
        self.exchange_name = value;
        self
    }

    pub fn with_routing_key(mut self, value: String) -> Self {
        self.routing_key = value;
        self
    }

    fn props(mut self, f: impl FnOnce(AMQPProperties) -> AMQPProperties) -> Self {
        self.properties = Some(f(self.properties.unwrap_or_default()));
        self
    }

    pub fn with_content_type(self, value: ShortString) -> Self {
        self.props(|p| p.with_content_type(value))
    }

    pub fn with_content_encoding(self, value: ShortString) -> Self {
        self.props(|p| p.with_content_encoding(value))
    }

    pub fn with_headers(self, value: FieldTable) -> Self {
        self.props(|p| p.with_headers(value))
    }

    pub fn with_delivery_mode(self, value: ShortShortUInt) -> Self {
        self.props(|p| p.with_delivery_mode(value))
    }

    pub fn with_priority(self, value: ShortShortUInt) -> Self {
        self.props(|p| p.with_priority(value))
    }

    pub fn with_correlation_id(self, value: ShortString) -> Self {
        self.props(|p| p.with_correlation_id(value))
    }

    pub fn with_reply_to(self, value: ShortString) -> Self {
        self.props(|p| p.with_reply_to(value))
    }

    pub fn with_expiration(self, value: ShortString) -> Self {
        self.props(|p| p.with_expiration(value))
    }

    pub fn with_message_id(self, value: ShortString) -> Self {
        self.props(|p| p.with_message_id(value))
    }

    pub fn with_timestamp(self, value: Timestamp) -> Self {
        self.props(|p| p.with_timestamp(value))
    }

    pub fn with_kind(self, value: ShortString) -> Self {
        self.props(|p| p.with_kind(value))
    }

    pub fn with_user_id(self, value: ShortString) -> Self {
        self.props(|p| p.with_user_id(value))
    }

    pub fn with_app_id(self, value: ShortString) -> Self {
        self.props(|p| p.with_app_id(value))
    }

    pub fn with_cluster_id(self, value: ShortString) -> Self {
        self.props(|p| p.with_cluster_id(value))
    }
}
