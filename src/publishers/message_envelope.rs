use crate::amqp::types::{FieldTable, ShortShortUInt, ShortString, Timestamp};
use crate::amqp::BasicProperties;

/// A message to be published via `Publisher`.
#[derive(Default)]
pub struct MessageEnvelope {
    /// The body of the message - as a sequence of bytes.
    pub payload: Vec<u8>,
    /// The name of the exchange we are publishing the message to.
    pub exchange_name: String,
    /// The routing key used by exchange listeners to determine if they are interested or not
    /// to the message.
    pub routing_key: String,
    /// AMQP headers attached to the message.
    /// It can be omitted by passing `None`.
    pub properties: BasicProperties,
}

impl MessageEnvelope {
    #[must_use]
    pub fn with_payload(mut self, value: Vec<u8>) -> Self {
        self.payload = value;
        self
    }
    #[must_use]
    pub fn with_exchange_name(mut self, value: String) -> Self {
        self.exchange_name = value;
        self
    }

    #[must_use]
    pub fn with_routing_key(mut self, value: String) -> Self {
        self.routing_key = value;
        self
    }

    #[must_use]
    fn props(mut self, f: impl FnOnce(BasicProperties) -> BasicProperties) -> Self {
        self.properties = f(self.properties);
        self
    }

    #[must_use]
    pub fn with_content_type(self, value: ShortString) -> Self {
        self.props(|p| p.with_content_type(value))
    }

    #[must_use]
    pub fn with_content_encoding(self, value: ShortString) -> Self {
        self.props(|p| p.with_content_encoding(value))
    }

    #[must_use]
    pub fn with_headers(self, value: FieldTable) -> Self {
        self.props(|p| p.with_headers(value))
    }

    #[must_use]
    pub fn with_delivery_mode(self, value: ShortShortUInt) -> Self {
        self.props(|p| p.with_delivery_mode(value))
    }

    #[must_use]
    pub fn with_priority(self, value: ShortShortUInt) -> Self {
        self.props(|p| p.with_priority(value))
    }

    #[must_use]
    pub fn with_correlation_id(self, value: ShortString) -> Self {
        self.props(|p| p.with_correlation_id(value))
    }

    #[must_use]
    pub fn with_reply_to(self, value: ShortString) -> Self {
        self.props(|p| p.with_reply_to(value))
    }

    #[must_use]
    pub fn with_expiration(self, value: ShortString) -> Self {
        self.props(|p| p.with_expiration(value))
    }

    #[must_use]
    pub fn with_message_id(self, value: ShortString) -> Self {
        self.props(|p| p.with_message_id(value))
    }

    #[must_use]
    pub fn with_timestamp(self, value: Timestamp) -> Self {
        self.props(|p| p.with_timestamp(value))
    }

    #[must_use]
    pub fn with_kind(self, value: ShortString) -> Self {
        self.props(|p| p.with_kind(value))
    }

    #[must_use]
    pub fn with_user_id(self, value: ShortString) -> Self {
        self.props(|p| p.with_user_id(value))
    }

    #[must_use]
    pub fn with_app_id(self, value: ShortString) -> Self {
        self.props(|p| p.with_app_id(value))
    }

    #[must_use]
    pub fn with_cluster_id(self, value: ShortString) -> Self {
        self.props(|p| p.with_cluster_id(value))
    }
}
