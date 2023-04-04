use lapin::types::AMQPValue;
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
