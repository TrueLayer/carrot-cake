use carrot_cake::amqp::{configuration::RabbitMqSettings, ConnectionFactory};
use carrot_cake::pool::{ChannelManager, ChannelPool, ConnectionPool};
use carrot_cake::publishers::{MessageEnvelope, Publisher};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // First of all we build the configuration for our connection factory.
    // We are using the out-of-the-box parameters for the default RabbitMq Docker image.
    let settings = RabbitMqSettings::default();
    let connection_factory = ConnectionFactory::new_from_config(&settings)?;
    let pool_size = 12;

    // Our RabbitMq publishers must be resilient to network issues.
    // To achieve resiliency, we require a channel pool in its constructor, which in turn relies
    // on a connection pool.
    // When building the pool we can set specify the maximum number of connections (10 in this case)
    // and the maximum number of channels (100 in this case).
    //
    // The maximum number of channels is, de facto, an upper limit on the maximum number of messages
    // you can publish concurrently.
    //
    // Why are we setting the maximum number of channels 10x higher than the maximum number of
    // connections?
    // A connection maps to an open TCP socket.
    // On top of a "physical" connection we can have multiple channels - a "logical" connection
    // to RabbitMq sharing the same underlying TCP connection. Channels are cheaper then connections,
    // therefore we can keep more of those open without burdening the server as much as an
    // open connection would.
    //
    // When building a channel pool, we also need to declare if we want publisher confirms to be
    // enabled or not (`WithConfirmation` or `WithoutConfirmation`).
    // E.g. if publisher confirms are enabled, publishing a message for which there is no listener
    // will fail. If they are disabled, publishing will succeed and the broker will simply
    // discard the message.
    let connection_pool = ConnectionPool::builder(connection_factory).build()?;
    let channel_pool = ChannelPool::builder(ChannelManager::new(connection_pool))
        .max_size(pool_size)
        .build()?;

    // We have a fluent API to assemble our publishers step-by-step.
    // You can also add middlewares to your publishers.
    let publisher = Publisher::builder(channel_pool)
        // Timeout on the publishing operation.
        .publish_timeout(std::time::Duration::from_secs(3))
        .build();

    let message = MessageEnvelope::default()
        .with_payload("Hello world!".as_bytes().into())
        .with_exchange_name("hello-exchange".into())
        .with_routing_key("hello".into());

    // We are using a publisher with confirmation enabled, but there is no listener for our messages,
    // therefore the publishing action should fail.
    assert!(publisher.publish(message).await.is_err());

    Ok(())
}
