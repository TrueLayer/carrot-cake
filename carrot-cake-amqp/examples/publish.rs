use carrot_cake_amqp::{
    configuration::RabbitMqSettings,
    rabbit_mq::{ConnectionFactory, WITH_PUBLISHER_CONFIRMATION},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = RabbitMqSettings::default();

    // Create a connection factory form the rabbit_mq settings that can
    // instantiate new connections.
    let connection_factory = ConnectionFactory::new_from_config(&settings)?;

    // Connect to the rabbit_mq instance
    let connection = connection_factory.new_connection().await?;

    // Create a publisher from the newly established connection.
    let publisher = connection
        .create_channel::<WITH_PUBLISHER_CONFIRMATION>()
        .await?;

    publisher
        .publish(
            "payload".as_bytes().to_vec(),
            "exchange_name",
            "routing_key",
            None,
        )
        .await?;

    Ok(())
}
