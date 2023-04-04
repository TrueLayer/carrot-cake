use carrot_cake_amqp::{
    configuration::RabbitMqSettings,
    rabbit_mq::{ConnectionFactory, WITH_PUBLISHER_CONFIRMATION},
};
use carrot_cake_amqp_pool::{ChannelManager, ChannelPool, ConnectionManager};

#[tokio::main]
async fn main() {
    // initialize rabbitmq connection details and config.
    let settings = RabbitMqSettings {
        uri: "localhost".into(),
        vhost: "/".into(),
        username: "rabbitmq".into(),
        password: "password".into(),
        connection_timeout_seconds: Default::default(),
        port: 6743,
        tls: None,
    };

    let max_connections = 5;

    let connection_pool =
        ConnectionManager::new(ConnectionFactory::new_from_config(&settings).unwrap())
            .max_connections(max_connections)
            .into_pool();

    // Create a channel manager that implements `deadpool::Manager` that will use
    //  the connection pool to create new connections when they break.
    let cm = ChannelManager::new(connection_pool);

    // determine the max poolsize.
    let pool_size = 16;

    // create a `ChannelPool` from the `ChannelManager`.
    let pool = ChannelPool::<WITH_PUBLISHER_CONFIRMATION>::builder(cm)
        .max_size(pool_size)
        .build()
        .expect("could not build channel pool");

    // get a new Channel from the pool.
    let channel = pool.get().await.unwrap();

    channel
        .publish(
            "publish_payload".as_bytes().to_vec(),
            "exchange_name",
            "routing_key",
            None,
        )
        .await
        .unwrap();
}
