mod fixtures;

use carrot_cake_amqp::rabbit_mq::{HealthStatus, WITH_PUBLISHER_CONFIRMATION};
use carrot_cake_amqp_pool::{ChannelManager, ChannelPool, ConnectionManager};

#[tokio::test]
async fn assert_pool_reconnects_on_broken_connection() {
    let connection_pool = ConnectionManager::new(fixtures::get_rabbitmq_factory())
        .max_connections(1)
        .into_pool();

    let manager = ChannelManager::new(connection_pool);

    let pool = ChannelPool::<WITH_PUBLISHER_CONFIRMATION>::builder(manager)
        .max_size(1)
        .build()
        .expect("could not build channel pool");

    let channel = pool.get().await.unwrap();
    assert_eq!(HealthStatus::Healthy, channel.status());

    channel
        .raw()
        .close(0, "closing for test")
        .await
        .expect("failed to close channel");

    // Assert that it has been closed.
    assert_eq!(HealthStatus::Unhealthy, channel.status());
    drop(channel);

    let channel = pool.get().await.unwrap();
    assert_eq!(HealthStatus::Healthy, channel.status());
}
