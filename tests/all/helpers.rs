use carrot_cake::amqp::{configuration::RabbitMqSettings, ConnectionFactory};
use carrot_cake::pool::{ChannelManager, ChannelPool, ConnectionPool};
use carrot_cake::publishers::Publisher;

pub fn get_rabbitmq_settings() -> RabbitMqSettings {
    RabbitMqSettings::default()
}

pub fn get_connection_factory() -> ConnectionFactory {
    ConnectionFactory::new_from_config(&get_rabbitmq_settings()).unwrap()
}

pub async fn get_publisher() -> Publisher {
    let connection_pool = ConnectionPool::builder(get_connection_factory())
        .max_size(2)
        .build()
        .unwrap();
    let channel_pool = ChannelPool::builder(ChannelManager::new(connection_pool))
        .max_size(10)
        .build()
        .unwrap();

    Publisher::builder(channel_pool)
        .publish_timeout(std::time::Duration::from_secs(3))
        .build()
}
