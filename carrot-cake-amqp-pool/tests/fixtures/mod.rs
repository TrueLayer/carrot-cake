use carrot_cake_amqp::{configuration::RabbitMqSettings, rabbit_mq::ConnectionFactory};
use std::path::PathBuf;

/// Retrieve settings for our rabbitmq server.
pub fn get_rabbitmq_factory() -> ConnectionFactory {
    // Load test settings from configuration/env variables.
    let mut test_config_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    test_config_path.push("tests/fixtures/test_config.yml");

    let rabbit_mq_settings: RabbitMqSettings = serde_yaml::from_reader(
        std::fs::File::open(test_config_path).expect("failed to open config file"),
    )
    .expect("faile to read rabbitmq config");

    ConnectionFactory::new_from_config(&rabbit_mq_settings).unwrap()
}
