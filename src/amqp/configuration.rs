//! Configuration types holding the parameters required to connect to a RabbitMq broker.
use anyhow::Context;
use lapin::uri::{AMQPAuthority, AMQPScheme, AMQPUri, AMQPUserInfo};
use native_tls::Certificate;
use redact::Secret;
use serde::Deserialize;
use serde_aux::field_attributes::deserialize_number_from_string;

#[derive(Debug, Deserialize, Clone)]
/// Configuration to establish a connection with a RabbitMq broker.
///
/// You can use `RabbitMqSettings::default()` to get the default configuration used by an
/// out-of-the-box RabbitMq installation (e.g. launched via the official Docker image).
pub struct RabbitMqSettings {
    /// The address of the RabbitMq broker.
    ///
    /// E.g. `localhost` if you are running a local instance of RabbitMq.
    pub uri: String,
    /// The name of the [virtual host](https://www.rabbitmq.com/vhosts.html) you want to connect to.
    ///
    /// E.g. `/` if you are using the default RabbitMq virtual host.
    pub vhost: String,
    /// The username used to authenticate with the RabbitMq broker.
    pub username: String,
    /// The password used to authenticate with the RabbitMq broker.
    pub password: Secret<String>,
    /// How long you should wait when trying to connect to a RabbitMq broker before giving up,
    /// in seconds.
    pub connection_timeout_seconds: Option<u64>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    /// The port you want to use to communicate with RabbitMq broker.
    pub port: u16,
    /// Configuration to establish an encrypted connection with the RabbitMq broker.
    /// If omitted the connection will be in plain text.
    pub tls: Option<RabbitMqTlsSettings>,
}

impl Default for RabbitMqSettings {
    fn default() -> Self {
        // The connection parameter used by an out-of-the-box installation of RabbitMq
        Self {
            uri: "localhost".into(),
            vhost: "/".into(),
            username: "guest".into(),
            password: "guest".to_owned().into(),
            connection_timeout_seconds: Some(10),
            port: 5672,
            tls: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
/// Configuration to establish an encrypted connection with a RabbitMq broker.
pub struct RabbitMqTlsSettings {
    /// The domain we expect as CN on the server certificate.
    /// If left unspecified, it defaults to the uri host.
    pub domain: Option<String>,
    /// Root certificate chain to be trusted when validating server certificates.
    ///
    /// To be specified in PEM format.
    ///
    /// If set to `None`, the system's trust root will be used by default.
    ///
    /// # Examples
    ///
    /// ## Single certificate
    ///
    /// ```text
    /// -----BEGIN CERTIFICATE-----
    /// <-- OMITTED -->
    /// -----END CERTIFICATE-----
    /// ```
    ///
    /// ## Certificate chain
    ///
    /// ```text
    /// -----BEGIN CERTIFICATE-----
    /// <-- OMITTED -->
    /// -----END CERTIFICATE-----
    /// -----BEGIN CERTIFICATE-----
    /// <-- OMITTED -->
    /// -----END CERTIFICATE-----
    /// ```
    pub ca_certificate_chain_pem: Option<String>,
}

impl RabbitMqTlsSettings {
    /// It parses the CA certificate chain and returns it in the strongly-typed format
    /// provided by the `native_tls` crate.
    pub fn ca_certificate_chain(&self) -> Result<Option<Certificate>, anyhow::Error> {
        self.ca_certificate_chain_pem
            .as_ref()
            .map(String::as_bytes)
            .map(Certificate::from_pem)
            .transpose()
            .context("Failed to decode PEM certificate chain for RabbitMQ TLS.")
    }
}

impl RabbitMqSettings {
    /// Combines all settings values to return a fully qualified AMQP uri.
    ///
    /// E.g. `amqp://user:pass@host:10000/vhost`
    pub fn amqp_uri(&self) -> AMQPUri {
        AMQPUri {
            authority: AMQPAuthority {
                userinfo: AMQPUserInfo {
                    username: self.username.clone(),
                    password: self.password.expose_secret().clone(),
                },
                host: self.uri.clone(),
                port: self.port,
            },
            scheme: AMQPScheme::AMQP,
            vhost: self.vhost.clone(),
            query: Default::default(),
        }
    }

    /// Retrieve the timeout observed when trying to connect to RabbitMq.
    /// It returns `None` if left unspecified.
    pub fn connection_timeout(&self) -> Option<std::time::Duration> {
        self.connection_timeout_seconds
            .map(std::time::Duration::from_secs)
    }
}
