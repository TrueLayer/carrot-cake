use crate::amqp::configuration::RabbitMqSettings;
use lapin::{
    tcp::{AMQPUriTcpExt, NativeTlsConnector},
    uri::{AMQPScheme, AMQPUri},
    ConnectionProperties,
};
use std::sync::Arc;
use tokio::time::timeout;
use tracing::warn;

#[derive(Clone)]
/// All the information required to connect to a RabbitMq broker.
pub struct ConnectionFactory {
    uri: AMQPUri,
    /// The timeout observed when trying to connect to RabbitMq.
    connection_timeout: std::time::Duration,
    /// TLS configuration for the connection to RabbitMq.
    /// If `None`, the connection will not be encrypted.
    tls: Option<Arc<Tls>>,
}

#[derive(Clone)]
struct Tls {
    connector: NativeTlsConnector,
    domain_name: String,
}

impl ConnectionFactory {
    /// Create a new connection factory from settings.
    ///
    /// It allows you to customize the TLS configuration.
    ///
    /// A connection timeout can be (optionally) specified in `settings`.
    /// If the connection timeout is left unspecified, it will be defaulted to 10 seconds.
    pub fn new_from_config(settings: &RabbitMqSettings) -> Result<Self, anyhow::Error> {
        let tls = settings
            .tls
            .as_ref()
            .map::<Result<Tls, anyhow::Error>, _>(|tls_settings| {
                let server_domain_name = tls_settings
                    .domain()
                    .clone()
                    .unwrap_or_else(|| settings.amqp_uri().authority.host);

                let mut connector_builder = NativeTlsConnector::builder();
                if let Some(certificate) = tls_settings.ca_certificate_chain()? {
                    connector_builder.add_root_certificate(certificate);
                }

                let connector = connector_builder.build().expect("TLS configuration failed");
                Ok(Tls {
                    domain_name: server_domain_name,
                    connector,
                })
            })
            .transpose()?;
        let connection_timeout = settings
            .connection_timeout()
            .unwrap_or_else(|| std::time::Duration::from_secs(10));
        Ok(Self {
            uri: settings.amqp_uri(),
            connection_timeout,
            tls: tls.map(Arc::new),
        })
    }

    /// Replaces the TLS Connector for the connection factory
    pub fn set_tls_connector(&mut self, connector: NativeTlsConnector) {
        self.set_tls_connector_with_domain(connector, self.uri.authority.host.clone());
    }

    /// Replaces the TLS Connector for the connection factory, along with the expected domain name for the certificate
    pub fn set_tls_connector_with_domain(
        &mut self,
        connector: NativeTlsConnector,
        domain_name: String,
    ) {
        self.tls = Some(Arc::new(Tls {
            connector,
            domain_name,
        }));
    }

    /// Create a new connection to a RabbitMq broker.
    ///
    /// It establishes an encrypted connection if `self.tls` is `Some`.
    /// It establishes an unencrypted connection if `self.tls` is `None`.
    #[tracing::instrument(name = "rabbitmq_connect", skip(self))]
    pub async fn new_connection(&self) -> Result<lapin::Connection, anyhow::Error> {
        let properties =
            ConnectionProperties::default().with_executor(tokio_executor_trait::Tokio::current());
        let connection = timeout(self.connection_timeout, async {
            match &self.tls {
                None => self.connect_without_tls(properties).await,
                Some(tls) => self.connect_with_tls(properties, Arc::clone(tls)).await,
            }
        })
        .await??;
        // Register a callback to log connection errors.
        connection.on_error(|e| {
            warn!("RabbitMQ broken connection: {:?}", e);
        });
        Ok(connection)
    }

    /// Establish a new unencrypted connection to a RabbitMq broker.
    async fn connect_without_tls(
        &self,
        properties: ConnectionProperties,
    ) -> Result<lapin::Connection, lapin::Error> {
        lapin::Connection::connect_uri(self.uri.clone(), properties).await
    }

    /// Establish a new TLS connection to a RabbitMq broker.
    async fn connect_with_tls(
        &self,
        properties: ConnectionProperties,
        tls_configuration: Arc<Tls>,
    ) -> Result<lapin::Connection, lapin::Error> {
        lapin::Connection::connector(
            self.uri.clone(),
            Box::new(move |uri| {
                // First establish a plain TCP connection using the AMQP protocol
                let mut amqp_uri = uri.clone();
                amqp_uri.scheme = AMQPScheme::AMQP;
                amqp_uri
                    .connect()
                    // Then perform a TLS handshake with custom settings
                    // including customisation of the expected domain for the server certificate
                    .and_then(|tcp| {
                        tcp.into_native_tls(
                            &tls_configuration.connector,
                            &tls_configuration.domain_name,
                        )
                    })
            }),
            properties,
        )
        .await
    }

    pub(crate) async fn get_channel(&self) -> Result<lapin::Channel, anyhow::Error> {
        Ok(self.new_connection().await?.create_channel().await?)
    }
}
