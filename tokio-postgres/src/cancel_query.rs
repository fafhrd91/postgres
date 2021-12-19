use crate::client::SocketConfig;
use crate::config::{Host, SslMode};
use crate::tls::MakeTlsConnect;
use crate::{cancel_query_raw, connect_socket, Error, Socket};
use std::io;

pub(crate) async fn cancel_query(
    config: Option<SocketConfig>,
    ssl_mode: SslMode,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error> {
    let config = match config {
        Some(config) => config,
        None => {
            return Err(Error::connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown host",
            )))
        }
    };

    let hostname = match &config.host {
        Host::Tcp(host) => &**host,
        // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
        #[cfg(unix)]
        Host::Unix(_) => "",
    };

    let socket = connect_socket::connect_socket(
        &config.host,
        config.port,
        config.connect_timeout,
        config.keepalives,
        config.keepalives_idle,
    )
    .await?;

    cancel_query_raw::cancel_query_raw(socket, ssl_mode, process_id, secret_key).await
}
