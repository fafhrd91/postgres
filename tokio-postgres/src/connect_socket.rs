use std::{future::Future, io, time::Duration};

use ntex::{connect, io::Io, rt, time};

use crate::config::Host;
use crate::{Error, Socket};

pub(crate) async fn connect_socket(
    host: &Host,
    port: u16,
    connect_timeout: Option<Duration>,
    keepalives: bool,
    keepalives_idle: Duration,
    cfg: ntex::SharedCfg,
) -> Result<Io, Error> {
    match host {
        Host::Tcp(host) => {
            let fut =
                connect::connect_with(connect::Connect::new(host.clone()).set_port(port), cfg);
            let socket = connect_with_timeout(
                async move {
                    fut.await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, "connect error"))
                },
                connect_timeout,
            )
            .await?;
            Ok(socket)
        }
        #[cfg(unix)]
        Host::Unix(path) => {
            panic!("not supported")
        }
    }
}

async fn connect_with_timeout<F, T>(connect: F, timeout: Option<Duration>) -> Result<T, Error>
where
    F: Future<Output = io::Result<T>>,
{
    match timeout {
        Some(timeout) => match time::timeout(timeout, connect).await {
            Ok(Ok(socket)) => Ok(socket),
            Ok(Err(e)) => Err(Error::connect(e)),
            Err(_) => Err(Error::connect(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection timed out",
            ))),
        },
        None => match connect.await {
            Ok(socket) => Ok(socket),
            Err(e) => Err(Error::connect(e)),
        },
    }
}
