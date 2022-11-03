use ntex::io::Io;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::config::SslMode;
use crate::tls::TlsConnect;
use crate::Error;

pub async fn cancel_query_raw(
    stream: Io,
    mode: SslMode,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error> {
    Ok(())
}
