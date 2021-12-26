use std::{collections::HashMap, io, pin::Pin, task::Context, task::Poll};

use fallible_iterator::FallibleIterator;
use futures::{ready, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use ntex::{channel::mpsc, io::Io, io::RecvError, util::poll_fn, util::BytesMut};
use postgres_protocol::authentication;
use postgres_protocol::authentication::sasl;
use postgres_protocol::authentication::sasl::ScramSha256;
use postgres_protocol::message::backend::{AuthenticationSaslBody, Message};
use postgres_protocol::message::frontend;

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::config::{self, Config};
use crate::connect_tls::connect_tls;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::tls::{TlsConnect, TlsStream};
use crate::{Client, Connection, Error};

pub struct StartupStream {
    io: Io,
    buf: BackendMessages,
}

impl Stream for StartupStream {
    type Item = io::Result<Message>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Message>>> {
        loop {
            match self.buf.next() {
                Ok(Some(message)) => return Poll::Ready(Some(Ok(message))),
                Ok(None) => {}
                Err(e) => return Poll::Ready(Some(Err(e))),
            }

            match ready!(self.io.poll_recv(&PostgresCodec, cx)) {
                Ok(BackendMessage::Normal { messages, .. }) => self.buf = messages,
                Ok(BackendMessage::Async(message)) => return Poll::Ready(Some(Ok(message))),
                Err(RecvError::PeerGone(_)) => return Poll::Ready(None),
                Err(e) => {
                    return Poll::Ready(Some(Err(io::Error::new(io::ErrorKind::Other, "error"))))
                }
            }
        }
    }
}

pub async fn connect_raw(io: Io, config: &Config) -> Result<(Client, Connection), Error> {
    let mut stream = StartupStream {
        io,
        buf: BackendMessages::empty(),
    };

    startup(&mut stream, config).await?;
    authenticate(&mut stream, config).await?;
    let (process_id, secret_key, parameters) = read_info(&mut stream).await?;

    let (sender, receiver) = mpsc::channel();
    let client = Client::new(sender, config.ssl_mode, process_id, secret_key);
    let connection = Connection::new(stream.io, parameters, receiver);

    Ok((client, connection))
}

async fn startup(stream: &mut StartupStream, config: &Config) -> Result<(), Error> {
    let mut params = vec![("client_encoding", "UTF8"), ("timezone", "UTC")];
    if let Some(user) = &config.user {
        params.push(("user", &**user));
    }
    if let Some(dbname) = &config.dbname {
        params.push(("database", &**dbname));
    }
    if let Some(options) = &config.options {
        params.push(("options", &**options));
    }
    if let Some(application_name) = &config.application_name {
        params.push(("application_name", &**application_name));
    }

    let mut buf = BytesMut::new();
    frontend::startup_message(params, &mut buf).map_err(Error::encode)?;

    stream
        .io
        .send(FrontendMessage::Raw(buf.freeze()), &PostgresCodec)
        .await
        .map_err(|e| Error::from(e.into_inner()))
}

async fn authenticate(stream: &mut StartupStream, config: &Config) -> Result<(), Error> {
    match stream.next().await {
        Some(Ok(Message::AuthenticationOk)) => {
            can_skip_channel_binding(config)?;
            return Ok(());
        }
        Some(Ok(Message::AuthenticationCleartextPassword)) => {
            can_skip_channel_binding(config)?;

            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            authenticate_password(stream, pass).await?;
        }
        Some(Ok(Message::AuthenticationMd5Password(body))) => {
            can_skip_channel_binding(config)?;

            let user = config
                .user
                .as_ref()
                .ok_or_else(|| Error::config("user missing".into()))?;
            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            let output = authentication::md5_hash(user.as_bytes(), pass, body.salt());
            authenticate_password(stream, output.as_bytes()).await?;
        }
        Some(Ok(Message::AuthenticationSasl(body))) => {
            authenticate_sasl(stream, body, config).await?;
        }
        Some(Ok(Message::AuthenticationKerberosV5))
        | Some(Ok(Message::AuthenticationScmCredential))
        | Some(Ok(Message::AuthenticationGss))
        | Some(Ok(Message::AuthenticationSspi)) => {
            return Err(Error::authentication(
                "unsupported authentication method".into(),
            ))
        }
        Some(Ok(Message::ErrorResponse(body))) => return Err(Error::db(body)),
        Some(_) => return Err(Error::unexpected_message()),
        None => return Err(Error::closed()),
    }

    match stream.try_next().await.map_err(Error::io)? {
        Some(Message::AuthenticationOk) => Ok(()),
        Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
        Some(_) => Err(Error::unexpected_message()),
        None => Err(Error::closed()),
    }
}

fn can_skip_channel_binding(config: &Config) -> Result<(), Error> {
    match config.channel_binding {
        config::ChannelBinding::Disable | config::ChannelBinding::Prefer => Ok(()),
        config::ChannelBinding::Require => Err(Error::authentication(
            "server did not use channel binding".into(),
        )),
    }
}

async fn authenticate_password(stream: &mut StartupStream, password: &[u8]) -> Result<(), Error> {
    let mut buf = BytesMut::new();
    frontend::password_message(password, &mut buf).map_err(Error::encode)?;

    stream
        .io
        .send(FrontendMessage::Raw(buf.freeze()), &PostgresCodec)
        .await
        .map_err(|e| Error::from(e.into_inner()))
}

async fn authenticate_sasl(
    stream: &mut StartupStream,
    body: AuthenticationSaslBody,
    config: &Config,
) -> Result<(), Error> {
    panic!()
}

async fn read_info(
    stream: &mut StartupStream,
) -> Result<(i32, i32, HashMap<String, String>), Error> {
    let mut process_id = 0;
    let mut secret_key = 0;
    let mut parameters = HashMap::default();

    loop {
        match stream.next().await {
            Some(Ok(Message::BackendKeyData(body))) => {
                process_id = body.process_id();
                secret_key = body.secret_key();
            }
            Some(Ok(Message::ParameterStatus(body))) => {
                parameters.insert(
                    body.name().map_err(Error::parse)?.to_string(),
                    body.value().map_err(Error::parse)?.to_string(),
                );
            }
            Some(Ok(Message::NoticeResponse(_))) => {}
            Some(Ok(Message::ReadyForQuery(_))) => return Ok((process_id, secret_key, parameters)),
            Some(Ok(Message::ErrorResponse(body))) => return Err(Error::db(body)),
            Some(Ok(_)) => return Err(Error::unexpected_message()),
            Some(Err(e)) => return Err(Error::io(e)),
            None => return Err(Error::closed()),
        }
    }
}
