use futures::future::{err, Either};
use futures::{ready, Stream};
use std::task::{Context, Poll};
use std::{collections::VecDeque, future::Future, pin::Pin};

use ntex::util::{BufMut, Bytes, BytesMut, BytesVec};
use postgres_protocol::message::backend::Message;

use crate::client::{InnerClient, Responses};
use crate::connection::{ConnectionState, Response};
use crate::types::{IsNull, ToSql};
use crate::{codec::FrontendMessage, frontend, Error, Portal, Row, Statement};

pub fn query<'a>(
    client: &'a InnerClient,
    statement: &'a Statement,
    params: &[&dyn ToSql],
) -> impl Future<Output = Result<Vec<Row>, Error>> + 'a {
    let receiver = {
        let mut st = client.con.borrow_mut();

        st.io
            .with_write_buf(|buf| {
                // make sure we've got room
                let remaining = buf.remaining_mut();
                if remaining < 1024 {
                    buf.reserve(256 * 1024 - remaining);
                }

                encode_bind_vec(statement, params, "", buf)?;
                frontend::execute_vec("", 0, buf);
                frontend::sync_vec(buf);

                Ok::<_, Error>(())
            })
            .map(|_| {
                let (sender, receiver) = client.pool.channel();
                st.responses.push_back(Response { sender });
                receiver
            })
    };

    async move {
        let mut messages = receiver?.await?;
        if let Message::BindComplete = messages[0] {
            let mut rows = Vec::with_capacity(messages.len() - 1);
            messages.pop_front();
            for msg in messages {
                match msg {
                    Message::DataRow(body) => rows.push(Row::new(statement.clone(), body)?),
                    Message::EmptyQueryResponse
                    | Message::CommandComplete(_)
                    | Message::PortalSuspended => break,
                    Message::ErrorResponse(body) => return Err(Error::db(body)),
                    _ => return Err(Error::unexpected_message()),
                }
            }
            Ok(rows)
        } else {
            Err(Error::unexpected_message())
        }
    }
}

pub fn query_one<'a>(
    client: &'a InnerClient,
    statement: &'a Statement,
    params: &[&dyn ToSql],
) -> impl Future<Output = Result<Row, Error>> + 'a {
    let receiver = {
        let mut st = client.con.borrow_mut();

        st.io
            .with_write_buf(|buf| {
                // make sure we've got room
                let remaining = buf.remaining_mut();
                if remaining < 1024 {
                    buf.reserve(256 * 1024 - remaining);
                }

                encode_bind_vec(statement, params, "", buf)?;
                frontend::execute_vec("", 0, buf);
                frontend::sync_vec(buf);
                Ok::<_, Error>(())
            })
            .map(|_| {
                let (sender, receiver) = client.pool.channel();
                st.responses.push_back(Response { sender });
                receiver
            })
    };

    async move {
        let mut messages = receiver?.await?;
        if let Message::BindComplete = messages[0] {
            messages.pop_front();
            if let Some(msg) = messages.pop_front() {
                match msg {
                    Message::DataRow(body) => return Row::new(statement.clone(), body),
                    Message::EmptyQueryResponse
                    | Message::CommandComplete(_)
                    | Message::PortalSuspended => (),
                    Message::ErrorResponse(body) => return Err(Error::db(body)),
                    _ => (),
                }
            }
            Err(Error::unexpected_message())
        } else {
            Err(Error::unexpected_message())
        }
    }
}

pub async fn query_portal(
    client: &InnerClient,
    portal: &Portal,
    max_rows: i32,
) -> Result<Vec<Row>, Error> {
    let buf = client.with_buf(|buf| {
        frontend::execute(portal.name(), max_rows, buf);
        frontend::sync(buf);
        Ok::<_, Error>(buf.split().freeze())
    })?;

    let statement = portal.statement().clone();
    let responses = client.send(FrontendMessage::Raw(buf))?;

    let messages = responses.receiver.await?;
    let mut rows = Vec::with_capacity(messages.len());
    for msg in messages {
        match msg {
            Message::DataRow(body) => rows.push(Row::new(statement.clone(), body)?),
            Message::EmptyQueryResponse
            | Message::CommandComplete(_)
            | Message::PortalSuspended => break,
            Message::ErrorResponse(body) => return Err(Error::db(body)),
            _ => return Err(Error::unexpected_message()),
        }
    }
    Ok(rows)
}

pub async fn execute(
    client: &InnerClient,
    statement: Statement,
    params: &[&dyn ToSql],
) -> Result<u64, Error> {
    let buf = encode(client, &statement, params)?;
    let statement = statement.clone();
    let messages = start(client, buf).await?;

    for msg in messages {
        match msg {
            Message::DataRow(_) => {}
            Message::CommandComplete(body) => {
                let rows = body
                    .tag()
                    .map_err(Error::parse)?
                    .rsplit(' ')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap_or(0);
                return Ok(rows);
            }
            Message::EmptyQueryResponse => return Ok(0),
            _ => return Err(Error::unexpected_message()),
        }
    }
    Err(Error::unexpected_message())
}

async fn start(client: &InnerClient, buf: Bytes) -> Result<VecDeque<Message>, Error> {
    let mut messages = client.send(FrontendMessage::Raw(buf))?.receiver.await?;

    if let Message::BindComplete = messages[0] {
        messages.pop_front();
        Ok(messages)
    } else {
        Err(Error::unexpected_message())
    }
}

pub fn encode(
    client: &InnerClient,
    statement: &Statement,
    params: &[&dyn ToSql],
) -> Result<Bytes, Error> {
    client.with_buf(|buf| {
        encode_bind(statement, params, "", buf)?;
        frontend::execute("", 0, buf);
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })
}

pub fn encode_bind(
    statement: &Statement,
    params: &[&dyn ToSql],
    portal: &str,
    buf: &mut BytesMut,
) -> Result<(), Error> {
    let params = params.iter();

    let mut error_idx = 0;
    let r = frontend::bind(
        portal,
        statement.name(),
        Some(1),
        params.zip(statement.params()).enumerate(),
        |(idx, (param, ty)), buf| match param.to_sql_checked(ty, buf) {
            Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
            Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
            Err(e) => {
                error_idx = idx;
                Err(e)
            }
        },
        Some(1),
        buf,
    );
    Ok(())
}

pub fn encode_bind_vec(
    statement: &Statement,
    params: &[&dyn ToSql],
    portal: &str,
    buf: &mut BytesVec,
) -> Result<(), Error> {
    let params = params.iter();

    let mut error_idx = 0;
    let r = frontend::bind_vec(
        portal,
        statement.name(),
        Some(1),
        params.zip(statement.params()).enumerate(),
        |(idx, (param, ty)), buf| match param.to_sql_checked_vec(ty, buf) {
            Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
            Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
            Err(e) => {
                error_idx = idx;
                Err(e)
            }
        },
        Some(1),
        buf,
    );
    Ok(())
}
