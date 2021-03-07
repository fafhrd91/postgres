use bytes::Bytes;
use fallible_iterator::FallibleIterator;
use futures::{ready, Stream};
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::VecDeque;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::{Error, SimpleQueryMessage, SimpleQueryRow};

pub async fn simple_query(client: &InnerClient, query: &str) -> Result<SimpleQueryStream, Error> {
    debug!("executing simple query: {}", query);

    let buf = encode(client, query)?;
    let messages = client.send(FrontendMessage::Raw(buf))?.receiver.await?;

    Ok(SimpleQueryStream {
        messages,
        columns: None,
    })
}

pub async fn batch_execute(client: &InnerClient, query: &str) -> Result<(), Error> {
    debug!("executing statement batch: {}", query);

    let buf = encode(client, query)?;
    let mut responses = client
        .send(FrontendMessage::Raw(buf))?
        .receiver
        .await?
        .into_iter();

    loop {
        match responses.next() {
            Some(Message::ReadyForQuery(_)) => return Ok(()),
            Some(Message::CommandComplete(_))
            | Some(Message::EmptyQueryResponse)
            | Some(Message::RowDescription(_))
            | Some(Message::DataRow(_)) => {}
            _ => return Err(Error::unexpected_message()),
        }
    }
}

fn encode(client: &InnerClient, query: &str) -> Result<Bytes, Error> {
    client.with_buf(|buf| {
        frontend::query(query, buf).map_err(Error::encode)?;
        Ok(buf.split().freeze())
    })
}

/// A stream of simple query results.
pub struct SimpleQueryStream {
    messages: VecDeque<Message>,
    columns: Option<Rc<[String]>>,
}

impl Stream for SimpleQueryStream {
    type Item = Result<SimpleQueryMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut();
        loop {
            match this.messages.pop_front() {
                Some(Message::CommandComplete(body)) => {
                    let rows = body
                        .tag()
                        .map_err(Error::parse)?
                        .rsplit(' ')
                        .next()
                        .unwrap()
                        .parse()
                        .unwrap_or(0);
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(rows))));
                }
                Some(Message::EmptyQueryResponse) => {
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(0))));
                }
                Some(Message::RowDescription(body)) => {
                    let columns = body
                        .fields()
                        .map(|f| Ok(f.name().to_string()))
                        .collect::<Vec<_>>()
                        .map_err(Error::parse)?
                        .into();
                    this.columns = Some(columns);
                }
                Some(Message::DataRow(body)) => {
                    let row = match &this.columns {
                        Some(columns) => SimpleQueryRow::new(columns.clone(), body)?,
                        None => return Poll::Ready(Some(Err(Error::unexpected_message()))),
                    };
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::Row(row))));
                }
                Some(Message::ReadyForQuery(_)) => return Poll::Ready(None),
                _ => return Poll::Ready(Some(Err(Error::unexpected_message()))),
            }
        }
    }
}
