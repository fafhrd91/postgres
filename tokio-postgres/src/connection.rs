use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};
use std::{cell::RefCell, future::Future, io, mem, pin::Pin, rc::Rc};

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures::{ready, Sink, Stream, StreamExt};
use log::trace;

use ntex::channel::{mpsc, pool};
use ntex::io::{Filter, Io, RecvError};
use ntex::util::Either;

use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::{error::DbError, AsyncMessage, Error, Notification};

pub struct Request {
    pub messages: FrontendMessage,
    pub sender: pool::Sender<VecDeque<Message>>,
}

pub struct Response {
    sender: pool::Sender<VecDeque<Message>>,
}

#[derive(PartialEq, Debug)]
enum State {
    Active,
    Terminating,
    Closing,
}

/// A connection to a PostgreSQL database.
///
/// This is one half of what is returned when a new connection is established. It performs the actual IO with the
/// server, and should generally be spawned off onto an executor to run in the background.
///
/// `Connection` implements `Future`, and only resolves when the connection is closed, either because a fatal error has
/// occurred, or because its associated `Client` has dropped and all outstanding work has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Connection {
    io: Io,
    parameters: HashMap<String, String>,
    receiver: mpsc::Receiver<Request>,
    responses: VecDeque<Response>,
    state: State,
    messages: VecDeque<Message>,
}

impl Connection {
    pub(crate) fn new(
        io: Io,
        parameters: HashMap<String, String>,
        receiver: mpsc::Receiver<Request>,
    ) -> Connection {
        Connection {
            io,
            parameters,
            receiver,
            responses: VecDeque::new(),
            state: State::Active,
            messages: VecDeque::new(),
        }
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Result<bool, Error> {
        if self.state != State::Active {
            trace!("poll_read: done");
            return Ok(false);
        }

        loop {
            let message = match self.io.poll_recv(&PostgresCodec, cx) {
                Poll::Ready(Ok(message)) => message,
                Poll::Ready(Err(RecvError::Stop)) => return Ok(false),
                Poll::Ready(Err(RecvError::PeerGone(None))) => return Ok(false),
                Poll::Ready(Err(RecvError::PeerGone(Some(e)))) => return Err(e.into()),
                Poll::Ready(Err(RecvError::WriteBackpressure)) => {
                    if self.io.poll_flush(cx, false).is_pending() {
                        return Ok(true);
                    }
                    continue;
                }
                Poll::Ready(Err(_)) => return Ok(false),
                Poll::Pending => return Ok(true),
            };

            let (mut messages, request_complete) = match message {
                BackendMessage::Normal {
                    messages,
                    request_complete,
                } => (messages, request_complete),
                _ => {
                    return Err(Error::io(io::Error::new(
                        io::ErrorKind::Other,
                        "unsupported",
                    )))
                }
            };

            loop {
                if let Some(item) = messages.next().map_err(Error::parse)? {
                    if let Message::ErrorResponse(body) = item {
                        return Err(Error::db(body));
                    }
                    if matches!(item, Message::ReadyForQuery(_)) {
                        continue;
                    }

                    let complete = matches!(item, Message::CommandComplete(_));
                    self.messages.push_back(item);

                    if complete {
                        let response = match self.responses.pop_front() {
                            Some(response) => response,
                            None => return Err(Error::unexpected_message()),
                        };

                        let _ = response.sender.send(mem::take(&mut self.messages));
                    }
                } else {
                    break;
                }
            }

            if request_complete && !self.messages.is_empty() {
                let response = match self.responses.pop_front() {
                    Some(response) => response,
                    None => return Err(Error::unexpected_message()),
                };

                let _ = response.sender.send(mem::take(&mut self.messages));
            }
        }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        if self.state == State::Closing {
            let _ = self.io.poll_shutdown(cx);
            return Ok(());
        }
        let mut idx = 0;

        loop {
            match self.receiver.poll_recv(cx) {
                Poll::Ready(Some(request)) => {
                    trace!("polled new request");

                    if request.messages.is_query() {
                        idx += 1;
                    } else {
                        idx = 0;
                    }
                    self.responses.push_back(Response {
                        sender: request.sender,
                    });
                    self.io
                        .encode(request.messages, &PostgresCodec)
                        .map_err(Error::io)?;
                    if self.state == State::Terminating {
                        trace!("poll_write: sent eof, closing");
                        self.state = State::Closing;
                    } else {
                        if idx > 50 {
                            idx = 0;
                            let mut request = BytesMut::new();
                            frontend::sync(&mut request);
                            self.io
                                .encode(FrontendMessage::Raw(request.freeze()), &PostgresCodec)
                                .map_err(Error::io)?;
                        }
                        continue;
                    }
                }
                Poll::Ready(None) => {
                    if self.responses.is_empty() && self.state == State::Active {
                        trace!("poll_write: at eof, terminating");
                        self.state = State::Terminating;
                        let mut request = BytesMut::new();
                        frontend::terminate(&mut request);
                        self.io
                            .encode(FrontendMessage::Raw(request.freeze()), &PostgresCodec)
                            .map_err(Error::io)?;
                        self.state = State::Closing;
                    } else {
                        trace!(
                            "poll_write: at eof, pending responses {}",
                            self.responses.len()
                        );
                    }
                }
                Poll::Pending => (),
            };

            break;
        }

        if idx != 0 {
            let mut request = BytesMut::new();
            frontend::sync(&mut request);
            self.io
                .encode(FrontendMessage::Raw(request.freeze()), &PostgresCodec)
                .map_err(Error::io)?;
        }

        Ok(())
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if !self.io.poll_shutdown(cx)?.is_ready() {
            if self.state != State::Closing {
                return Poll::Pending;
            }
        }
        Poll::Ready(Ok(()))
    }

    /// Returns the value of a runtime parameter for this connection.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| &**s)
    }
}

impl Future for Connection {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let _ = self.poll_write(cx)?;
        let active = self.poll_read(cx)?;

        if active {
            Poll::Pending
        } else {
            match self.poll_shutdown(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
