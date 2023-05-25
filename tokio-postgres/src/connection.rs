use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};
use std::{cell::RefCell, future::Future, io, mem, pin::Pin, rc::Rc};

use fallible_iterator::FallibleIterator;
use futures::{ready, Sink, Stream, StreamExt};
use log::trace;

use ntex::channel::{mpsc, pool};
use ntex::io::{Filter, Io, RecvError};
use ntex::util::{BytesMut, Either};

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
    pub sender: pool::Sender<VecDeque<Message>>,
}

#[derive(PartialEq, Debug)]
pub(crate) enum State {
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
    parameters: HashMap<String, String>,
    receiver: mpsc::Receiver<Request>,
    messages: VecDeque<Message>,
    inner: Rc<RefCell<ConnectionState>>,
}

pub(crate) struct ConnectionState {
    pub io: Io,
    pub responses: VecDeque<Response>,
    state: State,
}

impl Connection {
    pub(crate) fn new(
        io: Io,
        parameters: HashMap<String, String>,
        receiver: mpsc::Receiver<Request>,
    ) -> (Connection, Rc<RefCell<ConnectionState>>) {
        let inner = Rc::new(RefCell::new(ConnectionState {
            io,
            responses: VecDeque::new(),
            state: State::Active,
        }));
        (
            Connection {
                parameters,
                receiver,
                inner: inner.clone(),
                messages: VecDeque::new(),
            },
            inner,
        )
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Result<bool, Error> {
        let mut inner = self.inner.borrow_mut();

        if inner.state != State::Active {
            trace!("poll_read: done");
            return Ok(false);
        }

        loop {
            let message = match inner.io.decode(&PostgresCodec) {
                Ok(Some(message)) => message,
                Ok(None) => {
                    return match inner.io.poll_read_ready(cx) {
                        Poll::Ready(Ok(Some(()))) | Poll::Pending => Ok(true),
                        Poll::Ready(Ok(None)) => Ok(false),
                        Poll::Ready(Err(e)) => Err(e.into()),
                    }
                }
                Err(e) => return Ok(false),
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

            while let Some(item) = messages.next().map_err(Error::parse)? {
                if let Message::ErrorResponse(body) = item {
                    return Err(Error::db(body));
                }
                self.messages.push_back(item);
            }

            if request_complete {
                let response = match inner.responses.pop_front() {
                    Some(response) => response,
                    None => return Err(Error::unexpected_message()),
                };

                let _ = response.sender.send(mem::take(&mut self.messages));
            }
        }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();

        if inner.state == State::Closing {
            let _ = inner.io.poll_shutdown(cx);
            return Ok(());
        }

        loop {
            let result = match self.receiver.poll_next_unpin(cx) {
                Poll::Ready(Some(request)) => {
                    trace!("polled new request");
                    inner.responses.push_back(Response {
                        sender: request.sender,
                    });
                    Poll::Ready(Some(request.messages))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };

            match result {
                Poll::Ready(Some(request)) => {
                    inner
                        .io
                        .encode(request, &PostgresCodec)
                        .map_err(Error::io)?;
                    if inner.state == State::Terminating {
                        trace!("poll_write: sent eof, closing");
                        inner.state = State::Closing;
                    } else {
                        continue;
                    }
                }
                Poll::Ready(None) if inner.responses.is_empty() && inner.state == State::Active => {
                    trace!("poll_write: at eof, terminating");
                    inner.state = State::Terminating;
                    let mut request = BytesMut::new();
                    frontend::terminate(&mut request);
                    inner
                        .io
                        .encode(FrontendMessage::Raw(request.freeze()), &PostgresCodec)
                        .map_err(Error::io)?;
                    inner.state = State::Closing;
                }
                Poll::Ready(None) => {
                    trace!(
                        "poll_write: at eof, pending responses {}",
                        inner.responses.len()
                    );
                }
                Poll::Pending => {
                    trace!("poll_write: waiting for request");
                }
            };

            break;
        }
        Ok(())
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let inner = self.inner.borrow();
        if !inner.io.poll_shutdown(cx)?.is_ready() {
            if inner.state != State::Closing {
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
