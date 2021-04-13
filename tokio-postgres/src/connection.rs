use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};
use std::{cell::RefCell, future::Future, io, mem, pin::Pin, rc::Rc};

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures::{ready, Sink, Stream, StreamExt};
use log::trace;
use ntex::channel::{mpsc, pool};
use ntex::codec::{AsyncRead, AsyncWrite, Framed};
use ntex::framed::{ReadTask, State as IoState, WriteTask};

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
    io: IoState,
    codec: PostgresCodec,
    parameters: HashMap<String, String>,
    receiver: mpsc::Receiver<Request>,
    responses: VecDeque<Response>,
    state: State,
    messages: VecDeque<Message>,
}

impl Connection {
    pub(crate) fn new<S, T>(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        parameters: HashMap<String, String>,
        receiver: mpsc::Receiver<Request>,
    ) -> Connection
    where
        S: AsyncRead + AsyncWrite + Unpin + 'static,
        T: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let (stream, codec, io) = IoState::from_framed(stream);
        io.set_buffer_params(65535, 65535, 1024);

        let stream = Rc::new(RefCell::new(stream));
        ntex::rt::spawn(ReadTask::new(stream.clone(), io.clone()));
        ntex::rt::spawn(WriteTask::new(stream, io.clone()));

        Connection {
            io,
            codec,
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

        let read = self.io.read();

        loop {
            let message = match read.decode(&self.codec).map_err(|e| Error::io(e))? {
                Some(message) => message,
                None => {
                    read.wake(cx.waker());
                    return Ok(true);
                }
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
            self.io.shutdown_io();
            return Ok(());
        }

        let write = self.io.write();

        loop {
            let result = match self.receiver.poll_next_unpin(cx) {
                Poll::Ready(Some(request)) => {
                    trace!("polled new request");
                    self.responses.push_back(Response {
                        sender: request.sender,
                    });
                    Poll::Ready(Some(request.messages))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };

            match result {
                Poll::Ready(Some(request)) => {
                    write.encode(request, &self.codec).map_err(Error::io)?;
                    if self.state == State::Terminating {
                        trace!("poll_write: sent eof, closing");
                        self.state = State::Closing;
                    } else {
                        continue;
                    }
                }
                Poll::Ready(None) if self.responses.is_empty() && self.state == State::Active => {
                    trace!("poll_write: at eof, terminating");
                    self.state = State::Terminating;
                    let mut request = BytesMut::new();
                    frontend::terminate(&mut request);
                    write
                        .encode(FrontendMessage::Raw(request.freeze()), &self.codec)
                        .map_err(Error::io)?;
                    self.state = State::Closing;
                }
                Poll::Ready(None) => {
                    trace!(
                        "poll_write: at eof, pending responses {}",
                        self.responses.len()
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
        if self.state != State::Closing {
            return Poll::Pending;
        }
        self.io.shutdown_io();
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
        if self.io.is_dispatcher_stopped() {
            return Poll::Ready(Ok(()));
        }

        let active = self.poll_read(cx)?;
        let _ = self.poll_write(cx)?;

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
