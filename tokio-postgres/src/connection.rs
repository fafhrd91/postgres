use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::copy_in::CopyInReceiver;
use crate::error::DbError;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::{AsyncMessage, Error, Notification};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures::{ready, Sink, Stream, StreamExt};
use log::trace;
use ntex::channel::mpsc;
use ntex::codec::{AsyncRead, AsyncWrite, Framed};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub enum RequestMessages {
    Single(FrontendMessage),
    CopyIn(CopyInReceiver),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::Sender<BackendMessages>,
}

pub struct Response {
    sender: mpsc::Sender<BackendMessages>,
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
pub struct Connection<S, T> {
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    parameters: HashMap<String, String>,
    receiver: mpsc::Receiver<Request>,
    pending_request: Option<RequestMessages>,
    pending_response: Option<BackendMessage>,
    responses: VecDeque<Response>,
    state: State,
}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        parameters: HashMap<String, String>,
        receiver: mpsc::Receiver<Request>,
    ) -> Connection<S, T> {
        Connection {
            stream,
            parameters,
            receiver,
            pending_request: None,
            pending_response: None,
            responses: VecDeque::new(),
            state: State::Active,
        }
    }

    fn poll_response(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<BackendMessage, Error>>> {
        if let Some(message) = self.pending_response.take() {
            trace!("retrying pending response");
            return Poll::Ready(Some(Ok(message)));
        }

        Pin::new(&mut self.stream)
            .poll_next(cx)
            .map(|o| o.map(|r| r.map_err(Error::io)))
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Result<Option<AsyncMessage>, Error> {
        if self.state != State::Active {
            trace!("poll_read: done");
            return Ok(None);
        }

        loop {
            let message = match self.poll_response(cx)? {
                Poll::Ready(Some(message)) => message,
                Poll::Ready(None) => return Err(Error::closed()),
                Poll::Pending => {
                    trace!("poll_read: waiting on response");
                    return Ok(None);
                }
            };

            let (mut messages, request_complete) = match message {
                BackendMessage::Async(Message::NoticeResponse(body)) => {
                    let error = DbError::parse(&mut body.fields()).map_err(Error::parse)?;
                    return Ok(Some(AsyncMessage::Notice(error)));
                }
                BackendMessage::Async(Message::NotificationResponse(body)) => {
                    let notification = Notification {
                        process_id: body.process_id(),
                        channel: body.channel().map_err(Error::parse)?.to_string(),
                        payload: body.message().map_err(Error::parse)?.to_string(),
                    };
                    return Ok(Some(AsyncMessage::Notification(notification)));
                }
                BackendMessage::Async(Message::ParameterStatus(body)) => {
                    self.parameters.insert(
                        body.name().map_err(Error::parse)?.to_string(),
                        body.value().map_err(Error::parse)?.to_string(),
                    );
                    continue;
                }
                BackendMessage::Async(_) => unreachable!(),
                BackendMessage::Normal {
                    messages,
                    request_complete,
                } => (messages, request_complete),
            };

            let response = match self.responses.pop_front() {
                Some(response) => response,
                None => match messages.next().map_err(Error::parse)? {
                    Some(Message::ErrorResponse(error)) => return Err(Error::db(error)),
                    _ => return Err(Error::unexpected_message()),
                },
            };

            let _ = response.sender.send(messages);
            if !request_complete {
                self.responses.push_front(response);
            }
        }
    }

    fn poll_request(&mut self, cx: &mut Context<'_>) -> Poll<Option<RequestMessages>> {
        if let Some(messages) = self.pending_request.take() {
            trace!("retrying pending request");
            return Poll::Ready(Some(messages));
        }

        match self.receiver.poll_next_unpin(cx) {
            Poll::Ready(Some(request)) => {
                trace!("polled new request");
                self.responses.push_back(Response {
                    sender: request.sender,
                });
                Poll::Ready(Some(request.messages))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<bool, Error> {
        loop {
            if self.state == State::Closing {
                trace!("poll_write: done");
                return Ok(false);
            }

            if let Poll::Pending = Pin::new(&mut self.stream)
                .poll_ready(cx)
                .map_err(Error::io)?
            {
                trace!("poll_write: waiting on socket");
                return Ok(false);
            }

            let request = match self.poll_request(cx) {
                Poll::Ready(Some(request)) => request,
                Poll::Ready(None) if self.responses.is_empty() && self.state == State::Active => {
                    trace!("poll_write: at eof, terminating");
                    self.state = State::Terminating;
                    let mut request = BytesMut::new();
                    frontend::terminate(&mut request);
                    RequestMessages::Single(FrontendMessage::Raw(request.freeze()))
                }
                Poll::Ready(None) => {
                    trace!(
                        "poll_write: at eof, pending responses {}",
                        self.responses.len()
                    );
                    return Ok(true);
                }
                Poll::Pending => {
                    trace!("poll_write: waiting on request");
                    return Ok(true);
                }
            };

            match request {
                RequestMessages::Single(request) => {
                    Pin::new(&mut self.stream)
                        .start_send(request)
                        .map_err(Error::io)?;
                    if self.state == State::Terminating {
                        trace!("poll_write: sent eof, closing");
                        self.state = State::Closing;
                    }
                }
                RequestMessages::CopyIn(mut receiver) => {
                    let message = match receiver.poll_next_unpin(cx) {
                        Poll::Ready(Some(message)) => message,
                        Poll::Ready(None) => {
                            trace!("poll_write: finished copy_in request");
                            continue;
                        }
                        Poll::Pending => {
                            trace!("poll_write: waiting on copy_in stream");
                            self.pending_request = Some(RequestMessages::CopyIn(receiver));
                            return Ok(true);
                        }
                    };
                    Pin::new(&mut self.stream)
                        .start_send(message)
                        .map_err(Error::io)?;
                    self.pending_request = Some(RequestMessages::CopyIn(receiver));
                }
            }
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        match Pin::new(&mut self.stream)
            .poll_flush(cx)
            .map_err(Error::io)?
        {
            Poll::Ready(()) => trace!("poll_flush: flushed"),
            Poll::Pending => trace!("poll_flush: waiting on socket"),
        }
        Ok(())
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.state != State::Closing {
            return Poll::Pending;
        }

        match Pin::new(&mut self.stream)
            .poll_close(cx)
            .map_err(Error::io)?
        {
            Poll::Ready(()) => {
                trace!("poll_shutdown: complete");
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                trace!("poll_shutdown: waiting on socket");
                Poll::Pending
            }
        }
    }

    /// Returns the value of a runtime parameter for this connection.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| &**s)
    }

    /// Polls for asynchronous messages from the server.
    ///
    /// The server can send notices as well as notifications asynchronously to the client. Applications that wish to
    /// examine those messages should use this method to drive the connection rather than its `Future` implementation.
    pub fn poll_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AsyncMessage, Error>>> {
        let message = self.poll_read(cx)?;
        let want_flush = self.poll_write(cx)?;
        if want_flush {
            self.poll_flush(cx)?;
        }
        match message {
            Some(message) => Poll::Ready(Some(Ok(message))),
            None => match self.poll_shutdown(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(None),
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

impl<S, T> Future for Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        while let Some(_) = ready!(self.poll_message(cx)?) {}
        Poll::Ready(Ok(()))
    }
}
