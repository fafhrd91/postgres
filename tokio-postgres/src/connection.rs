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
use ntex::framed::{ReadTask, State as IoState, WriteTask};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};
use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc};

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
pub struct Connection {
    io: IoState,
    codec: PostgresCodec,
    parameters: HashMap<String, String>,
    receiver: mpsc::Receiver<Request>,
    responses: VecDeque<Response>,
    state: State,
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

        let io = io
            .low_watermark(1024)
            .read_high_watermark(65535)
            .write_high_watermark(65535);

        let steram = Rc::new(RefCell::new(stream));
        ntex::rt::spawn(ReadTask::new(stream.clone(), io.clone()));
        ntex::rt::spawn(WriteTask::new(stream, io.clone()));

        Connection {
            io,
            codec,
            parameters,
            receiver,
            responses: VecDeque::new(),
            state: State::Active,
        }
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Result<Option<AsyncMessage>, Error> {
        if self.state != State::Active {
            trace!("poll_read: done");
            return Ok(None);
        }

        loop {
            let message = match self.io.decode_item(&self.codec).map_err(|e| Error::io(e))? {
                Some(message) => message,
                None => {
                    self.io.dsp_read_more_data(cx.waker());
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

    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        if self.state == State::Closing {
            self.io.shutdown_io();
            return Ok(());
        }

        loop {
            match self.poll_request(cx) {
                Poll::Ready(Some(request)) => match request {
                    RequestMessages::Single(request) => {
                        self.io
                            .write_item(request, &self.codec)
                            .map_err(Error::io)?;
                        if self.state == State::Terminating {
                            trace!("poll_write: sent eof, closing");
                            self.state = State::Closing;
                        } else {
                            continue;
                        }
                    }
                    RequestMessages::CopyIn(mut receiver) => {
                        let recv = &mut receiver;
                        match recv.poll_next_unpin(cx) {
                            Poll::Ready(Some(message)) => {
                                self.io
                                    .write_item(message, &self.codec)
                                    .map_err(Error::io)?;
                            }
                            Poll::Ready(None) => {
                                trace!("poll_write: finished copy_in request");
                                break;
                            }
                            Poll::Pending => {
                                trace!("poll_write: waiting on copy_in stream");
                                continue;
                            }
                        }
                    }
                },
                Poll::Ready(None) if self.responses.is_empty() && self.state == State::Active => {
                    trace!("poll_write: at eof, terminating");
                    self.state = State::Terminating;
                    let mut request = BytesMut::new();
                    frontend::terminate(&mut request);
                    self.io
                        .write_item(FrontendMessage::Raw(request.freeze()), &self.codec)
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

    /// Polls for asynchronous messages from the server.
    ///
    /// The server can send notices as well as notifications asynchronously to the client. Applications that wish to
    /// examine those messages should use this method to drive the connection rather than its `Future` implementation.
    pub fn poll_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AsyncMessage, Error>>> {
        let message = self.poll_read(cx)?;
        let _ = self.poll_write(cx)?;

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

impl Future for Connection {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.io.is_dsp_stopped() {
            return Poll::Ready(Ok(()));
        }

        while let Some(_) = ready!(self.poll_message(cx)?) {}
        Poll::Ready(Ok(()))
    }
}
