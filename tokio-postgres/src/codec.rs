use fallible_iterator::FallibleIterator;
use ntex::codec::{Decoder, Encoder};
use ntex::util::{Buf, Bytes, BytesMut};
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend::CopyData;
use std::io;

pub enum FrontendMessage {
    Raw(Bytes),
    CopyData(CopyData<Box<dyn Buf>>),
}

pub enum BackendMessage {
    Normal {
        messages: BackendMessages,
        request_complete: bool,
    },
    Async(backend::Message),
}

#[derive(Debug)]
pub struct BackendMessages(Bytes);

impl BackendMessages {
    pub fn empty() -> BackendMessages {
        BackendMessages(Bytes::new())
    }
}

impl FallibleIterator for BackendMessages {
    type Item = backend::Message;
    type Error = io::Error;

    fn next(&mut self) -> io::Result<Option<backend::Message>> {
        let result = backend::Message::parse_buffer_bytes(&mut self.0)?;
        if let Some((tag, mut buf)) = result {
            backend::Message::parse(tag, &mut buf)
        } else {
            Ok(None)
        }
    }
}

pub struct PostgresCodec;

impl Encoder for PostgresCodec {
    type Item = FrontendMessage;
    type Error = io::Error;

    fn encode(&self, item: FrontendMessage, dst: &mut BytesMut) -> io::Result<()> {
        match item {
            FrontendMessage::Raw(buf) => dst.extend_from_slice(&buf),
            FrontendMessage::CopyData(data) => data.write(dst),
        }

        Ok(())
    }
}

impl Decoder for PostgresCodec {
    type Item = BackendMessage;
    type Error = io::Error;

    fn decode(&self, src: &mut BytesMut) -> Result<Option<BackendMessage>, io::Error> {
        let mut idx = 0;
        let mut request_complete = false;

        while let Some(header) = backend::Header::parse(&src[idx..])? {
            let len = header.len() as usize + 1;
            if src[idx..].len() < len {
                break;
            }

            match header.tag() {
                backend::NOTICE_RESPONSE_TAG
                | backend::NOTIFICATION_RESPONSE_TAG
                | backend::PARAMETER_STATUS_TAG => {
                    if idx == 0 {
                        let (tag, mut buf) = backend::Message::parse_buffer(src)?.unwrap();
                        let message = backend::Message::parse(tag, &mut buf)?.unwrap();
                        return Ok(Some(BackendMessage::Async(message)));
                    } else {
                        break;
                    }
                }
                _ => {}
            }

            idx += len;

            if header.tag() == backend::READY_FOR_QUERY_TAG {
                request_complete = true;
                break;
            }
        }

        if idx == 0 {
            Ok(None)
        } else {
            Ok(Some(BackendMessage::Normal {
                messages: BackendMessages(src.split_to(idx)),
                request_complete,
            }))
        }
    }
}
