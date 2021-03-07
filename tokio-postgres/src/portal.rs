use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::Statement;
use postgres_protocol::message::frontend;
use std::rc::{Rc, Weak};

struct Inner {
    client: Weak<InnerClient>,
    name: String,
    statement: Statement,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            let buf = client.with_buf(|buf| {
                frontend::close(b'P', &self.name, buf).unwrap();
                frontend::sync(buf);
                buf.split().freeze()
            });
            let _ = client.send(FrontendMessage::Raw(buf));
        }
    }
}

/// A portal.
///
/// Portals can only be used with the connection that created them, and only exist for the duration of the transaction
/// in which they were created.
#[derive(Clone)]
pub struct Portal(Rc<Inner>);

impl Portal {
    pub(crate) fn new(client: &Rc<InnerClient>, name: String, statement: Statement) -> Portal {
        Portal(Rc::new(Inner {
            client: Rc::downgrade(client),
            name,
            statement,
        }))
    }

    pub(crate) fn name(&self) -> &str {
        &self.0.name
    }

    pub(crate) fn statement(&self) -> &Statement {
        &self.0.statement
    }
}
