use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::types::ToSql;
use crate::{query, Error, Portal, Statement};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

pub async fn bind(
    client: &Rc<InnerClient>,
    statement: Statement,
    params: &[&(dyn ToSql)],
) -> Result<Portal, Error> {
    let name = format!("p{}", NEXT_ID.fetch_add(1, Ordering::SeqCst));
    let buf = client.with_buf(|buf| {
        query::encode_bind(&statement, params, &name, buf)?;
        frontend::sync(buf);
        Ok::<_, Error>(buf.split().freeze())
    })?;

    let responses = client.send(FrontendMessage::Raw(buf))?;
    let msg = responses.receiver.await?;
    if let Message::BindComplete = msg[0] {
        Ok(Portal::new(client, name, statement))
    } else {
        Err(Error::unexpected_message())
    }
}
