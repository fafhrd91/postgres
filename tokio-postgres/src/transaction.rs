use crate::codec::FrontendMessage;
use crate::query::RowStream;
#[cfg(feature = "runtime")]
use crate::tls::MakeTlsConnect;
use crate::tls::TlsConnect;
use crate::types::{ToSql, Type};
#[cfg(feature = "runtime")]
use crate::Socket;
use crate::{bind, query, Client, Error, Portal, Row, SimpleQueryMessage, Statement, ToStatement};
use bytes::Buf;
use futures::TryStreamExt;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncWrite};

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back when dropped. Use the `commit` method to commit the changes made in the
/// transaction. Transactions can be nested, with inner transactions implemented via safepoints.
pub struct Transaction<'a> {
    client: &'a mut Client,
    depth: u32,
    done: bool,
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if self.done {
            return;
        }

        let query = if self.depth == 0 {
            "ROLLBACK".to_string()
        } else {
            format!("ROLLBACK TO sp{}", self.depth)
        };
        let buf = self.client.inner().with_buf(|buf| {
            frontend::query(&query, buf).unwrap();
            buf.split().freeze()
        });
        let _ = self.client.inner().send(FrontendMessage::Raw(buf));
    }
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(client: &'a mut Client) -> Transaction<'a> {
        Transaction {
            client,
            depth: 0,
            done: false,
        }
    }

    /// Consumes the transaction, committing all changes made within it.
    pub async fn commit(mut self) -> Result<(), Error> {
        self.done = true;
        let query = if self.depth == 0 {
            "COMMIT".to_string()
        } else {
            format!("RELEASE sp{}", self.depth)
        };
        self.client.batch_execute(&query).await
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    pub async fn rollback(mut self) -> Result<(), Error> {
        self.done = true;
        let query = if self.depth == 0 {
            "ROLLBACK".to_string()
        } else {
            format!("ROLLBACK TO sp{}", self.depth)
        };
        self.client.batch_execute(&query).await
    }

    /// Like `Client::prepare`.
    pub async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.client.prepare(query).await
    }

    /// Like `Client::prepare_typed`.
    pub async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        self.client.prepare_typed(query, parameter_types).await
    }

    /// Like `Client::query`.
    pub async fn query(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql)],
    ) -> Result<Vec<Row>, Error> {
        self.client.query(statement, params).await
    }

    /// Like `Client::query_one`.
    pub async fn query_one(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql)],
    ) -> Result<Row, Error> {
        self.client.query_one(statement, params).await
    }

    /// Like `Client::query_opt`.
    pub async fn query_opt(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql)],
    ) -> Result<Option<Row>, Error> {
        self.client.query_opt(statement, params).await
    }

    /// Like `Client::query_raw`.
    pub async fn query_raw(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql)],
    ) -> Result<RowStream, Error> {
        self.client.query_raw(statement, params).await
    }

    /// Like `Client::execute`.
    pub async fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.execute(statement, params).await
    }

    /// Like `Client::execute_iter`.
    pub async fn execute_raw(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql)],
    ) -> Result<u64, Error> {
        self.client.execute_raw(statement, params).await
    }

    /// Binds a statement to a set of parameters, creating a `Portal` which can be incrementally queried.
    ///
    /// Portals only last for the duration of the transaction in which they are created, and can only be used on the
    /// connection that created them.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub async fn bind<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.bind_raw(statement, params).await
    }

    /// A maximally flexible version of [`bind`].
    ///
    /// [`bind`]: #method.bind
    pub async fn bind_raw<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = statement.__convert().into_statement(&self.client).await?;
        bind::bind(self.client.inner(), statement, params).await
    }

    /// Continues execution of a portal, returning a stream of the resulting rows.
    ///
    /// Unlike `query`, portals can be incrementally evaluated by limiting the number of rows returned in each call to
    /// `query_portal`. If the requested number is negative or 0, all rows will be returned.
    pub async fn query_portal(&self, portal: &Portal, max_rows: i32) -> Result<Vec<Row>, Error> {
        self.query_portal_raw(portal, max_rows)
            .await?
            .try_collect()
            .await
    }

    /// The maximally flexible version of [`query_portal`].
    ///
    /// [`query_portal`]: #method.query_portal
    pub async fn query_portal_raw(
        &self,
        portal: &Portal,
        max_rows: i32,
    ) -> Result<RowStream, Error> {
        query::query_portal(self.client.inner(), portal, max_rows).await
    }

    /// Like `Client::simple_query`.
    pub async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.client.simple_query(query).await
    }

    /// Like `Client::batch_execute`.
    pub async fn batch_execute(&self, query: &str) -> Result<(), Error> {
        self.client.batch_execute(query).await
    }

    /// Like `Client::transaction`.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        let depth = self.depth + 1;
        let query = format!("SAVEPOINT sp{}", depth);
        self.batch_execute(&query).await?;

        Ok(Transaction {
            client: self.client,
            depth,
            done: false,
        })
    }
}
