use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};
use std::{cell::RefCell, cell::UnsafeCell, future::Future, rc::Rc, time::Duration};

use fallible_iterator::FallibleIterator;
use futures::{future, pin_mut, ready, StreamExt, TryStreamExt};
use ntex::channel::{mpsc, pool};
use ntex::util::{Buf, BytesMut};
use postgres_protocol::message::backend::Message;

use crate::codec::{BackendMessages, FrontendMessage};
use crate::config::{Host, SslMode};
use crate::connection::{ConnectionState, Request};
use crate::tls::MakeTlsConnect;
use crate::tls::TlsConnect;
use crate::to_statement::ToStatement;
use crate::types::{Oid, ToSql, Type};
use crate::{cancel_query_raw, prepare, query, Error, Row, Socket, Statement};

pub struct Responses {
    pub receiver: pool::Receiver<VecDeque<Message>>,
}

struct State {
    typeinfo: Option<Statement>,
    typeinfo_composite: Option<Statement>,
    typeinfo_enum: Option<Statement>,
    types: HashMap<Oid, Type>,
    buf: BytesMut,
}

pub struct InnerClient {
    sender: mpsc::Sender<Request>,
    state: UnsafeCell<State>,
    pub(crate) pool: pool::Pool<VecDeque<Message>>,
    pub(crate) con: Rc<RefCell<ConnectionState>>,
}

impl InnerClient {
    pub fn send(&self, messages: FrontendMessage) -> Result<Responses, Error> {
        let (sender, receiver) = self.pool.channel();
        let request = Request { messages, sender };
        self.sender.send(request).map_err(|_| Error::closed())?;

        Ok(Responses { receiver })
    }

    fn lock(&self) -> &mut State {
        unsafe { &mut *self.state.get() }
    }

    pub fn typeinfo(&self) -> Option<Statement> {
        self.lock().typeinfo.clone()
    }

    pub fn set_typeinfo(&self, statement: &Statement) {
        self.lock().typeinfo = Some(statement.clone());
    }

    pub fn typeinfo_composite(&self) -> Option<Statement> {
        self.lock().typeinfo_composite.clone()
    }

    pub fn set_typeinfo_composite(&self, statement: &Statement) {
        self.lock().typeinfo_composite = Some(statement.clone());
    }

    pub fn typeinfo_enum(&self) -> Option<Statement> {
        self.lock().typeinfo_enum.clone()
    }

    pub fn set_typeinfo_enum(&self, statement: &Statement) {
        self.lock().typeinfo_enum = Some(statement.clone());
    }

    pub fn type_(&self, oid: Oid) -> Option<Type> {
        self.lock().types.get(&oid).cloned()
    }

    pub fn set_type(&self, oid: Oid, type_: &Type) {
        self.lock().types.insert(oid, type_.clone());
    }

    pub fn with_buf<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut BytesMut) -> R,
    {
        let state = self.lock();
        let r = f(&mut state.buf);
        state.buf.clear();
        r
    }
}

#[derive(Clone)]
pub(crate) struct SocketConfig {
    pub host: Host,
    pub port: u16,
    pub connect_timeout: Option<Duration>,
    pub keepalives: bool,
    pub keepalives_idle: Duration,
}

/// An asynchronous PostgreSQL client.
///
/// The client is one half of what is returned when a connection is established. Users interact with the database
/// through this client object.
#[derive(Clone)]
pub struct Client {
    inner: Rc<InnerClient>,
}

impl Client {
    pub(crate) fn new(
        sender: mpsc::Sender<Request>,
        ssl_mode: SslMode,
        process_id: i32,
        secret_key: i32,
        con: Rc<RefCell<ConnectionState>>,
    ) -> Client {
        Client {
            inner: Rc::new(InnerClient {
                con,
                sender,
                pool: pool::new(),
                state: UnsafeCell::new(State {
                    typeinfo: None,
                    typeinfo_composite: None,
                    typeinfo_enum: None,
                    types: HashMap::new(),
                    buf: BytesMut::new(),
                }),
            }),
        }
    }

    pub(crate) fn inner(&self) -> &Rc<InnerClient> {
        &self.inner
    }

    /// Creates a new prepared statement.
    ///
    /// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
    /// which are set when executed. Prepared statements can only be used with the connection that created them.
    pub async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare_typed(query, &[]).await
    }

    /// Like `prepare`, but allows the types of query parameters to be explicitly specified.
    ///
    /// The list of types may be smaller than the number of parameters - the types of the remaining parameters will be
    /// inferred. For example, `client.prepare_typed(query, &[])` is equivalent to `client.prepare(query)`.
    pub async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        prepare::prepare(&self.inner, query, parameter_types).await
    }

    /// Executes a statement, returning a vector of the resulting rows.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn query(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql)],
    ) -> impl Future<Output = Result<Vec<Row>, Error>> {
        self.query_raw(statement, params)
    }

    /// Executes a statement which returns a single row, returning it.
    ///
    /// Returns an error if the query does not return exactly one row.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn query_one(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql)],
    ) -> impl Future<Output = Result<Row, Error>> {
        query::query_one(&self.inner, statement, params)
    }

    /// Executes a statements which returns zero or one rows, returning it.
    ///
    /// Returns an error if the query returns more than one row.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub async fn query_opt(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql)],
    ) -> Result<Option<Row>, Error> {
        let rows = self.query_raw(statement, params).await?;

        let mut iter = rows.into_iter();
        let row = match iter.next() {
            Some(row) => row,
            None => return Ok(None),
        };

        if iter.next().is_some() {
            return Err(Error::row_count());
        }

        Ok(Some(row))
    }

    /// The maximally flexible version of [`query`].
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    ///
    /// [`query`]: #method.query
    pub fn query_raw(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql)],
    ) -> impl Future<Output = Result<Vec<Row>, Error>> {
        query::query(&self.inner, statement, params)
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// If the statement does not modify any rows (e.g. `SELECT`), 0 is returned.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub async fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.execute_raw(statement, params).await
    }

    /// The maximally flexible version of [`execute`].
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    ///
    /// [`execute`]: #method.execute
    pub async fn execute_raw<T>(&self, statement: &T, params: &[&(dyn ToSql)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = statement.__convert().into_statement(self).await?;
        query::execute(self.inner(), statement, params).await
    }

    pub(crate) async fn simple_query_raw(
        &self,
        query: &str,
    ) -> Result<crate::SimpleQueryStream, Error> {
        crate::simple_query::simple_query(self.inner(), query).await
    }
}
