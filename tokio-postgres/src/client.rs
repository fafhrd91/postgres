use bytes::{Buf, BytesMut};
use fallible_iterator::FallibleIterator;
use futures::{future, pin_mut, ready, StreamExt, TryStreamExt};
use ntex::channel::mpsc;
use ntex::codec::{AsyncRead, AsyncWrite};
use postgres_protocol::message::backend::Message;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

#[cfg(feature = "runtime")]
use crate::cancel_query;
use crate::codec::BackendMessages;
use crate::config::{Host, SslMode};
use crate::connection::{Request, RequestMessages};
use crate::copy_out::CopyOutStream;
use crate::query::RowStream;
use crate::simple_query::SimpleQueryStream;
#[cfg(feature = "runtime")]
use crate::tls::MakeTlsConnect;
use crate::tls::TlsConnect;
use crate::to_statement::ToStatement;
use crate::types::{Oid, ToSql, Type};
#[cfg(feature = "runtime")]
use crate::Socket;
use crate::{cancel_query_raw, copy_in, copy_out, query, CopyInSink, Transaction};
use crate::{prepare, SimpleQueryMessage};
use crate::{simple_query, Row};
use crate::{Error, Statement};

pub struct Responses {
    receiver: mpsc::Receiver<BackendMessages>,
    cur: BackendMessages,
}

impl Responses {
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Message, Error>> {
        loop {
            match self.cur.next().map_err(Error::parse)? {
                Some(Message::ErrorResponse(body)) => return Poll::Ready(Err(Error::db(body))),
                Some(message) => return Poll::Ready(Ok(message)),
                None => {}
            }

            match ready!(self.receiver.poll_next_unpin(cx)) {
                Some(messages) => self.cur = messages,
                None => return Poll::Ready(Err(Error::closed())),
            }
        }
    }

    pub async fn next(&mut self) -> Result<Message, Error> {
        future::poll_fn(|cx| self.poll_next(cx)).await
    }
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
}

impl InnerClient {
    pub fn send(&self, messages: RequestMessages) -> Result<Responses, Error> {
        let (sender, receiver) = mpsc::channel();
        let request = Request { messages, sender };
        self.sender.send(request).map_err(|_| Error::closed())?;

        Ok(Responses {
            receiver,
            cur: BackendMessages::empty(),
        })
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
    // #[cfg(feature = "runtime")]
    // socket_config: Option<SocketConfig>,
    // ssl_mode: SslMode,
    // process_id: i32,
    // secret_key: i32,
}

impl Client {
    pub(crate) fn new(
        sender: mpsc::Sender<Request>,
        ssl_mode: SslMode,
        process_id: i32,
        secret_key: i32,
    ) -> Client {
        Client {
            inner: Rc::new(InnerClient {
                sender,
                state: UnsafeCell::new(State {
                    typeinfo: None,
                    typeinfo_composite: None,
                    typeinfo_enum: None,
                    types: HashMap::new(),
                    buf: BytesMut::new(),
                }),
            }),
            //#[cfg(feature = "runtime")]
            //socket_config: None,
            //ssl_mode,
            //process_id,
            //secret_key,
        }
    }

    pub(crate) fn inner(&self) -> &Rc<InnerClient> {
        &self.inner
    }

    #[cfg(feature = "runtime")]
    pub(crate) fn set_socket_config(&mut self, socket_config: SocketConfig) {
        // self.socket_config = Some(socket_config);
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
    pub async fn query(
        &self,
        statement: &Statement,
        params: &[&(dyn ToSql)],
    ) -> Result<Vec<Row>, Error> {
        self.query_raw(statement, params).await?.try_collect().await
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
        let fut = self.query_raw(statement, params);

        async move {
            let stream = fut.await?;
            pin_mut!(stream);

            let row = match stream.try_next().await? {
                Some(row) => row,
                None => return Err(Error::row_count()),
            };

            if stream.try_next().await?.is_some() {
                return Err(Error::row_count());
            }

            Ok(row)
        }
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
        let stream = self.query_raw(statement, params).await?;
        pin_mut!(stream);

        let row = match stream.try_next().await? {
            Some(row) => row,
            None => return Ok(None),
        };

        if stream.try_next().await?.is_some() {
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
    ) -> impl Future<Output = Result<RowStream, Error>> {
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

    /// Executes a `COPY FROM STDIN` statement, returning a sink used to write the copy data.
    ///
    /// PostgreSQL does not support parameters in `COPY` statements, so this method does not take any. The copy *must*
    /// be explicitly completed via the `Sink::close` or `finish` methods. If it is not, the copy will be aborted.
    ///
    /// # Panics
    ///
    /// Panics if the statement contains parameters.
    pub async fn copy_in<T, U>(&self, statement: &T) -> Result<CopyInSink<U>, Error>
    where
        T: ?Sized + ToStatement,
        U: Buf + 'static,
    {
        let statement = statement.__convert().into_statement(self).await?;
        copy_in::copy_in(self.inner(), statement).await
    }

    /// Executes a `COPY TO STDOUT` statement, returning a stream of the resulting data.
    ///
    /// PostgreSQL does not support parameters in `COPY` statements, so this method does not take any.
    ///
    /// # Panics
    ///
    /// Panics if the statement contains parameters.
    pub async fn copy_out<T>(&self, statement: &T) -> Result<CopyOutStream, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = statement.__convert().into_statement(self).await?;
        copy_out::copy_out(self.inner(), statement).await
    }

    /// Executes a sequence of SQL statements using the simple query protocol, returning the resulting rows.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. The simple query protocol returns the values in rows as strings rather than in their binary encodings,
    /// so the associated row type doesn't work with the `FromSql` trait. Rather than simply returning a list of the
    /// rows, this method returns a list of an enum which indicates either the completion of one of the commands,
    /// or a row of data. This preserves the framing between the separate statements in the request.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query_raw(query).await?.try_collect().await
    }

    pub(crate) async fn simple_query_raw(&self, query: &str) -> Result<SimpleQueryStream, Error> {
        simple_query::simple_query(self.inner(), query).await
    }

    /// Executes a sequence of SQL statements using the simple query protocol.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. This is intended for use when, for example, initializing a database schema.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub async fn batch_execute(&self, query: &str) -> Result<(), Error> {
        simple_query::batch_execute(self.inner(), query).await
    }

    /// Begins a new database transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.batch_execute("BEGIN").await?;
        Ok(Transaction::new(self))
    }

    /// Attempts to cancel an in-progress query.
    ///
    /// The server provides no information about whether a cancellation attempt was successful or not. An error will
    /// only be returned if the client was unable to connect to the database.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    #[cfg(feature = "runtime")]
    pub async fn cancel_query<T>(&self, _tls: T) -> Result<(), Error>
    where
        T: MakeTlsConnect<Socket>,
    {
        // cancel_query::cancel_query(
        //     self.socket_config.clone(),
        //     self.ssl_mode,
        //     tls,
        //     self.process_id,
        //     self.secret_key,
        // )
        // .await
        Ok(())
    }

    /// Like `cancel_query`, but uses a stream which is already connected to the server rather than opening a new
    /// connection itself.
    pub async fn cancel_query_raw<S, T>(&self, _stream: S, _tls: T) -> Result<(), Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: TlsConnect<S>,
    {
        // cancel_query_raw::cancel_query_raw(
        //     stream,
        //     self.ssl_mode,
        //     tls,
        //     self.process_id,
        //     self.secret_key,
        // )
        // .await
        Ok(())
    }
}
