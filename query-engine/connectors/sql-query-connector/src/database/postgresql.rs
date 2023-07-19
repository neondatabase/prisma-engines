#![allow(implied_bounds_entailment)]

use super::connection::SqlConnection;
use crate::SqlError;
use async_trait::async_trait;
use connector_interface::{
    error::{ConnectorError, ErrorKind},
    Connection, Connector,
};
use psl::builtin_connectors::COCKROACH;
use quaint::{
    connector::IsolationLevel,
    prelude::{ConnectionInfo, Queryable, TransactionCapable},
    Value,
};
use std::{marker::PhantomData, time::Duration};

pub struct PostgreSql {}

impl PostgreSql {
    pub fn new() -> Self {
        Self {}
    }
}

pub struct ConsoleQueryable;

use quaint::prelude::Query;
use quaint::prelude::ResultSet;
use quaint::visitor::Visitor;

macro_rules! console_log {
    ($($t:tt)*) => (crate::log(&format_args!($($t)*).to_string()))
}

#[async_trait::async_trait]
impl Queryable for ConsoleQueryable {
    /// Execute the given query.
    async fn query(&self, q: Query<'_>) -> quaint::Result<ResultSet> {
        let (sql, params) = quaint::visitor::Postgres::build(q)?;
        console_log!("{}", sql);
        Ok(ResultSet::new(vec![], vec![]))
    }

    /// Execute a query given as SQL, interpolating the given parameters.
    async fn query_raw(&self, sql: &str, params: &[Value<'_>]) -> quaint::Result<ResultSet> {
        console_log!("{}", sql);
        Ok(ResultSet::new(vec![], vec![]))
    }

    /// Execute a query given as SQL, interpolating the given parameters.
    ///
    /// On Postgres, query parameters types will be inferred from the values
    /// instead of letting Postgres infer them based on their usage in the SQL query.
    ///
    /// NOTE: This method will eventually be removed & merged into Queryable::query_raw().
    async fn query_raw_typed(&self, sql: &str, params: &[Value<'_>]) -> quaint::Result<ResultSet> {
        console_log!("{}", sql);
        Ok(ResultSet::new(vec![], vec![]))
    }

    /// Execute the given query, returning the number of affected rows.
    async fn execute(&self, q: Query<'_>) -> quaint::Result<u64> {
        let (sql, params) = quaint::visitor::Postgres::build(q)?;
        console_log!("{}", sql);
        Ok(0)
    }

    /// Execute a query given as SQL, interpolating the given parameters and
    /// returning the number of affected rows.
    async fn execute_raw(&self, sql: &str, params: &[Value<'_>]) -> quaint::Result<u64> {
        console_log!("{}", sql);
        Ok(0)
    }

    /// Execute a query given as SQL, interpolating the given parameters and
    /// returning the number of affected rows.
    ///
    /// On Postgres, query parameters types will be inferred from the values
    /// instead of letting Postgres infer them based on their usage in the SQL query.
    ///
    /// NOTE: This method will eventually be removed & merged into Queryable::query_raw().
    async fn execute_raw_typed(&self, sql: &str, params: &[Value<'_>]) -> quaint::Result<u64> {
        console_log!("{}", sql);
        Ok(0)
    }

    /// Run a command in the database, for queries that can't be run using
    /// prepared statements.
    async fn raw_cmd(&self, cmd: &str) -> quaint::Result<()> {
        console_log!("{}", cmd);
        Ok(())
    }

    /// Return the version of the underlying database, queried directly from the
    /// source. This corresponds to the `version()` function on PostgreSQL for
    /// example. The version string is returned directly without any form of
    /// parsing or normalization.
    async fn version(&self) -> quaint::Result<Option<String>> {
        Ok(None)
    }

    /// Returns false, if connection is considered to not be in a working state.
    fn is_healthy(&self) -> bool {
        // TODO: use self.driver.is_healthy()
        true
    }

    /// Sets the transaction isolation level to given value.
    /// Implementers have to make sure that the passed isolation level is valid for the underlying database.
    async fn set_tx_isolation_level(&self, _isolation_level: IsolationLevel) -> quaint::Result<()> {
        Ok(())
    }

    /// Signals if the isolation level SET needs to happen before or after the tx BEGIN.
    fn requires_isolation_first(&self) -> bool {
        false
    }
}

impl TransactionCapable for ConsoleQueryable {}

#[async_trait]
impl Connector for PostgreSql {
    async fn get_connection<'a>(&'a self) -> connector_interface::Result<Box<dyn Connection + Send + Sync + 'static>> {
        let queryable = ConsoleQueryable;
        let conn = SqlConnection::new(queryable, &ConnectionInfo::Postgres(()), psl::PreviewFeatures::EMPTY);
        Ok(Box::new(conn) as Box<dyn Connection + Send + Sync + 'static>)
    }

    fn name(&self) -> &'static str {
        "postgres"
    }

    fn should_retry_on_transient_error(&self) -> bool {
        false
    }
}
