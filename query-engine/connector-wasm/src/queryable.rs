use async_trait::async_trait;
use js_sys::Promise;
use psl::builtin_connectors::COCKROACH;
use quaint::{
    connector::IsolationLevel,
    prelude::{ConnectionInfo, Queryable, TransactionCapable},
    Value as QuaintValue,
};
use query_connector::{
    error::{ConnectorError, ErrorKind},
    Connection, Connector,
};
use serde_json::json;
use sql_query_connector::{database::connection::SqlConnection, SqlError};
use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc, time::Duration};
use wasm_bindgen::prelude::*;

#[derive(Clone)]
#[wasm_bindgen]
pub struct Driver {
    /// Execute a query given as SQL, interpolating the given parameters.
    query_raw: js_sys::Function,

    /// Execute a query given as SQL, interpolating the given parameters and
    /// returning the number of affected rows.
    execute_raw: js_sys::Function,

    test_driver: bool,
}

macro_rules! console_log {
    ($($t:tt)*) => (crate::log(&format_args!($($t)*).to_string()))
}

#[wasm_bindgen]
impl Driver {
    #[wasm_bindgen(constructor)]
    pub fn new(query_raw: js_sys::Function, execute_raw: js_sys::Function) -> Self {
        Self {
            query_raw,
            execute_raw,
            test_driver: false,
        }
    }

    #[cfg(test)]
    pub fn new_for_test() -> Self {
        Self {
            query_raw: JsValue::NULL.into(),
            execute_raw: JsValue::NULL.into(),
            test_driver: true,
        }
    }
}

// It's WASM, we don't have threads.
unsafe impl Send for Driver {}
unsafe impl Sync for Driver {}

impl Driver {
    async fn query_raw_internal(&self, params: Query) -> anyhow::Result<JSResultSet> {
        let params = serde_wasm_bindgen::to_value(&params).unwrap();
        if !self.test_driver {
            let promise = self.query_raw.call1(&JsValue::null(), &params).unwrap();
            let future = wasm_bindgen_futures::JsFuture::from(Promise::from(promise));
            let value = future.await.unwrap();
            Ok(serde_wasm_bindgen::from_value(value).unwrap())
        } else {
            Ok(JSResultSet {
                column_names: vec!["id".to_string(), "firstname".to_string(), "company_id".to_string()],
                column_types: vec![ColumnType::Int64, ColumnType::Text, ColumnType::Int64],
                rows: vec![vec![json!(1), json!("Alberto"), json!(1)]],
            })
        }
    }

    async fn execute_raw_internal(&self, params: Query) -> anyhow::Result<u32> {
        if !self.test_driver {
            let params = serde_wasm_bindgen::to_value(&params).unwrap();
            let promise = self.execute_raw.call1(&JsValue::null(), &params).unwrap();
            let future = wasm_bindgen_futures::JsFuture::from(Promise::from(promise));
            let value = future.await.unwrap();
            Ok(serde_wasm_bindgen::from_value(value).unwrap())
        } else {
            Ok(32)
        }
    }

    pub fn query_raw(&self, params: Query) -> SendableFuture<impl Future<Output = anyhow::Result<JSResultSet>> + '_> {
        SendableFuture {
            f: self.query_raw_internal(params),
        }
    }

    pub fn execute_raw(&self, params: Query) -> SendableFuture<impl Future<Output = anyhow::Result<u32>> + '_> {
        SendableFuture {
            f: self.execute_raw_internal(params),
        }
    }
}

pub struct PostgreSql {
    driver: Arc<Driver>,
}

impl PostgreSql {
    pub fn new(driver: Arc<Driver>) -> Self {
        Self { driver }
    }
}

pub struct ConsoleQueryable {
    driver: Arc<Driver>,
}

use quaint::prelude::Query as QuaintQuery;
use quaint::prelude::ResultSet as QuaintResultSet;
use quaint::visitor;
use quaint::visitor::Visitor;

use serde::{Deserialize, Serialize};

macro_rules! console_log {
    ($($t:tt)*) => (crate::log(&format_args!($($t)*).to_string()))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub sql: String,
    pub args: Vec<serde_json::Value>,
}

use serde_repr::*;

#[derive(Debug, Deserialize_repr)]
#[repr(u8)]
pub enum ColumnType {
    Int32,
    Int64,
    Float,
    Double,
    Text,
    Enum,
    Bytes,
    Boolean,
    Char,
    Array,
    Numeric,
    Json,
    DateTime,
    Date,
    Time,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JSResultSet {
    pub column_types: Vec<ColumnType>,
    pub column_names: Vec<String>,
    // Note this might be encoded differently for performance reasons
    pub rows: Vec<Vec<serde_json::Value>>,
}

impl From<JSResultSet> for QuaintResultSet {
    fn from(mut val: JSResultSet) -> Self {
        // TODO: extract, todo: error rather than panic?
        let to_quaint_row = move |row: &mut Vec<serde_json::Value>| -> Vec<quaint::Value<'static>> {
            let mut res = Vec::with_capacity(row.len());

            for i in 0..row.len() {
                match &val.column_types[i] {
                    ColumnType::Int64 => match row.remove(0) {
                        serde_json::Value::Number(n) => {
                            res.push(QuaintValue::int64(n.as_i64().expect("number must be an i64")))
                        }
                        serde_json::Value::Null => res.push(QuaintValue::Int64(None)),
                        mismatch => panic!("Expected a number, found {:?}", mismatch),
                    },
                    ColumnType::Text => match row.remove(0) {
                        serde_json::Value::String(s) => res.push(QuaintValue::text(s)),
                        serde_json::Value::Null => res.push(QuaintValue::Text(None)),
                        mismatch => panic!("Expected a string, found {:?}", mismatch),
                    },
                    unimplemented => {
                        todo!("support column type: Column: {:?}", unimplemented)
                    }
                }
            }

            res
        };

        let names = val.column_names;
        let rows = val.rows.iter_mut().map(to_quaint_row).collect();

        QuaintResultSet::new(names, rows)
    }
}

impl ConsoleQueryable {
    async fn build_query(sql: &str, values: &[quaint::Value<'_>]) -> Query {
        let sql: String = sql.to_string();
        let args = values.iter().map(|v| v.clone().into()).collect();
        Query { sql, args }
    }

    async fn transform_result_set(result_set: JSResultSet) -> quaint::Result<QuaintResultSet> {
        Ok(QuaintResultSet::from(result_set))
    }

    async fn do_query_raw(&self, sql: &str, params: &[quaint::Value<'_>]) -> quaint::Result<QuaintResultSet> {
        let len = params.len();
        let query = Self::build_query(sql, params).await;
        let result_set = self.driver.query_raw(query).await.unwrap();
        let x = Self::transform_result_set(result_set).await?;
        Ok(x)
    }

    async fn do_execute_raw(&self, sql: &str, params: &[QuaintValue<'_>]) -> quaint::Result<u64> {
        let len = params.len();
        let query = Self::build_query(sql, params).await;
        let affected_rows = self.driver.execute_raw(query).await.unwrap();
        Ok(affected_rows as u64)
    }
}

use pin_project::pin_project;

#[pin_project]
pub struct SendableFuture<F> {
    #[pin]
    f: F,
}

// It's WASM, we don't have threads.
unsafe impl<F> Send for SendableFuture<F> {}
unsafe impl<F> Sync for SendableFuture<F> {}

impl<F: Future> Future for SendableFuture<F> {
    type Output = F::Output;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.project();
        this.f.poll(cx)
    }
}

#[async_trait::async_trait]
impl Queryable for ConsoleQueryable {
    /// Execute the given query.
    async fn query(&self, q: QuaintQuery<'_>) -> quaint::Result<QuaintResultSet> {
        let (sql, params) = visitor::Postgres::build(q)?;
        self.query_raw(&sql, &params).await
    }

    /// Execute a query given as SQL, interpolating the given parameters.
    async fn query_raw(&self, sql: &str, params: &[QuaintValue<'_>]) -> quaint::Result<QuaintResultSet> {
        self.do_query_raw(sql, params).await
    }

    /// Execute a query given as SQL, interpolating the given parameters.
    ///
    /// On Postgres, query parameters types will be inferred from the values
    /// instead of letting Postgres infer them based on their usage in the SQL query.
    ///
    /// NOTE: This method will eventually be removed & merged into Queryable::query_raw().
    async fn query_raw_typed(&self, sql: &str, params: &[QuaintValue<'_>]) -> quaint::Result<QuaintResultSet> {
        self.query_raw(sql, params).await
    }

    /// Execute the given query, returning the number of affected rows.
    async fn execute(&self, q: QuaintQuery<'_>) -> quaint::Result<u64> {
        let (sql, params) = visitor::Postgres::build(q)?;
        self.execute_raw(&sql, &params).await
    }

    /// Execute a query given as SQL, interpolating the given parameters and
    /// returning the number of affected rows.
    async fn execute_raw(&self, sql: &str, params: &[QuaintValue<'_>]) -> quaint::Result<u64> {
        self.do_execute_raw(sql, params).await
    }

    /// Execute a query given as SQL, interpolating the given parameters and
    /// returning the number of affected rows.
    ///
    /// On Postgres, query parameters types will be inferred from the values
    /// instead of letting Postgres infer them based on their usage in the SQL query.
    ///
    /// NOTE: This method will eventually be removed & merged into Queryable::query_raw().
    async fn execute_raw_typed(&self, sql: &str, params: &[QuaintValue<'_>]) -> quaint::Result<u64> {
        self.execute_raw(sql, params).await
    }

    /// Run a command in the database, for queries that can't be run using
    /// prepared statements.
    async fn raw_cmd(&self, cmd: &str) -> quaint::Result<()> {
        self.execute_raw(cmd, &[]).await?;

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
    async fn get_connection<'a>(&'a self) -> query_connector::Result<Box<dyn Connection + Send + Sync + 'static>> {
        let queryable = ConsoleQueryable {
            driver: self.driver.clone(),
        };
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
