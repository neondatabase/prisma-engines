#![deny(unsafe_code, rust_2018_idioms)]

#[macro_use]
extern crate tracing;

pub mod constants;
// pub mod executor;
pub mod protocol;
pub mod query_document;
pub mod query_graph_builder;
pub mod response_ir;
// pub mod telemetry;

mod executor {
    use crate::protocol::EngineProtocol;
    use prisma_models::PrismaValue;

    /// A timestamp that should be the `NOW()` value for the whole duration of a request. So all
    /// `@default(now())` and `@updatedAt` should use it.
    ///
    /// That panics if REQUEST_CONTEXT has not been set with with_request_context().
    ///
    /// If we had a query context we carry for the entire lifetime of the query, it would belong there.
    pub(crate) fn get_request_now() -> PrismaValue {
        return PrismaValue::DateTime(chrono::Utc::now().into());
    }

    /// The engine protocol used for the whole duration of a request.
    /// Use with caution to avoid creating implicit and unnecessary dependencies.
    ///
    /// That panics if REQUEST_CONTEXT has not been set with with_request_context().
    ///
    /// If we had a query context we carry for the entire lifetime of the query, it would belong there.
    pub(crate) fn get_engine_protocol() -> EngineProtocol {
        return EngineProtocol::Graphql;
    }
}

use thiserror::Error;
#[derive(Debug, Error, PartialEq)]
pub enum TransactionError {
    #[error("Unable to start a transaction in the given time.")]
    AcquisitionTimeout,

    #[error("Attempted to start a transaction inside of a transaction.")]
    AlreadyStarted,

    #[error("Transaction not found. Transaction ID is invalid, refers to an old closed transaction Prisma doesn't have information about anymore, or was obtained before disconnecting.")]
    NotFound,

    #[error("Transaction already closed: {reason}.")]
    Closed { reason: String },

    #[error("Unexpected response: {reason}.")]
    Unknown { reason: String },
}

pub use self::{
    error::{CoreError, FieldConversionError},
    // executor::{QueryExecutor, TransactionOptions},
    // interactive_transactions::{ExtendedTransactionUserFacingError, TransactionError, TxId},
    query_document::*,
    // telemetry::*,
};
pub use connector::{error::ConnectorError, Connector};

mod error;
// mod interactive_transactions;
mod interpreter;
mod query_ast;
mod query_graph;
mod result_ast;

use self::{
    // executor::*,
    // interactive_transactions::*,
    interpreter::{Env, ExpressionResult, Expressionista, InterpreterError, QueryInterpreter},
    query_ast::*,
    query_graph::*,
    query_graph_builder::*,
    response_ir::{IrSerializer, ResponseData},
    result_ast::*,
};

/// Result type tying all sub-result type hierarchies of the core together.
pub type Result<T> = std::result::Result<T, CoreError>;

// Re-exports
pub use schema;
