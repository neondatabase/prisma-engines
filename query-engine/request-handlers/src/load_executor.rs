use psl::{builtin_connectors::*, Datasource, PreviewFeatures};
use query_core::{Connector, QueryExecutor};
use sql_query_connector::*;
use std::collections::HashMap;
use tracing::trace;
use url::Url;

#[cfg(feature = "mongodb")]
use mongodb_query_connector::MongoDb;

/// Loads a query executor based on the parsed Prisma schema (datasource).
pub async fn load(
    source: &Datasource,
    features: PreviewFeatures,
    url: &str,
) -> query_core::Result<Box<dyn QueryExecutor + Send + Sync + 'static>> {
    unimplemented!()
}

fn sql_executor<T>(connector: T, force_transactions: bool) -> Box<dyn QueryExecutor + Send + Sync>
where
    T: Connector + Send + Sync + 'static,
{
    unimplemented!()
}
