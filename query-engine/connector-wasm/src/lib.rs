use psl::SourceFile;
use query_core::executor::InterpretingExecutor;
use query_core::{protocol::EngineProtocol, QueryDocument};
use request_handlers::RequestBody;
use schema::QuerySchema;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

struct RequestHandler {
    query_schema: Arc<QuerySchema>,
}

static mut QUERY_SCHEMA: Option<Arc<QuerySchema>> = None;

// lifted from the `console_log` example
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(a: &str);
}

macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

#[wasm_bindgen]
pub async fn start(datamodel_str: &str) {
    console_log!("initializing...");
    let allocated = datamodel_str.to_string();
    let arced_schema = Arc::new(psl::validate(SourceFile::new_allocated(allocated.into())));
    unsafe { QUERY_SCHEMA = Some(Arc::new(schema::build(arced_schema, false))) }
    console_log!("initialize complete...");
}

#[wasm_bindgen]
pub async fn to_sql(query: &str) {
    let body = RequestBody::try_from_slice(query.as_bytes(), EngineProtocol::Graphql).unwrap();
    let query_schema = unsafe { QUERY_SCHEMA.as_ref().unwrap() }.clone();
    match body.into_doc(&query_schema) {
        Ok(QueryDocument::Single(query)) => {
            use query_core::QueryExecutor;
            let connector = sql_query_connector::database::PostgreSql::new();
            let res = InterpretingExecutor::new(connector, false)
                .execute(None, query, query_schema.clone(), None, EngineProtocol::Graphql)
                .await;
        }
        _ => unimplemented!(),
    }
}
