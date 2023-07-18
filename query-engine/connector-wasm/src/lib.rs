use psl::SourceFile;
use schema::QuerySchema;
use wasm_bindgen::prelude::*;
use std::sync::Arc;
mod protocols;
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
    let mut arced_schema = Arc::new(psl::validate(SourceFile::new_allocated(allocated.into())));
    unsafe { QUERY_SCHEMA = Some(Arc::new(schema::build(arced_schema, false))) }
    console_log!("initialize complete...");
}

#[wasm_bindgen]
pub async fn to_sql(query: &str) {
    let serialized_body = RequestBody::try_from_slice(full_body.as_ref(), cx.engine_protocol());

}
