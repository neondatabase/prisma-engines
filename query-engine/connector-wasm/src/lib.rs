mod queryable;

use psl::SourceFile;
use query_core::executor::InterpretingExecutor;
use query_core::response_ir::ResponseData;
use query_core::{protocol::EngineProtocol, QueryDocument};
use queryable::Driver;
use request_handlers::{GQLResponse, RequestBody};
use schema::QuerySchema;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

struct RequestHandler {
    query_schema: Arc<QuerySchema>,
}

static mut QUERY_SCHEMA: Option<Arc<QuerySchema>> = None;
static mut DRIVER: Option<Arc<Driver>> = None;

// lifted from the `console_log` example
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    pub fn log(a: &str);
}

macro_rules! console_log {
    ($($t:tt)*) => (crate::log(&format_args!($($t)*).to_string()))
}

#[wasm_bindgen]
pub fn start(datamodel_str: &str, driver: Driver) {
    console_log!("initializing...");
    let base64 = base64::decode(&datamodel_str).unwrap();
    let allocated = String::from_utf8(base64).unwrap();
    let arced_schema = Arc::new(psl::validate(SourceFile::new_allocated(allocated.into())));
    let schema = Arc::new(schema::build(arced_schema, false));
    console_log!("initialize complete...");
    // for model in schema.internal_data_model.models() {
    //     console_log!("{:?}", model);
    // }
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    unsafe {
        QUERY_SCHEMA = Some(schema);
        DRIVER = Some(Arc::new(driver));
    }
}

#[wasm_bindgen]
pub async fn execute(query: &str) -> String {
    let body = RequestBody::try_from_slice(query.as_bytes(), EngineProtocol::Json).unwrap();
    let query_schema = unsafe { QUERY_SCHEMA.as_ref().unwrap() }.clone();
    match body.into_doc(&query_schema).unwrap() {
        QueryDocument::Single(query) => {
            use query_core::QueryExecutor;
            let connector = queryable::PostgreSql::new(unsafe { DRIVER.clone().unwrap() });
            let res = InterpretingExecutor::new(connector, false)
                .execute(None, query, query_schema.clone(), None, EngineProtocol::Json)
                .await
                .unwrap();
            let gqlresp = GQLResponse::from(res);
            console_log!("response: {:?}", gqlresp);
            serde_json::to_string_pretty(&gqlresp).unwrap()
        }
        x => unimplemented!("{:?}", x),
    }
}

#[wasm_bindgen]
pub fn hello_world() {
    console_log!("hello, world!");
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    async fn test_everything() {
        let x = "Z2VuZXJhdG9yIGNsaWVudCB7CiAgcHJvdmlkZXIgICAgICAgID0gInByaXNtYS1jbGllbnQtanMiCiAgb3V0cHV0ICAgICAgICAgID0gIi4uLy5wcmlzbWEvY2xpZW50IgogIHByZXZpZXdGZWF0dXJlcyA9IFtdCn0KCmRhdGFzb3VyY2UgZGIgewogIHByb3ZpZGVyID0gInBvc3RncmVzcWwiCiAgdXJsICAJPSBlbnYoIkRBVEFCQVNFX1VSTCIpCiAgZGlyZWN0VXJsID0gZW52KCJESVJFQ1RfVVJMIikKICAvLyBJZiB5b3Ugd2FudCB0byB1c2UgUHJpc21hIE1pZ3JhdGUsIHlvdSB3aWxsIG5lZWQgdG8gbWFudWFsbHkgY3JlYXRlIGEgc2hhZG93IGRhdGFiYXNlCiAgLy8gaHR0cHM6Ly9uZW9uLnRlY2gvZG9jcy9ndWlkZXMvcHJpc21hLW1pZ3JhdGUjY29uZmlndXJlLWEtc2hhZG93LWRhdGFiYXNlLWZvci1wcmlzbWEtbWlncmF0ZQogIC8vIG1ha2Ugc3VyZSB0byBhcHBlbmQgP2Nvbm5lY3RfdGltZW91dD0xMCB0byB0aGUgY29ubmVjdGlvbiBzdHJpbmcKICAvLyBzaGFkb3dEYXRhYmFzZVVybCA9IGVudijigJxTSEFET1dfREFUQUJBU0VfVVJM4oCdKQp9Cgptb2RlbCBMaW5rIHsKICBpZCAgICAgICAgU3RyaW5nICAgQGlkIEBkZWZhdWx0KHV1aWQoKSkKICBjcmVhdGVkQXQgRGF0ZVRpbWUgQGRlZmF1bHQobm93KCkpCiAgdXBkYXRlZEF0IERhdGVUaW1lIEB1cGRhdGVkQXQKICB1cmwgICAgICAgU3RyaW5nCiAgc2hvcnRVcmwgIFN0cmluZwogIHVzZXIgICAgICBVc2VyPyAgICBAcmVsYXRpb24oZmllbGRzOiBbdXNlcklkXSwgcmVmZXJlbmNlczogW2lkXSkKICB1c2VySWQgICAgU3RyaW5nPwp9Cgptb2RlbCBVc2VyIHsKICBpZCAgICAgICAgU3RyaW5nICAgQGlkIEBkZWZhdWx0KHV1aWQoKSkKICBjcmVhdGVkQXQgRGF0ZVRpbWUgQGRlZmF1bHQobm93KCkpCiAgdXBkYXRlZEF0IERhdGVUaW1lIEB1cGRhdGVkQXQKICBuYW1lICAgICAgU3RyaW5nPwogIGVtYWlsICAgICBTdHJpbmcgICBAdW5pcXVlCiAgbGlua3MgICAgIExpbmtbXQogIGRhdGUgICAgICBEYXRlVGltZT8KICBkZWNpbWFsICAgRGVjaW1hbD8KfQo=";
        let y = r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"user.1689714548354@prisma.io"}},"selection":{"$composites":true,"$scalars":true}}}"#;
        start(x);
        let driver = Driver::new_for_test();
        let res = to_sql(y, driver).await;
        println!("{}", res);
    }
}
